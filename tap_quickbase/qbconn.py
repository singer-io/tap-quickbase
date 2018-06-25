#!/usr/bin/python3
from xml.etree import ElementTree
import logging
import re
import requests

# This regex is used to transform the column name in `get_fields`
SEPARATORS_TRANSLATION = re.compile(r"[-\s]")
COLUMN_NAME_TRANSLATION = re.compile(r"[^a-zA-Z0-9_]")
UNDERSCORE_CONSOLIDATION = re.compile(r"_+")

def sanitize_field_name(name):
    result = name.lower()
    result = SEPARATORS_TRANSLATION.sub('_', result) # Replace separator characters with underscores
    result = COLUMN_NAME_TRANSLATION.sub('', result) # Remove all other non-alphanumeric characters
    return UNDERSCORE_CONSOLIDATION.sub('_', result) # Consolidate consecutive underscores

class QBConn:
    """
    QBConn was borrowed heavily from pybase
    https://github.com/QuickbaseAdmirer/Quickbase-Python-SDK
    """
    def __init__(self, url, appid, user_token=None, realm="", logger=None):

        self.url = url
        self.user_token = user_token
        self.appid = appid
        self.realm = realm  # This allows one QuickBase realm to proxy for another
        # Set after every API call.
        # A non-zero value indicates an error. A negative value indicates an error with this lib
        self.error = 0
        self.logger = logger or logging.getLogger(__name__)

    def request(self, params, url_ext, headers=None):
        """
        Adds the appropriate fields to the request and sends it to QB
        Takes a dict of parameter:value pairs and the url extension (main or your table ID, mostly)
        """
        headers = headers or dict()
        url = self.url
        url += url_ext

        # log the API request before adding sensitive info to the request
        self.logger.info("API GET {}, {}".format(url, params))
        params['usertoken'] = self.user_token
        params['realmhost'] = self.realm

        resp = requests.get(url, params, headers=headers)

        if re.match(r'^<\?xml version=', resp.content.decode("utf-8")) is None:
            print("No useful data received")
            self.error = -1
        else:
            tree = ElementTree.fromstring(resp.content)
            self.error_code = int(tree.find('errcode').text)
            if self.error_code != 0:
                error = tree.find('errdetail')
                error = tree.find('errtext') if error is None else error # XML nodes are falsy, so must explicitly check for None
                self.error =  error.text if error is not None else "No error description provided by Quick Base."
                raise Exception("Error response from Quick Base (Code {}): {}".format(self.error_code, self.error))
            return tree

    def query(self, table_id, query, headers=None):
        """
        Executes a query on tableID
        Returns a list of dicts containing fieldid:value pairs.
        record ID will always be specified by the "rid" key
        """
        headers = headers or dict()
        params = dict(query)
        params['act'] = "API_DoQuery"
        params['includeRids'] = '1'
        params['fmt'] = "structured"
        records = self.request(params, table_id, headers=headers).find('table').find('records')
        data = []
        for record in records:
            temp = dict()
            temp['rid'] = record.attrib['rid']
            for field in record:
                if field.tag == "f":
                    temp[field.attrib['id']] = field.text
            data.append(temp)
        return data

    def get_tables(self):
        if not self.appid:
            return {}

        params = {'act': 'API_GetSchema'}
        schema = self.request(params, self.appid)
        remote_tables = schema.find('table').find('chdbids')
        app_name = schema.find('table').find('name').text
        tables = []
        if remote_tables is None:
            raise Exception("Error discovering streams: The specified application contains no tables.")
        for remote_table in remote_tables:
            tables.append({
                'id': remote_table.text,
                'name': remote_table.attrib['name'][6:],
                'app_name': app_name,
                'app_id': self.appid
            })
        return tables

    def get_fields(self, table_id):
        params = {'act': 'API_GetSchema'}
        schema = self.request(params, table_id)
        remote_fields = schema.find('table').find('fields')

        id_to_field = {}
        field_to_ids = {}
        for remote_field in remote_fields:
            name = sanitize_field_name(remote_field.find('label').text.lower().replace('"', "'"))
            id_num =  remote_field.attrib['id']
            if field_to_ids.get(name):
                field_to_ids[name].append(id_num)
            else:
                field_to_ids[name] = [id_num]

            # pull out composite field info (child fields)
            composite_fields = []
            composite_fields_element = remote_field.find('compositeFields')
            if composite_fields_element:
                for composite_field_element in composite_fields_element:
                    composite_fields.append(composite_field_element.attrib['id'])

            # pull out parent field info (useful to know if field is a child)
            parent_field_id_element = remote_field.find('parentFieldID')
            if parent_field_id_element is not None:
                parent_field_id = parent_field_id_element.text
            else:
                parent_field_id = ""

            field_info = {
                'id': id_num,
                'name': name,
                'type': remote_field.attrib['field_type'],
                'base_type': remote_field.attrib['base_type'],
                'parent_field_id': parent_field_id,
                'composite_fields': composite_fields
            }

            id_to_field[id_num] = field_info

        # handle duplicate field names by appending id num to end of name
        for field_name, field_id_list in field_to_ids.items():
            field_id_list = [i for i in field_id_list if not id_to_field[i].get('parent_field_id')]
            if len(field_id_list) > 1:
                for dup_id in field_id_list:
                    dup_field_info = id_to_field[dup_id]
                    dup_field_info['name'] = dup_field_info['name'] + '_' + dup_id

        return id_to_field
