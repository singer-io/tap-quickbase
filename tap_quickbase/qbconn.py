#!/usr/bin/python3
from xml.etree import ElementTree
import logging
import re
import requests

COLUMN_NAME_TRANSLATION = re.compile(r"[^a-z0-9_ $!#%&'()*+,-./:;<=>?@[\]^~]")


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
            self.error = int(tree.find('errcode').text)
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
        database_name = schema.find('table').find('name').text
        tables = []
        for remote_table in remote_tables:
            tables.append({
                'id': remote_table.text,
                'name': remote_table.attrib['name'][6:],
                'database_name': database_name,
                'database_id': self.appid
            })
        return tables

    def get_fields(self, table_id):
        params = {'act': 'API_GetSchema'}
        schema = self.request(params, table_id)
        remote_fields = schema.find('table').find('fields')
        fields = []
        for remote_field in remote_fields:
            name = remote_field.find('label').text.lower().replace('"', "'")
            name = COLUMN_NAME_TRANSLATION.sub('', name)
            fields.append({
                'id': remote_field.attrib['id'],
                'name': name,
                'type': remote_field.attrib['field_type'],
                'base_type': remote_field.attrib['base_type'],
            })
        return fields
