#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name
import copy
import datetime
import os
import re

import dateutil.parser
import singer
from singer.catalog import Catalog, CatalogEntry
import singer.utils as singer_utils
import singer.metadata as singer_metadata
import singer.metrics as metrics
from singer.schema import Schema

from tap_quickbase import qbconn

REQUIRED_CONFIG_KEYS = ['qb_url', 'qb_appid', 'qb_user_token', 'start_date']
DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
CONFIG = {}
STATE = {}
NUM_RECORDS = 100
LOGGER = singer.get_logger()
REPLICATION_KEY = 'date modified'

DEBUG_FLAG = False

def format_child_field_name(parent_name, child_name):
    return "{}.{}".format(parent_name, child_name)

def format_epoch_milliseconds(epoch_timestamp):
    epoch_sec = int(epoch_timestamp) / 1000.0
    return datetime.datetime.utcfromtimestamp(epoch_sec).strftime(DATETIME_FMT)

def convert_to_epoch_milliseconds(dt_string):
    dt = datetime.datetime.strptime(dt_string, DATETIME_FMT)
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt-epoch).total_seconds() * 1000.0)

def build_state(raw_state, catalog):
    LOGGER.info(
        'Building State from raw state {}'.format(raw_state)
    )

    state = {}

    for catalog_entry in catalog.streams:
        start = singer.get_bookmark(raw_state, catalog_entry.tap_stream_id, REPLICATION_KEY)
        if not start:
            start = CONFIG.get(
                'start_date',
                datetime.datetime.utcfromtimestamp(0).strftime(DATETIME_FMT)
            )
        state = singer.write_bookmark(state, catalog_entry.tap_stream_id, REPLICATION_KEY, start)

    return state



def populate_schema_leaf(schema, field_info, id_num, breadcrumb, metadata):
    """
    Populates a leaf in the schema.  A leaf corresponds to a JSON boolean,
    number, or string field
    """
    #add metadata
    inclusion = 'available' if id_num != '2' else 'automatic'
    metadata.append(
        {
            'metadata': {
                'tap-quickbase.id': id_num,
                'inclusion': inclusion
            },
            'breadcrumb': [i for i in breadcrumb]
        }
    )

    #populate schema
    field_type = ['null']
    field_format = None
    # https://help.quickbase.com/user-assistance/field_types.html
    if field_info.get('base_type') == 'bool':
        field_type.append('boolean')
    elif field_info.get('base_type') == 'float':
        field_type.append('number')
    elif field_info.get('base_type') == 'int64':
        if field_info.get('type') in ('timestamp', 'date'):
            field_type.append('string')
            field_format = 'date-time'
        else:
            # `timeofday` comes out of the API as an integer for how many milliseconds
            #       through the day, 900000 would be 12:15am
            # `duration` comes out as an integer for how many milliseconds the duration is,
            #       1000 would be 1 second
            # let's just pass these as an integer
            field_type.append('integer')
    else:
        field_type.append('string')

    schema.type = field_type
    if field_format is not None:
        schema.format = field_format

def populate_schema_node(schema, field_info, id_field_map, breadcrumb, metadata):
    """
    Populates a node in the schema.  A node corresponds to a JSON object, which has
    properties (children)
    """
    # add metadata
    metadata.append(
        {
            'metadata': {
                'inclusion': 'available'
            },
            'breadcrumb': [i for i in breadcrumb]
        }
    )

    #populate schema
    schema.type = ['null','object']
    schema.properties = {}
    for id_num in field_info.get('composite_fields'):
        child_field_info = id_field_map[id_num]
        breadcrumb.extend(['properties',child_field_info.get('name')])

        child_schema = Schema()
        if child_field_info.get('composite_fields'):
            populate_schema_node(child_schema, child_field_info, id_field_map, breadcrumb, metadata)
        else:
            populate_schema_leaf(child_schema, child_field_info, id_num, breadcrumb, metadata)

        schema.properties[child_field_info.get('name')] = child_schema

        # remove 'properties' and 'child_field_name' from breadcrumb
        breadcrumb.pop()
        breadcrumb.pop()

def discover_catalog(conn):
    """Returns a Catalog describing the table structure of the target application"""
    entries = []

    for table in conn.get_tables():
        # the stream is in format app_name__table_name with all non alphanumeric
        # and `_` characters replaced with an `_`.
        stream = re.sub(
            '[^0-9a-z_]+',
            '_',
            "{}__{}".format(table.get('app_name').lower(), table.get('name')).lower()
        )

        # by default we will ALWAYS have 'rid' as an automatically included primary key field.
        schema = Schema(
            type=['null','object'],
            additionalProperties=False
        )
        schema.properties = {
            'rid': Schema(
                type=['string']
            )
        }
        metadata = [
            {
                'metadata': {
                    'inclusion': 'automatic'
                },
                'breadcrumb': ['properties','rid']
            },
            {
                'metadata': {
                    'tap-quickbase.app_id': conn.appid
                },
                'breadcrumb': []
            }
        ]

        # build hierarchial schema
        id_to_fields = conn.get_fields(table.get('id'))
        for id_num,field_info in id_to_fields.items():

            breadcrumb = ['properties', field_info.get('name')]

            # if this field has a parent, it will be added by the parent
            if field_info.get('parent_field_id'):
                continue

            # if this field has children, add them
            elif field_info.get('composite_fields'):
                node_schema = Schema()
                populate_schema_node(node_schema, field_info, id_to_fields, breadcrumb, metadata)
                schema.properties[field_info.get('name')] = node_schema

            #otherwise, add field
            else:
                leaf_schema = Schema()
                populate_schema_leaf(leaf_schema, field_info, id_num, breadcrumb, metadata)
                schema.properties[field_info.get('name')] = leaf_schema

        entry = CatalogEntry(
            table=table.get('id'),
            stream_alias=table.get('name'),
            stream=stream,
            tap_stream_id=stream,
            key_properties=['rid'],
            replication_key=REPLICATION_KEY,
            replication_method = 'INCREMENTAL',
            schema=schema,
            metadata=metadata
        )

        entries.append(entry)

    return Catalog(entries)


def do_discover(conn):
    discover_catalog(conn).dump()


def transform_data(data, schema):
    """
    By default everything from QB API is strings,
    convert to other datatypes where specified by the schema
    """
    for field_name, field_value in iter(data.items()):

        if field_value is not None and field_name in schema.properties:
            field_type = schema.properties.get(field_name).type
            field_format = schema.properties.get(field_name).format

            # date-time datatype
            if field_format == 'date-time':
                try:
                    # convert epoch timestamps to date strings
                    data[field_name] = format_epoch_milliseconds(field_value)
                except (ValueError, TypeError):
                    data[field_name] = None

            # number (float) datatype
            if field_type == "number" or "number" in field_type:
                try:
                    data[field_name] = float(field_value)
                except (ValueError, TypeError):
                    data[field_name] = None

            # boolean datatype
            if field_type == "boolean" or "boolean" in field_type:
                data[field_name] = field_value == "1"


@singer.utils.ratelimit(2, 1)
def request(conn, table_id, query_params):
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']
    return conn.query(table_id, query_params, headers=headers)

def build_field_lists(schema, metadata, breadcrumb):
    """
    Use the schema to build a field list for the query and a translation table for the returned data
    :return:
    """
    field_list = []
    ids_to_breadcrumbs = {}
    for name, sub_schema in schema.properties.items():
        breadcrumb.extend(['properties', name])

        field_id = singer_metadata.get(metadata, tuple(breadcrumb), 'tap-quickbase.id')
        selected = singer_metadata.get(metadata, tuple(breadcrumb), 'selected')
        inclusion = singer_metadata.get(metadata, tuple(breadcrumb), 'inclusion')
        if field_id and (selected or inclusion == 'automatic'):
            field_list.append(field_id)
            ids_to_breadcrumbs[field_id] = [i for i in breadcrumb]
        elif sub_schema.properties and (selected or inclusion == 'automatic'):
            for name, child_schema in sub_schema.properties.items():
                breadcrumb.extend(['properties', name]) # Select children of objects
                metadata = singer_metadata.write(metadata, tuple(breadcrumb), 'selected', True)
                breadcrumb.pop()
                breadcrumb.pop()
            sub_field_list, sub_ids_to_breadcrumbs = build_field_lists(sub_schema, metadata, breadcrumb)
            field_list.extend(sub_field_list)
            ids_to_breadcrumbs.update(sub_ids_to_breadcrumbs)

        breadcrumb.pop()
        breadcrumb.pop()

    return (field_list, ids_to_breadcrumbs)

def transform_bools(record, schema):
    for field_prop, sub_schema in schema['properties'].items():
        field_type = sub_schema.get('type')
        if not field_type:
            continue
        if not record.get(field_prop, None):
            continue
        if 'boolean' in field_type:
            record[field_prop] = 'false' if record.get(field_prop)=='0' else 'true'
        if 'object' in field_type:
            record[field_prop] = transform_bools(record[field_prop], sub_schema)
    return record


def build_record(row, ids_to_breadcrumbs):
    record = {}
    for field_id, field_value in row.items():
        if field_id=='rid':
            record['rid'] = field_value
        else:
            breadcrumb = ids_to_breadcrumbs[field_id]
            insert_value_at_breadcrumb(breadcrumb, field_value, record)
    return record

def insert_value_at_breadcrumb(breadcrumb, value, record):
    if len(breadcrumb) == 2:
        record[breadcrumb[1]] = value
    else:
        if record.get(breadcrumb[1]):
            insert_value_at_breadcrumb(breadcrumb[2:], value, record[breadcrumb[1]])
        else:
            record[breadcrumb[1]] = {}
            insert_value_at_breadcrumb(breadcrumb[2:], value, record[breadcrumb[1]])

def gen_request(conn, stream, params=None):
    """
    Fetch the data we need from Quickbase. Uses a modified version of the Quickbase API SDK.
    This will page through data num_records at a time and transform and then yield each result.
    """
    params = params or {}
    table_id = stream.table
    properties = stream.schema.properties
    metadata = singer_metadata.to_map(stream.metadata)

    if not properties:
        return

    field_list, ids_to_breadcrumbs = build_field_lists(stream.schema, metadata, [])
    if not field_list:
        return

    # we always want the Date Modified field
    if '2' not in field_list:
        LOGGER.warning(
            "Date Modified field not included for {}. Skipping.".format(stream.tap_stream_id)
        )

    query_params = {
        'clist': '.'.join(field_list),
        'slist': '2',  # 2 is always the modified date column we are keying off of
        'options': "num-{}".format(NUM_RECORDS),
    }

    start = None
    if 'start' in params:
        start = params['start']

    while True:
        if start:
            start_millis = str(convert_to_epoch_milliseconds(start))
            query_params['query'] = "{2.AF.%s}" % start_millis

        results = request(conn, table_id, query_params)
        for res in results:
            start = format_epoch_milliseconds(res['2'])  # update start to this record's updatedate for next page of query
            # translate column ids to column names
            new_res = build_record(res, ids_to_breadcrumbs)
            yield new_res

        # if we got less than the max number of records then we're at the end and can break
        if len(results) < NUM_RECORDS:
            break


def get_start(table_id, state):
    """
    default to the CONFIG's start_date if the table does not have an entry in STATE.
    """
    start = singer.get_bookmark(state, table_id, REPLICATION_KEY)
    if not start:
        start = CONFIG.get(
            'start_date',
            datetime.datetime.utcfromtimestamp(0).strftime(DATETIME_FMT)
        )
        singer.write_bookmark(state, table_id, REPLICATION_KEY, start)
    return start

def sync_table(conn, catalog_entry, state):
    metadata = singer_metadata.to_map(catalog_entry.metadata)
    LOGGER.info("Beginning sync for {}.{} table.".format(
        singer_metadata.get(metadata, tuple(), "tap-quickbase.app_id"), catalog_entry.table
    ))

    entity = catalog_entry.tap_stream_id
    if not entity:
        return

    start = get_start(entity, state)
    formatted_start = dateutil.parser.parse(start).strftime(DATETIME_FMT)
    params = {
        'start': formatted_start,
    }

    with metrics.record_counter(None) as counter:
        counter.tags['app'] = singer_metadata.get(metadata, tuple(), "tap-quickbase.app_id")
        counter.tags['table'] = catalog_entry.table

        extraction_time = singer_utils.now()
        for rows_saved, row in enumerate(gen_request(conn, catalog_entry, params)):
            counter.increment()
            rec = transform_bools(row, catalog_entry.schema.to_dict())
            rec = singer.transform(rec, catalog_entry.schema.to_dict(), singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING)

            yield singer.RecordMessage(
                stream=catalog_entry.stream,
                record=rec,
                time_extracted=extraction_time
            )

            state = singer.write_bookmark(
                state,
                catalog_entry.tap_stream_id,
                REPLICATION_KEY,
                format_epoch_milliseconds(row[REPLICATION_KEY])
            )
            if (rows_saved+1) % 1000 == 0:
                yield singer.StateMessage(value=copy.deepcopy(state))


def generate_messages(conn, catalog, state):
    for catalog_entry in catalog.streams:

        if not catalog_entry.is_selected():
            continue

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=catalog_entry.key_properties,
            bookmark_properties=[REPLICATION_KEY]
        )

        metadata = singer_metadata.to_map(catalog_entry.metadata)
        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['app'] = singer_metadata.get(metadata, tuple(), "tap-quickbase.app_id")
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(conn, catalog_entry, state):
                yield message

        # Emit a state message
        yield singer.StateMessage(value=copy.deepcopy(state))


def do_sync(conn, catalog, state):
    LOGGER.info("Starting QuickBase sync")

    for message in generate_messages(conn, catalog, state):
        singer.write_message(message)

def correct_base_url(url):
    result = url
    if url.startswith('http:'):
        LOGGER.warn("Replacing 'http' with 'https' for 'qb_url' configuration option. Quick Base requires https connections.")
        result = 'https:' + url[5:]

    if not url.endswith('/'):
        result = result + '/'

    return result

def main_impl():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    base_url = correct_base_url(CONFIG['qb_url'])
    conn = qbconn.QBConn(
        base_url,
        CONFIG['qb_appid'],
        user_token=CONFIG['qb_user_token'],
        logger=LOGGER
    )

    if args.discover:
        do_discover(conn)

    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = build_state(args.state, catalog)
        do_sync(conn, catalog, state)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
