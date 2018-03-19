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
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
CONFIG = {}
STATE = {}
NUM_RECORDS = 500
LOGGER = singer.get_logger()
REPLICATION_KEY = 'date modified'

DEBUG_FLAG = False

def format_field_name(field):
    return "{} - {}".format(field.get('id'), field.get('name'))

def format_child_field_name(parent_name, child_name):
    return "{}.{}".format(parent_name, child_name)

def build_state(raw_state, catalog):
    LOGGER.info(
        'Building State from raw state {} and catalog {}'.format(raw_state, catalog.to_dict())
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


def discover_catalog(conn):
    """Returns a Catalog describing the table structure of the target database"""
    entries = []

    for table in conn.get_tables():
        # the stream is in format database_name__table_name with all non alphanumeric
        # and `_` characters replaced with an `_`.
        stream = re.sub(
            '[^0-9a-z_]+',
            '_',
            "{}__{}".format(table.get('database_name').lower(), table.get('name')).lower()
        )

        # by default we will ALWAYS have 'rid' as an automatically included primary key field.
        schema = Schema(
            type='object',
            additionalProperties=False,
            properties={
                'rid': Schema(
                    type=['string'],
                    inclusion='automatic',
                )
            }
        )
        metadata = []

        table_fields = conn.get_fields(table.get('id'))
        table_fields.sort(key=lambda field: int(field.get('id')))
        table_fields_lookup = {}
        unique_meta_keys = {}
        for field in table_fields:
            field_type = ['null']
            field_format = None
            field_name = format_field_name(field)

            # if we have a parentFieldID grab the name of that field and add it to the field name to assure uniqueness
            # since the fields are sorted by id we will have already processed the parent field
            if field.get('parent_field_id'):
                field_name = format_child_field_name(table_fields_lookup.get(field.get('parent_field_id')), field_name)

            # https://help.quickbase.com/user-assistance/field_types.html
            if field.get('base_type') == 'bool':
                field_type.append('boolean')
            elif field.get('base_type') == 'float':
                field_type.append('number')
            elif field.get('base_type') == 'int64':
                if field.get('type') in ('timestamp', 'date'):
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

            property_schema = Schema(
                type=field_type,
                inclusion='available' if field.get('id') != '2' else 'automatic',
            )
            if field_format is not None:
                property_schema.format = field_format
            schema.properties[field_name] = property_schema
            if DEBUG_FLAG:
                if field_name in unique_meta_keys:
                    unique_meta_keys[field_name].append(field.get('id'))
                else:
                    unique_meta_keys[field_name] = [field.get('id')]


            metadata.append({
                'metadata': {
                    'tap-quickbase.id': field.get('id')
                },
                'breadcrumb': [
                    'properties',
                    field_name
                ]
            })

            table_fields_lookup[field.get('id')] = field_name
        if DEBUG_FLAG:
            for k,v in unique_meta_keys.items():
                if len(unique_meta_keys[k]) > 1:
                    LOGGER.info('MULTIPLE META KEYS for: ' + k + '  =  ' + str(unique_meta_keys[k]))

        entry = CatalogEntry(
            database=conn.appid,
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
                    data[field_name] = datetime.datetime.utcfromtimestamp(
                        int(field_value) / 1000.0
                    ).strftime(DATETIME_FMT)
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


def build_field_lists(properties, metadata):
    """
    Use the schema to build a field list for the query and a translation table for the returned data
    :return:
    """
    field_list = []
    ids_to_names = {}  # used to translate the column ids to names in returned results
    for name, prop in iter(properties.items()):
        field_id = singer_metadata.get(metadata, ('properties', name, ), 'tap-quickbase.id')
        if field_id  and (prop.selected == 'true' or prop.inclusion == 'automatic'):
            field_list.append(field_id)
            ids_to_names[field_id] = name
    return (field_list, ids_to_names, )



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

    field_list, ids_to_names = build_field_lists(properties, metadata)
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
            query_params['query'] = "{2.AF.%s}" % start

        results = request(conn, table_id, query_params)
        for res in results:
            start = res['2']  # update start to this record's updatedate for next page of query
            # translate column ids to column names
            new_res = {}
            for field_id, field_value in iter(res.items()):
                if field_id in ids_to_names:
                    new_res[ids_to_names[field_id]] = field_value
                else:
                    new_res[field_id] = field_value
            yield new_res

        # if we got less than the max number of records then we're at the end and can break
        if len(results) < NUM_RECORDS:
            break


def get_start(table_id):
    """
    default to the CONFIG's start_date if the table does not have an entry in STATE.
    """
    start = singer.get_bookmark(STATE, table_id, REPLICATION_KEY)
    if not start:
        start = CONFIG.get(
            'start_date',
            datetime.datetime.utcfromtimestamp(0).strftime(DATETIME_FMT)
        )
        singer.write_bookmark(STATE, table_id, REPLICATION_KEY, start)
    return start


def sync_table(conn, catalog_entry, state):
    LOGGER.info("Beginning sync for {}.{} table.".format(
        catalog_entry.database, catalog_entry.table
    ))

    entity = catalog_entry.tap_stream_id
    if not entity:
        return

    start = get_start(entity)
    formatted_start = dateutil.parser.parse(start).strftime(DATETIME_FMT)
    params = {
        'start': formatted_start,
    }

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = catalog_entry.database
        counter.tags['table'] = catalog_entry.table

        extraction_time = singer_utils.now()
        for rows_saved, row in enumerate(gen_request(conn, catalog_entry, params)):
            counter.increment()
            transform_data(row, catalog_entry.schema)

            yield singer.RecordMessage(
                stream=catalog_entry.stream,
                record=row,
                time_extracted=extraction_time
            )

            state = singer.write_bookmark(
                state,
                catalog_entry.tap_stream_id,
                REPLICATION_KEY,
                row[REPLICATION_KEY]
            )
            if (rows_saved+1) % 1000 == 0:
                yield singer.StateMessage(value=copy.deepcopy(state))


def generate_messages(conn, catalog, state):
    for catalog_entry in catalog.streams:

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=catalog_entry.key_properties,
            bookmark_properties=[REPLICATION_KEY]
        )

        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(conn, catalog_entry, state):
                yield message

        # Emit a state message
        yield singer.StateMessage(value=copy.deepcopy(state))


def do_sync(conn, catalog, state):
    LOGGER.info("Starting QuickBase sync")

    for message in generate_messages(conn, catalog, state):
        singer.write_message(message)


def main_impl():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    conn = qbconn.QBConn(
        CONFIG['qb_url'],
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
