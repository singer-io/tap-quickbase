from tap_quickbase.streams.apps import Apps
from tap_quickbase.streams.events import Events
from tap_quickbase.streams.roles import Roles
from tap_quickbase.streams.app_tables import AppTables
from tap_quickbase.streams.tables import Tables
from tap_quickbase.streams.table_relationships import TableRelationships
from tap_quickbase.streams.table_reports import TableReports
from tap_quickbase.streams.get_reports import GetReports
from tap_quickbase.streams.fields import Fields
from tap_quickbase.streams.get_fields import GetFields
from tap_quickbase.streams.fields_usage import FieldsUsage
from tap_quickbase.streams.get_field_usage import GetFieldUsage

STREAMS = {
    "apps": Apps,
    "events": Events,
    "roles": Roles,
    "app_tables": AppTables,
    "tables": Tables,
    "table_relationships": TableRelationships,
    "table_reports": TableReports,
    "get_reports": GetReports,
    "fields": Fields,
    "get_fields": GetFields,
    "fields_usage": FieldsUsage,
    "get_field_usage": GetFieldUsage,
}

