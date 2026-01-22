"""Stream registry for tap discovery and sync."""

from tap_quickbase.streams.apps import Apps
from tap_quickbase.streams.events import Events
from tap_quickbase.streams.roles import Roles
from tap_quickbase.streams.app_tables import AppTables
from tap_quickbase.streams.tables import Tables
from tap_quickbase.streams.table_relationships import TableRelationships
from tap_quickbase.streams.table_reports import TableReports
from tap_quickbase.streams.reports import Reports
from tap_quickbase.streams.fields import Fields
from tap_quickbase.streams.fields_usage import FieldsUsage

STREAMS = {
    "apps": Apps,
    "events": Events,
    "roles": Roles,
    "app_tables": AppTables,
    # "tables": Tables,
    # "table_relationships": TableRelationships,
    # "table_reports": TableReports,
    "reports": Reports,
    "fields": Fields,
    "fields_usage": FieldsUsage,
}
