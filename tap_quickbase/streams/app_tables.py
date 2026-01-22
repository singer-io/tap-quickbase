"""App tables stream definition."""

from tap_quickbase.streams.abstracts import PseudoIncrementalStream


class AppTables(PseudoIncrementalStream):
    """App tables stream."""
    tap_stream_id = "app_tables"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/tables?appId={appId}"
    parent = "apps"
    children = ["tables", "table_relationships", "table_reports", "fields", "fields_usage"]
    bookmark_field = "updated"
