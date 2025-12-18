from tap_quickbase.streams.abstracts import ChildBaseStream

class AppTables(ChildBaseStream):
    tap_stream_id = "app_tables"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated"]
    path = "v1/tables?appId={appId}"
    parent = "apps"
    bookmark_value = None
    children = ["tables", "table_relationships", "table_reports", "fields", "fields_usage"]
    # Note: get_fields, get_field_usage, get_reports removed - they need both parent ID and tableId
