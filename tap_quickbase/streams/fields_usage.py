from tap_quickbase.streams.abstracts import FullTableStream

class FieldsUsage(FullTableStream):
    tap_stream_id = "fields_usage"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage?tableId={tableId}"
    parent = "app_tables"
    # Note: get_field_usage removed as child since it requires tableId which is not available in fields_usage records


