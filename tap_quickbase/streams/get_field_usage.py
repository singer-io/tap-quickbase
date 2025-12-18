from tap_quickbase.streams.abstracts import FullTableStream

class GetFieldUsage(FullTableStream):
    tap_stream_id = "get_field_usage"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage/{fieldId}?tableId={tableId}"
    parent = "fields_usage"
    page_size = None  # Single resource endpoint

