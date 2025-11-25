from tap_quickbase.streams.abstracts import FullTableStream

class FieldsUsage(FullTableStream):
    tap_stream_id = "fields_usage"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage?tableId={tableId}"

