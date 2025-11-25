from tap_quickbase.streams.abstracts import FullTableStream

class GetFields(FullTableStream):
    tap_stream_id = "get_fields"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/{fieldId}?tableId={tableId}"
    parent = "fields"

