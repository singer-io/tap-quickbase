from tap_quickbase.streams.abstracts import FullTableStream

class Fields(FullTableStream):
    tap_stream_id = "fields"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields?tableId={tableId}"
    parent = "app_tables"
    # Note: get_fields removed as child since it requires tableId which is not available in field records


