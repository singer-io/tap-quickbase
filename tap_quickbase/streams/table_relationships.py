from tap_quickbase.streams.abstracts import FullTableStream

class TableRelationships(FullTableStream):
    tap_stream_id = "table_relationships"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "relationships"
    path = "v1/tables/{tableId}/relationships"

