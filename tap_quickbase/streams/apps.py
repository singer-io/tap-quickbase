from tap_quickbase.streams.abstracts import IncrementalStream

class Apps(IncrementalStream):
    tap_stream_id = "apps"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated"]
    path = "v1/apps/{appId}"

