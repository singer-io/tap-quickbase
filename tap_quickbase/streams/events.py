"""Events stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class Events(FullTableStream):
    """Events stream."""
    tap_stream_id = "events"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/apps/{appId}/events"
    parent = "apps"
