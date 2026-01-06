"""Roles stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class Roles(FullTableStream):
    """Roles stream."""
    tap_stream_id = "roles"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/apps/{appId}/roles"
    parent = "apps"
