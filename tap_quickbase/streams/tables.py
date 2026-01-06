"""Tables stream definition."""

from tap_quickbase.streams.abstracts import ChildBaseStream

class Tables(ChildBaseStream):
    """Tables stream."""
    tap_stream_id = "tables"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated"]
    path = "v1/tables/{tableId}?appId={appId}"
    parent = "app_tables"
    bookmark_value = None
    page_size = None  # Single resource endpoint
