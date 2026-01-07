"""Tables stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class Tables(FullTableStream):
    """Tables stream."""
    tap_stream_id = "tables"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/tables/{tableId}?appId={appId}"
    parent = "app_tables"
    page_size = None  # Single resource endpoint
