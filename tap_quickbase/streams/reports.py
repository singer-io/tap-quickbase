"""Reports stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream


class Reports(FullTableStream):
    """Reports stream."""
    tap_stream_id = "reports"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/reports/{reportId}?tableId={tableId}"
    parent = "table_reports"
