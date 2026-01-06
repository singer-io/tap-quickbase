"""TableReports stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class TableReports(FullTableStream):
    """TableReports stream."""
    tap_stream_id = "table_reports"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/reports?tableId={tableId}"
    parent = "app_tables"
