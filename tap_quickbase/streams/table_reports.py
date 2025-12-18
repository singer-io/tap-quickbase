from tap_quickbase.streams.abstracts import FullTableStream

class TableReports(FullTableStream):
    tap_stream_id = "table_reports"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/reports?tableId={tableId}"
    parent = "app_tables"
    # Note: get_reports removed since table_reports already provides complete report information
