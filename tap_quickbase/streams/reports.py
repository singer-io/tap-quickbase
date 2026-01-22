"""Reports stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream


class Reports(FullTableStream):
    """Reports stream."""
    tap_stream_id = "reports"
    key_properties = ["id", "tableId"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/reports/{reportId}?tableId={tableId}"
    parent = "table_reports"

    def modify_object(self, record, parent_record=None):
        """Add tableId from parent record to make composite primary key."""
        if parent_record:
            record['tableId'] = self._get_table_id(parent_record)
        return record
