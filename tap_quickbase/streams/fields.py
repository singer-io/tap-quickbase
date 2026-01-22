"""Fields stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream


class Fields(FullTableStream):
    """Fields stream."""
    tap_stream_id = "fields"
    key_properties = ["tableId", "id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields?tableId={tableId}"
    parent = "app_tables"
    children = []

    def modify_object(self, record, parent_record=None):
        """Add tableId from parent record to make composite primary key."""
        if parent_record:
            record['tableId'] = self._get_table_id(parent_record)
        return record
