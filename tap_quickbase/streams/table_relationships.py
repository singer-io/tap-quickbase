"""TableRelationships stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class TableRelationships(FullTableStream):
    """TableRelationships stream."""
    tap_stream_id = "table_relationships"
    key_properties = ["tableId", "id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "relationships"
    path = "v1/tables/{tableId}/relationships"
    parent = "app_tables"

    def modify_object(self, record, parent_record=None):
        """Add tableId from parent record to make composite primary key."""
        if parent_record:
            record['tableId'] = self._get_table_id(parent_record)
        return record
