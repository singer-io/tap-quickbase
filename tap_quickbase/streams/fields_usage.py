"""Fields usage stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream


class FieldsUsage(FullTableStream):
    """Fields usage stream."""
    tap_stream_id = "fields_usage"
    key_properties = ["tableId", "id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage?tableId={tableId}"
    parent = "app_tables"
    children = []

    def modify_object(self, record, parent_record=None):
        """Flatten field and usage objects and add tableId from parent record."""
        flattened = self.flatten_field_usage_record(record)
        if parent_record:
            flattened['tableId'] = self._get_table_id(parent_record)
        return flattened
