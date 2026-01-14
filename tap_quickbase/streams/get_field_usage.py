"""GetFieldUsage stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream


class GetFieldUsage(FullTableStream):
    """GetFieldUsage stream."""
    tap_stream_id = "get_field_usage"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage/{fieldId}?tableId={tableId}"
    parent = "fields_usage"

    def modify_object(self, record, parent_record=None):
        """Flatten field and usage objects with field.id as primary key."""
        return self.flatten_field_usage_record(record)
