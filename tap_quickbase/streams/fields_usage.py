from tap_quickbase.streams.abstracts import FullTableStream

class FieldsUsage(FullTableStream):
    tap_stream_id = "fields_usage"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/fields/usage?tableId={tableId}"
    parent = "app_tables"
    
    def modify_object(self, record, parent_record=None):
        """Flatten the API response structure - merge field and usage objects."""
        if not record:
            return record
        
        # Extract field info and usage stats
        field = record.get('field', {})
        usage = record.get('usage', {})
        
        # Merge them with field.id as the primary key
        return {'id': field.get('id'), **usage}


