"""Events stream definition."""

from tap_quickbase.streams.abstracts import FullTableStream

class Events(FullTableStream):
    """Events stream."""
    tap_stream_id = "events"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/apps/{appId}/events"
    parent = "apps"

    def modify_object(self, record, parent_record=None):
        """Extract owner.id as top-level id."""
        if record and 'owner' in record and 'id' in record['owner']:
            record['id'] = record['owner']['id']
        return record
