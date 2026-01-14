"""Apps stream definition."""

from tap_quickbase.streams.abstracts import PseudoIncrementalStream


class Apps(PseudoIncrementalStream):
    """Apps stream."""
    tap_stream_id = "apps"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "v1/apps/{appId}"
    children = ["events", "roles", "app_tables"]
    bookmark_field = "updated"

    def get_url_endpoint(self, parent_obj=None):
        """Get app by ID from config"""
        app_id = self.client.config.get('app_id')
        if not app_id:
            raise ValueError("app_id is required in config to sync apps")
        path = self.path.replace('{appId}', app_id)
        return f"{self.client.base_url}/{path}"
