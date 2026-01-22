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
        qb_appid = self.client.config.get('qb_appid')
        if not qb_appid:
            raise ValueError("qb_appid is required in config to sync apps")
        path = self.path.replace('{appId}', qb_appid)
        return f"{self.client.base_url}/{path}"
