import unittest
from unittest.mock import patch, MagicMock
from singer.catalog import CatalogEntry, Schema
from singer import metadata
from tap_quickbase.sync import (
    setup_children,
    sync,
    update_currently_syncing,
    _is_dynamic_stream,
    _build_stream,
)

class TestSync(unittest.TestCase):

    @patch('tap_quickbase.sync.STREAMS')
    def test_setup_children_only_parent_selected(self, mock_streams):
        mock_stream = MagicMock()
        mock_stream.children = ["events", "roles"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        mock_child = MagicMock()
        mock_child.children = []
        catalog.get_stream.return_value = MagicMock()
        mock_streams.__getitem__.return_value = lambda c, cat: mock_child

        setup_children(mock_stream, client, [], catalog)

        self.assertEqual(len(mock_stream.child_to_sync), 0)

    @patch('tap_quickbase.sync.STREAMS')
    def test_setup_children_parent_child_both_selected(self, mock_streams):
        mock_stream = MagicMock()
        mock_stream.children = ["events", "roles"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        mock_child = MagicMock()
        mock_child.children = []
        catalog.get_stream.return_value = MagicMock()
        mock_streams.__getitem__.return_value = lambda c, cat: mock_child

        setup_children(mock_stream, client, ["events"], catalog)

        self.assertEqual(len(mock_stream.child_to_sync), 1)

    @patch('tap_quickbase.sync.STREAMS')
    def test_setup_children_child_selected(self, mock_streams):
        mock_stream = MagicMock()
        mock_stream.children = ["events", "roles"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        mock_child = MagicMock()
        mock_child.children = []
        catalog.get_stream.return_value = MagicMock()
        mock_streams.__getitem__.return_value = lambda c, cat: mock_child

        setup_children(mock_stream, client, ["events", "roles"], catalog)

        self.assertEqual(len(mock_stream.child_to_sync), 2)

    @patch('tap_quickbase.sync.STREAMS')
    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    def test_sync_stream1_called(self, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema, mock_streams):
        mock_catalog = MagicMock()
        apps_stream = MagicMock()
        apps_stream.stream = "apps"
        apps_stream.parent = None
        
        tables_stream = MagicMock()
        tables_stream.stream = "tables"
        tables_stream.parent = None
        
        mock_catalog.get_selected_streams.return_value = [apps_stream, tables_stream]
        state = {}

        # Mock the STREAMS dictionary to return mock stream class that creates instances
        mock_stream_instance = MagicMock()
        mock_stream_instance.parent = None
        mock_stream_instance.children = []
        mock_stream_instance.child_to_sync = []
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 10  # Return a count
        mock_stream_instance.write_schema.return_value = None
        
        mock_stream_class = MagicMock(return_value=mock_stream_instance)
        mock_streams.__getitem__ = MagicMock(return_value=mock_stream_class)

        client = MagicMock()
        client.config = {'qb_appid': 'test_app_id', 'start_date': '2024-01-01T00:00:00Z'}
        config = {}

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_stream_instance.sync.call_count, 2)

    @patch('tap_quickbase.sync.STREAMS')
    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    def test_sync_child_selected(self, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema, mock_streams):
        mock_catalog = MagicMock()
        events_stream = MagicMock()
        events_stream.stream = "events"
        roles_stream = MagicMock()
        roles_stream.stream = "roles"
        mock_catalog.get_selected_streams.return_value = [
            events_stream,
            roles_stream
        ]
        state = {}

        # Mock child stream instances (events and roles have parent = "apps")
        mock_child_instance = MagicMock()
        mock_child_instance.parent = "apps"  # Child streams have a parent
        mock_child_instance.children = []
        mock_child_instance.child_to_sync = []
        mock_child_instance.is_selected.return_value = True
        mock_child_instance.sync.return_value = 5
        mock_child_instance.write_schema.return_value = None
        
        # Mock parent stream instance (apps)
        mock_parent_instance = MagicMock()
        mock_parent_instance.parent = None  # Parent streams have no parent
        mock_parent_instance.children = ["events", "roles"]
        mock_parent_instance.child_to_sync = []
        mock_parent_instance.is_selected.return_value = True
        mock_parent_instance.sync.return_value = 10
        mock_parent_instance.write_schema.return_value = None
        
        # Return child for events/roles, parent for apps
        def get_stream_class(stream_name):
            if stream_name == "apps":
                return MagicMock(return_value=mock_parent_instance)
            else:
                return MagicMock(return_value=mock_child_instance)
        
        mock_streams.__getitem__ = MagicMock(side_effect=get_stream_class)

        client = MagicMock()
        client.config = {'qb_appid': 'test_app_id', 'start_date': '2024-01-01T00:00:00Z'}
        config = {}

        sync(client, config, mock_catalog, state)

        # When only child streams are selected, parent is synced once
        self.assertEqual(mock_parent_instance.sync.call_count, 1)
        # Child streams are not synced directly (parent syncs them)
        self.assertEqual(mock_child_instance.sync.call_count, 0)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}

        update_currently_syncing(state, None)

        mock_get_currently_syncing.assert_called_once_with(state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state) 

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, "new_stream")

        mock_get_currently_syncing.assert_not_called()
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state) 


# ---------------------------------------------------------------------------
# Helpers: _is_dynamic_stream
# ---------------------------------------------------------------------------

def _make_catalog_entry(is_dynamic: bool) -> CatalogEntry:
    """Build a minimal CatalogEntry with or without the is_dynamic flag."""
    mdata = [{"breadcrumb": [], "metadata": {}}]
    if is_dynamic:
        mdata[0]["metadata"]["tap-quickbase.is_dynamic"] = True
    return CatalogEntry(
        stream="test_stream",
        tap_stream_id="test_stream",
        key_properties=["id"],
        schema=Schema.from_dict({"type": "object", "properties": {}}),
        metadata=mdata,
    )


class TestIsDynamicStream(unittest.TestCase):
    """Unit tests for the _is_dynamic_stream helper."""

    def test_dynamic_flag_true_returns_true(self):
        entry = _make_catalog_entry(is_dynamic=True)
        self.assertTrue(_is_dynamic_stream(entry))

    def test_no_dynamic_flag_returns_false(self):
        entry = _make_catalog_entry(is_dynamic=False)
        self.assertFalse(_is_dynamic_stream(entry))

    def test_empty_metadata_returns_false(self):
        entry = CatalogEntry(
            stream="empty",
            tap_stream_id="empty",
            key_properties=[],
            schema=Schema.from_dict({"type": "object"}),
            metadata=[],
        )
        self.assertFalse(_is_dynamic_stream(entry))

    def test_dynamic_flag_false_returns_false(self):
        """Explicit False value for the flag must not be treated as True."""
        mdata = [{"breadcrumb": [], "metadata": {"tap-quickbase.is_dynamic": False}}]
        entry = CatalogEntry(
            stream="s",
            tap_stream_id="s",
            key_properties=[],
            schema=Schema.from_dict({"type": "object"}),
            metadata=mdata,
        )
        self.assertFalse(_is_dynamic_stream(entry))


# ---------------------------------------------------------------------------
# Helpers: _build_stream
# ---------------------------------------------------------------------------

class TestBuildStream(unittest.TestCase):
    """Unit tests for the _build_stream routing helper."""

    def _make_catalog(self, is_dynamic: bool, stream_name: str = "test_stream"):
        catalog = MagicMock()
        catalog.get_stream.return_value = _make_catalog_entry(is_dynamic)
        return catalog

    @patch("tap_quickbase.sync.STREAMS")
    def test_static_stream_uses_streams_registry(self, mock_streams):
        """A static entry (no is_dynamic flag) must be looked up in STREAMS."""
        mock_cls = MagicMock()
        mock_streams.__getitem__ = MagicMock(return_value=mock_cls)

        client = MagicMock()
        catalog = self._make_catalog(is_dynamic=False)
        _build_stream("test_stream", client, catalog)

        mock_streams.__getitem__.assert_called_once_with("test_stream")
        mock_cls.assert_called_once()

    @patch("tap_quickbase.sync.DynamicTableStream")
    def test_dynamic_stream_uses_dynamic_class(self, mock_dynamic_cls):
        """A dynamic entry must be instantiated as DynamicTableStream."""
        client = MagicMock()
        catalog = self._make_catalog(is_dynamic=True)
        _build_stream("test_stream", client, catalog)

        mock_dynamic_cls.assert_called_once()
        # First positional arg should be the client
        args = mock_dynamic_cls.call_args[0]
        self.assertEqual(args[0], client)

    @patch("tap_quickbase.sync.DynamicTableStream")
    @patch("tap_quickbase.sync.STREAMS")
    def test_unknown_static_stream_falls_back_to_dynamic(
        self, mock_streams, mock_dynamic_cls
    ):
        """If STREAMS raises KeyError the stream falls back to DynamicTableStream."""
        mock_streams.__getitem__ = MagicMock(side_effect=KeyError("unknown"))

        client = MagicMock()
        catalog = self._make_catalog(is_dynamic=False)
        _build_stream("unknown_stream", client, catalog)

        mock_dynamic_cls.assert_called_once()
