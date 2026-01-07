import unittest
from unittest.mock import patch, MagicMock
from tap_quickbase.sync import setup_children, sync, update_currently_syncing

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
        client.config = {'appId': 'test_app_id', 'start_date': '2024-01-01T00:00:00Z'}
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
        client.config = {'appId': 'test_app_id', 'start_date': '2024-01-01T00:00:00Z'}
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
