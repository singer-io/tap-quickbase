"""
Unit tests for stream sync workflows.
"""
import unittest
from unittest.mock import MagicMock, patch
from parameterized import parameterized
from tap_quickbase.streams.abstracts import IncrementalStream, BaseStream
from singer import Transformer


class MockIncrementalStream(IncrementalStream):
    """Mock incremental stream for testing."""
    tap_stream_id = "test_incremental"
    replication_method = "INCREMENTAL"
    replication_keys = ["updated"]
    key_properties = ["id"]
    path = "v1/test"
    page_size = 100


class MockFullTableStream(BaseStream):
    """Mock full table stream for testing."""
    tap_stream_id = "test_full_table"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    path = "v1/test"
    page_size = 100
    
    def sync(self, state, transformer, parent_obj=None):
        """Simple sync implementation for testing."""
        return 0


class TestIncrementalSync(unittest.TestCase):
    """Test incremental sync workflow."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.client.config = {"start_date": "2023-01-01T00:00:00Z"}
        
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": "string"},
                "updated": {"type": "string", "format": "date-time"}
            }
        }
        self.catalog.metadata = []
        
        self.stream = MockIncrementalStream(self.client, self.catalog)

    def test_get_bookmark_returns_start_date_when_no_bookmark(self):
        """Test that get_bookmark returns start_date when no bookmark exists."""
        state = {}
        
        bookmark = self.stream.get_bookmark(state, "test_incremental")
        
        self.assertEqual(bookmark, "2023-01-01T00:00:00Z")

    def test_get_bookmark_returns_existing_bookmark(self):
        """Test that get_bookmark returns existing bookmark value."""
        state = {
            "bookmarks": {
                "test_incremental": {
                    "updated": "2023-06-15T10:00:00Z"
                }
            }
        }
        
        bookmark = self.stream.get_bookmark(state, "test_incremental")
        
        self.assertEqual(bookmark, "2023-06-15T10:00:00Z")

    def test_write_bookmark_updates_state(self):
        """Test that write_bookmark updates state correctly."""
        state = {}
        
        new_state = self.stream.write_bookmark(
            state, 
            "test_incremental", 
            value="2023-06-20T15:30:00Z"
        )
        
        self.assertIn("test_incremental", new_state.get("bookmarks", {}))
        self.assertEqual(
            new_state["bookmarks"]["test_incremental"]["updated"],
            "2023-06-20T15:30:00Z"
        )

    def test_write_bookmark_keeps_max_value(self):
        """Test that write_bookmark keeps the maximum value."""
        state = {
            "bookmarks": {
                "test_incremental": {
                    "updated": "2023-06-20T00:00:00Z"
                }
            }
        }
        
        # Try to write older bookmark
        new_state = self.stream.write_bookmark(
            state,
            "test_incremental",
            value="2023-06-10T00:00:00Z"
        )
        
        # Should keep the newer value
        self.assertEqual(
            new_state["bookmarks"]["test_incremental"]["updated"],
            "2023-06-20T00:00:00Z"
        )

    def test_sync_bookmark_handling(self):
        """Test that sync properly handles bookmarks."""
        state = {}
        transformer = Transformer()
        
        # Test that get_bookmark is called
        bookmark = self.stream.get_bookmark(state, "test_incremental")
        self.assertEqual(bookmark, "2023-01-01T00:00:00Z")


class TestURLEndpointGeneration(unittest.TestCase):
    """Test URL endpoint generation with parent objects."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.client.config = {"app_id": "app123"}
        
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []
        
        self.stream = MockFullTableStream(self.client, self.catalog)

    def test_get_url_endpoint_without_parent(self):
        """Test URL generation without parent object."""
        self.stream.path = "v1/apps"
        
        url = self.stream.get_url_endpoint()
        
        self.assertEqual(url, "https://api.quickbase.com/v1/apps")

    def test_get_url_endpoint_with_appId_placeholder(self):
        """Test URL generation with appId placeholder."""
        self.stream.path = "v1/apps/{appId}/tables"
        parent_obj = {"id": "app456"}
        
        url = self.stream.get_url_endpoint(parent_obj)
        
        # Should use appId from config
        self.assertIn("app123", url)

    def test_get_url_endpoint_with_tableId_placeholder(self):
        """Test URL generation with tableId placeholder."""
        self.stream.path = "v1/tables/{tableId}/fields"
        parent_obj = {"id": "table789"}
        
        url = self.stream.get_url_endpoint(parent_obj)
        
        self.assertIn("table789", url)

    def test_get_url_endpoint_with_nested_query(self):
        """Test URL generation with nested query structure."""
        self.stream.path = "v1/tables/{tableId}/reports"
        parent_obj = {"query": {"tableId": "table999"}}
        
        url = self.stream.get_url_endpoint(parent_obj)
        
        self.assertIn("table999", url)


class TestStreamModifications(unittest.TestCase):
    """Test stream record modifications."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []
        
        self.stream = MockFullTableStream(self.client, self.catalog)

    def test_modify_object_default_behavior(self):
        """Test that modify_object returns record unchanged by default."""
        record = {"id": "1", "name": "Test"}
        parent = {"id": "parent1"}
        
        modified = self.stream.modify_object(record, parent)
        
        self.assertEqual(modified, record)

    def test_update_params(self):
        """Test updating stream parameters."""
        self.stream.update_params(skip=100, top=50)
        
        self.assertEqual(self.stream.params["skip"], 100)
        self.assertEqual(self.stream.params["top"], 50)

    def test_update_data_payload(self):
        """Test updating stream data payload."""
        self.stream.update_data_payload(filter={"id": 123})
        
        self.assertEqual(self.stream.data_payload["filter"]["id"], 123)


class TestStreamSelection(unittest.TestCase):
    """Test stream selection logic."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []
        
        self.stream = MockFullTableStream(self.client, self.catalog)

    @patch("singer.metadata.get")
    def test_is_selected_returns_true_when_selected(self, mock_metadata_get):
        """Test is_selected returns True when stream is selected."""
        mock_metadata_get.return_value = True
        
        result = self.stream.is_selected()
        
        self.assertTrue(result)

    @patch("singer.metadata.get")
    def test_is_selected_returns_false_when_not_selected(self, mock_metadata_get):
        """Test is_selected returns False when stream is not selected."""
        mock_metadata_get.return_value = False
        
        result = self.stream.is_selected()
        
        self.assertFalse(result)


class TestSchemaWriting(unittest.TestCase):
    """Test schema writing."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {
            "properties": {
                "id": {"type": "string"}
            }
        }
        self.catalog.metadata = []
        
        self.stream = MockFullTableStream(self.client, self.catalog)

    @patch("tap_quickbase.streams.abstracts.write_schema")
    def test_write_schema_success(self, mock_write_schema):
        """Test successful schema writing."""
        self.stream.write_schema()
        
        mock_write_schema.assert_called_once_with(
            "test_full_table",
            self.stream.schema,
            self.stream.key_properties
        )

    @patch("tap_quickbase.streams.abstracts.write_schema", side_effect=OSError("File error"))
    def test_write_schema_handles_os_error(self, mock_write_schema):
        """Test that write_schema raises OSError when it occurs."""
        with self.assertRaises(OSError):
            self.stream.write_schema()


class TestParentChildRelationships(unittest.TestCase):
    """Test parent-child stream relationships."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

    def test_stream_has_children_attribute(self):
        """Test that streams have children attribute."""
        stream = MockFullTableStream(self.client, self.catalog)
        
        self.assertTrue(hasattr(stream, "children"))
        self.assertIsInstance(stream.children, list)

    def test_stream_has_parent_attribute(self):
        """Test that streams have parent attribute."""
        stream = MockFullTableStream(self.client, self.catalog)
        
        self.assertTrue(hasattr(stream, "parent"))

    def test_child_to_sync_initialization(self):
        """Test child_to_sync list is initialized."""
        stream = MockFullTableStream(self.client, self.catalog)
        
        self.assertTrue(hasattr(stream, "child_to_sync"))
        self.assertIsInstance(stream.child_to_sync, list)
        self.assertEqual(len(stream.child_to_sync), 0)


class TestHTTPMethodConfiguration(unittest.TestCase):
    """Test HTTP method configuration."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

    def test_stream_default_http_method(self):
        """Test that default HTTP method is GET."""
        stream = MockFullTableStream(self.client, self.catalog)
        
        self.assertEqual(stream.http_method, "GET")

    def test_stream_can_override_http_method(self):
        """Test that stream can override HTTP method."""
        class PostStream(MockFullTableStream):
            http_method = "POST"
        
        stream = PostStream(self.client, self.catalog)
        
        self.assertEqual(stream.http_method, "POST")
