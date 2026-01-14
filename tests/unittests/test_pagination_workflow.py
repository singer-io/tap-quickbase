"""
Unit tests for pagination workflow.
"""
import unittest
from unittest.mock import MagicMock, patch

from tap_quickbase.streams.abstracts import BaseStream


class MockStream(BaseStream):
    """Mock stream for testing."""
    tap_stream_id = "test_stream"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    path = "v1/test"

    def sync(self, state, transformer, parent_obj=None):
        """Simple sync implementation for testing."""
        return 0


class TestPagination(unittest.TestCase):
    """Test pagination logic."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.client.config = {"page_size": 100}  # Add config for page_size
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

        self.stream = MockStream(self.client, self.catalog)

    def test_pagination_params_set(self):
        """Test that pagination stops correctly with metadata."""
        self.client.config.get.return_value = 100
        self.stream.data_key = "data"

        response = {
            "data": [{"id": 1}],
            "metadata": {
                "totalRecords": 1
            }
        }

        self.client.make_request.return_value = response
        records = list(self.stream.get_records())

        # Verify make_request was called
        self.assertTrue(self.client.make_request.called)
        self.assertEqual(len(records), 1)

    def test_page_size_from_config(self):
        """Test that page_size is correctly read from config."""
        self.client.config.get.return_value = 25
        self.stream.data_key = "data"

        # Return fewer records than page_size to stop pagination
        response = {"data": [{"id": i} for i in range(10)]}

        self.client.make_request.return_value = response
        records = list(self.stream.get_records())

        self.client.config.get.assert_called_with("page_size", 100)
        self.assertEqual(len(records), 10)

    def test_pagination_stops_when_fewer_records_returned(self):
        """Test pagination stops when fewer records than page_size are returned."""
        self.client.config.get.return_value = 25
        self.stream.data_key = "data"

        response = {
            "data": [{"id": i} for i in range(10)],
            "metadata": {"totalRecords": 10}
        }

        self.client.make_request.return_value = response
        records = list(self.stream.get_records())

        self.assertEqual(self.client.make_request.call_count, 1)
        self.assertEqual(len(records), 10)

    def test_pagination_with_small_page_size(self):
        """Test pagination stops after first response when no metadata present."""
        self.client.config.get.return_value = 25
        self.stream.data_key = "data"

        response = {"data": [{"id": i} for i in range(100)]}

        self.client.make_request.return_value = response
        records = list(self.stream.get_records())

        self.assertEqual(self.client.make_request.call_count, 1)
        self.assertEqual(len(records), 100)

    def test_stops_when_no_records_returned(self):
        """Test pagination stops when API returns empty response."""
        self.client.config.get.return_value = 25
        self.stream.data_key = "data"

        response = {"data": []}

        self.client.make_request.return_value = response
        records = list(self.stream.get_records())

        # Should stop immediately when no records
        self.assertEqual(self.client.make_request.call_count, 1)
        self.assertEqual(len(records), 0)

    def test_max_iterations_safety_limit(self):
        """Test that max_iterations prevents infinite loops."""
        self.client.config.get.return_value = 100
        self.stream.data_key = "data"
        self.stream.tap_stream_id = "test_stream"

        response = {"data": [{"id": i} for i in range(100)], "metadata": {"totalRecords": 1000000}}

        self.client.make_request.return_value = response

        # Should stop at max_iterations limit (10000) instead of looping forever
        records = list(self.stream.get_records())

        self.assertEqual(self.client.make_request.call_count, 10000)
        self.assertEqual(len(records), 1000000)


class TestRecordExtraction(unittest.TestCase):
    """Test record extraction from different response formats."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.client.config = {"page_size": 100}  # Add config for page_size
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

        self.stream = MockStream(self.client, self.catalog)

    def test_extract_records_with_data_key(self):
        """Test extraction when data_key is specified."""
        self.stream.data_key = "relationships"

        response = {
            "relationships": [{"id": 1}, {"id": 2}],
            "metadata": {}
        }

        records = self.stream._extract_records(response)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], 1)

    def test_extract_records_list_response(self):
        """Test extraction from list response."""
        response = [{"id": 1}, {"id": 2}, {"id": 3}]

        records = self.stream._extract_records(response)

        self.assertEqual(len(records), 3)

    def test_extract_records_single_object(self):
        """Test extraction from single object response."""
        response = {"id": 1, "name": "Test"}

        records = self.stream._extract_records(response)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 1)

    def test_extract_records_empty_data_key(self):
        """Test extraction when data_key returns empty."""
        self.stream.data_key = "items"

        response = {"items": [], "metadata": {}}

        records = self.stream._extract_records(response)

        self.assertEqual(len(records), 0)

    def test_extract_records_missing_data_key(self):
        """Test extraction when data_key is missing."""
        self.stream.data_key = "missing_key"

        response = {"other_key": [1, 2, 3]}

        records = self.stream._extract_records(response)

        self.assertEqual(len(records), 0)
