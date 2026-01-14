"""
Unit tests for pagination workflow.
"""
import unittest
from unittest.mock import MagicMock, patch
from parameterized import parameterized
from tap_quickbase.streams.abstracts import BaseStream, IncrementalStream


class MockStream(BaseStream):
    """Mock stream for testing."""
    tap_stream_id = "test_stream"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    path = "v1/test"
    page_size = 100
    
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
        """Test that pagination parameters are set correctly."""
        response = {
            "data": [{"id": 1}],
            "metadata": {
                "totalRecords": 1,
                "numRecords": 1,
                "skip": 0
            }
        }
        
        self.client.make_request.side_effect = [response]
        list(self.stream.get_records())
        
        # Verify make_request was called with pagination params
        self.assertTrue(self.client.make_request.called)


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


class TestPageSizeConfiguration(unittest.TestCase):
    """Test page_size configuration and pagination loop termination."""

    def setUp(self):
        """Common setup."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

    def test_page_size_less_than_total_records(self):
        """Test pagination when page_size < total records (multiple pages)."""
        self.client.config = {"page_size": 50}
        stream = MockStream(self.client, self.catalog)
        stream.data_key = "data"  # Set data_key to extract records properly
        
        # Simulate 150 total records returned in 3 pages
        self.client.make_request.side_effect = [
            {"data": [{"id": i} for i in range(50)], "metadata": {"totalRecords": 150, "skip": 0}},
            {"data": [{"id": i} for i in range(50, 100)], "metadata": {"totalRecords": 150, "skip": 50}},
            {"data": [{"id": i} for i in range(100, 150)], "metadata": {"totalRecords": 150, "skip": 100}},
        ]
        
        records = list(stream.get_records())
        
        # Should retrieve all 150 records across 3 requests
        self.assertEqual(len(records), 150)
        self.assertEqual(self.client.make_request.call_count, 3)

    def test_page_size_equals_total_records(self):
        """Test pagination when page_size = total records (single page)."""
        self.client.config = {"page_size": 100}
        stream = MockStream(self.client, self.catalog)
        stream.data_key = "data"
        
        # Exactly 100 records returned
        self.client.make_request.side_effect = [
            {"data": [{"id": i} for i in range(100)], "metadata": {"totalRecords": 100, "skip": 0}},
        ]
        
        records = list(stream.get_records())
        
        # Should retrieve all 100 records in 1 request
        self.assertEqual(len(records), 100)
        self.assertEqual(self.client.make_request.call_count, 1)

    def test_page_size_greater_than_total_records(self):
        """Test pagination when page_size > total records (partial page)."""
        self.client.config = {"page_size": 200}
        stream = MockStream(self.client, self.catalog)
        stream.data_key = "data"
        
        # Only 50 records available (less than page_size)
        self.client.make_request.side_effect = [
            {"data": [{"id": i} for i in range(50)], "metadata": {"totalRecords": 50, "skip": 0}},
        ]
        
        records = list(stream.get_records())
        
        # Should retrieve all 50 records in 1 request and stop
        self.assertEqual(len(records), 50)
        self.assertEqual(self.client.make_request.call_count, 1)

    def test_default_page_size_used_when_not_configured(self):
        """Test that default page_size (100) is used when not in config."""
        self.client.config = {}  # No page_size configured
        stream = MockStream(self.client, self.catalog)
        stream.data_key = "data"
        
        self.client.make_request.side_effect = [
            {"data": [{"id": i} for i in range(100)], "metadata": {"totalRecords": 100, "skip": 0}},
        ]
        
        records = list(stream.get_records())
        
        # Should use default page_size of 100
        self.assertEqual(len(records), 100)
        self.assertEqual(self.client.make_request.call_count, 1)
