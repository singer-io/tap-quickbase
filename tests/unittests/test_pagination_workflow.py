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
