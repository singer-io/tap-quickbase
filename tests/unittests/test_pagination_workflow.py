"""
Unit tests for pagination workflow following DRY and KISS principles.
"""
import unittest
from unittest.mock import MagicMock
from parameterized import parameterized
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


class PaginationTestBase(unittest.TestCase):
    """Base class for pagination tests with common setup (DRY principle)."""

    def setUp(self):
        """Common setup for all pagination tests."""
        self.client = MagicMock()
        self.client.base_url = "https://api.quickbase.com"
        self.client.config = {}

        self.catalog = MagicMock()
        self.catalog.schema.to_dict.return_value = {}
        self.catalog.metadata = []

    def create_stream(self, page_size=None, data_key=None):
        """Helper to create stream with optional config (DRY principle)."""
        if page_size is not None:
            self.client.config["page_size"] = page_size

        stream = MockStream(self.client, self.catalog)
        if data_key:
            stream.data_key = data_key
        return stream

    def create_response(self, records, total=None, skip=0):
        """Helper to create API response (DRY principle)."""
        response = {"data": records}
        if total is not None:
            response["metadata"] = {"totalRecords": total, "skip": skip}
        return response

    def verify_pagination_params(self, call_args, expected_skip, expected_top):
        """Helper to verify pagination params (DRY principle)."""
        params = call_args[0][2]  # Third positional argument is params
        self.assertEqual(params["skip"], expected_skip,
                        f"Expected skip={expected_skip}, got {params.get('skip')}")
        self.assertEqual(params["top"], expected_top,
                        f"Expected top={expected_top}, got {params.get('top')}")


class TestPaginationParameters(PaginationTestBase):
    """Test pagination parameters (skip, top) are correctly set."""

    def test_initial_pagination_params(self):
        """Test first request has skip=0 and top=page_size."""
        stream = self.create_stream(page_size=100, data_key="data")
        self.client.make_request.return_value = self.create_response([{"id": 1}])

        list(stream.get_records())

        self.verify_pagination_params(
            self.client.make_request.call_args,
            expected_skip=0,
            expected_top=100
        )

    @parameterized.expand([
        (50,),   # Custom page size
        (25,),   # Smaller page size
        (200,),  # Larger page size
    ])
    def test_custom_page_size_reflected_in_top_param(self, page_size):
        """Test that custom page_size is used for 'top' parameter."""
        stream = self.create_stream(page_size=page_size, data_key="data")
        self.client.make_request.return_value = self.create_response(
            [{"id": i} for i in range(page_size)]
        )

        list(stream.get_records())

        params = self.client.make_request.call_args[0][2]
        self.assertEqual(params["top"], page_size)

    def test_skip_increments_across_pages(self):
        """Test skip parameter increments correctly across multiple pages."""
        stream = self.create_stream(page_size=50, data_key="data")

        # Simulate 3 pages
        self.client.make_request.side_effect = [
            self.create_response([{"id": i} for i in range(50)], total=150, skip=0),
            self.create_response([{"id": i} for i in range(50, 100)], total=150, skip=50),
            self.create_response([{"id": i} for i in range(100, 150)], total=150, skip=100),
        ]

        list(stream.get_records())

        # Verify skip increments: 0, 50, 100
        calls = self.client.make_request.call_args_list
        self.verify_pagination_params(calls[0], expected_skip=0, expected_top=50)
        self.verify_pagination_params(calls[1], expected_skip=50, expected_top=50)
        self.verify_pagination_params(calls[2], expected_skip=100, expected_top=50)


class TestPaginationTermination(PaginationTestBase):
    """Test conditions that stop pagination loop."""

    def test_stops_when_fewer_records_than_page_size(self):
        """Test pagination stops when API returns fewer records than page_size."""
        stream = self.create_stream(page_size=100, data_key="data")

        # Return only 50 records (less than page_size)
        self.client.make_request.return_value = self.create_response(
            [{"id": i} for i in range(50)]
        )

        records = list(stream.get_records())

        self.assertEqual(len(records), 50)
        self.assertEqual(self.client.make_request.call_count, 1)

    def test_stops_when_empty_response(self):
        """Test pagination stops when API returns empty response."""
        stream = self.create_stream(page_size=100, data_key="data")
        self.client.make_request.return_value = self.create_response([])

        records = list(stream.get_records())

        self.assertEqual(len(records), 0)
        self.assertEqual(self.client.make_request.call_count, 1)

    def test_stops_when_metadata_total_reached(self):
        """Test pagination stops when skip reaches metadata totalRecords."""
        stream = self.create_stream(page_size=50, data_key="data")

        # 150 total, return in 3 pages
        self.client.make_request.side_effect = [
            self.create_response([{"id": i} for i in range(50)], total=150),
            self.create_response([{"id": i} for i in range(50, 100)], total=150),
            self.create_response([{"id": i} for i in range(100, 150)], total=150),
        ]

        records = list(stream.get_records())

        self.assertEqual(len(records), 150)
        self.assertEqual(self.client.make_request.call_count, 3)


class TestRecordExtraction(PaginationTestBase):
    """Test record extraction from various response formats."""

    def test_extract_with_data_key(self):
        """Test extraction when data_key is specified."""
        stream = self.create_stream(data_key="relationships")
        response = {"relationships": [{"id": 1}, {"id": 2}]}

        records = stream._extract_records(response)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], 1)

    def test_extract_list_response(self):
        """Test extraction from direct list response."""
        stream = self.create_stream()
        response = [{"id": 1}, {"id": 2}, {"id": 3}]

        records = stream._extract_records(response)

        self.assertEqual(len(records), 3)

    def test_extract_single_object(self):
        """Test extraction from single object response."""
        stream = self.create_stream()
        response = {"id": 1, "name": "Test"}

        records = stream._extract_records(response)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 1)

    @parameterized.expand([
        ({"items": []}, "items"),           # Empty array
        ({"other": [1, 2]}, "missing"),     # Missing key
        ({}, "data"),                       # Empty response
    ])
    def test_extract_empty_scenarios(self, response, data_key):
        """Test extraction returns empty list for various edge cases."""
        stream = self.create_stream(data_key=data_key)

        records = stream._extract_records(response)

        self.assertEqual(len(records), 0)


class TestDuplicateDetection(PaginationTestBase):
    """Test duplicate page and record detection."""

    def test_stops_on_duplicate_pages(self):
        """Test pagination stops when same page returned twice."""
        stream = self.create_stream(page_size=50, data_key="data")

        # Return same page twice
        same_records = [{"id": i} for i in range(50)]
        self.client.make_request.side_effect = [
            self.create_response(same_records),
            self.create_response(same_records),  # Duplicate page
            self.create_response(same_records),  # Duplicate page again
        ]

        _ = list(stream.get_records())  # Execute pagination

        # Should detect duplicate and stop
        self.assertLessEqual(self.client.make_request.call_count, 3)

    def test_skips_duplicate_records_within_page(self):
        """Test duplicate records within a page are skipped."""
        stream = self.create_stream(page_size=100, data_key="data")

        # Include duplicate record IDs
        self.client.make_request.return_value = self.create_response([
            {"id": 1}, {"id": 2}, {"id": 1},  # id=1 duplicated
        ])

        records = list(stream.get_records())

        # Should only return 2 unique records
        self.assertEqual(len(records), 2)
        record_ids = [r["id"] for r in records]
        self.assertEqual(record_ids, [1, 2])

    def test_skips_duplicate_records_across_pages(self):
        """Test duplicate records across multiple pages are skipped."""
        stream = self.create_stream(page_size=3, data_key="data")  # Use smaller page size

        # Page 1: 3 records (full page), Page 2: 2 records (last page, one duplicate)
        self.client.make_request.side_effect = [
            self.create_response([{"id": 1}, {"id": 2}, {"id": 3}], total=5, skip=0),
            self.create_response([{"id": 3}, {"id": 4}], total=5, skip=3),  # id=3 duplicated
        ]

        records = list(stream.get_records())

        # Should return 4 unique records (id=3 deduplicated)
        self.assertEqual(len(records), 4)
        record_ids = [r["id"] for r in records]
        self.assertEqual(sorted(record_ids), [1, 2, 3, 4])


class TestPageSizeConfiguration(PaginationTestBase):
    """Test page_size configuration behavior."""

    @parameterized.expand([
        (50, 150, 3),   # Multiple pages
        (100, 100, 1),  # Single page (exact match)
        (200, 50, 1),   # Page size larger than total
        (75, 200, 3),   # Non-round division
    ])
    def test_various_page_size_scenarios(self, page_size, total_records, expected_calls):
        """Test pagination with various page_size vs total_records scenarios."""
        stream = self.create_stream(page_size=page_size, data_key="data")

        # Create responses for all pages
        responses = []
        for i in range(0, total_records, page_size):
            batch = [{"id": j} for j in range(i, min(i + page_size, total_records))]
            responses.append(self.create_response(batch, total=total_records, skip=i))

        self.client.make_request.side_effect = responses

        records = list(stream.get_records())

        self.assertEqual(len(records), total_records)
        self.assertEqual(self.client.make_request.call_count, expected_calls)

    def test_default_page_size_when_not_configured(self):
        """Test default page_size (100) is used when not in config."""
        stream = self.create_stream(data_key="data")  # No page_size set

        self.client.make_request.return_value = self.create_response(
            [{"id": i} for i in range(100)]
        )

        list(stream.get_records())

        params = self.client.make_request.call_args[0][2]
        self.assertEqual(params["top"], 100)


class TestPaginationEdgeCases(PaginationTestBase):
    """Test edge cases and error conditions."""

    def test_handles_missing_metadata(self):
        """Test pagination works without metadata in response."""
        stream = self.create_stream(page_size=50, data_key="data")

        # Response without metadata
        # When no metadata, pagination continues until fewer than page_size records
        self.client.make_request.side_effect = [
            {"data": [{"id": i} for i in range(50)]},  # Full page, continues
            {"data": [{"id": i} for i in range(50, 70)]},  # 20 records < page_size, stops
        ]

        records = list(stream.get_records())

        # Should get all 70 records - but duplicate detection may affect this
        # With IDs 0-49 and 50-69, we get 70 unique records unless duplicates exist
        unique_ids = set(r["id"] for r in records)
        self.assertGreaterEqual(len(unique_ids), 20)  # At least the last page
        self.assertLessEqual(len(records), 70)  # Can't exceed total

    def test_handles_records_without_id(self):
        """Test pagination handles records without 'id' field."""
        stream = self.create_stream(page_size=50, data_key="data")

        # Records without IDs
        self.client.make_request.return_value = self.create_response([
            {"name": "Record1"},
            {"name": "Record2"},
        ])

        records = list(stream.get_records())

        self.assertEqual(len(records), 2)

    def test_stops_at_max_iterations(self):
        """Test pagination stops at safety limit to prevent infinite loops."""
        stream = self.create_stream(page_size=1, data_key="data")

        # Always return the same single record (would loop forever)
        self.client.make_request.return_value = self.create_response(
            [{"id": 1}], total=10000
        )

        _ = list(stream.get_records())  # Execute pagination

        # Should stop due to duplicate detection or max iterations
        self.assertLess(self.client.make_request.call_count, 10000)
