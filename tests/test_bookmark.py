from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
import unittest


class QuickbaseBookMarkTest(BookmarkTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream.

    The test account has insufficient data for bookmark testing.
    - apps: 1 record with single timestamp
    - app_tables: 6 records with same timestamp
    - tables: 6 records with same timestamp
    """
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {}
    }

    @staticmethod
    def name():
        return "tap_tester_quickbase_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {
            'events',
            'roles',
            'table_relationships',
            'table_reports',
            'get_reports',
            'fields',
            'get_fields',
            'fields_usage',
            'get_field_usage'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
