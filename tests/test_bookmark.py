from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
import unittest


class QuickbaseBookMarkTest(BookmarkTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {}
    }
    @staticmethod
    def name():
        return "tap_tester_quickbase_bookmark_test"

    def streams_to_test(self):
        # All streams are FULL_TABLE - no incremental/bookmark support
        streams_to_exclude = {
            'apps',
            'events',
            'roles',
            'app_tables',
            'tables',
            'table_relationships',
            'table_reports',
            'get_reports',
            'fields',
            'get_fields',
            'fields_usage',
            'get_field_usage'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def setUp(self):
        """Skip all tests if no streams support bookmarks."""
        if not self.streams_to_test():
            self.skipTest("All streams use FULL_TABLE replication - no bookmark support")
        super().setUp()
