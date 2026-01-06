from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class QuickbaseBookMarkTest(BookmarkTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "apps": {},
            "app_tables": {},
            "tables": {},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_quickbase_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # Unsupported Full-Table Streams
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

    def calculate_new_bookmarks(self):
        """Calculate new bookmarks to sync at least 2 records in the next sync."""
        new_bookmarks = {
            "apps": {},
            "app_tables": {},
            "tables": {},
        }

        return new_bookmarks

