from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class QuickbaseBookMarkTest(BookmarkTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream.

    Targets only the six dynamic INCREMENTAL app-table streams
    (``data_connector_management__*``).
    Static streams are FULL_TABLE and therefore excluded from this test.
    """
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {}
    }

    @staticmethod
    def name():
        return "tap_tester_quickbase_bookmark_test"

    def streams_to_test(self):
        # Only the six dynamic INCREMENTAL app-table streams carry bookmarks.
        # Static streams are FULL_TABLE and must not appear in this test.
        return {name for name in self.expected_stream_names()
                if name.startswith("data_connector_management")}
