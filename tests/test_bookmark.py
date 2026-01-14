from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
import unittest


class QuickbaseBookMarkTest(BookmarkTest, QuickbaseBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream.

    NOTE: All streams in tap-quickbase are FULL_TABLE replication.
    Some streams (apps, app_tables, tables) are pseudo-incremental where
    filtering is done at the tap side using state, but they remain FULL_TABLE
    replication method. Since there are no true INCREMENTAL streams,
    this test class will skip all bookmark-related tests.
    """
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {}
    }

    @staticmethod
    def name():
        return "tap_tester_quickbase_bookmark_test"

    def streams_to_test(self):
        # All streams are FULL_TABLE - no incremental streams to test
        # Return empty set to skip all bookmark tests
        return set()
