from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest
import unittest


@unittest.skip("All streams in tap-quickbase are FULL_TABLE replication. "
               "Interrupted sync tests only apply to INCREMENTAL streams.")
class QuickbaseInterruptedSyncTest(InterruptedSyncTest, QuickbaseBaseTest):
    """Test tap resumes from interrupted sync.

    NOTE: All streams in tap-quickbase are FULL_TABLE replication.
    Some streams (apps, app_tables, tables) are pseudo-incremental where
    filtering is done at the tap side using state, but they remain FULL_TABLE
    replication method. Since all streams are FULL_TABLE, interrupted sync
    behavior is not applicable and this test class is skipped.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_interrupted_sync_test"

    def streams_to_test(self):
        # All streams are FULL_TABLE - interrupted sync doesn't apply
        return self.expected_stream_names()

    def manipulate_state(self):
        """Return state with currently_syncing set to first stream."""
        return {
            "currently_syncing": "apps",
            "bookmarks": {}
        }
