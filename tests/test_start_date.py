from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest
import unittest


@unittest.skip("All streams in tap-quickbase are FULL_TABLE replication and don't obey start_date. "
               "Start date tests only apply to streams with OBEYS_START_DATE=True.")
class QuickbaseStartDateTest(StartDateTest, QuickbaseBaseTest):
    """Test start date handling for tap-quickbase.

    NOTE: All streams in tap-quickbase are FULL_TABLE replication.
    Some streams (apps, app_tables, tables) are pseudo-incremental where
    filtering is done at the tap side using state, but they remain FULL_TABLE
    replication method. Since there are no streams that truly obey start_date,
    this test class is skipped.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_start_date_test"

    def streams_to_test(self):
        # All streams are FULL_TABLE and don't obey start_date
        return self.expected_stream_names()

    @property
    def start_date_1(self):
        return "2025-12-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2025-12-15T00:00:00Z"
