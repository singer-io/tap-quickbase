from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class QuickbaseStartDateTest(StartDateTest, QuickbaseBaseTest):
    """Test start date handling for the six dynamic INCREMENTAL app-table streams.

    Static streams are FULL_TABLE and do not obey start_date.
    Dynamic streams (``data_connector_management__*``) are INCREMENTAL with ``OBEYS_START_DATE=True``
    and send a server-side ``date_modified > bookmark_epoch_ms`` WHERE clause
    to the Quickbase API, so they correctly honour the start date.

    Two start dates are used to verify filtering:
    * ``start_date_1 = 2022-01-01`` - before all test data
      -> 8 records/stream (2 from Dec-2025 pre-seeded + 6 from Mar-2026 batch)
    * ``start_date_2 = 2026-01-01`` - after the Dec-2025 batch
      -> 6 records/stream (Mar-2026 only), satisfying assertGreater(8, 6).
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_start_date_test"

    def streams_to_test(self):
        # Only the six dynamic INCREMENTAL streams obey start_date.
        return {name for name in self.expected_stream_names()
                if name.startswith("data_connector_management")}

    @property
    def start_date_1(self):
        # Before all test data -> all 8 records/stream are returned.
        return "2022-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        # After the Dec-2025 pre-seeded records but before the Mar-2026 batch
        # -> only 6 records/stream are returned so assertGreater(8, 6) passes.
        return "2026-01-01T00:00:00Z"
