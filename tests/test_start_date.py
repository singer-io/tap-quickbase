from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class QuickbaseStartDateTest(StartDateTest, QuickbaseBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_quickbase_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2025-12-18T00:00:00Z"
    @property
    def start_date_2(self):
        return "2025-12-18T11:28:30Z"

