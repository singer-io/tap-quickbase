from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import QuickbaseBaseTest

class QuickbasePaginationTest(PaginationTest, QuickbaseBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_pagination_test"

    def streams_to_test(self):
        streams_to_exclude = {}
        return self.expected_stream_names().difference(streams_to_exclude)

