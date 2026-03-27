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
        # Exclude streams with insufficient records to guarantee multi-page data.
        streams_to_exclude = {
            'apps',
            'roles',
            'app_tables',
            'tables',
            'table_relationships',
            'events',
            'reports',
        }
        # Dynamic streams (data_connector_management__*) have 8 records each.
        # With page_size=5 they produce 2 pages, so they are included.
        return {
            name for name in self.expected_stream_names()
            if name not in streams_to_exclude
        }

    def get_properties(self, original: bool = True):
        """Configuration with reduced page_size to test pagination logic."""
        return {
            "start_date": self.start_date,
            "page_size": 5
        }

    def expected_page_size(self, stream):
        """Return the configured page_size used for all streams in this test."""
        return 5
