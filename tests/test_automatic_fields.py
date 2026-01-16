"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class QuickbaseAutomaticFields(MinimumSelectionTest, QuickbaseBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_quickbase_automatic_fields_test"

    def streams_to_test(self):
        # Exclude child streams with no test data or that cause test hangs
        streams_to_exclude = {
            'events',
            'get_reports',
            'get_field_usage',
            'get_fields',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

