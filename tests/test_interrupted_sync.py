from base import QuickbaseBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class QuickbaseInterruptedSyncTest(InterruptedSyncTest, QuickbaseBaseTest):
    """Test tap resumes from an interrupted sync for dynamic INCREMENTAL streams.

    Simulates an interruption mid-way through the six ``data_connector_management__*`` app-table
    streams: one stream is marked as ``currently_syncing`` (interrupted) while
    the others have bookmarks set at the tap ``start_date``.  The resuming sync
    must:
    - Process the interrupted stream first (``currently_syncing`` ordering).
    - Produce a final state identical to an uninterrupted first sync.
    """

    @staticmethod
    def name():
        return "tap_tester_quickbase_interrupted_sync_test"

    def streams_to_test(self):
        # Target only the six dynamic INCREMENTAL app-table streams.
        return {name for name in self.expected_stream_names()
                if name.startswith("data_connector_management")}

    def manipulate_state(self):
        """Return state with one dynamic stream marked as currently_syncing.

        ``data_connector_management__connectors`` is simulated as the interrupted stream.
        It appears in both ``currently_syncing`` AND ``bookmarks`` (with a
        partial/stale bookmark) to model a stream that was *in progress* when
        the sync was interrupted.  All six streams have bookmarks set at
        ``start_date`` so the resuming sync re-reads them from the beginning
        (same record set as the first sync).
        """
        return {
            "currently_syncing": "data_connector_management__connectors",
            "bookmarks": {
                "data_connector_management__connectors": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
                "data_connector_management__connector_versions": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
                "data_connector_management__connector_logs": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
                "data_connector_management__connector_performance": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
                "data_connector_management__connector_performance_alerts": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
                "data_connector_management__connector_log_attachments": {
                    "date_modified": "2025-12-01T00:00:00.000000Z"
                },
            },
        }
