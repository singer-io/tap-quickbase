"""Abstract stream base classes and shared sync/pagination helpers."""

from abc import ABC, abstractmethod
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, List, Iterator
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata,
    set_currently_syncing,
    write_state
)
import singer.utils as singer_utils

LOGGER = get_logger()
DEFAULT_PAGE_SIZE = 100


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    children = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    http_method = "GET"

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}
        self.data_payload = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        """Return whether the stream is selected in catalog metadata."""
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def _create_page_signature(self, raw_records: List) -> Any:
        """Create a signature for a page of records to detect duplicates."""
        if not raw_records:
            return None

        try:
            # Try to extract IDs from records for signature
            first_id = raw_records[0].get('id') or raw_records[0].get('field', {}).get('id')
            last_id = raw_records[-1].get('id') or raw_records[-1].get('field', {}).get('id')
            if first_id is not None and last_id is not None:
                return (first_id, last_id, len(raw_records))
            # Fall back to hash if IDs not available
            return hash(json.dumps(raw_records, sort_keys=True))
        except (KeyError, TypeError, ValueError):
            # If any error, use hash
            return hash(json.dumps(raw_records, sort_keys=True))

    def _check_duplicate_page(self, page_signature: Any, last_signature: Any,
                             dup_count: int) -> Tuple[bool, int]:
        """Check if current page is a duplicate of the last page.

        Returns:
            Tuple of (should_break, new_dup_count)
        """
        if page_signature == last_signature:
            new_dup_count = dup_count + 1
            if new_dup_count >= 2:
                LOGGER.warning(
                    "Stream %s: API returned identical page %s times. "
                    "API may not support pagination. Stopping to prevent duplicates.",
                    self.tap_stream_id,
                    new_dup_count + 1
                )
                return True, new_dup_count
            return False, new_dup_count
        return False, 0

    def _process_records_with_dedup(self, raw_records: List,
                                   seen_ids: set) -> Iterator:
        """Process records and skip duplicates.

        Yields:
            Non-duplicate records
        """
        for record in raw_records:
            try:
                # Try to extract a unique identifier
                record_id = record.get('id') or record.get('field', {}).get('id')
                if record_id is not None:
                    record_key = (self.tap_stream_id, record_id)
                    if record_key in seen_ids:
                        continue
                    seen_ids.add(record_key)
            except (KeyError, TypeError, AttributeError):
                pass

            yield record

    def _should_continue_pagination(self, num_records: int, page_size: int,
                                   response: Any, skip: int) -> Tuple[bool, int]:
        """Determine if pagination should continue.

        Returns:
            Tuple of (should_continue, new_skip)
        """
        # If API returns fewer records than requested, it's the last page
        if num_records < page_size:
            LOGGER.info(
                "Stream %s: Received %s records (less than page_size=%s). Last page.",
                self.tap_stream_id,
                num_records,
                page_size
            )
            return False, skip

        # If metadata available, use it to determine if more pages exist
        if isinstance(response, dict) and "metadata" in response:
            total_records = response["metadata"].get("totalRecords", 0)
            new_skip = skip + num_records
            if new_skip >= total_records:
                LOGGER.info(
                    "Stream %s: Reached total_records=%s from metadata",
                    self.tap_stream_id,
                    total_records
                )
                return False, new_skip
            return True, new_skip

        # No metadata - increment skip and continue
        return True, skip + num_records

    def get_records(self) -> Iterator:
        """Interacts with api client interaction and pagination."""
        # Initialize pagination state
        state = {
            'page_size': self.client.config.get("page_size", DEFAULT_PAGE_SIZE),
            'skip': 0,
            'iteration': 0,
            'seen_ids': set(),
            'dup_pages': 0,
            'last_sig': None
        }
        max_iterations = 10000  # Safety limit to prevent infinite loops

        while state['iteration'] < max_iterations:
            state['iteration'] += 1

            # Add pagination params for this request
            pagination_params = self.params.copy()
            pagination_params.update({"skip": state['skip'], "top": state['page_size']})

            response = self.client.make_request(
                self.http_method,
                self.url_endpoint,
                pagination_params,
                self.headers,
                body=json.dumps(self.data_payload) if self.data_payload else None,
                path=self.path
            )

            # Extract and yield records
            raw_records = self._extract_records(response)
            num_records = len(raw_records)

            if num_records == 0:
                LOGGER.info(
                    "No more records for stream %s after %s iterations",
                    self.tap_stream_id,
                    state['iteration']
                )
                break

            # Check for duplicate pages
            page_sig = self._create_page_signature(raw_records)
            should_break, state['dup_pages'] = self._check_duplicate_page(
                page_sig, state['last_sig'], state['dup_pages']
            )
            if should_break:
                break
            state['last_sig'] = page_sig

            # Process records and skip duplicates
            new_count = 0
            for record in self._process_records_with_dedup(raw_records, state['seen_ids']):
                new_count += 1
                yield record

            # If we yielded no new records, we've seen everything
            if new_count == 0:
                LOGGER.info(
                    "Stream %s: All %s records were duplicates. Pagination complete.",
                    self.tap_stream_id,
                    num_records
                )
                break

            # Determine if we should continue pagination
            should_continue, state['skip'] = self._should_continue_pagination(
                num_records, state['page_size'], response, state['skip']
            )
            if not should_continue:
                break

        # Log if we hit max iterations
        if state['iteration'] >= max_iterations:
            LOGGER.error(
                "Stream %s: Hit maximum iteration limit of %s. "
                "This indicates a serious pagination bug.",
                self.tap_stream_id,
                max_iterations
            )

    def _extract_records(self, response) -> List:
        """Extract records from API response."""
        if self.data_key:
            return response.get(self.data_key, [])
        if isinstance(response, list):
            return response
        return [response] if response else []

    def write_schema(self) -> None:
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error while writing schema for: %s", self.tap_stream_id)
            raise err

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)

    def update_data_payload(self, **kwargs) -> None:
        """
        Update JSON body for the stream
        """
        self.data_payload.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:  # pylint: disable=unused-argument
        """
        Modify the record before writing to the stream
        """
        return record

    @staticmethod
    def flatten_field_usage_record(record: Dict) -> Dict:
        """Helper to flatten field and usage objects with field.id as primary key."""
        if not record:
            return record
        field = record.get('field', {})
        usage = record.get('usage', {})
        return {'id': field.get('id'), **usage}

    def sync_child_streams(self, state: Dict, transformer: Transformer, record: Dict) -> None:
        """Write schema and sync child streams."""
        from tap_quickbase.exceptions import QuickbaseForbiddenError
        
        for child in self.child_to_sync:
            if child.is_selected():
                child.write_schema()
            set_currently_syncing(state, child.tap_stream_id)
            write_state(state)
            try:
                child.sync(state=state, transformer=transformer, parent_obj=record)
            except QuickbaseForbiddenError as err:
                # Log warning and continue if child resource is forbidden
                LOGGER.warning(
                    f"Skipping {child.tap_stream_id} for parent record {record.get('id')}: "
                    f"{err.message}"
                )
                continue

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Get the URL endpoint for the stream, handling path parameters.
        """
        if parent_obj is None:
            return f"{self.client.base_url}/{self.path}"

        formatted_path = self.path
        replacements = {
            '{appId}': str(self.client.config.get('app_id', parent_obj.get('id', ''))),
            '{tableId}': self._get_table_id(parent_obj),
            '{fieldId}': str(parent_obj.get('id', '')),
            '{reportId}': str(parent_obj.get('id', ''))
        }

        for placeholder, value in replacements.items():
            if placeholder in formatted_path:
                formatted_path = formatted_path.replace(placeholder, value)

        return f"{self.client.base_url}/{formatted_path}"

    def _get_table_id(self, parent_obj: Dict) -> str:
        """Extract tableId from parent object, handling nested query structure."""
        # First check for tableId directly on the record (fields, fields_usage)
        if 'tableId' in parent_obj:
            return str(parent_obj['tableId'])
        # Then check for tableId in nested query structure (table_reports)
        if 'query' in parent_obj and 'tableId' in parent_obj.get('query', {}):
            return str(parent_obj['query']['tableId'])
        # Fall back to id as last resort
        return str(parent_obj.get('id', ''))


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""


    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, key or self.replication_keys[0], value
        )


    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                record_bookmark = transformed_record[self.replication_keys[0]]
                if record_bookmark >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_bookmark
                    )

                    self.sync_child_streams(state, transformer, record)

            state = self.write_bookmark(
                state, self.tap_stream_id, value=current_max_bookmark_date
            )
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                self.sync_child_streams(state, transformer, record)

            return counter.value


class PseudoIncrementalStream(BaseStream):
    """
    Base Class for Pseudo-Incremental Stream.
    
    This class implements incremental behavior using tap-side filtering only.
    The API is still called as FULL_TABLE, but we filter records based on
    the 'updated' field and maintain bookmarks.
    
    This approach is used when:
    - The API does NOT support server-side filtering by updated timestamp
    - We want to reduce duplicate data in targets
    - We still need to comply with Singer spec (no replication_key declaration)
    """

    replication_keys = []
    bookmark_field = "updated"  # Field to use for bookmarking

    def _parse_datetime(self, dt_str: str) -> datetime:
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        try:
            return singer_utils.strptime_to_utc(dt_str)
        except ValueError as e:
            LOGGER.warning("Failed to parse datetime '%s': %s", dt_str, e)
            return None

    def _get_bookmark(self, state: Dict) -> datetime:
        """
        Get the last bookmark for this stream.
        Subtracts 1 second to ensure we don't miss records with the same timestamp.
        """
        bookmark_str = get_bookmark(state, self.tap_stream_id, self.bookmark_field)

        if not bookmark_str:
            # No bookmark, use start_date from config or epoch
            start_date = self.client.config.get("start_date", "1970-01-01T00:00:00Z")
            bookmark_dt = self._parse_datetime(start_date)
        else:
            bookmark_dt = self._parse_datetime(bookmark_str)

        # Subtract 1 second to avoid missing records with exact same timestamp
        if bookmark_dt:
            bookmark_dt = bookmark_dt - timedelta(seconds=1)

        return bookmark_dt

    def _write_bookmark(self, state: Dict, value: str) -> Dict:
        """Write bookmark for this stream."""
        if not value:
            return state

        current_bookmark = get_bookmark(state, self.tap_stream_id, self.bookmark_field)

        # Only update if new value is greater
        if current_bookmark:
            current_dt = self._parse_datetime(current_bookmark)
            new_dt = self._parse_datetime(value)
            if new_dt and current_dt and new_dt <= current_dt:
                return state

        state = write_bookmark(state, self.tap_stream_id, self.bookmark_field, value)
        write_state(state)
        return state

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Sync implementation with tap-side filtering.
        
        This method:
        1. Fetches ALL records from API (FULL_TABLE)
        2. Filters records where updated > last_bookmark (tap-side)
        3. Emits only filtered records
        4. Tracks max updated value seen
        5. Writes bookmark at end
        """
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        # Get last bookmark
        last_bookmark_dt = self._get_bookmark(state)
        max_updated_str = None
        max_updated_dt = last_bookmark_dt

        LOGGER.info(
            "Starting pseudo-incremental sync for %s. Last bookmark: %s",
            self.tap_stream_id,
            last_bookmark_dt.isoformat() if last_bookmark_dt else 'None'
        )

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)

                # Get updated field from record
                record_updated_str = record.get(self.bookmark_field)
                if not record_updated_str:
                    # No updated field, skip this record (or emit it?)
                    # For safety, we'll skip records without updated field
                    LOGGER.warning(
                        "Record in %s missing '%s' field, skipping",
                        self.tap_stream_id,
                        self.bookmark_field
                    )
                    continue

                record_updated_dt = self._parse_datetime(record_updated_str)
                if not record_updated_dt:
                    continue

                # Filter: only process records updated after bookmark
                if last_bookmark_dt and record_updated_dt <= last_bookmark_dt:
                    continue

                # Transform and emit record
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                # Track max updated
                if not max_updated_dt or record_updated_dt > max_updated_dt:
                    max_updated_dt = record_updated_dt
                    max_updated_str = record_updated_str

                # Sync children
                self.sync_child_streams(state, transformer, record)

            # Write bookmark if we found any records
            if max_updated_str:
                state = self._write_bookmark(state, max_updated_str)
                LOGGER.info(
                    "Completed sync for %s. Records emitted: %s. New bookmark: %s",
                    self.tap_stream_id,
                    counter.value,
                    max_updated_str
                )
            else:
                LOGGER.info(
                    "Completed sync for %s. No new records found. Bookmark unchanged.",
                    self.tap_stream_id
                )

            return counter.value


class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = (
            super().get_bookmark(state, stream) if self.is_selected() else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state, child.tap_stream_id, key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )

        return min_parent_bookmark

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            super().write_bookmark(
                state, child.tap_stream_id, key=bookmark_key, value=value
            )

        return state


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream."""
    def __init__(self, client, catalog):
        super().__init__(client, catalog)
        self.bookmark_value = None  # Initialize to avoid access-before-definition

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value
