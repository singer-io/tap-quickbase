"""Dynamic stream class for Quickbase application data tables.

Each QB app table discovered at runtime is represented by a
:class:`DynamicTableStream` instance.  The stream reads all metadata it
needs (QB table ID, field-ID → field-name map, replication key, etc.) from
the Singer catalog entry written during discovery, so **no code change is
needed when tables are added or modified in Quickbase**.

Sync strategy
~~~~~~~~~~~~~
* If the table has a "Date Modified" field (QB field ID 2):
  – Replication method is **INCREMENTAL**.
  – The QB ``records/query`` endpoint receives a ``where`` clause that
    limits results to records modified after the saved bookmark.
  – At the end of each sync run the bookmark is advanced to the maximum
    "date_modified" value seen.
* Otherwise:
  – Replication method is **FULL_TABLE** (every record every run).

Record extraction
~~~~~~~~~~~~~~~~~
The QB ``POST /v1/records/query`` response has the shape::

    {
        "data": [
            {"3": {"value": 1}, "6": {"value": "2023-01-01T00:00:00Z"}, ...},
            ...
        ],
        "fields": [...],
        "metadata": {"totalRecords": 42, "numRecords": 10, "skip": 0, ...}
    }

Field IDs are string keys in each record dict; values are ``{"value": <v>}``
objects (or plain scalars in some edge cases).  The field-ID → name mapping
stored in catalog metadata is used to convert each record to a name-keyed
dict before transformation and emission.
"""

import calendar
import json
from typing import Any, Dict, Iterator, List, Optional

import singer.utils as singer_utils
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_state,
)

from tap_quickbase.streams.abstracts import (
    BaseStream,
    DEFAULT_PAGE_SIZE,
    MAX_PAGINATION_ITERATIONS,
)

LOGGER = get_logger()


class DynamicTableStream(BaseStream):  # pylint: disable=too-many-instance-attributes
    """Singer stream backed by a Quickbase application data table.

    The class deliberately avoids hard-coded class-level attributes for
    stream identity (``tap_stream_id``, ``key_properties``, etc.) because
    these differ per table.  Instead, they are populated from the catalog
    entry in :meth:`__init__`.
    """

    # --- abstract overrides (set in __init__) ---
    tap_stream_id: str = ""
    key_properties: List[str] = []
    replication_method: str = "FULL_TABLE"
    replication_keys: List[str] = []

    # --- QB-specific ---
    http_method: str = "POST"
    data_key: str = "data"
    children: List = []
    parent: str = ""

    # ------------------------------------------------------------------ #
    # Construction                                                         #
    # ------------------------------------------------------------------ #

    def __init__(self, client: Any, catalog_entry: Any) -> None:
        super().__init__(client, catalog_entry)

        mdata_map = self.metadata

        # ---- stream identity (read back from catalog metadata) ----
        self.tap_stream_id = catalog_entry.tap_stream_id

        self.key_properties = (
            metadata.get(mdata_map, (), "table-key-properties") or []
        )

        valid_repl_keys = metadata.get(mdata_map, (), "valid-replication-keys") or []
        self.replication_keys = valid_repl_keys

        forced_method = metadata.get(mdata_map, (), "forced-replication-method")
        self.replication_method = forced_method or (
            "INCREMENTAL" if valid_repl_keys else "FULL_TABLE"
        )

        # ---- QB-specific metadata ----
        self.table_id: str = (
            metadata.get(mdata_map, (), "tap-quickbase.table_id") or ""
        )
        self.field_id_to_name: Dict[str, str] = (
            metadata.get(mdata_map, (), "tap-quickbase.field_id_map") or {}
        )
        self.name_to_field_id: Dict[str, str] = {
            v: k for k, v in self.field_id_to_name.items()
        }

        # Determine which field IDs are selected (all properties in schema)
        schema_props = self.schema.get("properties", {})
        self.selected_field_ids: List[int] = [
            int(fid)
            for fname, fid in self.name_to_field_id.items()
            if fname in schema_props
        ]

        # Date-modified field ID (QB field 2) – used in where clause
        repl_key_name = self.replication_keys[0] if self.replication_keys else None
        self.date_modified_field_id: Optional[int] = None
        if repl_key_name:
            fid_str = self.name_to_field_id.get(repl_key_name)
            if fid_str:
                self.date_modified_field_id = int(fid_str)

        # Will be set by sync() before get_records() is called
        self._bookmark_epoch_ms: Optional[int] = None

    # ------------------------------------------------------------------ #
    # Bookmark helpers                                                     #
    # ------------------------------------------------------------------ #

    @property
    def _replication_key(self) -> Optional[str]:
        return self.replication_keys[0] if self.replication_keys else None

    def _start_date(self) -> str:
        return self.client.config.get("start_date", "1970-01-01T00:00:00.000000Z")

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> str:
        """Return the current bookmark (or start_date) for this stream."""
        repl_key = key or self._replication_key or "date_modified"
        return get_bookmark(state, stream, repl_key, self._start_date())

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """Advance the bookmark if *value* is greater than the current value."""
        if not value:
            return state
        repl_key = key or self._replication_key or "date_modified"
        current = get_bookmark(state, stream, repl_key, self._start_date())
        value = max(current, value) if current else value
        return write_bookmark(state, stream, repl_key, value)

    # ------------------------------------------------------------------ #
    # Record fetching                                                      #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _to_epoch_ms(dt_str: str) -> int:
        """Convert an ISO-8601 datetime string to epoch milliseconds."""
        try:
            dt = singer_utils.strptime_to_utc(dt_str)
            return int(calendar.timegm(dt.timetuple()) * 1000)
        except Exception:  # pylint: disable=broad-except
            return 0

    def _build_query_body(self, skip: int, page_size: int) -> Dict[str, Any]:
        """Assemble the POST /v1/records/query request body."""
        body: Dict[str, Any] = {
            "from": self.table_id,
            "select": self.selected_field_ids,
            "options": {"skip": skip, "top": page_size},
        }

        if self.date_modified_field_id and self._bookmark_epoch_ms is not None:
            # QB query language: {fieldId.AF.'epoch_ms'} means "after epoch_ms"
            body["where"] = (
                f"{{{self.date_modified_field_id}.AF.'{self._bookmark_epoch_ms}'}}"
            )
            body["sortBy"] = [
                {"fieldId": self.date_modified_field_id, "order": "ASC"}
            ]

        return body

    def _convert_raw_record(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a QB record (field-ID keyed) to a field-name keyed dict."""
        record: Dict[str, Any] = {}
        for fid_key, fval in raw.items():
            fname = self.field_id_to_name.get(str(fid_key), f"field_{fid_key}")
            # QB returns {"value": <v>}; fall back to bare scalar just in case
            record[fname] = fval.get("value") if isinstance(fval, dict) else fval
        return record

    def get_records(self) -> Iterator[Dict[str, Any]]:
        """Page through records from ``POST /v1/records/query``."""
        page_size = int(self.client.config.get("page_size", DEFAULT_PAGE_SIZE))
        skip = 0
        iterations = 0
        endpoint = f"{self.client.base_url}/v1/records/query"

        while iterations < MAX_PAGINATION_ITERATIONS:
            iterations += 1
            body = self._build_query_body(skip, page_size)

            try:
                response = self.client.make_request(
                    "POST",
                    endpoint,
                    body=json.dumps(body),
                )
            except Exception as err:  # pylint: disable=broad-except
                LOGGER.error(
                    "DynamicTableStream '%s': error fetching records (skip=%s) – %s",
                    self.tap_stream_id, skip, err
                )
                break

            raw_records: List[Dict] = (
                response.get("data", []) if isinstance(response, dict) else []
            )
            num_records = len(raw_records)

            if num_records == 0:
                LOGGER.info(
                    "DynamicTableStream '%s': no more records after %s iterations",
                    self.tap_stream_id, iterations
                )
                break

            for raw in raw_records:
                yield self._convert_raw_record(raw)

            # Use metadata total if available
            resp_meta = response.get("metadata", {}) if isinstance(response, dict) else {}
            total = resp_meta.get("totalRecords")
            skip += num_records

            if num_records < page_size:
                break
            if total is not None and skip >= total:
                LOGGER.info(
                    "DynamicTableStream '%s': reached totalRecords=%s",
                    self.tap_stream_id, total
                )
                break

        if iterations >= MAX_PAGINATION_ITERATIONS:
            LOGGER.error(
                "DynamicTableStream '%s': hit MAX_PAGINATION_ITERATIONS=%s – possible infinite loop",
                self.tap_stream_id, MAX_PAGINATION_ITERATIONS
            )

    # ------------------------------------------------------------------ #
    # Sync                                                                 #
    # ------------------------------------------------------------------ #

    def sync(
        self,
        state: Dict[str, Any],
        transformer: Transformer,
        parent_obj: Dict[str, Any] = None,  # noqa: ARG002
    ) -> int:
        """Sync records for this dynamic table.

        Returns:
            Total number of records emitted.
        """
        repl_key = self._replication_key
        is_incremental = bool(repl_key and self.date_modified_field_id)

        # Determine starting bookmark
        if is_incremental:
            bookmark_str = self.get_bookmark(state, self.tap_stream_id)
            self._bookmark_epoch_ms = self._to_epoch_ms(bookmark_str)
        else:
            bookmark_str = None
            self._bookmark_epoch_ms = None

        current_max_bookmark: Optional[str] = bookmark_str

        LOGGER.info(
            "DynamicTableStream '%s': starting sync | incremental=%s bookmark='%s'",
            self.tap_stream_id, is_incremental, bookmark_str
        )

        # NOTE: singer Counter._pop() resets .value to 0 on __exit__, so the
        # bookmark writing and the return statement must both be INSIDE the
        # `with` block to read the correct count before the reset happens.
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record)

                transformed = transformer.transform(
                    record, self.schema, self.metadata
                )

                # For incremental, skip records older than bookmark (belt-and-
                # braces: the QB where clause should already handle this, but
                # apply tap-side filter as well for safety)
                if is_incremental and bookmark_str:
                    record_ts = transformed.get(repl_key)
                    if record_ts and record_ts < bookmark_str:
                        continue

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

                # Track maximum bookmark value seen
                if repl_key:
                    record_ts = transformed.get(repl_key)
                    if record_ts:
                        if current_max_bookmark is None or record_ts > current_max_bookmark:
                            current_max_bookmark = record_ts

                # Dynamic tables have no children, but call the hook for
                # completeness / future extensibility
                self.sync_child_streams(state, transformer, record)

            # Write final bookmark (inside with block – before counter resets)
            if repl_key and current_max_bookmark and current_max_bookmark != bookmark_str:
                state = self.write_bookmark(
                    state, self.tap_stream_id, value=current_max_bookmark
                )
                write_state(state)
                LOGGER.info(
                    "DynamicTableStream '%s': bookmark advanced to '%s' | records=%s",
                    self.tap_stream_id, current_max_bookmark, counter.value
                )
            else:
                LOGGER.info(
                    "DynamicTableStream '%s': sync complete | records=%s bookmark unchanged",
                    self.tap_stream_id, counter.value
                )

            return counter.value
