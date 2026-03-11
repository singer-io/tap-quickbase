"""Sync orchestration for selected streams.

Both static streams (registered in :data:`~tap_quickbase.streams.STREAMS`)
and dynamic app-table streams are handled here.  A stream is considered
*dynamic* when its catalog entry carries the ``tap-quickbase.is_dynamic``
metadata flag written by :mod:`tap_quickbase.dynamic_schema`.
"""

from typing import Dict

import singer
from singer import metadata
from tap_quickbase.streams import STREAMS
from tap_quickbase.client import Client
from tap_quickbase.streams.dynamic import DynamicTableStream

LOGGER = singer.get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_dynamic_stream(catalog_entry: singer.catalog.CatalogEntry) -> bool:
    """Return *True* if the catalog entry represents a dynamic table stream."""
    mdata_map = metadata.to_map(catalog_entry.metadata)
    return bool(mdata_map.get((), {}).get("tap-quickbase.is_dynamic"))


def _build_stream(stream_name: str, client: Client,
                  catalog: singer.Catalog) -> object:
    """Instantiate the correct stream class for *stream_name*.

    Static streams are looked up in the ``STREAMS`` registry.  For streams
    not in the registry (i.e. dynamic QB app-table streams) a
    :class:`~tap_quickbase.streams.dynamic.DynamicTableStream` is created.
    """
    catalog_entry = catalog.get_stream(stream_name)

    # Dynamic catalog entries carry the is_dynamic metadata flag.
    # Check this first so we never look up dynamic streams in STREAMS.
    if _is_dynamic_stream(catalog_entry):
        return DynamicTableStream(client, catalog_entry)

    # Try the static registry.  Use try/except so this works even when
    # STREAMS is replaced by a MagicMock in unit tests (which only set up
    # __getitem__, not __contains__).
    try:
        stream_cls = STREAMS[stream_name]
        return stream_cls(client, catalog_entry)
    except (KeyError, TypeError):
        pass

    # Unknown stream → treat as dynamic
    return DynamicTableStream(client, catalog_entry)


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schemas_recursive(stream) -> None:
    """
    Write schema for stream and all its children recursively
    """
    if stream.is_selected():
        stream.write_schema()

    for child in stream.child_to_sync:
        write_schemas_recursive(child)


def setup_children(stream, client, streams_to_sync, catalog) -> None:
    """Setup children for static streams that are selected to sync (recursively).

    Dynamic streams have no children so this is a no-op for them.
    """
    for child in stream.children:
        if child in streams_to_sync:
            child_obj = _build_stream(child, client, catalog)
            stream.child_to_sync.append(child_obj)
            setup_children(child_obj, client, streams_to_sync, catalog)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:  # pylint: disable=unused-argument
    """Sync selected streams from catalog.

    Static and dynamic streams are treated uniformly:
    - Dynamic streams are identified via catalog metadata.
    - Both honour bookmarks, parent-child relationships (where applicable),
      and incremental/full-table replication.
    """
    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    with singer.Transformer() as transformer:
        index = 0
        while index < len(streams_to_sync):
            stream_name = streams_to_sync[index]
            index += 1

            stream = _build_stream(stream_name, client, catalog)

            # Static streams may have a parent; dynamic streams do not
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            # Setup children relationships (no-op for dynamic streams)
            setup_children(stream, client, streams_to_sync, catalog)

            # Write all schemas (parent and children) at the start
            write_schemas_recursive(stream)

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: %s, total_records: %s", stream_name, total_records
            )

            # Write final state after stream completes
            singer.write_state(state)
