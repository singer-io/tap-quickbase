"""JSON schema and metadata generation for tap discovery.

Two kinds of schemas are produced:

1. **Static schemas** – loaded from ``schemas/*.json`` files for well-known
   QB metadata streams (apps, tables, fields, …).

2. **Dynamic schemas** – generated at runtime by querying the QB REST API
   for the application's tables and their field definitions.  Only produced
   when a :class:`~tap_quickbase.client.Client` is supplied.
"""

import os
import json
from typing import Any, Dict, Optional, Tuple
import singer
from singer import metadata
from tap_quickbase.streams import STREAMS
from tap_quickbase.dynamic_schema import discover_dynamic_streams

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """
    Load the schema files from the schema folder and return the schema references.
    """
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(
            os.path.join(shared_schema_path, shared_schema_file), encoding="utf-8"
        ) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def get_static_schemas() -> Tuple[Dict, Dict]:
    """Load schemas and metadata for all statically-defined streams.

    Returns:
        ``(schemas, field_metadata)`` – both keyed by stream name.
    """
    schemas: Dict[str, Any] = {}
    field_metadata: Dict[str, Any] = {}

    refs = load_schema_references()
    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata = metadata.to_map(mdata)

        automatic_keys = getattr(stream_obj, "replication_keys") or []
        for field_name in schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        parent_tap_stream_id = getattr(stream_obj, "parent", None)
        if parent_tap_stream_id:
            mdata = metadata.write(
                mdata, (), "parent-tap-stream-id", parent_tap_stream_id
            )

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata


def get_schemas(client: Optional[Any] = None) -> Tuple[Dict, Dict]:
    """Return combined schemas and metadata for static *and* dynamic streams.

    When *client* is ``None`` only the static schemas are returned (backwards-
    compatible behaviour).  When a live
    :class:`~tap_quickbase.client.Client` is supplied the QB REST API is also
    queried and the resulting dynamic schemas are merged in.

    Dynamic stream entries *never* overwrite static stream entries; if a table
    name collides with an existing static stream name the dynamic entry is
    skipped with a warning.

    Args:
        client: Optional authenticated QB API client.

    Returns:
        ``(schemas, field_metadata)`` – both keyed by stream name.
    """
    schemas, field_metadata = get_static_schemas()

    if client is not None:
        try:
            dyn_schemas, dyn_metadata = discover_dynamic_streams(client)

            for stream_name, schema in dyn_schemas.items():
                if stream_name in schemas:
                    LOGGER.warning(
                        "get_schemas: dynamic stream '%s' conflicts with a static stream "
                        "– keeping the static definition",
                        stream_name,
                    )
                    continue
                schemas[stream_name] = schema
                field_metadata[stream_name] = dyn_metadata[stream_name]

            LOGGER.info(
                "get_schemas: %s static + %s dynamic streams discovered",
                len(STREAMS),
                len(dyn_schemas),
            )
        except Exception as err:  # pylint: disable=broad-except
            LOGGER.warning(
                "get_schemas: dynamic discovery failed (%s) – continuing with static streams only",
                err,
            )

    return schemas, field_metadata
