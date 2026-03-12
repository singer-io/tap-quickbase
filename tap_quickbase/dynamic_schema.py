"""
Dynamic schema generation for Quickbase application table streams.

This module discovers QB app tables via the REST API and generates
Singer-compatible schemas and metadata for each table. The approach
mirrors the old v2.0.2 XML-API implementation, adapted for the modern
Quickbase REST API (v1).

Discovery flow:
  1. GET /v1/tables?appId={appId}  → list of table objects
  2. GET /v1/fields?tableId={id}   → list of field objects per table
  3. Build JSON schema from field types
  4. Build Singer metadata (key-properties, replication-keys, etc.)
"""

import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import singer
from singer import metadata

LOGGER = singer.get_logger()

# ---------------------------------------------------------------------------
# Field-name sanitization (ported from old qbconn.py)
# ---------------------------------------------------------------------------
_SEP = re.compile(r"[-\s]")
_NON_ALNUM = re.compile(r"[^a-zA-Z0-9_]")
_CONSEC_UNDER = re.compile(r"_+")
_STREAM_NON_ALNUM = re.compile(r"[^0-9a-z_]+")

# ---------------------------------------------------------------------------
# QB field-type → JSON-schema mapping
# ---------------------------------------------------------------------------
# (json_types, json_format_or_None)
_FIELD_TYPE_MAP: Dict[str, Tuple[List[str], Optional[str]]] = {
    "checkbox":   (["null", "boolean"], None),
    "numeric":    (["null", "number"], None),
    "rating":     (["null", "number"], None),
    "currency":   (["null", "number"], None),
    "percent":    (["null", "number"], None),
    "duration":   (["null", "number"], None),
    "timestamp":  (["null", "string"], "date-time"),
    "date":       (["null", "string"], "date-time"),
    "datetime":   (["null", "string"], "date-time"),
    "timeofday":  (["null", "integer"], None),
    "recordid":   (["null", "integer"], None),
    "user":       (["null", "string"], None),
    "multiuser":  (["null", "string"], None),
    "email":      (["null", "string"], None),
    "url":        (["null", "string"], None),
    "dblink":     (["null", "string"], None),
    "file":       (["null", "string"], None),
    "text":       (["null", "string"], None),
    "richtext":   (["null", "string"], None),
    "multitext":  (["null", "string"], None),
    "predecessor": (["null", "string"], None),
    "lookup":     (["null", "string"], None),
    "summary":    (["null", "string"], None),
    "formula":    (["null", "string"], None),
}

# QB always assigns field ID 2 = "Date Modified" and field ID 3 = "Record ID#"
DATE_MODIFIED_FIELD_ID = 2
RECORD_ID_FIELD_ID = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_field_name(name: str) -> str:
    """Convert a QB field label to a snake_case identifier.

    Mirrors the transformation in the old qbconn.py so stream names remain
    predictable and round-trippable.
    """
    result = name.lower()
    result = _SEP.sub("_", result)           # spaces / hyphens → _
    result = _NON_ALNUM.sub("", result)      # drop everything non-alphanumeric
    result = _CONSEC_UNDER.sub("_", result)  # collapse "__" → "_"
    result = result.strip("_")
    return result or "unknown"


def sanitize_stream_name(app_name: str, table_name: str) -> str:
    """Build a canonical stream name from the app and table names.

    Format: ``<app_name>__<table_name>`` (lower-cased; non-alphanumeric
    characters replaced with ``_``).
    """
    raw = f"{app_name}__{table_name}".lower()
    return _STREAM_NON_ALNUM.sub("_", raw)


def field_type_to_json_schema(field_type: str) -> Dict[str, Any]:
    """Convert a QB ``fieldType`` string to a JSON schema property dict."""
    json_type, json_format = _FIELD_TYPE_MAP.get(field_type, (["null", "string"], None))
    prop: Dict[str, Any] = {"type": json_type}
    if json_format:
        prop["format"] = json_format
    return prop


# ---------------------------------------------------------------------------
# Schema builder
# ---------------------------------------------------------------------------

def build_schema_from_fields(
    fields: List[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, str], Optional[str], List[str]]:
    """Build a JSON schema dict and various metadata from QB field objects.

    Args:
        fields: List of field dicts returned by ``GET /v1/fields?tableId=…``.

    Returns:
        A 4-tuple of:
        - ``schema``           – JSON schema dict (type: object, properties: …)
        - ``field_id_to_name`` – mapping ``{str(field_id): field_name}``
        - ``replication_key``  – the sanitized name of field-ID 2, or ``None``
        - ``key_properties``   – list with the sanitized name of field-ID 3 (or
                                  the first field as a fallback)
    """
    # First pass: count occurrences of each sanitized name to detect duplicates.
    # Counter is O(n) single-pass, equivalent to the manual dict accumulation.
    name_counts: Counter = Counter(
        sanitize_field_name(f.get("label", f"field_{f.get('id', 0)}")) for f in fields
    )

    properties: Dict[str, Any] = {}
    field_id_to_name: Dict[str, str] = {}
    replication_key: Optional[str] = None
    key_properties: List[str] = []

    for field in fields:
        fid = str(field.get("id", ""))
        if not fid:
            continue

        raw_name = sanitize_field_name(field.get("label", f"field_{fid}"))

        # Disambiguate duplicate labels by appending the field ID
        if name_counts.get(raw_name, 0) > 1:
            field_name = f"{raw_name}_{fid}"
        else:
            field_name = raw_name

        field_id_to_name[fid] = field_name
        properties[field_name] = field_type_to_json_schema(field.get("fieldType", "text"))

        if int(fid) == DATE_MODIFIED_FIELD_ID:
            replication_key = field_name
        if int(fid) == RECORD_ID_FIELD_ID:
            key_properties = [field_name]

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }

    # Fallback: use the first field as key if record-ID field not present
    if not key_properties and properties:
        key_properties = [next(iter(properties))]

    return schema, field_id_to_name, replication_key, key_properties


# ---------------------------------------------------------------------------
# Metadata builder
# ---------------------------------------------------------------------------

def build_metadata_for_dynamic_stream(
    schema: Dict[str, Any],
    key_properties: List[str],
    replication_key: Optional[str],
    table_id: str,
    field_id_to_name: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Build a Singer metadata list for a dynamic stream.

    In addition to standard Singer metadata, we persist QB-specific
    information that the sync code needs to reconstruct the stream:

    - ``tap-quickbase.table_id``    – QB table ID used in records/query
    - ``tap-quickbase.field_id_map`` – ``{str(field_id): field_name}`` mapping
    - ``tap-quickbase.is_dynamic``  – sentinel flag (``True``)
    """
    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema,
        key_properties=key_properties,
        valid_replication_keys=[replication_key] if replication_key else [],
        replication_method="INCREMENTAL" if replication_key else "FULL_TABLE",
    )
    mdata = metadata.to_map(mdata)

    # Mark replication key as automatic (must always be included)
    if replication_key and replication_key in schema.get("properties", {}):
        mdata = metadata.write(
            mdata, ("properties", replication_key), "inclusion", "automatic"
        )

    # Store QB-specific metadata for use during sync
    mdata = metadata.write(mdata, (), "tap-quickbase.table_id", table_id)
    mdata = metadata.write(mdata, (), "tap-quickbase.field_id_map", field_id_to_name)
    mdata = metadata.write(mdata, (), "tap-quickbase.is_dynamic", True)

    return metadata.to_list(mdata)


# ---------------------------------------------------------------------------
# Per-table processing helper
# ---------------------------------------------------------------------------

def _process_table(
    client: Any, table: Dict[str, Any], app_name: str
) -> Optional[Tuple[str, Any, Any]]:
    """Fetch fields for one QB table and return ``(stream_name, schema, mdata)``.

    Returns ``None`` when the table should be skipped (missing id, API error,
    or no fields).

    Args:
        app_name: Human-readable app name (already fetched); used as the stream
            name prefix (e.g. ``data_connector_management__connectors``).
    """
    table_id = table.get("id")
    table_name = table.get("name", table_id)

    if not table_id:
        LOGGER.warning("dynamic_schema: table entry has no 'id', skipping: %s", table)
        return None

    stream_name = sanitize_stream_name(app_name, table_name)

    try:
        fields_response = client.make_request(
            "GET",
            f"{client.base_url}/v1/fields",
            params={"tableId": table_id},
        )
        fields = fields_response if isinstance(fields_response, list) else []
    except Exception as err:  # pylint: disable=broad-except
        LOGGER.warning(
            "dynamic_schema: failed to get fields for table '%s' (%s) – %s",
            table_name, table_id, err,
        )
        return None

    if not fields:
        LOGGER.warning(
            "dynamic_schema: no fields returned for table '%s' (%s), skipping",
            table_name, table_id,
        )
        return None

    schema, field_id_to_name, replication_key, key_properties = (
        build_schema_from_fields(fields)
    )
    mdata = build_metadata_for_dynamic_stream(
        schema=schema,
        key_properties=key_properties,
        replication_key=replication_key,
        table_id=table_id,
        field_id_to_name=field_id_to_name,
    )

    LOGGER.info(
        "dynamic_schema: stream='%s' table_id='%s' fields=%s replication_key='%s'",
        stream_name, table_id, len(fields), replication_key,
    )
    return stream_name, schema, mdata


# ---------------------------------------------------------------------------
# Main discovery entry point
# ---------------------------------------------------------------------------

def discover_dynamic_streams(client: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Discover dynamic app-table streams via the Quickbase REST API.

    For each table in the configured QB application, queries the API for its
    field list, generates a JSON schema, and builds Singer metadata.

    Args:
        client: An instance of :class:`tap_quickbase.client.Client`.

    Returns:
        A 2-tuple of ``(schemas, field_metadata)`` in the same format as
        :func:`tap_quickbase.schema.get_schemas`.
    """
    schemas: Dict[str, Any] = {}
    field_metadata: Dict[str, Any] = {}

    app_id = client.config.get("qb_appid")
    if not app_id:
        LOGGER.warning(
            "dynamic_schema: no 'qb_appid' in config – skipping dynamic discovery"
        )
        return schemas, field_metadata

    # ------------------------------------------------------------------
    # 1. List tables for this app
    # ------------------------------------------------------------------
    try:
        tables_response = client.make_request(
            "GET",
            f"{client.base_url}/v1/tables",
            params={"appId": app_id},
        )
        tables = tables_response if isinstance(tables_response, list) else []
    except Exception as err:  # pylint: disable=broad-except
        LOGGER.warning("dynamic_schema: failed to list tables – %s", err)
        return schemas, field_metadata

    LOGGER.info(
        "dynamic_schema: discovered %s tables for app '%s'", len(tables), app_id
    )

    # ------------------------------------------------------------------
    # 2. Resolve human-readable app name for use as stream name prefix.
    #    Prefer the app's display name over the raw app ID so streams are
    #    readable, e.g. ``data_connector_management__connectors``.
    # ------------------------------------------------------------------
    app_name: str = app_id
    try:
        app_response = client.make_request(
            "GET",
            f"{client.base_url}/v1/apps/{app_id}",
        )
        app_name = (app_response or {}).get("name", "") or app_id
    except Exception as err:  # pylint: disable=broad-except
        LOGGER.warning(
            "dynamic_schema: failed to fetch app name for '%s' – %s; "
            "falling back to app ID as stream prefix",
            app_id, err,
        )

    LOGGER.info(
        "dynamic_schema: app='%s' using prefix '%s'",
        app_id, app_name,
    )

    # ------------------------------------------------------------------
    # 3. For each table, fetch fields and build stream spec.
    # ------------------------------------------------------------------
    for table in tables:
        result = _process_table(client, table, app_name)
        if result is not None:
            stream_name, schema, mdata = result
            schemas[stream_name] = schema
            field_metadata[stream_name] = mdata

    return schemas, field_metadata
