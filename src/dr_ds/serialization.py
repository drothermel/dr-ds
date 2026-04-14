"""Serialization helpers for normalizing loose values into stable wire formats."""

from __future__ import annotations

from collections.abc import Mapping
import json
from datetime import datetime, timezone
from typing import Any

import srsly

# Largest integer preserved as an integer before parquet-oriented coercion
# converts it to float to avoid downstream overflow/compatibility issues.
DEFAULT_MAX_INT = 2**31 - 1


def serialize_timestamp(value: Any) -> str | None:
    """Return `value` as a timestamp string suitable for serialized payloads.

    Datetimes are normalized to UTC ISO 8601 strings. `None` stays `None`,
    and every other value is stringified without additional validation.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def to_jsonable(value: Any, *, seen_ids: set[int] | None = None) -> Any:
    """Recursively normalize values into JSON-safe Python data.

    The conversion is intentionally opinionated:
    - mappings are copied with stringified keys
    - tuples become lists
    - sets become deterministically ordered lists
    - plain objects are serialized from public, non-callable attributes
    - recursive references are replaced with the literal string
      `"<recursion>"`

    Values that cannot be meaningfully introspected fall back to `str(value)`.
    """
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if seen_ids is None:
        seen_ids = set()
    value_id = id(value)
    if value_id in seen_ids:
        return "<recursion>"
    seen_ids.add(value_id)
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, Mapping):
            return {
                str(key): to_jsonable(nested, seen_ids=seen_ids)
                for key, nested in value.items()
            }
        if isinstance(value, list):
            return [to_jsonable(nested, seen_ids=seen_ids) for nested in value]
        if isinstance(value, tuple):
            return [to_jsonable(nested, seen_ids=seen_ids) for nested in value]
        if isinstance(value, set):
            items = [
                to_jsonable(nested, seen_ids=seen_ids) for nested in value
            ]
            try:
                return sorted(items)
            except TypeError:
                return sorted(
                    items,
                    key=lambda item: json.dumps(item, sort_keys=True),
                )
        return _object_to_jsonable(value, seen_ids=seen_ids)
    finally:
        seen_ids.discard(value_id)


def _object_to_jsonable(value: Any, *, seen_ids: set[int]) -> Any:
    """Serialize plain-object public attributes with recursion protection."""
    try:
        attributes = vars(value)
    except TypeError:
        return str(value)

    normalized_attributes = {
        str(key): (
            "<recursion>"
            if id(nested) in seen_ids
            else to_jsonable(nested, seen_ids=seen_ids)
        )
        for key, nested in attributes.items()
        if not str(key).startswith("_") and not callable(nested)
    }
    if normalized_attributes:
        return normalized_attributes
    return str(value)


def convert_large_ints(value: Any, *, max_int: int = DEFAULT_MAX_INT) -> Any:
    """Recursively convert oversized integers to floats.

    This is mainly intended for parquet/dataframe pipelines that can become
    awkward or lossy with very large integer values. Containers preserve their
    original shape except for tuples and sets, which keep their tuple/set
    container types here.
    """
    if isinstance(value, dict):
        return {
            str(key): convert_large_ints(nested, max_int=max_int)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [
            convert_large_ints(nested, max_int=max_int) for nested in value
        ]
    if isinstance(value, tuple):
        return tuple(
            convert_large_ints(nested, max_int=max_int) for nested in value
        )
    if isinstance(value, set):
        return {
            convert_large_ints(nested, max_int=max_int) for nested in value
        }
    if isinstance(value, int) and abs(value) > max_int:
        return float(value)
    return value


def parse_jsonish(value: Any) -> Any:
    """Parse JSON-like strings while leaving ordinary strings untouched.

    Blank strings, invalid JSON, and non-string values are returned as-is.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped == "":
        return value
    try:
        return srsly.json_loads(stripped)
    except Exception:
        return value
