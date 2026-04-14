"""Helpers for normalizing loosely typed values into stable serialized forms."""

from __future__ import annotations

from collections.abc import Mapping
import json
from datetime import datetime, timezone
from typing import Any

import srsly

DEFAULT_MAX_INT = 2**31 - 1


def serialize_timestamp(value: Any) -> str | None:
    """Return a UTC ISO timestamp for datetimes and stringify other values."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def utc_now_iso() -> str:
    """Return the current UTC timestamp as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def to_jsonable(value: Any, *, seen_ids: set[int] | None = None) -> Any:
    """Normalize values into JSON-safe forms and replace recursive references."""
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
    """Serialize object public attributes and guard against recursive references."""
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
    """Convert integers outside the parquet-safe bound to floats recursively."""
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
    """Parse JSON-like strings and leave non-JSON-looking values unchanged."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped == "":
        return value
    try:
        return srsly.json_loads(stripped)
    except Exception:
        return value
