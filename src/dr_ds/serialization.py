from __future__ import annotations

from collections.abc import Mapping
import json
from datetime import datetime, timezone
from typing import Any

import srsly

DEFAULT_MAX_INT = 2**31 - 1


def serialize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [to_jsonable(nested) for nested in value]
    if isinstance(value, tuple):
        return [to_jsonable(nested) for nested in value]
    if isinstance(value, set):
        items = [to_jsonable(nested) for nested in value]
        try:
            return sorted(items)
        except TypeError:
            return sorted(
                items,
                key=lambda item: json.dumps(item, sort_keys=True),
            )
    return _object_to_jsonable(value)


def _object_to_jsonable(value: Any) -> Any:
    try:
        attributes = vars(value)
    except TypeError:
        return str(value)

    normalized_attributes = {
        str(key): to_jsonable(nested)
        for key, nested in attributes.items()
        if not str(key).startswith("_") and not callable(nested)
    }
    if normalized_attributes:
        return normalized_attributes
    return str(value)


def convert_large_ints(value: Any, *, max_int: int = DEFAULT_MAX_INT) -> Any:
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
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped == "":
        return value
    try:
        return srsly.json_loads(stripped)
    except Exception:
        return value
