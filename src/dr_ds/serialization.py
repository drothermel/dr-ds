from __future__ import annotations

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
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
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
    return value


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
