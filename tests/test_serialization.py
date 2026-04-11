from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import srsly

from dr_ds.serialization import (
    convert_large_ints,
    dump_json_atomic,
    parse_jsonish,
    serialize_timestamp,
    to_jsonable,
)


def test_to_jsonable_normalizes_nested_values() -> None:
    value = {
        "timestamp": datetime(2024, 1, 2, 3, 4, tzinfo=UTC),
        "tuple": (1, 2),
        "set": {3, 1, 2},
    }

    result = to_jsonable(value)

    assert result == {
        "timestamp": "2024-01-02T03:04:00+00:00",
        "tuple": [1, 2],
        "set": [1, 2, 3],
    }


def test_convert_large_ints_respects_max_int() -> None:
    value = {
        "small": 3,
        "large": 10,
        "nested": [1, 20],
        "tuple": (4, 30),
        "set": {5, 40},
    }

    result = convert_large_ints(value, max_int=9)

    assert result == {
        "small": 3,
        "large": 10.0,
        "nested": [1, 20.0],
        "tuple": (4, 30.0),
        "set": {5, 40.0},
    }


def test_parse_jsonish_parses_json_like_strings_only() -> None:
    assert parse_jsonish('{"a": 1}') == {"a": 1}
    assert parse_jsonish("[1, 2]") == [1, 2]
    assert parse_jsonish("123") == 123
    assert parse_jsonish("true") is True
    assert parse_jsonish("null") is None
    assert parse_jsonish("plain-text") == "plain-text"


def test_serialize_timestamp_handles_datetime_and_none() -> None:
    expected = datetime(2024, 1, 2, 3, 4, tzinfo=UTC).isoformat()
    assert (
        serialize_timestamp(datetime(2024, 1, 2, 3, 4, tzinfo=UTC)) == expected
    )
    assert serialize_timestamp(None) is None


def test_to_jsonable_handles_mixed_type_sets_deterministically() -> None:
    result = to_jsonable({1, "two", (3, 4)})

    assert result == ["two", 1, [3, 4]]


def test_dump_json_atomic_writes_json_file(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"
    payload = {"b": 2, "a": 1}

    dump_json_atomic(path, payload)

    assert srsly.read_json(path) == payload
