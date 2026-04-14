"""Adapters between Python record dictionaries and parquet-friendly dataframes."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from dr_ds.serialization import (
    DEFAULT_MAX_INT,
    convert_large_ints,
    parse_jsonish,
    to_jsonable,
)


def records_to_parquet_frame(
    records: list[dict[str, Any]],
    *,
    json_columns: set[str],
    max_int: int = DEFAULT_MAX_INT,
) -> pd.DataFrame:
    """Convert record dictionaries into a parquet-friendly dataframe.

    Columns named in `json_columns` are normalized through `to_jsonable`,
    large integers inside those JSON payloads are softened through
    `convert_large_ints`, and the final structured value is stored as a JSON
    string. Non-JSON columns only coerce oversized top-level integers.
    """
    normalized: list[dict[str, Any]] = []
    for record in records:
        row: dict[str, Any] = {}
        for key, value in record.items():
            if key in json_columns:
                row[key] = json.dumps(
                    to_jsonable(convert_large_ints(value, max_int=max_int)),
                    sort_keys=True,
                )
            elif isinstance(value, int) and abs(value) > max_int:
                row[key] = float(value)
            else:
                row[key] = value
        normalized.append(row)
    return pd.DataFrame(normalized)


def parquet_frame_to_records(
    frame: pd.DataFrame, *, json_columns: set[str]
) -> list[dict[str, Any]]:
    """Restore JSON-designated dataframe columns back into Python values.

    JSON columns are parsed with `parse_jsonish`, while null-like dataframe
    values round-trip back to `None`.
    """
    records = frame.to_dict(orient="records")
    normalized: list[dict[str, Any]] = []
    for record in records:
        row: dict[str, Any] = {}
        for key, value in record.items():
            if key in json_columns:
                if value is None or pd.isna(value):
                    row[key] = None
                else:
                    row[key] = parse_jsonish(value)
            else:
                row[key] = value
        normalized.append(row)
    return normalized
