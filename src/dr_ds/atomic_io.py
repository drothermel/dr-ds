from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from dr_ds.parquet import records_to_parquet_frame
from dr_ds.serialization import to_jsonable
import srsly


def dump_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=path.name,
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(srsly.json_dumps(payload, indent=2, sort_keys=True))
        handle.flush()
        os.fsync(handle.fileno())
        tmp_name = handle.name
    os.replace(tmp_name, path)


def atomic_write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=path.name,
        suffix=".tmp",
        delete=False,
    ) as handle:
        for record in records:
            handle.write(
                srsly.json_dumps(to_jsonable(record), sort_keys=True) + "\n"
            )
        handle.flush()
        os.fsync(handle.fileno())
        tmp_name = handle.name
    os.replace(tmp_name, path)


def atomic_write_parquet_records(
    path: Path,
    records: list[dict[str, Any]],
    *,
    json_columns: set[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        prefix=path.name,
        suffix=".tmp",
        delete=False,
    ) as handle:
        tmp_path = Path(handle.name)
    frame = records_to_parquet_frame(records, json_columns=json_columns)
    frame.to_parquet(tmp_path)
    with open(tmp_path, "rb") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)
