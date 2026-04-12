from __future__ import annotations

from pathlib import Path

from dr_ds.atomic_io import (
    atomic_write_jsonl,
    atomic_write_parquet_records,
    dump_json_atomic,
)
from dr_ds.parquet import parquet_frame_to_records
import pandas as pd
import srsly


def test_dump_json_atomic_writes_json_file(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"
    payload = {"b": 2, "a": 1}

    dump_json_atomic(path, payload)

    assert srsly.read_json(path) == payload


def test_dump_json_atomic_overwrites_existing_json_file(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"

    dump_json_atomic(path, {"a": 1})
    dump_json_atomic(path, {"b": 2})

    assert srsly.read_json(path) == {"b": 2}


def test_atomic_write_jsonl_writes_line_delimited_json(tmp_path: Path) -> None:
    path = tmp_path / "records.jsonl"
    records = [{"run_id": "a"}, {"run_id": "b", "metrics": {"loss": 0.5}}]

    atomic_write_jsonl(path, records)

    assert list(srsly.read_jsonl(path)) == records


def test_atomic_write_jsonl_overwrites_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "records.jsonl"

    atomic_write_jsonl(path, [{"run_id": "a"}])
    atomic_write_jsonl(path, [{"run_id": "b"}])

    assert list(srsly.read_jsonl(path)) == [{"run_id": "b"}]


def test_atomic_write_parquet_records_writes_records(tmp_path: Path) -> None:
    path = tmp_path / "records.parquet"
    records = [
        {"run_id": "a", "metrics": {"loss": 0.5}, "status": "done"},
    ]

    atomic_write_parquet_records(path, records, json_columns={"metrics"})

    frame = pd.read_parquet(path)
    restored = parquet_frame_to_records(frame, json_columns={"metrics"})
    assert restored == records


def test_atomic_write_parquet_records_overwrites_existing_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "records.parquet"

    atomic_write_parquet_records(
        path,
        [{"run_id": "a", "metrics": {"loss": 0.5}, "status": "done"}],
        json_columns={"metrics"},
    )
    atomic_write_parquet_records(
        path,
        [{"run_id": "b", "metrics": {"loss": 0.1}, "status": "queued"}],
        json_columns={"metrics"},
    )

    frame = pd.read_parquet(path)
    restored = parquet_frame_to_records(frame, json_columns={"metrics"})
    assert restored == [
        {"run_id": "b", "metrics": {"loss": 0.1}, "status": "queued"}
    ]
