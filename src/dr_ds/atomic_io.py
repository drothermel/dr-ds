"""Atomic file-writing helpers for JSON, JSONL, and parquet-backed records."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from dr_ds.parquet import records_to_parquet_frame
from dr_ds.serialization import to_jsonable
import srsly


def _fsync_parent_dir(path: Path) -> None:
    """Flush the parent directory entry after an atomic replace."""
    fd: int | None = None
    try:
        fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(fd)
    finally:
        if fd is not None:
            os.close(fd)


def dump_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Write one JSON payload atomically, replacing any existing file.

    The write is performed through a sibling temporary file plus `os.replace`,
    then the parent directory is fsynced so the rename is durably recorded.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    replaced = False
    try:
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
        replaced = True
        _fsync_parent_dir(path)
    except Exception:
        if not replaced and tmp_name is not None:
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass
        raise


def atomic_write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    """Write JSONL records atomically after JSON-safe row normalization.

    Each record is normalized through `to_jsonable` before serialization so
    callers can pass plain objects and nested containers without pre-flattening
    them themselves.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    replaced = False
    try:
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
                    srsly.json_dumps(to_jsonable(record), sort_keys=True)
                    + "\n"
                )
            handle.flush()
            os.fsync(handle.fileno())
            tmp_name = handle.name
        os.replace(tmp_name, path)
        replaced = True
        _fsync_parent_dir(path)
    except Exception:
        if not replaced and tmp_name is not None:
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass
        raise


def atomic_write_parquet_records(
    path: Path,
    records: list[dict[str, Any]],
    *,
    json_columns: set[str],
) -> None:
    """Write record dictionaries to parquet atomically via the dataframe adapter.

    JSON-designated columns are normalized through `records_to_parquet_frame`
    before the parquet file is written, then the final file is atomically
    swapped into place.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    replaced = False
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=path.name,
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_name = handle.name
        tmp_path = Path(tmp_name)
        frame = records_to_parquet_frame(records, json_columns=json_columns)
        frame.to_parquet(tmp_path)
        with open(tmp_path, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        replaced = True
        _fsync_parent_dir(path)
    except Exception:
        if not replaced and tmp_name is not None:
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass
        raise
