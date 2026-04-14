"""Public re-exports for the shared dr-ds utility surface."""

from dr_ds.atomic_io import (
    atomic_write_jsonl,
    atomic_write_parquet_records,
    dump_json_atomic,
)
from dr_ds.coerce import coerce_float, coerce_int, coerce_number
from dr_ds.parquet import parquet_frame_to_records, records_to_parquet_frame
from dr_ds.serialization import (
    DEFAULT_MAX_INT,
    convert_large_ints,
    parse_jsonish,
    serialize_timestamp,
    to_jsonable,
    utc_now_iso,
)

__all__ = [
    "DEFAULT_MAX_INT",
    "atomic_write_jsonl",
    "atomic_write_parquet_records",
    "coerce_float",
    "coerce_int",
    "coerce_number",
    "convert_large_ints",
    "dump_json_atomic",
    "parquet_frame_to_records",
    "parse_jsonish",
    "records_to_parquet_frame",
    "serialize_timestamp",
    "to_jsonable",
    "utc_now_iso",
]
