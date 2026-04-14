# dr-ds

Small, typed data science helpers for serialization, atomic file writes,
and dataframe-oriented record normalization.

## Install

```bash
uv add dr-ds
```

`dr-ds` currently targets Python 3.12+.

## Included Helpers

`dr_ds.atomic_io` provides:
- `dump_json_atomic`
- `atomic_write_jsonl`
- `atomic_write_parquet_records`

`dr_ds.serialization` provides:
- `serialize_timestamp`
- `utc_now_iso`
- `to_jsonable`
- `convert_large_ints`
- `parse_jsonish`

`dr_ds.parquet` provides:
- `records_to_parquet_frame`
- `parquet_frame_to_records`

These helpers are aimed at a common pattern in data workflows:
- start with `list[dict[str, Any]]` records or similarly loose Python data
- normalize nested containers and plain Python objects into JSON-safe values
- persist those records atomically or adapt them for dataframe/parquet workflows
- recover structured JSON-like columns on read without rebuilding ad hoc parsing logic

## Design Goals

- Prefer small, reusable utilities with stable behavior over framework-heavy abstractions.
- Be explicit about lossy or opinionated conversions.
- Keep serialization helpers deterministic so downstream tests and diffs stay readable.
- Make the common data-science path easy: Python dicts in, JSON/parquet-safe data out.

## Serialization Contracts

`to_jsonable` is the main normalization helper for nested Python values.

- `datetime` values become UTC ISO 8601 strings.
- Mapping keys are stringified.
- Tuples become lists.
- Sets become deterministically ordered lists.
- Plain Python objects are serialized from their public, non-callable attributes.
- Recursive references are replaced with the literal string `"<recursion>"`.
- Values that cannot be meaningfully introspected fall back to `str(value)`.

`convert_large_ints` is intentionally narrower:

- it recursively converts integers whose absolute value exceeds `DEFAULT_MAX_INT`
  into floats
- it preserves tuple and set container types
- it is mainly intended to keep dataframe/parquet pipelines practical when very
  large integers appear in nested payloads

`parse_jsonish` only parses strings that are valid JSON. Invalid JSON, blank
strings, and non-string values are returned unchanged.

## Atomic IO Example

```python
from pathlib import Path

from dr_ds.atomic_io import atomic_write_jsonl, dump_json_atomic
from dr_ds.serialization import to_jsonable

payload = to_jsonable(
    {
        "metrics": {"loss": 0.42},
        "tags": {"baseline", "v1"},
        "owner": {"name": "baseline-bot", "id": 7},
    }
)

dump_json_atomic(Path("summary.json"), payload)

atomic_write_jsonl(
    Path("runs.jsonl"),
    [
        {"run_id": "run-1", "summary": payload},
        {"run_id": "run-2", "summary": {"loss": 0.39}},
    ],
)
```

All atomic writers use a sibling temporary file plus `os.replace`, then fsync
the parent directory so the rename is durably recorded.

## Parquet Example

```python
from dr_ds.parquet import parquet_frame_to_records, records_to_parquet_frame

records = [
    {
        "run_id": "abc123",
        "metrics": {"loss": 0.42, "token_count": 2**35},
    }
]

frame = records_to_parquet_frame(records, json_columns={"metrics"})
restored = parquet_frame_to_records(frame, json_columns={"metrics"})
```

`records_to_parquet_frame` prepares records for dataframe/parquet workflows.
It does not write parquet files directly.

Behavior to rely on:

- columns listed in `json_columns` are normalized through `to_jsonable`
- large integers nested inside those JSON columns are converted with
  `convert_large_ints`
- top-level large integers in non-JSON columns are also softened to floats
- `parquet_frame_to_records` restores JSON columns with `parse_jsonish`
  and converts dataframe null-like values in those columns back to `None`

## Coercion Helpers

`coerce_int`, `coerce_number`, and `coerce_float` are intentionally forgiving.

- invalid inputs return `None` instead of raising
- booleans are rejected even though Python considers them integers
- `coerce_number` preserves integral numeric values as `int`
- `coerce_float` is the lossy "give me a float if possible" variant

## License

MIT

## Development

Run the standard checks before committing:

```bash
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```
