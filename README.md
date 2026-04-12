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
- start with `list[dict[str, Any]]` records
- normalize nested JSON-like columns into strings for dataframe/parquet compatibility
- recover those structured columns on read

## Atomic IO Example

```python
from pathlib import Path

from dr_ds.atomic_io import dump_json_atomic
from dr_ds.serialization import to_jsonable

payload = to_jsonable(
    {
        "metrics": {"loss": 0.42},
        "tags": {"baseline", "v1"},
    }
)

dump_json_atomic(Path("summary.json"), payload)
```

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
