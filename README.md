# dr-ds

General data science helpers for small data-wrangling and serialization tasks.

## Included Helpers

`dr_ds.serialization` provides:
- `serialize_timestamp`
- `utc_now_iso`
- `to_jsonable`
- `convert_large_ints`
- `parse_jsonish`
- `dump_json_atomic`

`dr_ds.parquet` provides:
- `records_to_parquet_frame`
- `parquet_frame_to_records`

The parquet helpers are aimed at a common pattern in data workflows:
- start with `list[dict[str, Any]]` records
- normalize nested JSON-like columns into strings for dataframe/parquet compatibility
- recover those structured columns on read

## Example

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

## Development

Run the standard checks before committing:

```bash
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```
