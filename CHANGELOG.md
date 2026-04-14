# Changelog

## 0.1.4

- Expanded `dr_ds.serialization.to_jsonable` to normalize plain Python
  objects with public attributes into JSON-safe values instead of leaving
  them as raw objects.
- Added regression tests covering object normalization, filtering of
  private and callable attributes, and string fallback for opaque values.
- Updated the README to describe the broader `to_jsonable` behavior.
- Added a first pass of public API docstrings and a repo-level
  documentation style guide for the shared utility surface.

## 0.1.0

- Added `dr_ds.serialization` with JSON-safe normalization, timestamp,
  and atomic JSON write helpers.
- Added `dr_ds.parquet` with record-to-dataframe normalization helpers
  for nested JSON-like columns.
- Added tests for serialization and parquet helpers.
