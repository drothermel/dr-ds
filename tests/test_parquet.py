from __future__ import annotations

from dr_ds.parquet import parquet_frame_to_records, records_to_parquet_frame


def test_records_to_parquet_frame_and_back_round_trip() -> None:
    records = [
        {
            "run_id": "abc",
            "step": 1,
            "metrics": {"loss": 0.5, "count": 2**35},
            "status": "done",
        }
    ]

    frame = records_to_parquet_frame(records, json_columns={"metrics"})
    restored = parquet_frame_to_records(frame, json_columns={"metrics"})

    assert restored == [
        {
            "run_id": "abc",
            "step": 1,
            "metrics": {"loss": 0.5, "count": float(2**35)},
            "status": "done",
        }
    ]


def test_records_to_parquet_frame_respects_custom_max_int() -> None:
    frame = records_to_parquet_frame(
        [{"value": 10, "nested": {"count": 11}}],
        json_columns={"nested"},
        max_int=9,
    )

    assert frame.to_dict(orient="records") == [
        {"value": 10.0, "nested": '{"count": 11.0}'}
    ]
