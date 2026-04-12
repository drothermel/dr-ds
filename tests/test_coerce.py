from __future__ import annotations

import math
from typing import cast

import pytest

from dr_ds.coerce import Coercible, coerce_float, coerce_int, coerce_number


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, None),
        (False, None),
        (float("nan"), None),
        ("abc", None),
        (0, 0),
        (1, 1),
        (-7, -7),
        (3.0, 3),
        (3.5, 3),
        (-3.5, -3),
        (float("inf"), None),
        (float("-inf"), None),
        ("42", 42),
        ("  42 ", 42),
        ("-17", -17),
        ("3.0", None),
        ("3.5", None),
        (10**30, 10**30),
    ],
)
def test_coerce_int(value: Coercible, expected: int | None) -> None:
    result = coerce_int(value)
    assert result == expected
    if result is not None:
        assert type(result) is int


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, None),
        (False, None),
        (float("nan"), None),
        ("abc", None),
        (0, 0),
        (5, 5),
        (-2, -2),
        (3.0, 3),
        (3.5, 3.5),
        (-0.25, -0.25),
        ("42", 42),
        ("3.0", 3),
        ("3.5", 3.5),
        ("-1.5", -1.5),
        (float("inf"), float("inf")),
    ],
)
def test_coerce_number(value: Coercible, expected: int | float | None) -> None:
    result = coerce_number(value)
    if expected is None:
        assert result is None
        return
    assert result is not None
    if isinstance(expected, float) and math.isinf(expected):
        assert isinstance(result, float)
        assert math.isinf(result)
        assert (result > 0) == (expected > 0)
        return
    assert result == expected
    if isinstance(expected, int) and not isinstance(expected, bool):
        assert type(result) is int
    else:
        assert type(result) is float


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, None),
        (False, None),
        (float("nan"), None),
        ("abc", None),
        (0, 0.0),
        (7, 7.0),
        (3.0, 3.0),
        (3.5, 3.5),
        ("42", 42.0),
        ("3.0", 3.0),
        ("3.5", 3.5),
    ],
)
def test_coerce_float(value: Coercible, expected: float | None) -> None:
    result = coerce_float(value)
    if expected is None:
        assert result is None
        return
    assert result == expected
    assert type(result) is float


def test_coerce_int_catches_type_error_from_bad_input() -> None:
    class Bad:
        def __int__(self) -> int:
            raise TypeError("nope")

    assert coerce_int(cast(Coercible, Bad())) is None


def test_coerce_number_handles_string_integer_that_parses_as_float() -> None:
    assert coerce_number("1e2") == 100
    assert type(coerce_number("1e2")) is int
    assert coerce_number("1.5e1") == 15
    assert type(coerce_number("1.5e1")) is int
