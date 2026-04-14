"""Small coercion helpers for turning loose scalar inputs into numeric types."""

from __future__ import annotations

import math

Coercible = int | float | str | None


def coerce_int(value: Coercible) -> int | None:
    """Best-effort coerce a scalar into `int`, returning `None` on invalid input."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def coerce_number(value: Coercible) -> int | float | None:
    """Coerce a scalar into `int` or `float`, preserving integral floats as ints."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return int(value)
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    if math.isfinite(numeric) and numeric.is_integer():
        return int(numeric)
    return numeric


def coerce_float(value: Coercible) -> float | None:
    """Best-effort coerce a scalar into `float`, returning `None` on invalid input."""
    coerced = coerce_number(value)
    if coerced is None:
        return None
    return float(coerced)
