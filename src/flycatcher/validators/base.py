"""Base mixin for validator expressions & operations."""

from __future__ import annotations

from typing import Any

import polars as pl


class _ExpressionMixin:
    """Mixin providing common conversion methods for expressions."""

    def _to_polars(self, obj: Any) -> pl.Expr:
        """Convert object to Polars expression."""
        if hasattr(obj, "to_polars"):
            return obj.to_polars()  # type: ignore[no-any-return]
        return pl.lit(obj)

    def _to_python(self, obj: Any, values: Any) -> Any:
        """Convert object to Python value."""
        if hasattr(obj, "to_python"):
            return obj.to_python(values)
        return obj
