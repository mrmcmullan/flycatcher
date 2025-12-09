"""Membership operations for the validator DSL."""

from __future__ import annotations

from typing import Any, Literal

import polars as pl
from loguru import logger

ClosedInterval = Literal["both", "left", "right", "none"]


class _MembershipMixin:
    """Mixin adding membership-style helper operations."""

    def is_in(self, other: Any, *, nulls_equal: bool = False) -> "MembershipOp":
        """Check whether value is contained in a sequence or Series."""
        return MembershipOp("is_in", self, other, nulls_equal=nulls_equal)

    def is_between(
        self,
        lower_bound: Any,
        upper_bound: Any,
        *,
        closed: ClosedInterval = "both",
    ) -> "MembershipOp":
        """
        Check whether value lies between two bounds.

        Parameters mirror Polars:
        - lower_bound / upper_bound accept expressions; strings are treated as
          column references, other non-expressions become literals.
        - closed controls interval inclusivity ('both', 'left', 'right', 'none').
        """
        return MembershipOp(
            "is_between", self, (lower_bound, upper_bound), closed=closed
        )


class MembershipOp(_MembershipMixin):
    """Membership-style operations (is_in, is_between) for expressions."""

    VALID_CLOSED: set[ClosedInterval] = {"both", "left", "right", "none"}

    def __init__(
        self,
        op: str,
        operand: Any,
        arg: Any,
        *,
        nulls_equal: bool = False,
        closed: ClosedInterval = "both",
    ):
        self.op = op
        self.operand = operand
        self.arg = arg
        self.nulls_equal = nulls_equal
        self.closed = closed

    def to_polars(self) -> pl.Expr:
        expr = self._to_polars_value(self.operand)

        if self.op == "is_in":
            other = self._prepare_other_polars(self.arg)
            return expr.is_in(other, nulls_equal=self.nulls_equal)

        if self.op == "is_between":
            lower_bound, upper_bound = self.arg
            self._validate_closed()
            self._warn_if_inverted_raw(lower_bound, upper_bound)
            lower = self._prepare_bound_polars(lower_bound)
            upper = self._prepare_bound_polars(upper_bound)
            return expr.is_between(lower, upper, closed=self.closed)

        raise ValueError(f"Unknown membership op: {self.op}")

    def to_python(self, values: Any) -> Any:
        value = self._to_python_value(self.operand, values)

        if self.op == "is_in":
            other = self._prepare_other_python(self.arg, values)
            if value is None:
                if self.nulls_equal:
                    return self._contains(other, None)
                return None
            return self._contains(other, value)

        if self.op == "is_between":
            lower_bound, upper_bound = self.arg
            self._validate_closed()
            if value is None:
                return None
            lower = self._prepare_bound_python(lower_bound, values)
            upper = self._prepare_bound_python(upper_bound, values)
            self._warn_if_inverted_values(lower, upper)
            if lower is None or upper is None:
                return None
            return self._evaluate_between(value, lower, upper)

        raise ValueError(f"Unknown membership op: {self.op}")

    def _validate_closed(self) -> None:
        if self.closed not in self.VALID_CLOSED:
            raise ValueError(
                f"Invalid closed value '{self.closed}'. "
                "Expected one of {'both', 'left', 'right', 'none'}."
            )

    def _prepare_other_polars(self, other: Any) -> Any:
        if hasattr(other, "to_polars"):
            return other.to_polars()  # type: ignore[no-any-return]
        if isinstance(other, pl.Expr):
            return other
        if isinstance(other, pl.Series):
            return other
        if isinstance(other, (list, tuple, set)):
            return list(other)
        raise TypeError("is_in() expects a sequence, Series, or expression for 'other'")

    def _prepare_bound_polars(self, bound: Any) -> Any:
        if bound is None:
            return None
        if hasattr(bound, "to_polars"):
            return bound.to_polars()  # type: ignore[no-any-return]
        if isinstance(bound, pl.Expr):
            return bound
        if isinstance(bound, str):
            return pl.col(bound)
        return pl.lit(bound)

    def _prepare_other_python(self, other: Any, values: Any) -> Any:
        if hasattr(other, "to_python"):
            return other.to_python(values)
        if isinstance(other, pl.Series):
            return other.to_list()
        return other

    def _prepare_bound_python(self, bound: Any, values: Any) -> Any:
        if bound is None:
            return None
        if hasattr(bound, "to_python"):
            return bound.to_python(values)
        if isinstance(bound, str):
            return self._get_from_values(values, bound)
        return bound

    @staticmethod
    def _contains(container: Any, value: Any) -> bool | None:
        if container is None:
            return False
        try:
            return value in container
        except Exception:
            return False

    def _evaluate_between(self, value: Any, lower: Any, upper: Any) -> bool:
        if self.closed == "both":
            return bool(value >= lower and value <= upper)
        if self.closed == "left":
            return bool(value >= lower and value < upper)
        if self.closed == "right":
            return bool(value > lower and value <= upper)
        # closed == "none"
        return bool(value > lower and value < upper)

    def _warn_if_inverted_raw(self, lower: Any, upper: Any) -> None:
        """Warn when literal bounds make the interval empty."""
        if self._should_skip_inversion_check(lower, upper):
            return
        try:
            if lower > upper:
                logger.warning(
                    "is_between called with lower_bound > upper_bound; "
                    "interval is empty "
                    f"(lower={lower!r}, upper={upper!r})"
                )
        except Exception:
            return

    def _warn_if_inverted_values(self, lower: Any, upper: Any) -> None:
        """Warn after python evaluation when we can compare concrete values."""
        if self._should_skip_inversion_check(lower, upper):
            return
        try:
            if lower > upper:
                logger.warning(
                    "is_between evaluated with lower_bound > upper_bound; "
                    "interval is empty "
                    f"(lower={lower!r}, upper={upper!r})"
                )
        except Exception:
            return

    @staticmethod
    def _should_skip_inversion_check(lower: Any, upper: Any) -> bool:
        if lower is None or upper is None:
            return True
        if hasattr(lower, "to_polars") or hasattr(upper, "to_polars"):
            return True
        if isinstance(lower, (pl.Expr, pl.Series)) or isinstance(
            upper, (pl.Expr, pl.Series)
        ):
            return True
        if isinstance(lower, str) or isinstance(upper, str):
            return True
        return False

    @staticmethod
    def _to_polars_value(obj: Any) -> pl.Expr:
        if hasattr(obj, "to_polars"):
            return obj.to_polars()  # type: ignore[no-any-return]
        return pl.lit(obj)

    @staticmethod
    def _to_python_value(obj: Any, values: Any) -> Any:
        if hasattr(obj, "to_python"):
            return obj.to_python(values)
        return obj

    @staticmethod
    def _get_from_values(values: Any, name: str) -> Any:
        if hasattr(values, name):
            return getattr(values, name)
        try:
            return values[name]
        except Exception:
            raise AttributeError(f"Field '{name}' not found in values")
