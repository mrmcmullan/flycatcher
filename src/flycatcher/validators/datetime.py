"""Datetime operations for the validator DSL."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, cast

import polars as pl

from .core import BinaryOp, UnaryOp, _ExpressionMixin


class DateTimeAccessor:
    """Accessor for datetime operations on expressions."""

    def __init__(self, expr: Any):
        self.expr = expr

    def year(self) -> "DateTimeOp":
        """Extract the year component from a datetime or date value."""
        return DateTimeOp("year", self.expr, None)

    def month(self) -> "DateTimeOp":
        """Extract the month component (1-12) from a datetime or date value."""
        return DateTimeOp("month", self.expr, None)

    def day(self) -> "DateTimeOp":
        """Extract the day component (1-31) from a datetime or date value."""
        return DateTimeOp("day", self.expr, None)

    def hour(self) -> "DateTimeOp":
        """Extract the hour component (0-23) from a datetime value."""
        return DateTimeOp("hour", self.expr, None)

    def minute(self) -> "DateTimeOp":
        """Extract the minute component (0-59) from a datetime value."""
        return DateTimeOp("minute", self.expr, None)

    def second(self) -> "DateTimeOp":
        """Extract the second component (0-59) from a datetime value."""
        return DateTimeOp("second", self.expr, None)

    def total_days(self, other: Any) -> "DateTimeOp":
        """
        Calculate the difference in days between this value and another.

        Parameters
        ----------
        other : datetime, date, or FieldRef
            The value to compare against.

        Returns
        -------
        DateTimeOp
            An expression evaluating to the number of days difference (float),
            positive if this value is later.
        """
        return DateTimeOp("total_days", self.expr, other)


class DateTimeOp(_ExpressionMixin):
    """Datetime operation that can compile to both Polars and Python.

    This class represents datetime operations (like extracting year, month, or
    calculating differences) that work seamlessly in both Polars DataFrame
    validation and Pydantic row-level validation contexts.
    """

    POLARS_COMPONENTS: dict[str, str] = {
        "year": "year",
        "month": "month",
        "day": "day",
        "hour": "hour",
        "minute": "minute",
        "second": "second",
    }

    def __init__(self, op: str, operand: Any, arg: Any = None):
        self.op = op
        self.operand = operand
        self.arg = arg

    def to_polars(self) -> pl.Expr:
        operand_expr = self._to_polars(self.operand)

        if self.op in self.POLARS_COMPONENTS:
            component = self.POLARS_COMPONENTS[self.op]
            method = getattr(operand_expr.dt, component)
            return cast(pl.Expr, method())

        if self.op == "total_days":
            other_expr = self._to_polars(self.arg)
            return (operand_expr - other_expr).dt.total_days()

        raise ValueError(f"Unknown datetime op: {self.op}")

    def to_python(self, values: Any) -> Any:
        operand_val = self._to_python(self.operand, values)

        if self.op in self.POLARS_COMPONENTS:
            return self._extract_component(operand_val, self.POLARS_COMPONENTS[self.op])

        if self.op == "total_days":
            other_val = self._to_python(self.arg, values)
            if operand_val is None or other_val is None:
                return None
            delta = operand_val - other_val
            return delta.total_seconds() / 86_400

        raise ValueError(f"Unknown datetime op: {self.op}")

    @staticmethod
    def _extract_component(value: Any, attr: str) -> Any:
        if value is None:
            return None
        if not isinstance(value, (datetime, date)):
            raise ValueError(
                f"Expected date or datetime for .dt.{attr}, got {type(value).__name__}"
            )
        if (
            attr in {"hour", "minute", "second"}
            and isinstance(value, date)
            and not isinstance(value, datetime)
        ):
            raise ValueError(f"Time component .dt.{attr} requires datetime, got date")
        return getattr(value, attr)

    def __gt__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, ">", other)

    def __ge__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, ">=", other)

    def __lt__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "<", other)

    def __le__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "<=", other)

    def __eq__(self, other: Any) -> BinaryOp:  # type: ignore[override]
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> BinaryOp:  # type: ignore[override]
        return BinaryOp(self, "!=", other)

    def __add__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "+", other)

    def __sub__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "-", other)

    def __mul__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "*", other)

    def __truediv__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "/", other)

    def __and__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "&", other)

    def __or__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "|", other)

    @property
    def dt(self) -> "DateTimeAccessor":
        """Access datetime operations on this expression (for chaining)."""
        return DateTimeAccessor(self)

    def abs(self) -> UnaryOp:
        """Absolute value (for numeric results like differences)."""
        return UnaryOp("abs", self)

    def __invert__(self) -> UnaryOp:
        """Negation (for boolean results)."""
        return UnaryOp("~", self)
