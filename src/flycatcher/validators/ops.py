"""Binary & unary operations for the validator DSL."""

from __future__ import annotations

import builtins
import math
from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from .base import _ExpressionMixin
from .membership import _MembershipMixin

if TYPE_CHECKING:  # pragma: no cover
    from .datetime import DateTimeAccessor
    from .string import StringAccessor


class _MathOpsMixin:
    """Shared math-style operations for expressions."""

    def round(self, decimals: int = 0) -> "UnaryOp":
        """Round to a fixed number of decimal places.

        Parameters
        ----------
        decimals : int, default 0
            Number of fractional digits to round to. Negative values round to
            powers of ten.

        Returns
        -------
        UnaryOp
            Expression representing the rounding operation.
        """

        return UnaryOp("round", self, decimals)

    def floor(self) -> "UnaryOp":
        """Round down to the nearest integer.

        Returns
        -------
        UnaryOp
            Expression representing the floor operation.
        """

        return UnaryOp("floor", self)

    def ceil(self) -> "UnaryOp":
        """Round up to the nearest integer.

        Returns
        -------
        UnaryOp
            Expression representing the ceiling operation.
        """

        return UnaryOp("ceil", self)

    def sqrt(self) -> "UnaryOp":
        """Compute the square root element-wise.

        Returns
        -------
        UnaryOp
            Expression representing the square root.
        """

        return UnaryOp("sqrt", self)

    def pow(self, exponent: Any) -> "UnaryOp":
        """Raise the expression to a power.

        Parameters
        ----------
        exponent : int | float
            Exponent to raise the expression to.

        Returns
        -------
        UnaryOp
            Expression representing the power operation.
        """

        return UnaryOp("pow", self, exponent)


class BinaryOp(_MathOpsMixin, _ExpressionMixin, _MembershipMixin):
    """Binary operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[builtins.str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b,
        "&": lambda a, b: a & b,
        "|": lambda a, b: a | b,
    }

    PYTHON_OPS = {
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b,
        "&": lambda a, b: a and b,
        "|": lambda a, b: a or b,
    }

    def __init__(self, left: Any, op: builtins.str, right: Any):
        self.left = left
        self.op = op
        self.right = right

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        left_expr = self._to_polars(self.left)
        right_expr = self._to_polars(self.right)
        return self.POLARS_OPS[self.op](left_expr, right_expr)

    def to_python(self, values: Any) -> Any:
        """Evaluate in Python context."""
        left_val = self._to_python(self.left, values)
        right_val = self._to_python(self.right, values)
        return self.PYTHON_OPS[self.op](left_val, right_val)

    # Support chaining operations
    def __gt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">", other)

    def __ge__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">=", other)

    def __lt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<", other)

    def __le__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<=", other)

    def __eq__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
        return BinaryOp(self, "!=", other)

    def __add__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "+", other)

    def __sub__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "-", other)

    def __mul__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "*", other)

    def __truediv__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "/", other)

    def __and__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "&", other)

    def __or__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "|", other)

    def abs(self) -> "UnaryOp":
        """Absolute value."""
        return UnaryOp("abs", self)

    def __invert__(self) -> "UnaryOp":
        """Negation."""
        return UnaryOp("~", self)

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this expression."""
        from .string import StringAccessor

        return StringAccessor(self)

    @property
    def dt(self) -> "DateTimeAccessor":
        """Access datetime operations on this expression."""
        from .datetime import DateTimeAccessor

        return DateTimeAccessor(self)


def _python_round(value: Any, decimals: int) -> Any:
    if value is None:
        return None
    result = round(value, decimals)
    if isinstance(value, int):
        return int(result)
    return result


def _python_sqrt(value: Any) -> Any:
    if value is None:
        return None
    try:
        return math.sqrt(value)
    except ValueError:
        return math.nan


class UnaryOp(_MathOpsMixin, _ExpressionMixin, _MembershipMixin):
    """Unary operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[builtins.str, Callable[[pl.Expr, Any], pl.Expr]] = {
        "abs": lambda expr, _: expr.abs(),
        "~": lambda expr, _: ~expr,
        "is_null": lambda expr, _: expr.is_null(),
        "is_not_null": lambda expr, _: expr.is_not_null(),
        "round": lambda expr, decimals: expr.round(decimals=decimals),
        "floor": lambda expr, _: expr.floor(),
        "ceil": lambda expr, _: expr.ceil(),
        "sqrt": lambda expr, _: expr.sqrt(),
        "pow": lambda expr, exponent: expr.pow(exponent),
    }

    PYTHON_OPS = {
        "abs": lambda val, _arg: abs(val),
        "~": lambda val, _arg: not val,
        "is_null": lambda val, _arg: val is None,
        "is_not_null": lambda val, _arg: val is not None,
        "round": lambda val, decimals: None
        if val is None
        else _python_round(val, decimals),
        "floor": lambda val, _arg: None if val is None else math.floor(val),
        "ceil": lambda val, _arg: None if val is None else math.ceil(val),
        "sqrt": lambda val, _arg: _python_sqrt(val),
        "pow": lambda val, exponent: None if val is None else pow(val, exponent),
    }

    def __init__(self, op: builtins.str, operand: Any, arg: Any | None = None):
        self.op = op
        self.operand = operand
        self.arg = arg

    def _prepare_polars_arg(self) -> Any:
        if self.op == "round":
            decimals = 0 if self.arg is None else self.arg
            if not isinstance(decimals, int):
                raise TypeError("round() decimals must be an integer")
            return decimals
        if self.op == "pow":
            if self.arg is None:
                raise ValueError("pow() requires an exponent")
            if not isinstance(self.arg, (int, float)):
                raise TypeError("pow() exponent must be a number")
            return self.arg
        return self.arg

    def _prepare_python_arg(self, values: Any) -> Any:
        if self.op == "round":
            decimals = 0 if self.arg is None else self.arg
            if hasattr(decimals, "to_python"):
                decimals = decimals.to_python(values)
            if not isinstance(decimals, int):
                raise TypeError("round() decimals must be an integer")
            return decimals

        if self.op == "pow":
            if self.arg is None:
                raise ValueError("pow() requires an exponent")
            exponent = self.arg
            if hasattr(exponent, "to_python"):
                exponent = exponent.to_python(values)
            if not isinstance(exponent, (int, float)):
                raise TypeError("pow() exponent must be a number")
            return exponent

        return self.arg

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        operand_expr = self._to_polars(self.operand)
        if self.op not in self.POLARS_OPS:
            raise ValueError(f"Unknown unary op: {self.op}")
        arg = self._prepare_polars_arg()
        return self.POLARS_OPS[self.op](operand_expr, arg)

    def to_python(self, values: Any) -> Any:
        """Evaluate in Python context."""
        operand_val = self._to_python(self.operand, values)
        if self.op not in self.PYTHON_OPS:
            raise ValueError(f"Unknown unary op: {self.op}")
        arg_val = self._prepare_python_arg(values)
        return self.PYTHON_OPS[self.op](operand_val, arg_val)

    # Support chaining operations
    def __gt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">", other)

    def __ge__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">=", other)

    def __lt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<", other)

    def __le__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<=", other)

    def __eq__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> "BinaryOp":  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
        return BinaryOp(self, "!=", other)

    def __add__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "+", other)

    def __sub__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "-", other)

    def __mul__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "*", other)

    def __truediv__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "/", other)

    def __and__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "&", other)

    def __or__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "|", other)

    def abs(self) -> "UnaryOp":
        """Absolute value."""
        return UnaryOp("abs", self)

    def __invert__(self) -> "UnaryOp":
        """Negation."""
        return UnaryOp("~", self)

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this expression."""
        from .string import StringAccessor

        return StringAccessor(self)

    @property
    def dt(self) -> "DateTimeAccessor":
        """Access datetime operations on this expression."""
        from .datetime import DateTimeAccessor

        return DateTimeAccessor(self)
