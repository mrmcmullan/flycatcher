"""Binary & unary operations for the validator DSL."""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from .base import _ExpressionMixin
from .membership import _MembershipMixin

if TYPE_CHECKING:  # pragma: no cover
    from .datetime import DateTimeAccessor
    from .string import StringAccessor


class BinaryOp(_ExpressionMixin, _MembershipMixin):
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


class UnaryOp(_ExpressionMixin, _MembershipMixin):
    """Unary operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[builtins.str, Callable[[pl.Expr], pl.Expr]] = {
        "abs": lambda expr: expr.abs(),
        "~": lambda expr: ~expr,
        "is_null": lambda expr: expr.is_null(),
        "is_not_null": lambda expr: expr.is_not_null(),
    }

    PYTHON_OPS = {
        "abs": abs,
        "~": lambda val: not val,
        "is_null": lambda val: val is None,
        "is_not_null": lambda val: val is not None,
    }

    def __init__(self, op: builtins.str, operand: Any):
        self.op = op
        self.operand = operand

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        operand_expr = self._to_polars(self.operand)
        if self.op not in self.POLARS_OPS:
            raise ValueError(f"Unknown unary op: {self.op}")
        return self.POLARS_OPS[self.op](operand_expr)

    def to_python(self, values: Any) -> Any:
        """Evaluate in Python context."""
        operand_val = self._to_python(self.operand, values)
        if self.op not in self.PYTHON_OPS:
            raise ValueError(f"Unknown unary op: {self.op}")
        return self.PYTHON_OPS[self.op](operand_val)

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
