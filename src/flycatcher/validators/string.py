"""String operations for the validator DSL."""

from __future__ import annotations

import builtins
import re
from typing import Any, Callable

import polars as pl

from flycatcher.validators.datetime import DateTimeAccessor

from .base import _ExpressionMixin
from .membership import _MembershipMixin
from .ops import BinaryOp, UnaryOp


class StringAccessor:
    """Accessor for string operations on expressions."""

    def __init__(self, expr: Any):
        self.expr = expr

    def contains(self, pattern: str) -> "StringOp":
        return StringOp("contains", self.expr, pattern)

    def starts_with(self, prefix: str) -> "StringOp":
        return StringOp("starts_with", self.expr, prefix)

    def ends_with(self, suffix: str) -> "StringOp":
        return StringOp("ends_with", self.expr, suffix)

    def len_chars(self) -> "StringOp":
        return StringOp("len_chars", self.expr, None)

    def strip_chars(self) -> "StringOp":
        return StringOp("strip_chars", self.expr, None)

    def to_lowercase(self) -> "StringOp":
        return StringOp("to_lowercase", self.expr, None)

    def to_uppercase(self) -> "StringOp":
        return StringOp("to_uppercase", self.expr, None)

    def replace(self, pattern: str, value: str) -> "StringOp":
        return StringOp("replace", self.expr, (pattern, value))

    def extract(self, pattern: str, group_index: int = 0) -> "StringOp":
        return StringOp("extract", self.expr, (pattern, group_index))

    def slice(self, offset: int, length: int | None = None) -> "StringOp":
        return StringOp("slice", self.expr, (offset, length))

    def count_matches(self, pattern: str) -> "StringOp":
        return StringOp("count_matches", self.expr, pattern)


class StringOp(_ExpressionMixin, _MembershipMixin):
    """String operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[builtins.str, Callable[[pl.Expr, Any], pl.Expr]] = {
        "contains": lambda expr, pattern: expr.str.contains(pattern),
        "starts_with": lambda expr, prefix: expr.str.starts_with(prefix),
        "ends_with": lambda expr, suffix: expr.str.ends_with(suffix),
        "len_chars": lambda expr, _: expr.str.len_chars(),
        "strip_chars": lambda expr, _: expr.str.strip_chars(),
        "to_lowercase": lambda expr, _: expr.str.to_lowercase(),
        "to_uppercase": lambda expr, _: expr.str.to_uppercase(),
        "replace": lambda expr, args: expr.str.replace_all(args[0], args[1]),
        "extract": lambda expr, args: expr.str.extract(args[0], group_index=args[1]),
        "slice": lambda expr, args: (
            expr.str.slice(args[0], length=args[1])
            if len(args) > 1 and args[1] is not None
            else expr.str.slice(args[0])
        ),
        "count_matches": lambda expr, pattern: expr.str.count_matches(pattern),
    }

    PYTHON_OPS: dict[builtins.str, Callable[[builtins.str, Any], Any]] = {
        "contains": lambda val, pattern: (
            bool(re.search(pattern, val)) if val is not None else False
        ),
        "starts_with": (
            lambda val, prefix: val.startswith(prefix) if val is not None else False
        ),
        "ends_with": (
            lambda val, suffix: val.endswith(suffix) if val is not None else False
        ),
        "len_chars": lambda val, _: len(val) if val is not None else 0,
        "strip_chars": lambda val, _: val.strip() if val is not None else None,
        "to_lowercase": lambda val, _: val.lower() if val is not None else None,
        "to_uppercase": lambda val, _: val.upper() if val is not None else None,
        "replace": lambda val, args: (
            re.sub(args[0], args[1], val) if val is not None else None
        ),
        "extract": lambda val, args: (
            (lambda m: m.group(args[1]) if m else None)(re.search(args[0], val))
            if val is not None
            else None
        ),
        "slice": lambda val, args: (
            val[args[0] : args[0] + args[1]]
            if val is not None and len(args) > 1 and args[1] is not None
            else val[args[0] :]
            if val is not None
            else None
        ),
        "count_matches": lambda val, pattern: (
            len(re.findall(pattern, val)) if val is not None else 0
        ),
    }

    def __init__(self, op: builtins.str, operand: Any, arg: Any = None):
        self.op = op
        self.operand = operand
        self.arg = arg

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        operand_expr = self._to_polars(self.operand)
        if self.op not in self.POLARS_OPS:
            raise ValueError(f"Unknown string op: {self.op}")
        return self.POLARS_OPS[self.op](operand_expr, self.arg)

    def to_python(self, values: Any) -> Any:
        """Evaluate in Python context."""
        operand_val = self._to_python(self.operand, values)
        if self.op not in self.PYTHON_OPS:
            raise ValueError(f"Unknown string op: {self.op}")
        return self.PYTHON_OPS[self.op](operand_val, self.arg)

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
    def str(self) -> "StringAccessor":
        """Access string operations on this expression (for chaining)."""
        return StringAccessor(self)

    @property
    def dt(self) -> "DateTimeAccessor":
        """Access datetime operations on this expression (for chaining)."""
        from .datetime import DateTimeAccessor

        return DateTimeAccessor(self)

    def abs(self) -> UnaryOp:
        """Absolute value (for numeric results like len_chars)."""
        return UnaryOp("abs", self)

    def __invert__(self) -> UnaryOp:
        """Negation (for boolean results)."""
        return UnaryOp("~", self)
