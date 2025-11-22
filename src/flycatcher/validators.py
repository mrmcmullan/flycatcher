"""Validator DSL for cross-platform (Polars + Pydantic) validation."""

from typing import Any

import polars as pl


class _ExpressionMixin:
    """Mixin providing common conversion methods for expressions."""

    def _to_polars(self, obj: Any) -> pl.Expr:
        """Convert object to Polars expression."""
        if hasattr(obj, "to_polars"):
            return obj.to_polars()
        return pl.lit(obj)

    def _to_python(self, obj: Any, values: Any) -> Any:
        """Convert object to Python value."""
        if hasattr(obj, "to_python"):
            return obj.to_python(values)
        return obj


class FieldRef:
    """
    Reference to a field that can compile to Polars expressions and Python callables.

    Usage:
        fl.col('price') > 0
        fl.col('check_out') > fl.col('check_in')
    """

    def __init__(self, name: str):
        self.name = name

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        return pl.col(self.name)

    def to_python(self, values: Any) -> Any:
        """Evaluate in Python context."""
        if hasattr(values, self.name):
            return getattr(values, self.name)
        try:
            return values[self.name]
        except (KeyError, TypeError) as e:
            raise AttributeError(f"Field '{self.name}' not found in values") from e

    def __gt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">", other)

    def __ge__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, ">=", other)

    def __lt__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<", other)

    def __le__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "<=", other)

    def __eq__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> "BinaryOp":
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

    def __invert__(self) -> "UnaryOp":
        return UnaryOp("~", self)

    def abs(self) -> "UnaryOp":
        """Absolute value."""
        return UnaryOp("abs", self)


class BinaryOp(_ExpressionMixin):
    """Binary operation that can compile to both Polars and Python."""

    POLARS_OPS = {
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

    def __init__(self, left: Any, op: str, right: Any):
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

    def __eq__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> "BinaryOp":
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


class UnaryOp(_ExpressionMixin):
    """Unary operation that can compile to both Polars and Python."""

    POLARS_OPS = {
        "abs": lambda expr: expr.abs(),
        "~": lambda expr: ~expr,
    }

    PYTHON_OPS = {
        "abs": abs,
        "~": lambda val: not val,
    }

    def __init__(self, op: str, operand: Any):
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

    def __eq__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> "BinaryOp":
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


# Convenience alias
col = FieldRef


class ValidatorResult:
    """
    Wrapper for validator results that can be either:
    1. DSL expression (compiles to both)
    2. Dict with explicit polars/pydantic implementations
    """

    def __init__(self, result: Any):
        self.result = result

    def get_polars_validator(self) -> tuple[pl.Expr, str]:
        """Extract Polars validator as (expression, message) tuple."""
        if isinstance(self.result, dict):
            # Explicit format: {'polars': ..., 'pydantic': ...}
            if "polars" not in self.result:
                raise ValueError(
                    "Dict validator must have 'polars' key. "
                    f"Got keys: {list(self.result.keys())}"
                )
            polars_val = self.result["polars"]
            # Handle both (expr, msg) tuple and just expr
            if isinstance(polars_val, tuple):
                return polars_val
            return (polars_val, "Validation failed")
        elif hasattr(self.result, "to_polars"):
            # DSL format: compile to Polars
            expr = self.result.to_polars()
            return (expr, "Validation failed")
        else:
            raise ValueError(
                f"Invalid validator result type: {type(self.result).__name__}. "
                "Expected dict, or object with 'to_polars' method."
            )

    def get_pydantic_validator(self) -> Any | None:
        """Extract Pydantic validator callable, or None if not available."""
        if isinstance(self.result, dict):
            # Explicit format
            if "pydantic" not in self.result:
                return None  # Polars-only
            return self.result["pydantic"]
        elif hasattr(self.result, "to_python"):
            # DSL format: compile to Python
            def validator(values: Any) -> Any:
                try:
                    result = self.result.to_python(values)
                    if not result:
                        raise ValueError("Validation failed")
                    return values
                except Exception as e:
                    raise ValueError(f"Validation failed: {e}") from e

            return validator
        else:
            return None

    def has_pydantic_validator(self) -> bool:
        """Check if Pydantic validator is available."""
        return self.get_pydantic_validator() is not None
