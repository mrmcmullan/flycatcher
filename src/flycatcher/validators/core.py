"""Core validator DSL primitives (field refs, binary/unary ops, results)."""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any, Callable

import polars as pl
from loguru import logger

if TYPE_CHECKING:  # pragma: no cover
    from .string import StringAccessor


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


class FieldRef:
    """
    Reference to a field that can compile to Polars expressions and Python callables.
    """

    def __init__(self, name: builtins.str):
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

    def is_null(self) -> "UnaryOp":
        """Check if the field value is null/None."""
        return UnaryOp("is_null", self)

    def is_not_null(self) -> "UnaryOp":
        """Check if the field value is not null/None."""
        return UnaryOp("is_not_null", self)

    def __invert__(self) -> "UnaryOp":
        return UnaryOp("~", self)

    def abs(self) -> "UnaryOp":
        """Absolute value."""
        return UnaryOp("abs", self)

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this field."""
        from .string import StringAccessor

        return StringAccessor(self)


class BinaryOp(_ExpressionMixin):
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


class UnaryOp(_ExpressionMixin):
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


def col(name: str) -> FieldRef:
    """Create a field reference for use in validator expressions."""
    return FieldRef(name)


class ValidatorResult:
    """Wrapper for validator results supporting multiple formats."""

    def __init__(self, result: Any):
        self.result = result

    def get_polars_validator(self) -> tuple[pl.Expr, str]:
        """Extract Polars validator as (expression, message) tuple."""
        if isinstance(self.result, dict):
            if "polars" not in self.result:
                raise ValueError(
                    "Dict validator must have 'polars' key. "
                    f"Got keys: {list(self.result.keys())}"
                )
            polars_val = self.result["polars"]
            if isinstance(polars_val, tuple):
                return polars_val
            return (polars_val, "Validation failed")
        elif isinstance(self.result, tuple) and len(self.result) == 2:
            expr, msg = self.result
            if hasattr(expr, "to_polars"):
                return (expr.to_polars(), msg)
            elif isinstance(expr, pl.Expr):
                return (expr, msg)
            else:
                raise ValueError(
                    f"Invalid expression in tuple: {type(expr).__name__}. "
                    "Expected DSL expression or pl.Expr."
                )
        elif hasattr(self.result, "to_polars"):
            expr = self.result.to_polars()
            return (expr, "Validation failed")
        else:
            raise ValueError(
                f"Invalid validator result type: {type(self.result).__name__}. "
                "Expected dict, tuple of (expr, msg), or object with "
                "'to_polars' method."
            )

    def get_pydantic_validator(self) -> Any | None:
        """Extract Pydantic validator callable, or None if not available."""
        if isinstance(self.result, dict):
            if "pydantic" not in self.result:
                logger.warning(
                    "Dict validator does not have 'pydantic' key. "
                    "This validator will only be used for Polars validation."
                )
                return None
            return self.result["pydantic"]
        elif isinstance(self.result, tuple) and len(self.result) == 2:
            expr, msg = self.result
            if hasattr(expr, "to_python"):

                def validator(values: Any) -> Any:
                    try:
                        result = expr.to_python(values)
                        if not result:
                            raise ValueError(msg)
                        return values
                    except ValueError:
                        raise
                    except Exception as e:
                        raise ValueError(f"{msg}: {e}") from e

                return validator
            else:
                return None
        elif hasattr(self.result, "to_python"):

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
