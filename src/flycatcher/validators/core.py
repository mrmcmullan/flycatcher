"""Core validator DSL primitives (field refs, binary/unary ops, results)."""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any

import polars as pl
from loguru import logger

from .membership import _MembershipMixin
from .ops import BinaryOp, UnaryOp

if TYPE_CHECKING:  # pragma: no cover
    from .datetime import DateTimeAccessor
    from .string import StringAccessor


class FieldRef(_MembershipMixin):
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

    def __gt__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, ">", other)

    def __ge__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, ">=", other)

    def __lt__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "<", other)

    def __le__(self, other: Any) -> BinaryOp:
        return BinaryOp(self, "<=", other)

    def __eq__(self, other: Any) -> BinaryOp:  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
        return BinaryOp(self, "==", other)

    def __ne__(self, other: Any) -> BinaryOp:  # type: ignore[override]
        # Intentional override: DSL returns expression objects, not bool
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

    def is_null(self) -> UnaryOp:
        """Check if the field value is null/None."""
        return UnaryOp("is_null", self)

    def is_not_null(self) -> UnaryOp:
        """Check if the field value is not null/None."""
        return UnaryOp("is_not_null", self)

    def __invert__(self) -> UnaryOp:
        return UnaryOp("~", self)

    def abs(self) -> UnaryOp:
        """Absolute value."""
        return UnaryOp("abs", self)

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this field."""
        from .string import StringAccessor

        return StringAccessor(self)

    @property
    def dt(self) -> "DateTimeAccessor":
        """Access datetime operations on this field."""
        from .datetime import DateTimeAccessor

        return DateTimeAccessor(self)


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
