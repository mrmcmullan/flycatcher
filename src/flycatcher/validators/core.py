"""Core validator DSL primitives (field refs, binary/unary ops, results)."""

from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any

import polars as pl
from loguru import logger

from .membership import _MembershipMixin
from .ops import BinaryOp, UnaryOp, _MathOpsMixin

if TYPE_CHECKING:  # pragma: no cover
    from .datetime import DateTimeAccessor
    from .string import StringAccessor


class FieldRef(_MathOpsMixin, _MembershipMixin):
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

    def fill_null(self, value: Any) -> UnaryOp:
        """Replace null values with the specified value."""
        return UnaryOp("fill_null", self, value)

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


def col(*names: str) -> "FieldRef | MultiColumnRef":
    """
    Create a field reference for use in validator expressions.

    Can reference single or multiple columns. Multiple columns are useful
    for Polars-style operations that apply to several columns at once.

    Parameters
    ----------
    *names : str
        One or more column names. If a single name starting with '^' and
        ending with '$' is provided, it's treated as a regex pattern.

    Returns
    -------
    FieldRef | MultiColumnRef
        Single field reference for one name, multi-column reference for multiple.

    Examples
    --------
    Single column:

        >>> col('age') >= 18

    Multiple columns (Polars-style):

        >>> col('price', 'discount').is_not_null()

    Regex pattern (Polars-style):

        >>> col('^amount_.*$') > 0
    """
    if len(names) == 1:
        name = names[0]
        # Check for regex pattern (^...$)
        if name.startswith("^") and name.endswith("$"):
            return MultiColumnRef(pattern=name)
        return FieldRef(name)
    elif len(names) > 1:
        return MultiColumnRef(columns=list(names))
    else:
        raise ValueError("col() requires at least one column name")


class MultiColumnRef:
    """
    Reference to multiple columns for Polars-style multi-column operations.

    This is primarily useful for Polars validation where operations can
    apply to multiple columns simultaneously. For Pydantic validation,
    multi-column references are not directly supported.
    """

    def __init__(
        self,
        columns: list[str] | None = None,
        pattern: str | None = None,
    ):
        self.columns = columns
        self.pattern = pattern

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression selecting multiple columns."""
        if self.pattern:
            return pl.col(self.pattern)
        elif self.columns:
            return pl.col(*self.columns)
        raise ValueError("MultiColumnRef requires columns or pattern")

    def to_python(self, values: Any) -> Any:
        """
        Evaluate in Python context.

        For multi-column references, returns a dict of column values.
        """
        if self.pattern:
            import re

            regex = re.compile(self.pattern)
            if hasattr(values, "__dict__"):
                return {
                    k: v
                    for k, v in vars(values).items()
                    if regex.match(k) and not k.startswith("_")
                }
            elif isinstance(values, dict):
                return {k: v for k, v in values.items() if regex.match(k)}
            raise TypeError("Cannot apply regex pattern to non-dict values")

        elif self.columns:
            result = {}
            for col_name in self.columns:
                if hasattr(values, col_name):
                    result[col_name] = getattr(values, col_name)
                elif isinstance(values, dict):
                    result[col_name] = values.get(col_name)
                else:
                    raise AttributeError(f"Field '{col_name}' not found")
            return result

        raise ValueError("MultiColumnRef requires columns or pattern")

    def is_not_null(self) -> "MultiColumnOp":
        """Check if all referenced columns are not null."""
        return MultiColumnOp("is_not_null", self)

    def is_null(self) -> "MultiColumnOp":
        """Check if any referenced columns are null."""
        return MultiColumnOp("is_null", self)

    def __gt__(self, other: Any) -> "MultiColumnOp":
        return MultiColumnOp(">", self, other)

    def __ge__(self, other: Any) -> "MultiColumnOp":
        return MultiColumnOp(">=", self, other)

    def __lt__(self, other: Any) -> "MultiColumnOp":
        return MultiColumnOp("<", self, other)

    def __le__(self, other: Any) -> "MultiColumnOp":
        return MultiColumnOp("<=", self, other)

    def __eq__(self, other: Any) -> "MultiColumnOp":  # type: ignore[override]
        return MultiColumnOp("==", self, other)

    def __ne__(self, other: Any) -> "MultiColumnOp":  # type: ignore[override]
        return MultiColumnOp("!=", self, other)


class MultiColumnOp:
    """Operation applied to multiple columns."""

    def __init__(self, op: str, ref: MultiColumnRef, value: Any = None):
        self.op = op
        self.ref = ref
        self.value = value

    def to_polars(self) -> pl.Expr:
        """Compile to Polars expression."""
        expr = self.ref.to_polars()

        if self.op == "is_null":
            return expr.is_null()
        elif self.op == "is_not_null":
            return expr.is_not_null()
        elif self.op == ">":
            return expr > self.value
        elif self.op == ">=":
            return expr >= self.value
        elif self.op == "<":
            return expr < self.value
        elif self.op == "<=":
            return expr <= self.value
        elif self.op == "==":
            return expr == self.value
        elif self.op == "!=":
            return expr != self.value

        raise ValueError(f"Unknown multi-column op: {self.op}")

    def to_python(self, values: Any) -> Any:
        """
        Evaluate in Python context.

        Returns True only if the condition holds for ALL columns.
        """
        col_values = self.ref.to_python(values)

        if self.op == "is_null":
            return all(v is None for v in col_values.values())
        elif self.op == "is_not_null":
            return all(v is not None for v in col_values.values())
        elif self.op == ">":
            return all(v > self.value for v in col_values.values() if v is not None)
        elif self.op == ">=":
            return all(v >= self.value for v in col_values.values() if v is not None)
        elif self.op == "<":
            return all(v < self.value for v in col_values.values() if v is not None)
        elif self.op == "<=":
            return all(v <= self.value for v in col_values.values() if v is not None)
        elif self.op == "==":
            return all(v == self.value for v in col_values.values())
        elif self.op == "!=":
            return all(v != self.value for v in col_values.values())

        raise ValueError(f"Unknown multi-column op: {self.op}")

    def __and__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "&", other)

    def __or__(self, other: Any) -> "BinaryOp":
        return BinaryOp(self, "|", other)


class Validator:
    """
    Typed validator for explicit dual Polars/Pydantic implementations.

    Use this when you need different validation logic for Polars (DataFrame)
    and Pydantic (row-level) validation. This is cleaner than the dict syntax
    and provides better IDE support.

    Parameters
    ----------
    polars : pl.Expr
        Polars expression for DataFrame validation.
    pydantic : Callable[[Any], bool], optional
        Python callable for Pydantic validation. Receives the model instance
        and should return True if valid. If None, validation is Polars-only.
    message : str, default "Validation failed"
        Error message when validation fails.

    Examples
    --------
    Different logic for Polars vs Pydantic:

        >>> from flycatcher import Schema, model_validator, Validator
        >>> import polars as pl
        >>>
        >>> class StatsSchema(Schema):
        ...     price: float
        ...     threshold: float
        ...
        ...     @model_validator
        ...     def check_price():
        ...         return Validator(
        ...             polars=pl.col('price').rolling_mean(5) > pl.col('threshold'),
        ...             pydantic=lambda v: v.price > v.threshold,  # Simplified
        ...             message="Price must exceed threshold"
        ...         )

    Polars-only validation:

        >>> @model_validator
        ... def check_rolling():
        ...     return Validator(
        ...         polars=pl.col('value').rolling_std(10) < 1.0,
        ...         message="Rolling std must be < 1.0"
        ...     )
    """

    def __init__(
        self,
        polars: pl.Expr,
        pydantic: Any | None = None,
        message: str = "Validation failed",
    ):
        self.polars = polars
        self.pydantic = pydantic
        self.message = message

    def get_polars(self) -> tuple[pl.Expr, str]:
        """Get Polars expression and message."""
        return (self.polars, self.message)

    def get_pydantic(self) -> Any | None:
        """Get Pydantic validator callable, or None if not available."""
        if self.pydantic is None:
            logger.info(
                f"Validator has no pydantic callable. "
                f"Validation '{self.message}' will only run on DataFrames."
            )
            return None

        def validator(values: Any) -> Any:
            try:
                result = self.pydantic(values)
                if not result:
                    raise ValueError(self.message)
                return values
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"{self.message}: {e}") from e

        return validator


class ValidatorResult:
    """Wrapper for validator results supporting multiple formats."""

    def __init__(self, result: Any):
        self.result = result

    def get_polars_validator(self) -> tuple[pl.Expr, str]:
        """Extract Polars validator as (expression, message) tuple."""
        # Handle Validator class (preferred typed approach)
        if isinstance(self.result, Validator):
            return self.result.get_polars()
        elif isinstance(self.result, dict):
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
                "Expected Validator, dict, tuple of (expr, msg), or object with "
                "'to_polars' method."
            )

    def get_pydantic_validator(self) -> Any | None:
        """Extract Pydantic validator callable, or None if not available."""
        # Handle Validator class (preferred typed approach)
        if isinstance(self.result, Validator):
            return self.result.get_pydantic()
        elif isinstance(self.result, dict):
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
