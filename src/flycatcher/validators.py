"""Validator DSL for cross-platform (Polars + Pydantic) validation."""

from typing import Any, Callable

import polars as pl
from loguru import logger


class _ExpressionMixin:
    """Mixin providing common conversion methods for expressions."""

    def _to_polars(self, obj: Any) -> pl.Expr:
        """Convert object to Polars expression."""
        if hasattr(obj, "to_polars"):
            # obj has to_polars method, returns pl.Expr
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

    FieldRef objects are created using the `col()` function and support
    various operations that compile to both Polars and Pydantic validators.

    Primary purpose is to reference fields in custom/complex validation logic
    (e.g. `@model_validator` decorators).

    Parameters
    ----------
    name : str
        Name of the field to reference.

    Examples
    --------
        >>> from flycatcher import col
        >>> from flycatcher.base import model_validator
        >>> @model_validator
        ... def check_complex_logic():
        ...     return (col('age') >= 18) & (col('name') != '')
    """

    def __init__(self, name: str):
        self.name = name

    def to_polars(self) -> pl.Expr:
        """
        Compile to Polars expression.

        Returns
        -------
        pl.Expr
            A Polars column expression for this field.

        Examples
        --------
            >>> import polars as pl
            >>> ref = col('price')
            >>> expr = ref.to_polars()
            >>> # expr is equivalent to pl.col('price')
        """
        return pl.col(self.name)

    def to_python(self, values: Any) -> Any:
        """
        Evaluate in Python context.

        Parameters
        ----------
        values : Any
            Object with field value (dict, Pydantic model, or object with attribute).

        Returns
        -------
        Any
            The value of the referenced field.

        Raises
        ------
        AttributeError
            If the field is not found in the values object.

        Examples
        --------
            >>> ref = col('age')
            >>> ref.to_python({'age': 25})
            25
            >>> ref.to_python(type('obj', (), {'age': 30})())
            30
        """
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


class BinaryOp(_ExpressionMixin):
    """Binary operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
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


class UnaryOp(_ExpressionMixin):
    """Unary operation that can compile to both Polars and Python."""

    POLARS_OPS: dict[str, Callable[[pl.Expr], pl.Expr]] = {
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


def col(name: str) -> FieldRef:
    """
    Create a field reference for use in validator expressions.

    This is a convenience function that creates a `FieldRef` object.
    It's the primary way to reference fields in validator DSL expressions.

    Parameters
    ----------
    name : str
        Name of the field to reference.

    Returns
    -------
    FieldRef
        A field reference that can be used in expressions.

    Examples
    --------
        >>> from flycatcher import Schema, Integer, Float, col, model_validator
        >>> class ProductSchema(Schema):
        ...     price = Float()
        ...     discount = Float(nullable=True)
        ...
        ...     @model_validator
        ...     def check_discount():
        ...         return (
        ...             col('discount').is_null() | (col('discount') < col('price')),
        ...             "Discount must be less than price"
        ...         )

    See Also
    --------
    FieldRef : The class returned by this function.
    """
    return FieldRef(name)


class ValidatorResult:
    """
    Wrapper for validator results supporting multiple formats.

    Validator results can be in one of three formats:

    1. DSL expression - compiles to both Polars and Pydantic
    2. Dict with explicit 'polars' and/or 'pydantic' keys
    3. Tuple of (expression, error_message) for DSL expressions

    Parameters
    ----------
    result : Any
        The validator result in any supported format.

    Examples
    --------
    DSL expression (recommended):

        >>> from flycatcher import col
        >>> from flycatcher.validators import ValidatorResult
        >>> result = ValidatorResult(col('age') >= 18)
        >>> polars_expr, msg = result.get_polars_validator()
        >>> pydantic_validator = result.get_pydantic_validator()

    Explicit format:

        >>> import polars as pl
        >>> from flycatcher.validators import ValidatorResult
        >>> result = ValidatorResult({
        ...     'polars': (pl.col('age') >= 18, "Must be 18+"),
        ...     'pydantic': lambda v: v.age >= 18 or ValueError("Must be 18+")
        ... })
    """

    def __init__(self, result: Any):
        self.result = result

    def get_polars_validator(self) -> tuple[pl.Expr, str]:
        """
        Extract Polars validator as (expression, message) tuple.

        Returns
        -------
        tuple[pl.Expr, str]
            A tuple of (Polars expression, error message).

        Raises
        ------
        ValueError
            If the validator result format is invalid or missing 'polars' key.

        Examples
        --------
            >>> from flycatcher import col
            >>> from flycatcher.validators import ValidatorResult
            >>> result = ValidatorResult(col('age') >= 18)
            >>> expr, msg = result.get_polars_validator()
        """
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
        """
        Extract Pydantic validator callable, or None if not available.

        Returns
        -------
        Callable | None
            A Pydantic validator function, or None if no Pydantic validator
            is available (e.g., Polars-only validation).

        Examples
        --------
            >>> from flycatcher import col
            >>> from flycatcher.validators import ValidatorResult
            >>> result = ValidatorResult(col('age') >= 18)
            >>> validator = result.get_pydantic_validator()
            >>> validator is not None
            True
            >>> # Validator can be called (returns validated data)
            >>> validated = validator({'age': 20})
            >>> validated['age']
            20
        """
        if isinstance(self.result, dict):
            # Explicit format
            # NOTE: Do we want to raise an error if 'pydantic' is not present?
            if "pydantic" not in self.result:
                logger.warning(
                    "Dict validator does not have 'pydantic' key. "
                    "This validator will only be used for Polars validation."
                )
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
        """
        Check if Pydantic validator is available.

        Returns
        -------
        bool
            True if a Pydantic validator is available, False otherwise.

        Examples
        --------
            >>> from flycatcher import col
            >>> from flycatcher.validators import ValidatorResult
            >>> result = ValidatorResult(col('age') >= 18)
            >>> result.has_pydantic_validator()
            True
        """
        return self.get_pydantic_validator() is not None
