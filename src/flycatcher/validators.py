"""Validator DSL for cross-platform (Polars + Pydantic) validation."""

import re
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

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this field."""
        return StringAccessor(self)


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

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this expression."""
        return StringAccessor(self)


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

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this expression."""
        return StringAccessor(self)


class StringAccessor:
    """
    Accessor for string operations on expressions.

    Provides Polars-style string methods that compile to both Polars and Python.

    Examples
    --------
        >>> from flycatcher import col, model_validator
        >>> @model_validator
        ... def check_email():
        ...     return col('email').str.contains('@')
        >>> @model_validator
        ... def check_name():
        ...     return col('name').str.starts_with('Dr.')
        >>> @model_validator
        ... def check_tag_length():
        ...     return col('tag').str.len_chars() <= 20
    """

    def __init__(self, expr: Any):
        """Initialize with the expression to operate on."""
        self.expr = expr

    def contains(self, pattern: str) -> "StringOp":
        """
        Check if string contains pattern.

        Parameters
        ----------
        pattern : str
            Pattern to search for (supports regex).

        Returns
        -------
        StringOp
            Boolean expression that can be used in validators.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_email():
            ...     return col('email').str.contains('@')
        """
        return StringOp("contains", self.expr, pattern)

    def starts_with(self, prefix: str) -> "StringOp":
        """
        Check if string starts with prefix.

        Parameters
        ----------
        prefix : str
            Prefix to check for.

        Returns
        -------
        StringOp
            Boolean expression that can be used in validators.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_name():
            ...     return col('name').str.starts_with('Dr.')
        """
        return StringOp("starts_with", self.expr, prefix)

    def ends_with(self, suffix: str) -> "StringOp":
        """
        Check if string ends with suffix.

        Parameters
        ----------
        suffix : str
            Suffix to check for.

        Returns
        -------
        StringOp
            Boolean expression that can be used in validators.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_email_domain():
            ...     return col('email').str.ends_with('.com')
        """
        return StringOp("ends_with", self.expr, suffix)

    def len_chars(self) -> "StringOp":
        """
        Get the length of the string in characters.

        Returns
        -------
        StringOp
            Numeric expression representing string length.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_tag_length():
            ...     return col('tag').str.len_chars() <= 20
        """
        return StringOp("len_chars", self.expr, None)

    def strip_chars(self) -> "StringOp":
        """
        Remove leading and trailing whitespace.

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_name_not_empty():
            ...     # Check that name has content after stripping whitespace
            ...     return col('name').str.strip_chars().str.len_chars() > 0
        """
        return StringOp("strip_chars", self.expr, None)

    def to_lowercase(self) -> "StringOp":
        """
        Convert string to lowercase.

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_email_lowercase():
            ...     # Validate that email is already lowercase
            ...     return col('email').str.to_lowercase() == col('email')
        """
        return StringOp("to_lowercase", self.expr, None)

    def to_uppercase(self) -> "StringOp":
        """
        Convert string to uppercase.

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_code_uppercase():
            ...     # Validate that code is already uppercase
            ...     return col('code').str.to_uppercase() == col('code')
        """
        return StringOp("to_uppercase", self.expr, None)

    def replace(self, pattern: str, value: str) -> "StringOp":
        """
        Replace matching substrings with a new value.

        Parameters
        ----------
        pattern : str
            Pattern to match (supports regex).
        value : str
            Replacement value.

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_phone_format():
            ...     # Validate that phone has exactly 10 digits when cleaned
            ...     cleaned = col('phone').str.replace(r'[^\\d]', '')
            ...     return cleaned.str.len_chars() == 10
        """
        return StringOp("replace", self.expr, (pattern, value))

    def extract(self, pattern: str, group_index: int = 0) -> "StringOp":
        """
        Extract the target capture group from provided pattern.

        Parameters
        ----------
        pattern : str
            Regex pattern with capture groups.
        group_index : int, default 0
            Index of the capture group to extract (0 = full match).

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_email_has_domain():
            ...     # Validate that email has a domain (extract and check it exists)
            ...     domain = col('email').str.extract(r'@(.+)', 1)
            ...     return domain.is_not_null()
        """
        return StringOp("extract", self.expr, (pattern, group_index))

    def slice(self, offset: int, length: int | None = None) -> "StringOp":
        """
        Extract a substring from each string value.

        Parameters
        ----------
        offset : int
            Starting position (0-indexed).
        length : int, optional
            Length of substring. If None, extracts to end.

        Returns
        -------
        StringOp
            String expression that can be chained.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_code_prefix():
            ...     # Validate that code starts with 'ABC'
            ...     prefix = col('code').str.slice(0, 3)
            ...     return prefix == 'ABC'
        """
        return StringOp("slice", self.expr, (offset, length))

    def count_matches(self, pattern: str) -> "StringOp":
        """
        Count all successive non-overlapping regex matches.

        Parameters
        ----------
        pattern : str
            Regex pattern to match.

        Returns
        -------
        StringOp
            Numeric expression representing match count.

        Examples
        --------
            >>> from flycatcher import col, model_validator
            >>> @model_validator
            ... def check_has_numbers():
            ...     return col('text').str.count_matches(r'\\d+') >= 1
        """
        return StringOp("count_matches", self.expr, pattern)


class StringOp(_ExpressionMixin):
    """
    String operation that can compile to both Polars and Python.

    Supports both boolean-returning operations (contains, starts_with, etc.)
    and string-returning operations (strip, lower, upper) that can be chained.
    """

    # Polars string operations
    POLARS_OPS: dict[str, Callable[[pl.Expr, Any], pl.Expr]] = {
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

    # Python string operations
    PYTHON_OPS: dict[str, Callable[[str, Any], Any]] = {
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

    def __init__(self, op: str, operand: Any, arg: Any = None):
        """
        Initialize string operation.

        Parameters
        ----------
        op : str
            Operation name (contains, starts_with, etc.).
        operand : Any
            The expression to operate on.
        arg : Any, optional
            Additional argument for operations like contains(pattern).
        """
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

    # Support chaining operations - string ops can be compared, used in binary ops, etc.
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

    @property
    def str(self) -> "StringAccessor":
        """Access string operations on this expression (for chaining)."""
        return StringAccessor(self)

    def abs(self) -> "UnaryOp":
        """Absolute value (for numeric results like len_chars)."""
        return UnaryOp("abs", self)

    def __invert__(self) -> "UnaryOp":
        """Negation (for boolean results)."""
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
