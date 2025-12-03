"""Field type definitions with custom validation support."""

import warnings
from datetime import date, datetime
from typing import Any, Callable

import polars as pl

# Sentinel value to distinguish "no default provided" from "default is None"
_MISSING = object()


class Field:
    """
    Base field class for schema definitions.

    All field types inherit from this class. Fields define the structure,
    constraints, and metadata for schema attributes.

    Parameters
    ----------
    primary_key : bool, default False
        Mark this field as the primary key (for database operations).
    nullable : bool, default False
        Allow None values for this field.
    default : Any, optional
        Default value for this field. Only applies to missing columns
        unless `fill_nulls=True` is used in validation.
    description : str, optional
        Human-readable description of this field.
    unique : bool, default False
        Enforce uniqueness constraint (for database operations).
    index : bool, default False
        Create an index on this field (for database operations).
    autoincrement : bool, optional
        Enable auto-increment for integer fields. If None, auto-detected
        for primary key integer fields.

    Examples
    --------
        >>> from flycatcher import Schema, Integer, String
        >>> class UserSchema(Schema):
        ...     id = Integer(primary_key=True, autoincrement=True)
        ...     email = String(unique=True, description="User email address")
        ...     age = Integer(nullable=True, default=0)
    """

    def __init__(
        self,
        *,
        primary_key: bool = False,
        nullable: bool = False,
        default: Any = _MISSING,
        description: str | None = None,
        unique: bool = False,
        index: bool = False,
        autoincrement: bool | None = None,
    ):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.description = description
        self.unique = unique
        self.index = index
        self.autoincrement = autoincrement
        self.name: str | None = None  # Set by Schema metaclass

        # Warn about ambiguous configuration
        if nullable and default is not _MISSING:
            # Defer warning until name is set by metaclass
            self._needs_warning = True
        else:
            self._needs_warning = False

        # Custom validators
        self.validators: list[Callable] = []

    def get_python_type(self) -> type:
        """Return the Python type for this field."""
        raise NotImplementedError

    def get_polars_dtype(self):
        """Return the Polars dtype for this field."""
        raise NotImplementedError

    def get_sqlalchemy_type(self):
        """Return the SQLAlchemy type for this field."""
        raise NotImplementedError

    def get_polars_constraints(self) -> list[tuple[Any, str]]:
        """
        Return list of (expression, error_message) tuples for validation.

        Each tuple contains a Polars expression that evaluates to a boolean mask
        and an error message to display when the constraint fails.

        Note: Subclasses that override this method should call super() first
        to ensure field name is set and warnings are emitted.
        """
        if self.name is None:
            raise RuntimeError(
                f"{self.__class__.__name__} constraints require field name "
                f"to be set by Schema metaclass"
            )

        # Emit warning about nullable + default now that name is set
        if self._needs_warning:
            warnings.warn(
                f"Field '{self.name}' is nullable=True with a default value. "
                f"Default will only be used for missing columns, not null values. "
                f"Use fill_nulls=True in validate() to replace nulls with defaults.",
                UserWarning,
                stacklevel=2,
            )
            self._needs_warning = False  # Only warn once

        return []

    def add_validator(self, func: Callable):
        """Add a custom validator function."""
        self.validators.append(func)
        return self


class Integer(Field):
    """
    Integer field type with numeric constraints.

    Parameters
    ----------
    gt : int, optional
        Value must be greater than this number.
    ge : int, optional
        Value must be greater than or equal to this number.
    lt : int, optional
        Value must be less than this number.
    le : int, optional
        Value must be less than or equal to this number.
    multiple_of : int, optional
        Value must be a multiple of this number.
    **kwargs
        Additional arguments passed to `Field` (primary_key, nullable, etc.).

    Examples
    --------
        >>> from flycatcher import Schema, Integer
        >>> class UserSchema(Schema):
        ...     age = Integer(ge=0, le=120)
        ...     score = Integer(gt=0, multiple_of=10)
        ...     id = Integer(primary_key=True, autoincrement=True)
    """

    def __init__(
        self,
        *,
        gt: int | None = None,  # Greater than
        ge: int | None = None,  # Greater than or equal
        lt: int | None = None,  # Less than
        le: int | None = None,  # Less than or equal
        multiple_of: int | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le
        self.multiple_of = multiple_of

    def get_python_type(self):
        return int

    def get_polars_dtype(self):
        return pl.Int64

    def get_sqlalchemy_type(self):
        from sqlalchemy import Integer as SAInteger

        return SAInteger

    def get_polars_constraints(self) -> list[tuple[Any, str]]:
        """Generate Polars validation expressions."""
        constraints = list(super().get_polars_constraints())
        assert self.name is not None  # Checked by base class
        col = pl.col(self.name)

        # Range constraints
        if self.gt is not None:
            constraints.append((col > self.gt, f"{self.name} must be > {self.gt}"))
        if self.ge is not None:
            constraints.append((col >= self.ge, f"{self.name} must be >= {self.ge}"))
        if self.lt is not None:
            constraints.append((col < self.lt, f"{self.name} must be < {self.lt}"))
        if self.le is not None:
            constraints.append((col <= self.le, f"{self.name} must be <= {self.le}"))

        # Multiple of constraint
        if self.multiple_of is not None:
            constraints.append(
                (
                    col % self.multiple_of == 0,
                    f"{self.name} must be multiple of {self.multiple_of}",
                )
            )

        return constraints

    def get_pydantic_field_kwargs(self) -> dict[str, Any]:
        """Return kwargs for Pydantic Field()."""
        kwargs = {}
        if self.gt is not None:
            kwargs["gt"] = self.gt
        if self.ge is not None:
            kwargs["ge"] = self.ge
        if self.lt is not None:
            kwargs["lt"] = self.lt
        if self.le is not None:
            kwargs["le"] = self.le
        if self.multiple_of is not None:
            kwargs["multiple_of"] = self.multiple_of
        return kwargs


class Float(Field):
    """
    Float field type with numeric constraints.

    Parameters
    ----------
    gt : float, optional
        Value must be greater than this number.
    ge : float, optional
        Value must be greater than or equal to this number.
    lt : float, optional
        Value must be less than this number.
    le : float, optional
        Value must be less than or equal to this number.
    **kwargs
        Additional arguments passed to `Field` (primary_key, nullable, etc.).

    Examples
    --------
        >>> from flycatcher import Schema, Float
        >>> class ProductSchema(Schema):
        ...     price = Float(gt=0.0)
        ...     discount = Float(ge=0.0, le=1.0, nullable=True)
    """

    def __init__(
        self,
        *,
        gt: float | None = None,
        ge: float | None = None,
        lt: float | None = None,
        le: float | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le

    def get_python_type(self):
        return float

    def get_polars_dtype(self):
        return pl.Float64

    def get_sqlalchemy_type(self):
        from sqlalchemy import Float as SAFloat

        return SAFloat

    def get_polars_constraints(self) -> list[tuple[Any, str]]:
        """Generate Polars validation expressions."""
        constraints = list(super().get_polars_constraints())
        assert self.name is not None  # Checked by base class
        col = pl.col(self.name)

        if self.gt is not None:
            constraints.append((col > self.gt, f"{self.name} must be > {self.gt}"))
        if self.ge is not None:
            constraints.append((col >= self.ge, f"{self.name} must be >= {self.ge}"))
        if self.lt is not None:
            constraints.append((col < self.lt, f"{self.name} must be < {self.lt}"))
        if self.le is not None:
            constraints.append((col <= self.le, f"{self.name} must be <= {self.le}"))

        return constraints

    def get_pydantic_field_kwargs(self) -> dict[str, Any]:
        """Return kwargs for Pydantic Field()."""
        kwargs = {}
        if self.gt is not None:
            kwargs["gt"] = self.gt
        if self.ge is not None:
            kwargs["ge"] = self.ge
        if self.lt is not None:
            kwargs["lt"] = self.lt
        if self.le is not None:
            kwargs["le"] = self.le
        return kwargs


class String(Field):
    r"""
    String field type with length and pattern constraints.

    Parameters
    ----------
    min_length : int, optional
        Minimum length of the string (inclusive).
    max_length : int, optional
        Maximum length of the string (inclusive).
    pattern : str, optional
        Regular expression pattern that the string must match.
    **kwargs
        Additional arguments passed to `Field` (primary_key, nullable, etc.).

    Examples
    --------
        >>> from flycatcher import Schema, String
        >>> class UserSchema(Schema):
        ...     name = String(min_length=1, max_length=100)
        ...     email = String(pattern=r'^[^@]+@[^@]+\.[^@]+$')
        ...     bio = String(max_length=500, nullable=True)
    """

    def __init__(
        self,
        *,
        max_length: int | None = None,
        min_length: int | None = None,
        pattern: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.max_length = max_length
        self.min_length = min_length
        self.pattern = pattern

    def get_python_type(self):
        return str

    def get_polars_dtype(self):
        return pl.Utf8

    def get_sqlalchemy_type(self):
        from sqlalchemy import String as SAString
        from sqlalchemy import Text

        if self.max_length:
            return SAString(self.max_length)
        return Text

    def get_polars_constraints(self) -> list[tuple[Any, str]]:
        """Generate Polars validation expressions."""
        constraints = list(super().get_polars_constraints())
        assert self.name is not None  # Checked by base class
        col = pl.col(self.name)

        # Length constraints
        if self.min_length is not None:
            constraints.append(
                (
                    col.str.len_chars() >= self.min_length,
                    f"{self.name} must have at least {self.min_length} characters",
                )
            )
        if self.max_length is not None:
            constraints.append(
                (
                    col.str.len_chars() <= self.max_length,
                    f"{self.name} must have at most {self.max_length} characters",
                )
            )

        # Regex pattern
        if self.pattern is not None:
            constraints.append(
                (
                    col.str.contains(self.pattern),
                    f"{self.name} must match pattern: {self.pattern}",
                )
            )

        return constraints

    def get_pydantic_field_kwargs(self) -> dict[str, Any]:
        """Return kwargs for Pydantic Field()."""
        kwargs: dict[str, Any] = {}
        if self.min_length is not None:
            kwargs["min_length"] = self.min_length
        if self.max_length is not None:
            kwargs["max_length"] = self.max_length
        if self.pattern is not None:
            kwargs["pattern"] = self.pattern
        return kwargs


class Boolean(Field):
    """
    Boolean field type.

    Examples
    --------
        >>> from flycatcher import Schema, Boolean
        >>> class UserSchema(Schema):
        ...     is_active = Boolean(default=True)
        ...     is_verified = Boolean(nullable=True)
    """

    def get_python_type(self):
        return bool

    def get_polars_dtype(self):
        return pl.Boolean

    def get_sqlalchemy_type(self):
        from sqlalchemy import Boolean as SABoolean

        return SABoolean


class Datetime(Field):
    """
    Datetime field type for datetime.datetime values.

    Examples
    --------
        >>> from flycatcher import Schema, Datetime
        >>> from datetime import datetime
        >>> class EventSchema(Schema):
        ...     created_at = Datetime()
        ...     updated_at = Datetime(nullable=True)
    """

    def __init__(
        self,
        *,
        gt: datetime | None = None,  # Greater than
        ge: datetime | None = None,  # Greater than or equal
        lt: datetime | None = None,  # Less than
        le: datetime | None = None,  # Less than or equal
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le

    def get_python_type(self):
        return datetime

    def get_polars_dtype(self):
        return pl.Datetime

    def get_sqlalchemy_type(self):
        from sqlalchemy import DateTime

        return DateTime

    def get_polars_constraints(self) -> list[tuple[Any, str]]:
        """Generate Polars validation expressions."""
        constraints = list(super().get_polars_constraints())
        assert self.name is not None  # Checked by base class
        col = pl.col(self.name)

        if self.gt is not None:
            constraints.append(
                (col > self.gt, f"{self.name} must be > {self.gt.isoformat()}")
            )
        if self.ge is not None:
            constraints.append(
                (col >= self.ge, f"{self.name} must be >= {self.ge.isoformat()}")
            )
        if self.lt is not None:
            constraints.append(
                (col < self.lt, f"{self.name} must be < {self.lt.isoformat()}")
            )
        if self.le is not None:
            constraints.append(
                (col <= self.le, f"{self.name} must be <= {self.le.isoformat()}")
            )

        return constraints

    def get_pydantic_field_kwargs(self) -> dict[str, Any]:
        """Return kwargs for Pydantic Field()."""
        kwargs: dict[str, Any] = {}
        if self.gt is not None:
            kwargs["gt"] = self.gt
        if self.ge is not None:
            kwargs["ge"] = self.ge
        if self.lt is not None:
            kwargs["lt"] = self.lt
        if self.le is not None:
            kwargs["le"] = self.le
        return kwargs


class Date(Field):
    """
    Date field type for datetime.date values.

    Examples
    --------
        >>> from flycatcher import Schema, Date
        >>> from datetime import date
        >>> class BookingSchema(Schema):
        ...     check_in = Date()
        ...     check_out = Date()
    """

    def get_python_type(self):
        return date

    def get_polars_dtype(self):
        return pl.Date

    def get_sqlalchemy_type(self):
        from sqlalchemy import Date as SADate

        return SADate
