"""Field type definitions with custom validation support."""

import warnings
from datetime import date, datetime
from typing import Any, Callable

import polars as pl

# Sentinel value to distinguish "no default provided" from "default is None"
_MISSING = object()

# Type mapping from Python types to Field classes (populated at module end)
_TYPE_MAP: dict[type, type["FieldBase"]] = {}


class FieldInfo:
    """
    Stores field metadata and constraints from Field() function calls.

    This class is used internally to capture constraints specified via the
    Pydantic-style Field() function, which are then merged with the appropriate
    Field subclass based on the type annotation.

    Users should use the Field() function, not this class directly.
    """

    def __init__(
        self,
        *,
        # Base field options
        primary_key: bool = False,
        nullable: bool = False,
        default: Any = _MISSING,
        description: str | None = None,
        unique: bool = False,
        index: bool = False,
        autoincrement: bool | None = None,
        # Numeric constraints (Integer, Float, Datetime)
        gt: int | float | datetime | None = None,
        ge: int | float | datetime | None = None,
        lt: int | float | datetime | None = None,
        le: int | float | datetime | None = None,
        multiple_of: int | None = None,
        # String constraints
        min_length: int | None = None,
        max_length: int | None = None,
        pattern: str | None = None,
    ):
        # Base options
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.description = description
        self.unique = unique
        self.index = index
        self.autoincrement = autoincrement

        # Numeric constraints
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le
        self.multiple_of = multiple_of

        # String constraints
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = pattern

    def to_field_kwargs(self) -> dict[str, Any]:
        """Convert to kwargs dict for Field subclass constructors."""
        kwargs: dict[str, Any] = {}

        # Base options (always applicable)
        kwargs["primary_key"] = self.primary_key
        kwargs["nullable"] = self.nullable
        if self.default is not _MISSING:
            kwargs["default"] = self.default
        if self.description is not None:
            kwargs["description"] = self.description
        kwargs["unique"] = self.unique
        kwargs["index"] = self.index
        if self.autoincrement is not None:
            kwargs["autoincrement"] = self.autoincrement

        # Numeric constraints (only include if set)
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

        # String constraints (only include if set)
        if self.min_length is not None:
            kwargs["min_length"] = self.min_length
        if self.max_length is not None:
            kwargs["max_length"] = self.max_length
        if self.pattern is not None:
            kwargs["pattern"] = self.pattern

        return kwargs


def Field(  # noqa: N802 - Capitalized to match Pydantic's Field() API
    default: Any = _MISSING,
    *,
    # Base field options
    primary_key: bool = False,
    nullable: bool = False,
    description: str | None = None,
    unique: bool = False,
    index: bool = False,
    autoincrement: bool | None = None,
    # Numeric constraints (Integer, Float, Datetime)
    gt: int | float | datetime | None = None,
    ge: int | float | datetime | None = None,
    lt: int | float | datetime | None = None,
    le: int | float | datetime | None = None,
    multiple_of: int | None = None,
    # String constraints
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
) -> Any:
    """
    Declare field metadata and constraints for Pydantic-style schema definitions.

    Use this function with type annotations to define schema fields with
    constraints, similar to Pydantic's Field() function.

    Parameters
    ----------
    default : Any, optional
        Default value for the field. Can be provided as first positional argument.
    primary_key : bool, default False
        Mark this field as the primary key (for database operations).
    nullable : bool, default False
        Allow None values for this field.
    description : str, optional
        Human-readable description of this field.
    unique : bool, default False
        Enforce uniqueness constraint (for database operations).
    index : bool, default False
        Create an index on this field (for database operations).
    autoincrement : bool, optional
        Enable auto-increment for integer fields.
    gt : numeric, optional
        Value must be greater than this (for int, float, datetime fields).
    ge : numeric, optional
        Value must be greater than or equal to this.
    lt : numeric, optional
        Value must be less than this.
    le : numeric, optional
        Value must be less than or equal to this.
    multiple_of : int, optional
        Value must be a multiple of this (for integer fields).
    min_length : int, optional
        Minimum string length (for string fields).
    max_length : int, optional
        Maximum string length (for string fields).
    pattern : str, optional
        Regex pattern the string must match (for string fields).

    Returns
    -------
    FieldInfo
        A FieldInfo instance that will be processed by the Schema metaclass.

    Examples
    --------
    Basic usage with type annotations:

        >>> from flycatcher import Schema, Field
        >>> from datetime import datetime
        >>> class UserSchema(Schema):
        ...     # Simple fields - just annotations
        ...     name: str
        ...     created_at: datetime
        ...
        ...     # Fields with defaults
        ...     is_active: bool = True
        ...
        ...     # Nullable fields
        ...     bio: str | None = None
        ...
        ...     # Fields with constraints
        ...     age: int = Field(ge=0, le=120)
        ...     email: str = Field(pattern=r'^[^@]+@[^@]+\\.[^@]+$')
        ...
        ...     # Database-specific options
        ...     id: int = Field(primary_key=True, autoincrement=True)

    With default value as positional argument:

        >>> class ConfigSchema(Schema):
        ...     timeout: int = Field(default=30, ge=1, le=300)
        ...     retries: int = Field(default=3, ge=0)
    """
    return FieldInfo(
        primary_key=primary_key,
        nullable=nullable,
        default=default,
        description=description,
        unique=unique,
        index=index,
        autoincrement=autoincrement,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
    )


class FieldBase:
    """
    Base field class for schema definitions.

    All field types inherit from this class. Fields define the structure,
    constraints, and metadata for schema attributes.

    This class is used internally. For the public API, use type annotations
    with the Field() function for Pydantic-style definitions, or use the
    typed field classes (Integer, String, etc.) directly.

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
    Pydantic-style:

        >>> from flycatcher import Schema, Field
        >>> class UserSchema(Schema):
        ...     id: int = Field(primary_key=True, autoincrement=True)
        ...     email: str = Field(unique=True, description="User email address")
        ...     age: int | None = Field(default=0)
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


class Integer(FieldBase):
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
        >>> from flycatcher import Field, Schema
        >>> class UserSchema(Schema):
        ...     age: int = Field(ge=0, le=120)
        ...     score: int = Field(gt=0, multiple_of=10)
        ...     id: int = Field(primary_key=True, autoincrement=True)
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


class Float(FieldBase):
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
        >>> from flycatcher import Field, Schema
        >>> class ProductSchema(Schema):
        ...     price: float = Field(gt=0.0)
        ...     discount: float | None = Field(default=None, ge=0.0, le=1.0)
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


class String(FieldBase):
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
        >>> from flycatcher import Field, Schema
        >>> class UserSchema(Schema):
        ...     name: str = Field(min_length=1, max_length=100)
        ...     email: str = Field(pattern=r'^[^@]+@[^@]+\.[^@]+$')
        ...     bio: str | None = Field(default=None, max_length=500)
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


class Boolean(FieldBase):
    """
    Boolean field type.

    Examples
    --------
        >>> from flycatcher import Field, Schema
        >>> class UserSchema(Schema):
        ...     is_active: bool = True
        ...     is_verified: bool | None = None
    """

    def get_python_type(self):
        return bool

    def get_polars_dtype(self):
        return pl.Boolean

    def get_sqlalchemy_type(self):
        from sqlalchemy import Boolean as SABoolean

        return SABoolean


class Datetime(FieldBase):
    """
    Datetime field type for datetime.datetime values.

    Examples
    --------
        >>> from datetime import datetime
        >>> from flycatcher import Schema
        >>> class EventSchema(Schema):
        ...     created_at: datetime
        ...     updated_at: datetime | None = None
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


class Date(FieldBase):
    """
    Date field type for datetime.date values.

    Examples
    --------
        >>> from datetime import date
        >>> from flycatcher import Schema, Date
        >>> class BookingSchema(Schema):
        ...     check_in: date
        ...     check_out: date
    """

    def get_python_type(self):
        return date

    def get_polars_dtype(self):
        return pl.Date

    def get_sqlalchemy_type(self):
        from sqlalchemy import Date as SADate

        return SADate


# Populate type mapping from Python types to Field classes
# This is used by the Schema metaclass to create fields from type annotations
_TYPE_MAP.update(
    {
        int: Integer,
        str: String,
        float: Float,
        bool: Boolean,
        datetime: Datetime,
        date: Date,
    }
)


def get_field_class_for_type(python_type: type) -> type[FieldBase] | None:
    """
    Get the appropriate Field class for a Python type.

    Parameters
    ----------
    python_type : type
        A Python type (int, str, float, bool, datetime, date).

    Returns
    -------
    type[FieldBase] | None
        The corresponding Field class, or None if not found.
    """
    return _TYPE_MAP.get(python_type)
