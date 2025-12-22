"""Core `Schema` class with metaclass magic and custom validators."""

from __future__ import annotations

import json
import sys
import types
import typing
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Union, get_args, get_origin

from .fields import _MISSING, FieldBase, FieldInfo, get_field_class_for_type

if TYPE_CHECKING:
    from pydantic import BaseModel
    import polars as pl


class SchemaMeta(type):
    """
    Metaclass that collects Field definitions and validators from class body.

    Fields are defined using Pydantic-style type annotations:

        class UserSchema(Schema):
            name: str
            age: int = Field(ge=0)
            bio: str | None = None
    """

    def __new__(mcs, name, bases, namespace):
        # Collect inherited fields and validators from parent Schema classes
        inherited_fields: dict[str, FieldBase] = {}
        inherited_validators: list[Callable] = []

        # Walk the MRO in reverse order so that closer parents override further ones
        for base in reversed(bases):
            if hasattr(base, "_fields") and isinstance(base._fields, dict):
                # Copy parent fields (child will override if same name)
                for field_name, field in base._fields.items():
                    inherited_fields[field_name] = field
            if hasattr(base, "_model_validators") and isinstance(
                base._model_validators, list
            ):
                # Extend with parent validators (avoid duplicates)
                for validator in base._model_validators:
                    if validator not in inherited_validators:
                        inherited_validators.append(validator)

        # Start with inherited fields, will be overridden by current class fields
        fields: dict[str, FieldBase] = dict(inherited_fields)
        model_validators: list[Callable] = list(inherited_validators)

        # Get type annotations (supports both Python 3.10+ and older)
        annotations = namespace.get("__annotations__", {})

        # Process type annotations
        for field_name, type_hint in annotations.items():
            # Skip private attributes and classvars
            if field_name.startswith("_"):
                continue

            # Check origin for Union types (e.g., str | None, Optional[str])
            origin = get_origin(type_hint)
            nullable = False
            actual_type = type_hint

            # Handle Union types (including T | None syntax from Python 3.10+)
            # Python 3.10+ uses types.UnionType for X | Y syntax
            # typing.Union is used for Union[X, Y] and Optional[X]
            is_union = origin is Union or (
                sys.version_info >= (3, 10) and isinstance(type_hint, types.UnionType)
            )

            if is_union:
                args = get_args(type_hint)
                # Check for Optional pattern (T | None)
                none_types = [a for a in args if a is type(None)]
                non_none_types = [a for a in args if a is not type(None)]

                if none_types and len(non_none_types) == 1:
                    nullable = True
                    actual_type = non_none_types[0]
                elif len(non_none_types) > 1:
                    # Complex union like int | str - not supported yet
                    raise TypeError(
                        f"Field '{field_name}': Union types other than "
                        f"Optional (T | None) are not supported. Got: {type_hint}"
                    )

            # Get the class attribute value (default or Field())
            class_value = namespace.get(field_name, _MISSING)

            # Check for explicit style usage (no longer supported)
            if isinstance(class_value, FieldBase):
                raise TypeError(
                    f"Field '{field_name}': Explicit field style is no longer "
                    f"supported. Use Pydantic-style type annotations instead:\n"
                    f"  Instead of: {field_name} = "
                    f"{class_value.__class__.__name__}(...)\n"
                    f"  Use: {field_name}: {actual_type} = Field(...)"
                )

            # Case 1: FieldInfo from Field() function (Pydantic-style with constraints)
            if isinstance(class_value, FieldInfo):
                # Get the appropriate Field class for the type
                field_class = get_field_class_for_type(actual_type)
                if field_class is None:
                    raise TypeError(
                        f"Field '{field_name}': Unsupported type '{actual_type}'. "
                        f"Supported types: int, str, float, bool, datetime, date"
                    )

                # Create field with kwargs from FieldInfo
                kwargs = class_value.to_field_kwargs()

                # Merge nullable from annotation
                if nullable:
                    kwargs["nullable"] = True

                # Filter kwargs to only those accepted by this field class
                field = _create_field_with_valid_kwargs(field_class, kwargs)

            # Case 2: Raw default value or no value (simple Pydantic-style)
            else:
                # Get the appropriate Field class for the type
                field_class = get_field_class_for_type(actual_type)
                if field_class is None:
                    raise TypeError(
                        f"Field '{field_name}': Unsupported type '{actual_type}'. "
                        f"Supported types: int, str, float, bool, datetime, date"
                    )

                # Create field with nullable and optional default
                kwargs: dict[str, typing.Any] = {"nullable": nullable}
                if class_value is not _MISSING:
                    kwargs["default"] = class_value

                field = field_class(**kwargs)

            field.name = field_name
            fields[field_name] = field

        # Collect model validators
        for _key, value in list(namespace.items()):
            if callable(value) and getattr(value, "_is_model_validator", False):
                model_validators.append(value)
            elif isinstance(value, classmethod):
                # Handle @classmethod decorator - check the underlying function
                func = value.__func__
                if getattr(func, "_is_model_validator", False):
                    model_validators.append(value)

        # Store fields and validators in the class
        namespace["_fields"] = fields
        namespace["_model_validators"] = model_validators

        return super().__new__(mcs, name, bases, namespace)


def _create_field_with_valid_kwargs(
    field_class: type[FieldBase], kwargs: dict[str, typing.Any]
) -> FieldBase:
    """
    Create a Field instance, filtering kwargs to only valid parameters.

    Different Field subclasses accept different constraint parameters.
    This function filters the kwargs to only include valid parameters
    for the specific field class.
    """
    import inspect

    # Get valid parameters for this field class
    sig = inspect.signature(field_class.__init__)
    valid_params = set(sig.parameters.keys()) - {"self"}

    # Check if the signature accepts **kwargs (VAR_KEYWORD)
    has_var_keyword = any(
        param.kind == inspect.Parameter.VAR_KEYWORD for param in sig.parameters.values()
    )

    # If **kwargs is present, the function accepts any keyword argument
    # So we can pass all kwargs through (Python will handle any duplicates)
    if has_var_keyword:
        filtered_kwargs = kwargs
    else:
        # No **kwargs, so only allow explicitly named parameters
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}

    return field_class(**filtered_kwargs)


class Schema(metaclass=SchemaMeta):
    """
    Base schema class for defining data models.

    Define your schema by subclassing `Schema` and adding field definitions.
    The metaclass automatically collects fields and validators.

    Schemas can be used in three ways:

    1. **Instantiate directly** (like Pydantic): ``user = UserSchema(name="Alice")``
    2. **Validate DataFrames**: ``UserSchema.validate(df)``
    3. **Generate outputs**: ``.to_pydantic()``, ``.to_polars_validator()``, ``.to_sqlalchemy()``

    Fields are defined using Pydantic-style type annotations with optional Field()
    for constraints.

    Examples
    --------
    Basic schema definition and instantiation:

        >>> from flycatcher import Schema, Field
        >>> class UserSchema(Schema):
        ...     name: str
        ...     age: int = Field(ge=0, le=120)
        ...     is_active: bool = True
        >>> user = UserSchema(name="Alice", age=25)
        >>> user.name
        'Alice'
        >>> user.model_dump()
        {'name': 'Alice', 'age': 25, 'is_active': True}

    DataFrame validation:

        >>> import polars as pl
        >>> df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30], "is_active": [True, True]})
        >>> validated_df = UserSchema.validate(df)

    With cross-field validation:

        >>> from flycatcher import Schema, col, model_validator
        >>> class BookingSchema(Schema):
        ...     check_in: int
        ...     check_out: int
        ...
        ...     @model_validator
        ...     def check_dates():
        ...         return col('check_out') > col('check_in')
        >>> booking = BookingSchema(check_in=1, check_out=5)
        >>> booking.check_out
        5
    """

    _fields: dict[str, FieldBase] = {}
    _model_validators: list[Callable] = []
    _pydantic_model_cache: ClassVar[type[BaseModel] | None] = None

    def __init__(self, **data: Any) -> None:
        """
        Create a new Schema instance with validated data.

        Parameters
        ----------
        **data : Any
            Field values to validate and store.

        Raises
        ------
        pydantic.ValidationError
            If validation fails.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> class UserSchema(Schema):
            ...     name: str
            ...     age: int = Field(ge=0)
            >>> user = UserSchema(name="Alice", age=25)
            >>> user.name
            'Alice'
        """
        # Get or create the cached Pydantic model
        model_class = self._get_pydantic_model()

        # Validate data using Pydantic
        validated_instance = model_class(**data)

        # Copy validated values to self
        for field_name in self.__class__._fields:
            setattr(self, field_name, getattr(validated_instance, field_name))

        # Store the internal Pydantic instance for delegation
        self._pydantic_instance = validated_instance

    @classmethod
    def _get_pydantic_model(cls) -> type[BaseModel]:
        """Get or create the cached Pydantic model for this schema."""
        if cls._pydantic_model_cache is None:
            cls._pydantic_model_cache = cls.to_pydantic()
        return cls._pydantic_model_cache

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """
        Convert the schema instance to a dictionary.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments passed to Pydantic's model_dump().

        Returns
        -------
        dict[str, Any]
            Dictionary of field names to values.

        Examples
        --------
            >>> from flycatcher import Schema
            >>> class UserSchema(Schema):
            ...     name: str
            ...     age: int
            >>> user = UserSchema(name="Alice", age=25)
            >>> user.model_dump()
            {'name': 'Alice', 'age': 25}
        """
        return self._pydantic_instance.model_dump(**kwargs)

    def model_dump_json(self, **kwargs: Any) -> str:
        """
        Convert the schema instance to a JSON string.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments passed to Pydantic's model_dump_json().

        Returns
        -------
        str
            JSON string representation.

        Examples
        --------
            >>> from flycatcher import Schema
            >>> class UserSchema(Schema):
            ...     name: str
            ...     age: int
            >>> user = UserSchema(name="Alice", age=25)
            >>> user.model_dump_json()
            '{"name":"Alice","age":25}'
        """
        return self._pydantic_instance.model_dump_json(**kwargs)

    def __repr__(self) -> str:
        """Return a string representation of the schema instance."""
        field_strs = [
            f"{name}={getattr(self, name)!r}" for name in self.__class__._fields
        ]
        return f"{self.__class__.__name__}({', '.join(field_strs)})"

    def __eq__(self, other: object) -> bool:
        """Check equality based on field values."""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return all(
            getattr(self, name) == getattr(other, name)
            for name in self.__class__._fields
        )

    @classmethod
    def validate(
        cls,
        df: pl.DataFrame,
        *,
        strict: bool = True,
        show_violations: bool = False,
        fill_nulls: bool = False,
    ) -> pl.DataFrame:
        """
        Validate a Polars DataFrame against this schema.

        This is a convenience method equivalent to:
        ``cls.to_polars_validator().validate(df, ...)``

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to validate.
        strict : bool, default True
            If True, raise on validation errors. If False, filter out invalid rows.
        show_violations : bool, default False
            If True, log violations for debugging.
        fill_nulls : bool, default False
            If True, fill null values with field defaults.

        Returns
        -------
        pl.DataFrame
            Validated DataFrame.

        Raises
        ------
        ValueError
            If validation fails and strict=True.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> import polars as pl
            >>> class UserSchema(Schema):
            ...     name: str
            ...     age: int = Field(ge=0)
            >>> df = pl.DataFrame({"name": ["Alice"], "age": [25]})
            >>> validated = UserSchema.validate(df)
        """
        validator = cls.to_polars_validator()
        return validator.validate(
            df, strict=strict, show_violations=show_violations, fill_nulls=fill_nulls
        )

    @classmethod
    def to_pydantic(cls) -> type:
        """
        Generate a Pydantic BaseModel from this schema.

        Returns
        -------
        type
            A dynamically created Pydantic BaseModel class.

        Examples
        --------
            >>> from flycatcher import Field, Schema
            >>> class UserSchema(Schema):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>> UserModel = UserSchema.to_pydantic()
            >>> user = UserModel(id=1, name="Alice")
            >>> user.model_dump()
            {'id': 1, 'name': 'Alice'}
        """
        from .generators.pydantic import create_pydantic_model

        return create_pydantic_model(cls)

    @classmethod
    def to_polars_validator(cls):
        """
        Generate a Polars validator from this schema.

        Returns
        -------
        PolarsValidator
            A validator instance for validating Polars DataFrames.

        Examples
        --------
            >>> from flycatcher import Field, Schema
            >>> import polars as pl
            >>> class UserSchema(Schema):
            ...     id: int = Field(primary_key=True)
            ...     name: str = Field(min_length=1)
            >>> validator = UserSchema.to_polars_validator()
            >>> df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> validated_df = validator.validate(df, strict=True)
        """
        from .generators.polars import create_polars_validator

        return create_polars_validator(cls)

    @classmethod
    def to_sqlalchemy(cls, table_name: str | None = None, metadata=None):
        """
        Generate a SQLAlchemy Table from this schema.

        Parameters
        ----------
        table_name : str, optional
            Name for the SQL table. If not provided, auto-generated from
            schema class name (removes "Schema" suffix, lowercases, adds "s").
        metadata : sqlalchemy.MetaData, optional
            MetaData instance to attach the table to. If not provided,
            a new MetaData instance is created.

        Returns
        -------
        sqlalchemy.Table
            A SQLAlchemy Table object.

        Examples
        --------
            >>> from flycatcher import Field, Schema
            >>> from sqlalchemy import MetaData, create_engine
            >>> class UserSchema(Schema):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>> metadata = MetaData()
            >>> table = UserSchema.to_sqlalchemy(table_name="users", metadata=metadata)
            >>> engine = create_engine("sqlite:///example.db")
            >>> metadata.create_all(engine)
        """
        from .generators.sqlalchemy import create_sqlalchemy_table

        return create_sqlalchemy_table(cls, table_name=table_name, metadata=metadata)

    @classmethod
    def fields(cls) -> dict[str, FieldBase]:
        """
        Return all fields defined in this schema.

        Returns
        -------
        dict[str, FieldBase]
            Dictionary mapping field names to Field instances.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> class UserSchema(Schema):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>> fields = UserSchema.fields()
            >>> list(fields.keys())
            ['id', 'name']
        """
        return cls._fields.copy()

    @classmethod
    def model_validators(cls) -> list[Callable]:
        """
        Return all model validators defined in this schema.

        Returns
        -------
        list[Callable]
            List of validator functions decorated with @model_validator.

        Examples
        --------
            >>> from flycatcher import Schema, col, model_validator
            >>> class UserSchema(Schema):
            ...     age: int
            ...
            ...     @model_validator
            ...     def check_age():
            ...         return col('age') >= 18
            >>> validators = UserSchema.model_validators()
            >>> len(validators)
            1
        """
        return cls._model_validators.copy()


def model_validator(func: Callable) -> Callable:
    """
    Decorator for cross-field validation.

    Use this decorator to add custom validation logic that involves multiple
    fields. The validator function can return either:

    1. A DSL expression (recommended) - compiles to both Polars and Pydantic
    2. A dict with 'polars' and/or 'pydantic' keys for explicit implementations

    The function can optionally accept a `cls` parameter, but it's not required
    for most use cases.

    Parameters
    ----------
    func : Callable
        The validation function to decorate.

    Returns
    -------
    Callable
        The decorated function, marked as a model validator.

    Examples
    --------
    Simple DSL expression:

        >>> from flycatcher import Schema, col, model_validator
        >>> class BookingSchema(Schema):
        ...     check_in: int
        ...     check_out: int
        ...
        ...     @model_validator
        ...     def check_dates():
        ...         return col('check_out') > col('check_in')

    With error message:

        >>> class BookingSchema(Schema):
        ...     check_in: int
        ...     check_out: int
        ...
        ...     @model_validator
        ...     def check_dates():
        ...         return (
        ...             col('check_out') > col('check_in'),
        ...             "Check-out date must be after check-in date"
        ...         )

    Complex validation with multiple conditions:

        >>> from flycatcher import Field, Schema, col, model_validator
        >>> class ProductSchema(Schema):
        ...     price: float
        ...     discount_price: float | None = None
        ...
        ...     @model_validator
        ...     def check_discount():
        ...         return (
        ...             (col('discount_price').is_null()) |
        ...             (col('discount_price') < col('price')),
        ...             "Discount price must be less than regular price"
        ...         )
    """
    # Mark the function as a model validator
    func._is_model_validator = True  # type: ignore[attr-defined]
    return func
