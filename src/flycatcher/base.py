"""Core `Schema` class with metaclass magic and custom validators."""

import sys
import types
import typing
from typing import Callable, Union, get_args, get_origin

from .fields import _MISSING, FieldBase, FieldInfo, get_field_class_for_type


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
        # TODO: Support field inheritance - walk MRO of bases and merge parent
        # fields/validators before collecting current class fields. Child fields
        # should override parent fields with same name. See test_schema.py
        # test_inherited_fields_collected for expected behavior.

        # Collect all Field instances
        fields: dict[str, FieldBase] = {}
        model_validators: list[Callable] = []

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

    Fields are defined using Pydantic-style type annotations with optional Field()
    for constraints.

    Examples
    --------
    Basic schema definition:

        >>> from flycatcher import Schema, Field
        >>> from datetime import datetime
        >>> class UserSchema(Schema):
        ...     # Simple fields - just type annotations
        ...     name: str
        ...     created_at: datetime
        ...
        ...     # Nullable fields
        ...     bio: str | None = None
        ...
        ...     # Fields with defaults
        ...     is_active: bool = True
        ...
        ...     # Fields with constraints
        ...     age: int = Field(ge=0, le=120)
        ...     email: str = Field(min_length=5, max_length=100)
        ...
        ...     # Database options
        ...     id: int = Field(primary_key=True, autoincrement=True)

    With cross-field validation:

        >>> from flycatcher import Schema, Field, col, model_validator
        >>> class BookingSchema(Schema):
        ...     check_in: datetime
        ...     check_out: datetime
        ...
        ...     @model_validator
        ...     def check_dates():
        ...         return (
        ...             col('check_out') > col('check_in'),
        ...             "Check-out must be after check-in"
        ...         )

    Generate outputs:

        >>> from datetime import datetime
        >>> # Generate Pydantic model
        >>> UserModel = UserSchema.to_pydantic()
        >>> user = UserModel(
        ...     id=1,
        ...     name="Alice",
        ...     age=25,
        ...     email="alice@example.com",
        ...     created_at=datetime.now(),
        ... )
        >>>
        >>> # Generate Polars validator
        >>> import polars as pl
        >>> validator = UserSchema.to_polars_validator()
        >>> df = pl.DataFrame({
        ...     "id": [1],
        ...     "name": ["Alice"],
        ...     "age": [25],
        ...     "email": ["alice@example.com"],
        ...     "created_at": [datetime.now()],
        ... })
        >>> validated_df = validator.validate(df, strict=True)
        >>>
        >>> # Generate SQLAlchemy table
        >>> table = UserSchema.to_sqlalchemy(table_name="users")
    """

    _fields: dict[str, FieldBase] = {}
    _model_validators: list[Callable] = []

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
