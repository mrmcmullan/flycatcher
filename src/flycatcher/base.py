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
    from sqlalchemy import Engine
    import polars as pl
    from .db.query import QueryBuilder


class SchemaMeta(type):
    """
    Metaclass that collects Field definitions and validators from class body.

    Fields are defined using Pydantic-style type annotations:

        class UserSchema(Schema):
            name: str
            age: int = Field(ge=0)
            bio: str | None = None

    The `table=True/False` parameter controls SQLAlchemy table generation:

        class UserSchema(Schema, table=True):
            id: int = Field(primary_key=True)
            name: str
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        table: bool = True,
        **kwargs: Any,
    ) -> type:
        # Collect inherited fields and validators from parent Schema classes
        inherited_fields: dict[str, FieldBase] = {}
        inherited_model_validators: list[Callable] = []
        inherited_field_validators: dict[str, list[Callable]] = {}
        inherited_frame_validators: list[Callable] = []
        inherited_computed_fields: dict[str, Callable] = {}

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
                    if validator not in inherited_model_validators:
                        inherited_model_validators.append(validator)
            if hasattr(base, "_field_validators") and isinstance(
                base._field_validators, dict
            ):
                # Merge parent field validators
                for field_name, validators in base._field_validators.items():
                    if field_name not in inherited_field_validators:
                        inherited_field_validators[field_name] = []
                    inherited_field_validators[field_name].extend(validators)
            if hasattr(base, "_frame_validators") and isinstance(
                base._frame_validators, list
            ):
                # Extend with parent frame validators
                for validator in base._frame_validators:
                    if validator not in inherited_frame_validators:
                        inherited_frame_validators.append(validator)
            if hasattr(base, "_computed_fields") and isinstance(
                base._computed_fields, dict
            ):
                # Copy parent computed fields
                for field_name, compute_fn in base._computed_fields.items():
                    inherited_computed_fields[field_name] = compute_fn

        # Start with inherited, will be overridden by current class
        fields: dict[str, FieldBase] = dict(inherited_fields)
        model_validators: list[Callable] = list(inherited_model_validators)
        field_validators: dict[str, list[Callable]] = {
            k: list(v) for k, v in inherited_field_validators.items()
        }
        frame_validators: list[Callable] = list(inherited_frame_validators)
        computed_fields: dict[str, Callable] = dict(inherited_computed_fields)

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

        # Collect model validators, field validators, frame validators, and computed fields
        for key, value in list(namespace.items()):
            if callable(value) and getattr(value, "_is_model_validator", False):
                model_validators.append(value)
            elif callable(value) and getattr(value, "_is_field_validator", False):
                # Field validator: maps to specific field(s)
                target_fields = getattr(value, "_field_validator_fields", [])
                for target_field in target_fields:
                    if target_field not in field_validators:
                        field_validators[target_field] = []
                    field_validators[target_field].append(value)
            elif callable(value) and getattr(value, "_is_frame_validator", False):
                # Frame validator: validates entire DataFrame
                frame_validators.append(value)
            elif callable(value) and getattr(value, "_is_computed_field", False):
                # Computed field: derived from other fields
                field_name = getattr(value, "_computed_field_name", key)
                computed_fields[field_name] = value
            elif isinstance(value, classmethod):
                # Handle @classmethod decorator - check the underlying function
                func = value.__func__
                if getattr(func, "_is_model_validator", False):
                    model_validators.append(value)
                elif getattr(func, "_is_field_validator", False):
                    target_fields = getattr(func, "_field_validator_fields", [])
                    for target_field in target_fields:
                        if target_field not in field_validators:
                            field_validators[target_field] = []
                        field_validators[target_field].append(value)
                elif getattr(func, "_is_frame_validator", False):
                    frame_validators.append(value)
                elif getattr(func, "_is_computed_field", False):
                    field_name = getattr(func, "_computed_field_name", key)
                    computed_fields[field_name] = value

        # Store fields, validators, and table flag in the class
        namespace["_fields"] = fields
        namespace["_model_validators"] = model_validators
        namespace["_field_validators"] = field_validators
        namespace["_frame_validators"] = frame_validators
        namespace["_computed_fields"] = computed_fields
        namespace["_is_table"] = table

        return super().__new__(mcs, name, bases, namespace, **kwargs)


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
    _field_validators: dict[str, list[Callable]] = {}
    _frame_validators: list[Callable] = []
    _computed_fields: dict[str, Callable] = {}
    _is_table: bool = True
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

        # Copy computed fields to self
        for field_name in self.__class__._computed_fields:
            if hasattr(validated_instance, field_name):
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
        cast: bool = True,
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
        cast : bool, default True
            If True, automatically cast columns to schema types.
            If False, columns must already have correct types or validation fails.

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

        Disable automatic casting:

            >>> validated = UserSchema.validate(df, cast=False)
        """
        validator = cls.to_polars_validator()
        return validator.validate(
            df,
            strict=strict,
            show_violations=show_violations,
            fill_nulls=fill_nulls,
            cast=cast,
        )

    @classmethod
    def insert(
        cls,
        engine: "Engine",
        df: pl.DataFrame,
        *,
        validate: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        """
        Insert a DataFrame into the database using bulk insert.

        Validates the DataFrame (optional), then inserts rows in batches.

        Parameters
        ----------
        engine : sqlalchemy.Engine
            SQLAlchemy engine connected to the database.
        df : polars.DataFrame
            DataFrame containing the data to insert.
        validate : bool, default True
            Whether to validate the DataFrame before inserting.
        chunk_size : int, default 1000
            Number of rows to insert per batch.

        Returns
        -------
        int
            Number of rows inserted.

        Raises
        ------
        ValueError
            If schema has `table=False` or validation fails.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> from sqlalchemy import create_engine
            >>> import polars as pl
            >>>
            >>> class UserSchema(Schema, table=True):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>>
            >>> engine = create_engine("sqlite:///users.db")
            >>> users = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> rows = UserSchema.insert(engine, users)
        """
        from .db.operations import bulk_insert

        return bulk_insert(
            cls,
            engine,
            df,
            validate=validate,
            chunk_size=chunk_size,
        )

    @classmethod
    def update(
        cls,
        engine: "Engine",
        df: pl.DataFrame,
        *,
        on: list[str],
        validate: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        """
        Update rows in the database using bulk update.

        Matches rows by the specified key columns and updates the remaining columns.

        Parameters
        ----------
        engine : sqlalchemy.Engine
            SQLAlchemy engine connected to the database.
        df : polars.DataFrame
            DataFrame containing the data to update.
        on : list[str]
            Column name(s) to match on for the UPDATE WHERE clause.
        validate : bool, default True
            Whether to validate the DataFrame before updating.
        chunk_size : int, default 1000
            Number of rows to update per batch.

        Returns
        -------
        int
            Number of rows updated.

        Raises
        ------
        ValueError
            If schema has `table=False`, key columns are missing, or validation fails.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> from sqlalchemy import create_engine
            >>> import polars as pl
            >>>
            >>> class UserSchema(Schema, table=True):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>>
            >>> engine = create_engine("sqlite:///users.db")
            >>> updates = pl.DataFrame({"id": [1], "name": ["Alice Updated"]})
            >>> rows = UserSchema.update(engine, updates, on=['id'])
        """
        from .db.operations import bulk_update

        return bulk_update(
            cls,
            engine,
            df,
            on=on,
            validate=validate,
            chunk_size=chunk_size,
        )

    @classmethod
    def upsert(
        cls,
        engine: "Engine",
        df: pl.DataFrame,
        *,
        on: list[str],
        validate: bool = True,
        chunk_size: int = 1000,
    ) -> int:
        """
        Upsert (insert or update) a DataFrame into the database.

        Uses dialect-specific SQL:
        - PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
        - MySQL/MariaDB: INSERT ... ON DUPLICATE KEY UPDATE
        - SQLite: INSERT ... ON CONFLICT DO UPDATE

        Parameters
        ----------
        engine : sqlalchemy.Engine
            SQLAlchemy engine connected to the database.
        df : polars.DataFrame
            DataFrame containing the data to upsert.
        on : list[str]
            Column name(s) that define the conflict (primary key or unique).
        validate : bool, default True
            Whether to validate the DataFrame before upserting.
        chunk_size : int, default 1000
            Number of rows to upsert per batch.

        Returns
        -------
        int
            Number of rows affected (inserted + updated).

        Raises
        ------
        ValueError
            If schema has `table=False`, conflict columns are missing, or validation fails.
        NotImplementedError
            If the database dialect is not supported.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> from sqlalchemy import create_engine
            >>> import polars as pl
            >>>
            >>> class UserSchema(Schema, table=True):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>>
            >>> engine = create_engine("postgresql://...")
            >>> users = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> rows = UserSchema.upsert(engine, users, on=['id'])
        """
        from .db.operations import bulk_upsert

        return bulk_upsert(
            cls,
            engine,
            df,
            on=on,
            validate=validate,
            chunk_size=chunk_size,
        )

    @classmethod
    def query(cls, engine: "Engine") -> "QueryBuilder":
        """
        Create a query builder for SELECT operations.

        Returns a fluent QueryBuilder that can be chained with filter(),
        order_by(), limit(), and executed with execute().

        Parameters
        ----------
        engine : sqlalchemy.Engine
            SQLAlchemy engine connected to the database.

        Returns
        -------
        QueryBuilder
            A query builder instance for this schema.

        Examples
        --------
            >>> from flycatcher import Schema, Field
            >>> from sqlalchemy import create_engine
            >>> import flycatcher as fl
            >>>
            >>> class UserSchema(Schema, table=True):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            ...     age: int
            >>>
            >>> engine = create_engine("sqlite:///users.db")
            >>> users = (
            ...     UserSchema.query(engine)
            ...     .filter(fl.col('age') > 21)
            ...     .limit(100)
            ...     .execute()
            ... )
        """
        from .db.query import QueryBuilder

        # Check if schema is a table
        if not getattr(cls, "_table", True):
            raise ValueError(
                f"Schema '{cls.__name__}' has table=False and cannot be "
                "used for database operations. Use table=True to enable."
            )

        return QueryBuilder(cls, engine)

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

        Raises
        ------
        ValueError
            If the schema was defined with `table=False`.

        Examples
        --------
            >>> from flycatcher import Field, Schema
            >>> from sqlalchemy import MetaData, create_engine
            >>> class UserSchema(Schema, table=True):
            ...     id: int = Field(primary_key=True)
            ...     name: str
            >>> metadata = MetaData()
            >>> table = UserSchema.to_sqlalchemy(table_name="users", metadata=metadata)
            >>> engine = create_engine("sqlite:///example.db")
            >>> metadata.create_all(engine)
        """
        if not cls._is_table:
            raise ValueError(
                f"Schema '{cls.__name__}' was defined with table=False. "
                f"Use table=True to generate SQLAlchemy tables."
            )

        from .generators.sqlalchemy import create_sqlalchemy_table

        return create_sqlalchemy_table(cls, table_name=table_name, metadata=metadata)

    @classmethod
    def is_table(cls) -> bool:
        """
        Return whether this schema is intended for database table generation.

        Returns
        -------
        bool
            True if the schema was defined with `table=True` (default).

        Examples
        --------
            >>> from flycatcher import Schema
            >>> class UserSchema(Schema, table=True):
            ...     name: str
            >>> UserSchema.is_table()
            True
            >>> class ValidationOnlySchema(Schema, table=False):
            ...     name: str
            >>> ValidationOnlySchema.is_table()
            False
        """
        return cls._is_table

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

    @classmethod
    def field_validators(cls) -> dict[str, list[Callable]]:
        """
        Return all field validators defined in this schema.

        Returns
        -------
        dict[str, list[Callable]]
            Dictionary mapping field names to lists of validator functions.

        Examples
        --------
            >>> from flycatcher import Schema, col, field_validator
            >>> class UserSchema(Schema):
            ...     email: str
            ...
            ...     @field_validator('email')
            ...     def normalize_email():
            ...         return col('email').str.to_lowercase()
            >>> validators = UserSchema.field_validators()
            >>> 'email' in validators
            True
        """
        return {k: list(v) for k, v in cls._field_validators.items()}

    @classmethod
    def frame_validators(cls) -> list[Callable]:
        """
        Return all frame validators defined in this schema.

        Returns
        -------
        list[Callable]
            List of validator functions decorated with @frame_validator.

        Examples
        --------
            >>> import polars as pl
            >>> from flycatcher import Schema, frame_validator
            >>> class OrderSchema(Schema):
            ...     customer_id: int
            ...     amount: float
            ...
            ...     @frame_validator
            ...     def no_duplicates():
            ...         return pl.col('customer_id').n_unique() == pl.len()
            >>> validators = OrderSchema.frame_validators()
            >>> len(validators)
            1
        """
        return cls._frame_validators.copy()

    @classmethod
    def computed_fields(cls) -> dict[str, Callable]:
        """
        Return all computed fields defined in this schema.

        Returns
        -------
        dict[str, Callable]
            Dictionary mapping computed field names to their compute functions.

        Examples
        --------
            >>> from flycatcher import Schema, col, computed_field
            >>> class UserSchema(Schema):
            ...     first_name: str
            ...     last_name: str
            ...
            ...     @computed_field
            ...     def full_name():
            ...         return col('first_name') + ' ' + col('last_name')
            >>> computed = UserSchema.computed_fields()
            >>> 'full_name' in computed
            True
        """
        return cls._computed_fields.copy()


def model_validator(
    func: Callable | None = None,
    *,
    polars_only: bool = False,
) -> Callable:
    """
    Decorator for cross-field validation.

    Use this decorator to add custom validation logic that involves multiple
    fields. The validator function can return either:

    1. A DSL expression (recommended) - compiles to both Polars and Pydantic
    2. A dict with 'polars' and/or 'pydantic' keys for explicit implementations
    3. A raw Polars expression (when `polars_only=True`)

    The function can optionally accept a `cls` parameter, but it's not required
    for most use cases.

    Parameters
    ----------
    func : Callable, optional
        The validation function (when used without parentheses).
    polars_only : bool, default False
        If True, the validator uses raw Polars expressions and is skipped
        during Pydantic validation. Use this for Polars-specific operations
        like rolling windows, complex aggregations, etc.

    Returns
    -------
    Callable
        The decorated function, marked as a model validator.

    Examples
    --------
    Simple DSL expression (compiles to both Polars and Pydantic):

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

    Polars-only validator (skipped for Pydantic):

        >>> import polars as pl
        >>> class StatsSchema(Schema):
        ...     price: float
        ...     threshold: float
        ...
        ...     @model_validator(polars_only=True)
        ...     def check_rolling_avg():
        ...         # Uses raw Polars expression - not available in Pydantic
        ...         return (
        ...             pl.col('price').rolling_mean(5) > pl.col('threshold'),
        ...             "Rolling average must exceed threshold"
        ...         )
    """

    def decorator(fn: Callable) -> Callable:
        fn._is_model_validator = True  # type: ignore[attr-defined]
        fn._polars_only = polars_only  # type: ignore[attr-defined]
        return fn

    # Handle both @model_validator and @model_validator(polars_only=True)
    if func is not None:
        # Called without parentheses: @model_validator
        return decorator(func)
    else:
        # Called with parentheses: @model_validator(polars_only=True)
        return decorator


def field_validator(*fields: str, mode: str = "after") -> Callable[[Callable], Callable]:
    """
    Decorator for single-field validation or transformation.

    Use this decorator to validate or transform individual fields. The validator
    function should return a DSL expression that either:

    1. Transforms the value (e.g., ``col('email').str.to_lowercase()``)
    2. Validates the value (e.g., ``col('age') >= 18, "Must be adult"``)

    For transformations, the expression result becomes the new field value.
    For validation (tuple with error message), if the condition is False,
    a ValueError is raised.

    Parameters
    ----------
    *fields : str
        Field names this validator applies to.
    mode : str, default "after"
        When to run the validator:
        - "after": Run after type coercion (default)
        - "before": Run before type coercion

    Returns
    -------
    Callable
        The decorator function.

    Examples
    --------
    Transformation (normalize value):

        >>> from flycatcher import Schema, col, field_validator
        >>> class UserSchema(Schema):
        ...     email: str
        ...
        ...     @field_validator('email')
        ...     def normalize_email():
        ...         return col('email').str.to_lowercase()

    Validation (check condition):

        >>> class UserSchema(Schema):
        ...     age: int
        ...
        ...     @field_validator('age')
        ...     def check_adult():
        ...         return col('age') >= 18, "Must be 18 or older"

    Multiple fields:

        >>> class UserSchema(Schema):
        ...     first_name: str
        ...     last_name: str
        ...
        ...     @field_validator('first_name', 'last_name')
        ...     def strip_names():
        ...         # Note: This applies the same transformation to each field
        ...         return col('first_name').str.strip_chars()
    """

    def decorator(func: Callable) -> Callable:
        func._is_field_validator = True  # type: ignore[attr-defined]
        func._field_validator_fields = list(fields)  # type: ignore[attr-defined]
        func._field_validator_mode = mode  # type: ignore[attr-defined]
        return func

    return decorator


def frame_validator(
    func: Callable | None = None,
    *,
    group_by: list[str] | None = None,
) -> Callable:
    """
    Decorator for DataFrame-level validation.

    Use this decorator to validate the entire DataFrame using Polars expressions
    directly. Frame validators are only applied during Polars validation (not Pydantic).

    Unlike ``@model_validator`` which validates individual rows, ``@frame_validator``
    can use aggregations, window functions, and group-level constraints.

    Parameters
    ----------
    func : Callable, optional
        The validation function (when used without parentheses).
    group_by : list[str], optional
        Column names to group by before applying the validation.
        When set, the validation is applied per group.

    Returns
    -------
    Callable
        The decorated function, marked as a frame validator.

    Examples
    --------
    Basic frame validation (no grouping):

        >>> import polars as pl
        >>> from flycatcher import Schema, frame_validator
        >>> class OrderSchema(Schema):
        ...     customer_id: int
        ...     amount: float
        ...
        ...     @frame_validator
        ...     def no_duplicate_customers():
        ...         return pl.col('customer_id').n_unique() == pl.len(), "Duplicate customers"

    Group validation:

        >>> class OrderSchema(Schema):
        ...     customer_id: int
        ...     amount: float
        ...
        ...     @frame_validator(group_by=['customer_id'])
        ...     def min_orders_per_customer():
        ...         return pl.len() >= 3, "Each customer needs at least 3 orders"

    Using Polars expressions directly:

        >>> class StatsSchema(Schema):
        ...     value: float
        ...
        ...     @frame_validator
        ...     def check_variance():
        ...         return pl.col('value').std() < 10, "Values too spread out"
    """

    def decorator(fn: Callable) -> Callable:
        fn._is_frame_validator = True  # type: ignore[attr-defined]
        fn._frame_validator_group_by = group_by  # type: ignore[attr-defined]
        return fn

    # Handle both @frame_validator and @frame_validator(group_by=[...])
    if func is not None:
        # Called without parentheses: @frame_validator
        return decorator(func)
    else:
        # Called with parentheses: @frame_validator(group_by=[...])
        return decorator


def computed_field(
    func: Callable | None = None,
    *,
    alias: str | None = None,
) -> Callable:
    """
    Decorator for computed (derived) fields.

    Use this decorator to define fields that are computed from other fields.
    Computed fields are:
    - Included in model_dump() output (for Pydantic)
    - Added as columns during Polars validation
    - NOT included in SQLAlchemy table (they are virtual)

    The decorated function should return a DSL expression that computes
    the field value.

    Parameters
    ----------
    func : Callable, optional
        The compute function (when used without parentheses).
    alias : str, optional
        Alternative name for the computed field.

    Returns
    -------
    Callable
        The decorated function, marked as a computed field.

    Examples
    --------
    Basic computed field:

        >>> from flycatcher import Schema, col, computed_field
        >>> class UserSchema(Schema):
        ...     first_name: str
        ...     last_name: str
        ...
        ...     @computed_field
        ...     def full_name():
        ...         return col('first_name') + ' ' + col('last_name')

    With alias:

        >>> class ProductSchema(Schema):
        ...     price: float
        ...     tax_rate: float
        ...
        ...     @computed_field(alias='totalPrice')
        ...     def total_price():
        ...         return col('price') * (1 + col('tax_rate'))
    """

    def decorator(fn: Callable) -> Callable:
        fn._is_computed_field = True  # type: ignore[attr-defined]
        fn._computed_field_name = alias or fn.__name__  # type: ignore[attr-defined]
        return fn

    # Handle both @computed_field and @computed_field(alias=...)
    if func is not None:
        # Called without parentheses: @computed_field
        return decorator(func)
    else:
        # Called with parentheses: @computed_field(alias=...)
        return decorator
