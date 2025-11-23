"""Core Schema class with metaclass magic and custom validators."""

from typing import Callable

from .fields import Field


class SchemaMeta(type):
    """Metaclass that collects Field definitions and validators from class body."""

    def __new__(mcs, name, bases, namespace):
        # TODO: Support field inheritance - walk MRO of bases and merge parent
        # fields/validators before collecting current class fields. Child fields
        # should override parent fields with same name. See test_schema.py
        # test_inherited_fields_collected for expected behavior.
        # Collect all Field instances
        fields: dict[str, Field] = {}
        model_validators: list[Callable] = []

        for key, value in list(namespace.items()):
            if isinstance(value, Field):
                value.name = key
                fields[key] = value
            elif callable(value) and getattr(value, "_is_model_validator", False):
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


class Schema(metaclass=SchemaMeta):
    """
    Base schema class for defining data models.

    Define your schema by subclassing `Schema` and adding field definitions.
    The metaclass automatically collects fields and validators.

    Examples
    --------
    Basic schema definition:

        from flycatcher import Schema, Integer, String, Datetime

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String(min_length=1, max_length=100)
            age = Integer(ge=0, le=120)
            created_at = Datetime()

    With cross-field validation:

        from flycatcher import Schema, Integer, String, col, model_validator

        class PlayerSchema(Schema):
            id = Integer(primary_key=True)
            name = String()
            age = Integer(nullable=True, ge=0, le=120)
            created_at = Datetime()

            @model_validator
            def check_age_name_logic():
                return (
                    (col('age') >= 18) | col('name').str.contains('_junior'),
                    "Users under 18 must have '_junior' in their name"
                )

    Generate outputs:

        # Generate Pydantic model
        UserModel = UserSchema.to_pydantic()
        user = UserModel(id=1, name="Alice", age=25, created_at=datetime.now())

        # Generate Polars validator
        validator = UserSchema.to_polars_model()
        df = validator.validate(df, strict=True)

        # Generate SQLAlchemy table
        table = UserSchema.to_sqlalchemy(table_name="users")
    """

    _fields: dict[str, Field] = {}
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
            class UserSchema(Schema):
                id = Integer(primary_key=True)
                name = String()

            UserModel = UserSchema.to_pydantic()
            user = UserModel(id=1, name="Alice")
            user.model_dump()  # {'id': 1, 'name': 'Alice'}
        """
        from .generators.pydantic import create_pydantic_model

        return create_pydantic_model(cls)

    @classmethod
    def to_polars_model(cls):
        """
        Generate a Polars validation model from this schema.

        Returns
        -------
        PolarsValidator
            A validator instance for validating Polars DataFrames.

        Examples
        --------
            import polars as pl

            class UserSchema(Schema):
                id = Integer(primary_key=True)
                name = String(min_length=1)

            validator = UserSchema.to_polars_model()
            df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            validated_df = validator.validate(df, strict=True)
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
            from sqlalchemy import MetaData, create_engine

            class UserSchema(Schema):
                id = Integer(primary_key=True)
                name = String()

            metadata = MetaData()
            table = UserSchema.to_sqlalchemy(table_name="users", metadata=metadata)
            engine = create_engine("sqlite:///example.db")
            metadata.create_all(engine)
        """
        from .generators.sqlalchemy import create_sqlalchemy_table

        return create_sqlalchemy_table(cls, table_name=table_name, metadata=metadata)

    @classmethod
    def fields(cls) -> dict[str, Field]:
        """
        Return all fields defined in this schema.

        Returns
        -------
        dict[str, Field]
            Dictionary mapping field names to Field instances.

        Examples
        --------
            class UserSchema(Schema):
                id = Integer(primary_key=True)
                name = String()

            fields = UserSchema.fields()
            list(fields.keys())  # ['id', 'name']
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
            class UserSchema(Schema):
                age = Integer()

                @model_validator
                def check_age():
                    return col('age') >= 18

            validators = UserSchema.model_validators()
            len(validators)  # 1
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

        from flycatcher import Schema, Integer, col, model_validator

        class BookingSchema(Schema):
            check_in = Integer()
            check_out = Integer()

            @model_validator
            def check_dates():
                return col('check_out') > col('check_in')

    With error message:

        class BookingSchema(Schema):
            check_in = Integer()
            check_out = Integer()

            @model_validator
            def check_dates():
                return (
                    col('check_out') > col('check_in'),
                    "Check-out date must be after check-in date"
                )

    Complex validation with multiple conditions:

        class ProductSchema(Schema):
            price = Float()
            discount_price = Float(nullable=True)

            @model_validator
            def check_discount():
                return (
                    (col('discount_price').is_null()) | (col('discount_price') < col('price')),
                    "Discount price must be less than regular price"
                )
    """
    # Mark the function as a model validator
    func._is_model_validator = True  # type: ignore[attr-defined]
    return func
