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

    Example:
        class PlayerSchema(Schema):
            id = Integer(primary_key=True)
            name = String()
            age = Integer(nullable=True, ge=0, le=120)
            created_at = Datetime()

            @model_validator
            def check_age_name_logic():
                '''Custom cross-field validation'''
                import flycatcher as fl
                return (
                    (fl.col('age') >= 18) | fl.col('name').str.contains('_junior'),
                    "Users under 18 must have '_junior' in their name"
                )
    """

    _fields: dict[str, Field] = {}
    _model_validators: list[Callable] = []

    @classmethod
    def to_pydantic(cls) -> type:
        """Generate a Pydantic BaseModel from this schema."""
        from .generators.pydantic import create_pydantic_model

        return create_pydantic_model(cls)

    @classmethod
    def to_polars_model(cls):
        """Generate a Polars validation model from this schema."""
        from .generators.polars import create_polars_validator

        return create_polars_validator(cls)

    @classmethod
    def to_sqlalchemy(cls, table_name: str | None = None, metadata=None):
        """Generate a SQLAlchemy Table from this schema."""
        from .generators.sqlalchemy import create_sqlalchemy_table

        return create_sqlalchemy_table(cls, table_name=table_name, metadata=metadata)

    @classmethod
    def fields(cls) -> dict[str, Field]:
        """Return all fields defined in this schema."""
        return cls._fields.copy()

    @classmethod
    def model_validators(cls) -> list[Callable]:
        """Return all model validators defined in this schema."""
        return cls._model_validators.copy()


def model_validator(func: Callable) -> Callable:
    """
    Decorator for cross-field validation.

    The function should return a tuple of (expression, error_message)
    or a DSL expression.

    The `cls` parameter is optional - you can omit it for more ergonomic use:

    Example:
        @model_validator
        def check_start_end_date():
            import flycatcher as fl
            return fl.col('end_date') > fl.col('start_date')
    """
    # Mark the function as a model validator
    func._is_model_validator = True  # type: ignore[attr-defined]
    return func
