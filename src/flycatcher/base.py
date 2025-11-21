"""Core Schema class with metaclass magic and custom validators."""

from typing import Callable

from .fields import Field


class SchemaMeta(type):
    """Metaclass that collects Field definitions and validators from class body."""

    def __new__(mcs, name, bases, namespace):
        # Collect all Field instances
        fields: dict[str, Field] = {}
        model_validators: list[Callable] = []

        for key, value in list(namespace.items()):
            if isinstance(value, Field):
                value.name = key
                fields[key] = value
            elif callable(value) and getattr(value, "_is_model_validator", False):
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
            def check_age_name_logic(cls):
                '''Custom cross-field validation'''
                return (
                    (pl.col('age') >= 18) | (pl.col('name').str.contains('_junior')),
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
    def to_sqlalchemy(cls, table_name: str | None = None):
        """Generate a SQLAlchemy Table from this schema."""
        from .generators.sqlalchemy import create_sqlalchemy_table

        return create_sqlalchemy_table(cls, table_name=table_name)

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

    The function should return a tuple of (expression, error_message).

    Example:
        @model_validator
        def check_start_end_date(cls):
            return (
                pl.col('end_date') > pl.col('start_date'),
                "end_date must be after start_date"
            )
    """
    # Mark the function as a model validator
    func._is_model_validator = True  # type: ignore[attr-defined]
    return func
