"""
Flycatcher: DataFrame-Native Data Layer

Define your schema once. Validate at scale. Stay columnar.

Usage
-----
For the Polars-like idiom, import as `fl`:

    import flycatcher as fl
    from flycatcher import Schema, Field, model_validator

    class UserSchema(Schema):
        name: str

        @model_validator
        def check():
            return fl.col('name').str.len_chars() > 0
"""

from .base import Schema, model_validator
from .fields import (
    Date,
    Field,
    FieldBase,
    FieldInfo,
)
from .validators import FieldRef, col

__version__ = "0.2.0"

__all__ = [
    # Core
    "Schema",
    "Field",
    "Date",
    "model_validator",
    # DSL
    "col",
    "FieldRef",
    # Internal (for advanced use)
    "FieldBase",
    "FieldInfo",
]
