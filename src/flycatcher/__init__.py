"""
Flycatcher: DataFrame-Native Data Layer

Define your schema once. Validate at scale. Stay columnar.
"""

from .base import Schema, model_validator
from .fields import (
    Boolean,
    Date,
    Datetime,
    Field,
    Float,
    Integer,
    String,
)
from .validators import FieldRef, col

__version__ = "0.1.0"

__all__ = [
    "Schema",
    "model_validator",
    "col",
    "FieldRef",
    "Field",
    "Integer",
    "String",
    "Float",
    "Boolean",
    "Datetime",
    "Date",
]
