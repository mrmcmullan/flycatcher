"""
Flycatcher: DataFrame-Native Data Layer

Define your schema once. Validate at scale. Stay columnar.
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
