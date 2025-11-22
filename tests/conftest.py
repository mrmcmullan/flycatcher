"""Shared fixtures for flycatcher tests."""

from datetime import datetime

import polars as pl
import pytest

from flycatcher import (
    Boolean,
    Date,
    Datetime,
    Float,
    Integer,
    Schema,
    String,
    model_validator,
)


@pytest.fixture
def simple_schema():
    """Simple schema with basic fields."""

    class SimpleSchema(Schema):
        id = Integer(primary_key=True)
        name = String()
        age = Integer(nullable=True)

    return SimpleSchema


@pytest.fixture
def constrained_schema():
    """Schema with various constraints."""

    class ConstrainedSchema(Schema):
        id = Integer(primary_key=True, ge=1)
        name = String(min_length=1, max_length=100)
        age = Integer(ge=0, le=120)
        price = Float(gt=0.0)
        email = String(pattern=r"^[^@]+@[^@]+\.[^@]+$")
        is_active = Boolean(default=True)
        created_at = Datetime()

    return ConstrainedSchema


@pytest.fixture
def schema_with_validator():
    """Schema with cross-field model validator."""

    class ValidatedSchema(Schema):
        start_date = Date()
        end_date = Date()

        @model_validator
        def check_dates():
            from flycatcher.validators import FieldRef

            start_ref = FieldRef("start_date")
            end_ref = FieldRef("end_date")
            return end_ref > start_ref

    return ValidatedSchema


@pytest.fixture
def schema_with_defaults():
    """Schema with default values."""

    class DefaultsSchema(Schema):
        id = Integer(primary_key=True)
        name = String(default="unknown")
        count = Integer(default=0)
        is_active = Boolean(default=True)
        created_at = Datetime(default=datetime(2024, 1, 1))

    return DefaultsSchema


@pytest.fixture
def sample_dataframe():
    """Sample Polars DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )


@pytest.fixture
def invalid_dataframe():
    """DataFrame with validation errors for constrained_schema."""
    return pl.DataFrame(
        {
            "id": [1, 2, -1],  # Negative ID violates ge=1
            "name": ["Alice", "", "Charlie"],  # Empty name violates min_length=1
            "age": [25, 150, 35],  # Age 150 violates le=120
            "price": [10.5, 20.0, 5.0],  # Required for constrained_schema
            "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],  # Required
            "created_at": [datetime.now(), datetime.now(), datetime.now()],  # Required
        }
    )
