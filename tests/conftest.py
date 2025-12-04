"""Shared fixtures for flycatcher tests."""

from datetime import date, datetime

import polars as pl
import pytest

from flycatcher import Field, Schema, model_validator


@pytest.fixture
def simple_schema():
    """Simple schema with basic fields."""

    class SimpleSchema(Schema):
        id: int = Field(primary_key=True)
        name: str
        age: int | None = None

    return SimpleSchema


@pytest.fixture
def constrained_schema():
    """Schema with various constraints."""

    class ConstrainedSchema(Schema):
        id: int = Field(primary_key=True, ge=1)
        name: str = Field(min_length=1, max_length=100)
        age: int = Field(ge=0, le=120)
        price: float = Field(gt=0.0)
        email: str = Field(pattern=r"^[^@]+@[^@]+\.[^@]+$")
        is_active: bool = True
        created_at: datetime

    return ConstrainedSchema


@pytest.fixture
def schema_with_validator():
    """Schema with cross-field model validator."""

    class ValidatedSchema(Schema):
        start_date: date
        end_date: date

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
        id: int = Field(primary_key=True)
        name: str = "unknown"
        count: int = 0
        is_active: bool = True
        created_at: datetime = datetime(2024, 1, 1)

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
