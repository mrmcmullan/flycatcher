"""Tests for Pydantic model generation."""

import sys
from datetime import datetime

import pytest
from pydantic import ValidationError

from flycatcher import Boolean, Date, Datetime, Float, Integer, Schema, String

# Skip Pydantic tests on Python 3.14+ due to compatibility issues with Pydantic v2
PYTHON_314_PLUS = sys.version_info >= (3, 14)


class TestPydanticModelGeneration:
    """Test Pydantic model generation from schemas."""

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_simple_model_generation(self, simple_schema):
        """Basic model generation works."""
        UserModel = simple_schema.to_pydantic()

        # Model can be instantiated
        user = UserModel(id=1, name="Alice", age=25)
        assert user.id == 1
        assert user.name == "Alice"
        assert user.age == 25

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_model_with_constraints_validates(self, constrained_schema):
        """Generated model enforces constraints."""
        UserModel = constrained_schema.to_pydantic()

        # Valid data passes
        user = UserModel(
            id=1,
            name="Alice",
            age=30,
            price=10.5,
            email="alice@example.com",
            created_at=datetime.now(),
        )
        assert user.id == 1

        # Invalid data raises ValidationError
        with pytest.raises(ValidationError):
            UserModel(
                id=-1,
                name="Alice",
                age=30,
                price=10.5,
                email="alice@example.com",
                created_at=datetime.now(),
            )

        with pytest.raises(ValidationError):
            UserModel(
                id=1,
                name="",
                age=30,
                price=10.5,
                email="alice@example.com",
                created_at=datetime.now(),
            )

        with pytest.raises(ValidationError):
            UserModel(
                id=1,
                name="Alice",
                age=150,
                price=10.5,
                email="alice@example.com",
                created_at=datetime.now(),
            )

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_nullable_fields(self):
        """Nullable fields accept None."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String()
            age = Integer(nullable=True)

        UserModel = UserSchema.to_pydantic()

        # age can be None
        user = UserModel(id=1, name="Alice", age=None)
        assert user.age is None

        # name cannot be None (not nullable)
        with pytest.raises(ValidationError):
            UserModel(id=1, name=None, age=25)

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_default_values(self, schema_with_defaults):
        """Default values are applied correctly."""
        UserModel = schema_with_defaults.to_pydantic()

        # Can omit fields with defaults
        user = UserModel(id=1)
        assert user.name == "unknown"
        assert user.count == 0
        assert user.is_active is True
        assert user.created_at == datetime(2024, 1, 1)

        # Can override defaults
        user2 = UserModel(id=2, name="Bob", count=5)
        assert user2.name == "Bob"
        assert user2.count == 5

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_all_field_types(self):
        """All field types generate correct Pydantic models."""

        class AllTypesSchema(Schema):
            int_field = Integer()
            str_field = String()
            float_field = Float()
            bool_field = Boolean()
            datetime_field = Datetime()
            date_field = Date()

        Model = AllTypesSchema.to_pydantic()

        user = Model(
            int_field=1,
            str_field="test",
            float_field=1.5,
            bool_field=True,
            datetime_field=datetime.now(),
            date_field=datetime.now().date(),
        )

        assert isinstance(user.int_field, int)
        assert isinstance(user.str_field, str)
        assert isinstance(user.float_field, float)
        assert isinstance(user.bool_field, bool)
        assert isinstance(user.datetime_field, datetime)
        assert isinstance(user.date_field, type(datetime.now().date()))


class TestPydanticModelValidators:
    """Test model validators in Pydantic models."""

    @pytest.mark.skipif(
        PYTHON_314_PLUS, reason="Pydantic v2 compatibility issue with Python 3.14+"
    )
    def test_model_validator_integration(self):
        """Model validators are integrated into Pydantic models."""
        from flycatcher import Date, Schema, model_validator
        from flycatcher.validators import FieldRef

        # Create a schema with DSL validator
        class DateRangeSchema(Schema):
            start = Date()
            end = Date()

            @model_validator
            def check_range():
                start_ref = FieldRef("start")
                end_ref = FieldRef("end")
                return end_ref > start_ref

        Model = DateRangeSchema.to_pydantic()

        from datetime import date

        # Valid: end > start
        valid = Model(start=date(2024, 1, 1), end=date(2024, 1, 2))
        assert valid.start < valid.end

        # Invalid: end <= start
        with pytest.raises(ValidationError):
            Model(start=date(2024, 1, 2), end=date(2024, 1, 1))
