"""Tests for Polars validation generation."""

import polars as pl
import pytest

from flycatcher import (
    Boolean,
    Integer,
    Schema,
    String,
    model_validator,
)


class TestPolarsValidatorCreation:
    """Test Polars validator creation."""

    def test_validator_creation(self, simple_schema):
        """Validator can be created from schema."""
        validator = simple_schema.to_polars_validator()
        assert validator is not None
        assert hasattr(validator, "validate")

    def test_schema_property(self, simple_schema):
        """Validator exposes Polars schema."""
        validator = simple_schema.to_polars_validator()
        schema = validator.schema

        assert "id" in schema
        assert "name" in schema
        assert "age" in schema
        assert schema["id"] == pl.Int64
        assert schema["name"] == pl.Utf8


class TestPolarsValidation:
    """Test DataFrame validation."""

    def test_valid_dataframe_passes(self, simple_schema, sample_dataframe):
        """Valid DataFrame passes validation."""
        validator = simple_schema.to_polars_validator()
        result = validator.validate(sample_dataframe, strict=True)

        assert result.height == 3
        assert list(result.columns) == ["id", "name", "age"]

    def test_missing_required_column_raises(self, simple_schema):
        """Missing required column raises error."""
        validator = simple_schema.to_polars_validator()

        # Missing id or name should raise
        df_missing = pl.DataFrame({"id": [1, 2]})  # Missing name
        with pytest.raises(ValueError, match="Missing required columns"):
            validator.validate(df_missing, strict=True)

    def test_constraint_violations_strict_mode(
        self, constrained_schema, invalid_dataframe
    ):
        """Strict mode raises on constraint violations."""
        validator = constrained_schema.to_polars_validator()

        # Constraint violations in strict mode raise ValueError
        with pytest.raises(ValueError, match="Constraint violation"):
            validator.validate(invalid_dataframe, strict=True)

    def test_constraint_violations_non_strict_mode(
        self, constrained_schema, invalid_dataframe
    ):
        """Non-strict mode filters invalid rows."""
        validator = constrained_schema.to_polars_validator()
        result = validator.validate(invalid_dataframe, strict=False)

        # Should filter out invalid rows
        assert result.height < invalid_dataframe.height

    def test_nullable_fields(self):
        """Nullable fields allow nulls."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String()
            age = Integer(nullable=True)

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, None]})

        result = validator.validate(df, strict=True)
        assert result.height == 2
        assert result["age"].null_count() == 1

    def test_non_nullable_fields_reject_nulls_strict(self):
        """Non-nullable fields reject nulls in strict mode."""

        class UserSchema(Schema):
            id = Integer(primary_key=True)
            name = String()  # Not nullable

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", None]})

        with pytest.raises(ValueError, match="null values"):
            validator.validate(df, strict=True)

    def test_default_values_added(self, schema_with_defaults):
        """Missing columns with defaults are added."""
        validator = schema_with_defaults.to_polars_validator()
        df = pl.DataFrame({"id": [1, 2]})  # Missing name, count, etc.

        result = validator.validate(df, strict=True)

        assert "name" in result.columns
        assert "count" in result.columns
        assert result["name"][0] == "unknown"
        assert result["count"][0] == 0

    def test_fill_nulls_with_defaults(self, schema_with_defaults):
        """fill_nulls=True replaces nulls with defaults."""
        validator = schema_with_defaults.to_polars_validator()
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", None],  # None should be filled
                "count": [5, None],  # None should be filled
            }
        )

        result = validator.validate(df, strict=True, fill_nulls=True)

        assert result["name"].null_count() == 0
        assert result["count"].null_count() == 0
        assert result["name"][1] == "unknown"
        assert result["count"][1] == 0


class TestPolarsConstraints:
    """Test constraint validation in Polars."""

    def test_integer_range_constraints(self):
        """Integer range constraints are enforced."""

        class UserSchema(Schema):
            age = Integer(ge=18, le=65)

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"age": [25, 17, 70, 30]})

        # Non-strict mode filters invalid rows
        result = validator.validate(df, strict=False)
        assert result.height == 2  # Only 25 and 30 pass (17 fails ge, 70 fails le)

    def test_string_length_constraints(self):
        """String length constraints are enforced."""

        class UserSchema(Schema):
            name = String(min_length=3, max_length=10)

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"name": ["Alice", "Al", "VeryLongName"]})

        result = validator.validate(df, strict=False)
        assert result.height == 1  # Only "Alice" passes

    def test_string_pattern_constraints(self):
        """String pattern constraints are enforced."""

        class UserSchema(Schema):
            email = String(pattern=r"^[^@]+@[^@]+\.[^@]+$")

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"email": ["alice@example.com", "invalid", "bob@test"]})

        result = validator.validate(df, strict=False)
        assert result.height == 1  # Only valid email passes


class TestPolarsModelValidators:
    """Test model validators in Polars validation."""

    def test_cross_field_validator(self, schema_with_validator):
        """Cross-field validators work in Polars."""
        from datetime import date

        validator = schema_with_validator.to_polars_validator()

        # Valid: end > start
        df_valid = pl.DataFrame(
            {
                "start_date": [date(2024, 1, 1), date(2024, 1, 2)],
                "end_date": [date(2024, 1, 2), date(2024, 1, 3)],
            }
        )
        result = validator.validate(df_valid, strict=True)
        assert result.height == 2

        # Invalid: end <= start
        df_invalid = pl.DataFrame(
            {"start_date": [date(2024, 1, 2)], "end_date": [date(2024, 1, 1)]}
        )
        result = validator.validate(df_invalid, strict=False)
        assert result.height == 0  # Invalid row filtered out

    def test_dsl_validator_in_polars(self):
        """DSL validators compile to Polars expressions."""

        class UserSchema(Schema):
            age = Integer()
            is_adult = Boolean()

            @model_validator
            def check_adult():
                from flycatcher.validators import FieldRef

                age_ref = FieldRef("age")
                return (age_ref >= 18) == FieldRef("is_adult")

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"age": [20, 15, 25], "is_adult": [True, False, True]})

        result = validator.validate(df, strict=False)
        # Should filter rows where (age >= 18) != is_adult
        assert result.height == 3  # All pass: 20>=18==True, 15>=18==False, 25>=18==True
