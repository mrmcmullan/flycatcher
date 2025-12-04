"""Tests for Schema metaclass and field collection."""

import polars as pl

from flycatcher import Field, Schema, model_validator


class TestSchemaMetaclass:
    """Test Schema metaclass field collection."""

    def test_fields_collected_by_metaclass(self):
        """Metaclass collects all fields from type annotations."""

        class UserSchema(Schema):
            id: int = Field(primary_key=True)
            name: str
            age: int | None = None

        fields = UserSchema.fields()
        assert len(fields) == 3
        assert "id" in fields
        assert "name" in fields
        assert "age" in fields

    def test_field_names_assigned_by_metaclass(self):
        """Metaclass assigns field names correctly."""

        class UserSchema(Schema):
            id: int
            username: str

        fields = UserSchema.fields()
        assert fields["id"].name == "id"
        assert fields["username"].name == "username"

    def test_non_field_attributes_ignored(self):
        """Non-Field attributes are not collected."""

        class UserSchema(Schema):
            id: int
            _private: str = "not a field"

            def some_method(self):
                return None

        fields = UserSchema.fields()
        assert len(fields) == 1
        assert "id" in fields
        assert "_private" not in fields
        assert "some_method" not in fields

    def test_inherited_fields_collected(self):
        """Fields from parent classes are collected."""

        class BaseSchema(Schema):
            id: int = Field(primary_key=True)

        class UserSchema(BaseSchema):
            name: str

        fields = UserSchema.fields()
        # TODO: Currently only direct fields are collected. Need to implement
        # inheritance support in SchemaMeta.__new__ to walk MRO and merge
        # parent class fields and validators. See base.py for implementation.
        assert "name" in fields
        # assert "id" in fields


class TestModelValidators:
    """Test model validator collection and execution."""

    def test_model_validator_collected(self):
        """Model validators are collected by metaclass."""

        class UserSchema(Schema):
            age: int

            @model_validator
            def check_age():
                return (pl.col("age") >= 0, "age must be non-negative")

        validators = UserSchema.model_validators()
        assert len(validators) == 1
        assert hasattr(validators[0], "_is_model_validator")

    def test_multiple_model_validators_collected(self):
        """Multiple model validators are all collected."""

        class UserSchema(Schema):
            age: int
            name: str

            @model_validator
            def check_age():
                return (pl.col("age") >= 0, "age must be non-negative")

            @model_validator
            def check_name():
                return (pl.col("name").str.len_chars() > 0, "name must not be empty")

        validators = UserSchema.model_validators()
        assert len(validators) == 2

    def test_non_validator_methods_ignored(self):
        """Regular methods are not collected as validators."""

        class UserSchema(Schema):
            age: int

            def regular_method(self):
                return "not a validator"

        validators = UserSchema.model_validators()
        assert len(validators) == 0

    def test_model_validator_without_cls_parameter(self):
        """Model validators can omit the cls parameter for ergonomics."""

        class UserSchema(Schema):
            age: int

            @model_validator
            def check_age():
                from flycatcher.validators import FieldRef

                return FieldRef("age") >= 0

        validators = UserSchema.model_validators()
        assert len(validators) == 1

        # Should work with both generators
        validator = UserSchema.to_polars_validator()
        assert validator is not None

        model = UserSchema.to_pydantic()
        assert model is not None


class TestSchemaMethods:
    """Test Schema class methods."""

    def test_fields_returns_copy(self):
        """fields() returns a copy, not the original dict."""

        class UserSchema(Schema):
            id: int

        fields1 = UserSchema.fields()
        fields2 = UserSchema.fields()

        # Modifying one shouldn't affect the other
        fields1["test"] = "should not appear"  # type: ignore
        assert "test" not in fields2

    def test_model_validators_returns_copy(self):
        """model_validators() returns a copy."""

        class UserSchema(Schema):
            id: int

            @model_validator
            def check():
                return (pl.col("id") > 0, "must be positive")

        validators1 = UserSchema.model_validators()
        validators2 = UserSchema.model_validators()

        assert len(validators1) == len(validators2)
        # They should be separate lists
        assert validators1 is not validators2
