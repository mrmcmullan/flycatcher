"""Tests for field types and constraints."""

from datetime import date, datetime

import polars as pl
import pytest
from sqlalchemy import (
    Boolean as SABoolean,
)
from sqlalchemy import (
    Date as SADate,
)
from sqlalchemy import (
    DateTime,
    Text,
)
from sqlalchemy import (
    Float as SAFloat,
)
from sqlalchemy import (
    Integer as SAInteger,
)
from sqlalchemy import (
    String as SAString,
)

from flycatcher import Field, Schema
from flycatcher.fields import (
    _MISSING,
    Boolean,
    Date,
    Datetime,
    Float,
    Integer,
    String,
)


class TestFieldTypes:
    """Test basic field type functionality."""

    def test_integer_type_definitions(self):
        """Integer field getter methods return correct type information."""
        field = Integer()
        assert field.get_python_type() is int
        assert field.get_polars_dtype() == pl.Int64
        assert field.get_sqlalchemy_type() == SAInteger

    def test_string_type_definitions(self):
        """String field getter methods return correct type information."""
        field = String()
        assert field.get_python_type() is str
        assert field.get_polars_dtype() == pl.Utf8
        # String() without max_length returns Text class
        assert field.get_sqlalchemy_type() == Text

    def test_float_type_definitions(self):
        """Float field getter methods return correct type information."""
        field = Float()
        assert field.get_python_type() is float
        assert field.get_polars_dtype() == pl.Float64
        assert field.get_sqlalchemy_type() == SAFloat

    def test_boolean_type_definitions(self):
        """Boolean field getter methods return correct type information."""
        field = Boolean()
        assert field.get_python_type() is bool
        assert field.get_polars_dtype() == pl.Boolean
        assert field.get_sqlalchemy_type() == SABoolean

    def test_datetime_type_definitions(self):
        """Datetime field getter methods return correct type information."""
        field = Datetime()
        assert field.get_python_type() == datetime
        assert field.get_polars_dtype() == pl.Datetime
        assert field.get_sqlalchemy_type() == DateTime

    def test_date_type_definitions(self):
        """Date field getter methods return correct type information."""
        field = Date()
        assert field.get_python_type() == date
        assert field.get_polars_dtype() == pl.Date
        assert field.get_sqlalchemy_type() == SADate


class TestIntegerConstraints:
    """Test Integer field constraints."""

    def test_integer_range_constraints(self):
        """Integer constraints generate correct Polars expressions."""
        field = Integer(ge=0, le=100)
        field.name = "age"  # Simulate metaclass assignment

        constraints = field.get_polars_constraints()
        assert len(constraints) == 2

        # Check expressions work
        df = pl.DataFrame({"age": [50, 150, -10]})
        expr1, msg1 = constraints[0]  # ge constraint
        expr2, msg2 = constraints[1]  # le constraint

        result1 = df.filter(expr1)
        result2 = df.filter(expr2)

        assert result1.height == 2  # 50 and 150 pass ge=0
        assert result2.height == 2  # 50 and -10 pass le=100 (150 fails)
        assert "must be >=" in msg1
        assert "must be <=" in msg2

    def test_integer_multiple_of_constraint(self):
        """Integer multiple_of constraint works."""
        field = Integer(multiple_of=5)
        field.name = "count"

        constraints = field.get_polars_constraints()
        assert len(constraints) == 1

        df = pl.DataFrame({"count": [5, 10, 13, 20]})
        expr, msg = constraints[0]
        result = df.filter(expr)

        assert result.height == 3  # 5, 10, 20 pass
        assert "multiple of" in msg

    def test_integer_pydantic_kwargs(self):
        """Integer constraints translate to Pydantic field kwargs."""
        field = Integer(ge=0, le=100, multiple_of=5)
        kwargs = field.get_pydantic_field_kwargs()

        assert kwargs["ge"] == 0
        assert kwargs["le"] == 100
        assert kwargs["multiple_of"] == 5


class TestStringConstraints:
    """Test String field constraints."""

    def test_string_length_constraints(self):
        """String length constraints generate correct Polars expressions."""
        field = String(min_length=3, max_length=10)
        field.name = "name"

        constraints = field.get_polars_constraints()
        assert len(constraints) == 2

        df = pl.DataFrame({"name": ["abc", "ab", "abcdefghij", "abcdefghijk"]})
        expr1, msg1 = constraints[0]  # min_length
        expr2, msg2 = constraints[1]  # max_length

        result1 = df.filter(expr1)
        result2 = df.filter(expr2)

        assert result1.height == 3  # "abc", "abcdefghij", "abcdefghijk" pass min
        assert (
            result2.height == 3
        )  # "abc", "ab", "abcdefghij" pass max (all except last)
        assert "at least" in msg1
        assert "at most" in msg2

    def test_string_pattern_constraint(self):
        """String pattern constraint works."""
        field = String(pattern=r"^[A-Z]+$")
        field.name = "code"

        constraints = field.get_polars_constraints()
        assert len(constraints) == 1

        df = pl.DataFrame({"code": ["ABC", "abc", "A1B"]})
        expr, msg = constraints[0]
        result = df.filter(expr)

        assert result.height == 1  # Only "ABC" matches
        assert "pattern" in msg

    def test_string_pydantic_kwargs(self):
        """String constraints translate to Pydantic field kwargs."""
        field = String(min_length=1, max_length=100, pattern=r"^[a-z]+$")
        kwargs = field.get_pydantic_field_kwargs()

        assert kwargs["min_length"] == 1
        assert kwargs["max_length"] == 100
        assert kwargs["pattern"] == r"^[a-z]+$"

    def test_string_sqlalchemy_type_with_max_length(self):
        """String with max_length uses SAString, without uses Text."""
        field_with_length = String(max_length=100)
        field_without_length = String()

        sa_type_with = field_with_length.get_sqlalchemy_type()
        sa_type_without = field_without_length.get_sqlalchemy_type()

        assert isinstance(sa_type_with, type(SAString(100)))
        assert sa_type_without == Text


class TestFloatConstraints:
    """Test Float field constraints."""

    def test_float_range_constraints(self):
        """Float constraints generate correct Polars expressions."""
        field = Float(gt=0.0, lt=100.0)
        field.name = "price"

        constraints = field.get_polars_constraints()
        assert len(constraints) == 2

        df = pl.DataFrame({"price": [10.5, 0.0, -5.0, 150.0]})
        expr1, msg1 = constraints[0]  # gt
        expr2, msg2 = constraints[1]  # lt

        result1 = df.filter(expr1)
        result2 = df.filter(expr2)

        assert result1.height == 2  # 10.5, 150.0 pass gt=0
        assert result2.height == 3  # 10.5, 0.0, -5.0 pass lt=100 (150 fails)
        assert "must be >" in msg1
        assert "must be <" in msg2

    def test_float_pydantic_kwargs(self):
        """Float constraints translate to Pydantic field kwargs."""
        field = Float(ge=0.0, le=1.0)
        kwargs = field.get_pydantic_field_kwargs()

        assert kwargs["ge"] == 0.0
        assert kwargs["le"] == 1.0


class TestDatetimeConstraints:
    """Test Datetime field constraints."""

    def test_datetime_range_constraints(self):
        """Datetime constraints generate correct Polars expressions."""
        lower = datetime(2024, 1, 1, 0, 0, 0)
        upper = datetime(2024, 1, 10, 0, 0, 0)
        field = Datetime(gt=lower, lt=upper)
        field.name = "ts"

        constraints = field.get_polars_constraints()
        assert len(constraints) == 2

        df = pl.DataFrame(
            {
                "ts": [
                    datetime(2024, 1, 5, 0, 0, 0),  # between
                    datetime(2024, 1, 1, 0, 0, 0),  # equal to lower
                    datetime(2024, 1, 10, 0, 0, 0),  # equal to upper
                ]
            }
        )

        expr1, msg1 = constraints[0]  # gt constraint
        expr2, msg2 = constraints[1]  # lt constraint

        result1 = df.filter(expr1)
        result2 = df.filter(expr2)

        # For gt, middle and upper values are > lower
        assert result1.height == 2
        # For lt, lower and middle values are < upper
        assert result2.height == 2
        assert "must be >" in msg1
        assert "must be <" in msg2

    def test_datetime_pydantic_kwargs(self):
        """Datetime constraints translate to Pydantic field kwargs."""
        lower = datetime(2024, 1, 1, 0, 0, 0)
        upper = datetime(2024, 1, 10, 0, 0, 0)
        field = Datetime(gt=lower, le=upper)

        kwargs = field.get_pydantic_field_kwargs()

        assert kwargs["gt"] == lower
        assert kwargs["le"] == upper


class TestFieldProperties:
    """Test field metadata properties."""

    def test_field_default_value(self):
        """Field default value is stored correctly."""
        field = Integer(default=42)
        assert field.default == 42

    def test_field_nullable(self):
        """Field nullable flag is stored correctly."""
        field = String(nullable=True)
        assert field.nullable is True

    def test_field_primary_key(self):
        """Field primary_key flag is stored correctly."""
        field = Integer(primary_key=True)
        assert field.primary_key is True

    def test_field_unique_and_index(self):
        """Field unique and index flags are stored correctly."""
        field = String(unique=True, index=True)
        assert field.unique is True
        assert field.index is True

    def test_field_without_default_uses_missing(self):
        """Field without default uses _MISSING sentinel."""
        field = Integer()
        assert field.default is _MISSING

    def test_constraints_require_field_name(self):
        """Getting constraints without field name raises error."""
        field = Integer(ge=0)
        # name not set by metaclass yet
        with pytest.raises(RuntimeError, match="require field name"):
            field.get_polars_constraints()


class TestPydanticStyleFields:
    """Test Pydantic-style field definitions in schemas."""

    def test_pydantic_style_simple_fields(self):
        """Simple Pydantic-style fields work correctly."""

        class UserSchema(Schema):
            id: int
            name: str
            age: int | None = None

        fields = UserSchema.fields()
        assert len(fields) == 3
        assert fields["id"].get_python_type() is int
        assert fields["name"].get_python_type() is str
        assert fields["age"].get_python_type() is int
        assert fields["age"].nullable is True

    def test_pydantic_style_with_field_function(self):
        """Pydantic-style with Field() function works correctly."""

        class UserSchema(Schema):
            id: int = Field(primary_key=True, ge=1)
            name: str = Field(min_length=1, max_length=100)
            age: int = Field(ge=0, le=120)
            email: str = Field(pattern=r"^[^@]+@[^@]+\.[^@]+$")

        fields = UserSchema.fields()
        assert fields["id"].primary_key is True
        assert fields["id"].ge == 1
        assert fields["name"].min_length == 1
        assert fields["name"].max_length == 100
        assert fields["age"].ge == 0
        assert fields["age"].le == 120
        assert fields["email"].pattern == r"^[^@]+@[^@]+\.[^@]+$"

    def test_pydantic_style_with_defaults(self):
        """Pydantic-style fields with defaults work correctly."""

        class UserSchema(Schema):
            id: int = Field(primary_key=True)
            name: str = "unknown"
            count: int = 0
            is_active: bool = True

        fields = UserSchema.fields()
        assert fields["name"].default == "unknown"
        assert fields["count"].default == 0
        assert fields["is_active"].default is True

    def test_pydantic_style_nullable_fields(self):
        """Pydantic-style nullable fields work correctly."""

        class UserSchema(Schema):
            id: int
            bio: str | None = None
            age: int | None = Field(default=25)

        fields = UserSchema.fields()
        assert fields["bio"].nullable is True
        assert fields["bio"].default is None
        assert fields["age"].nullable is True
        assert fields["age"].default == 25
