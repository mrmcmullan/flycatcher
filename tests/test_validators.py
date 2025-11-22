"""Tests for validator DSL and validation execution."""

import sys

import polars as pl
import pytest
from pydantic import ValidationError

from flycatcher.validators import FieldRef, ValidatorResult, col


class TestFieldRef:
    """Test FieldRef compilation."""

    def test_fieldref_to_polars(self):
        """FieldRef compiles to Polars column expression."""
        ref = FieldRef("age")
        expr = ref.to_polars()

        # Verify it's a Polars expression
        df = pl.DataFrame({"age": [1, 2, 3]})
        result = df.select(expr)
        assert "age" in result.columns

    def test_fieldref_to_python(self):
        """FieldRef evaluates in Python context."""
        ref = FieldRef("age")

        # With object attribute
        class Obj:
            age = 25

        assert ref.to_python(Obj()) == 25

        # With dict
        assert ref.to_python({"age": 30}) == 30

        # Missing field raises
        with pytest.raises(AttributeError):
            ref.to_python({"name": "Alice"})


class TestBinaryOp:
    """Test binary operations."""

    def test_comparison_operations(self):
        """Comparison operations compile correctly."""
        age = FieldRef("age")

        # Greater than
        gt_expr = (age > 18).to_polars()
        df = pl.DataFrame({"age": [20, 15, 25]})
        result = df.filter(gt_expr)
        assert result.height == 2  # 20 and 25 pass

        # Less than or equal
        le_expr = (age <= 20).to_polars()
        result = df.filter(le_expr)
        assert result.height == 2  # 15 and 20 pass

    def test_arithmetic_operations(self):
        """Arithmetic operations compile correctly."""
        price = FieldRef("price")
        tax = FieldRef("tax")

        # Addition
        total_expr = (price + tax).to_polars()
        df = pl.DataFrame({"price": [10, 20], "tax": [1, 2]})
        result = df.select(total_expr.alias("total"))
        assert result["total"][0] == 11
        assert result["total"][1] == 22

    def test_logical_operations_polars(self):
        """Logical operations compile to Polars."""
        age = FieldRef("age")
        is_active = FieldRef("is_active")

        # AND - need to call to_polars() to get the expression
        and_expr = (age >= 18) & is_active
        df = pl.DataFrame({"age": [20, 15, 25], "is_active": [True, True, False]})
        result = df.filter(and_expr.to_polars())
        assert result.height == 1  # Only first row passes

    def test_logical_operations_python(self):
        """Logical operations evaluate in Python."""
        age = FieldRef("age")
        is_active = FieldRef("is_active")

        # AND in Python (uses 'and' not '&')
        and_expr = (age >= 18) & is_active

        assert and_expr.to_python({"age": 20, "is_active": True}) is True
        assert and_expr.to_python({"age": 15, "is_active": True}) is False
        assert and_expr.to_python({"age": 25, "is_active": False}) is False

    def test_chained_operations(self):
        """Operations can be chained."""
        age = FieldRef("age")

        # Chain: (age > 18) & (age < 65)
        expr = (age > 18) & (age < 65)
        df = pl.DataFrame({"age": [20, 15, 70, 30]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # 20 and 30 pass


class TestUnaryOp:
    """Test unary operations."""

    def test_negation_polars(self):
        """Negation compiles to Polars."""
        is_active = FieldRef("is_active")
        not_active = ~is_active

        df = pl.DataFrame({"is_active": [True, False, True]})
        result = df.filter(not_active.to_polars())
        assert result.height == 1  # Only False passes

    def test_negation_python(self):
        """Negation evaluates in Python."""
        is_active = FieldRef("is_active")
        not_active = ~is_active

        assert not_active.to_python({"is_active": True}) is False
        assert not_active.to_python({"is_active": False}) is True

    def test_abs_polars(self):
        """Absolute value compiles to Polars."""
        value = FieldRef("value")
        abs_value = value.abs()

        df = pl.DataFrame({"value": [-5, 5, -10]})
        result = df.select(abs_value.to_polars().alias("abs_value"))
        assert result["abs_value"][0] == 5
        assert result["abs_value"][1] == 5
        assert result["abs_value"][2] == 10


class TestValidatorResult:
    """Test ValidatorResult wrapper."""

    def test_dsl_result_to_polars(self):
        """DSL expression compiles to Polars validator."""
        age = FieldRef("age")
        result = ValidatorResult(age > 18)

        polars_expr, msg = result.get_polars_validator()
        assert msg == "Validation failed"

        # Verify it works
        df = pl.DataFrame({"age": [20, 15]})
        filtered = df.filter(polars_expr)
        assert filtered.height == 1

    def test_dict_result_to_polars(self):
        """Dict format validator extracts Polars expression."""
        result = ValidatorResult(
            {
                "polars": (pl.col("age") > 18, "Age must be over 18"),
                "pydantic": lambda v: v.age > 18,
            }
        )

        polars_expr, msg = result.get_polars_validator()
        assert msg == "Age must be over 18"

        df = pl.DataFrame({"age": [20, 15]})
        filtered = df.filter(polars_expr)
        assert filtered.height == 1

    def test_dict_result_missing_polars_raises(self):
        """Dict without 'polars' key raises error."""
        result = ValidatorResult({"pydantic": lambda v: True})

        with pytest.raises(ValueError, match="must have 'polars' key"):
            result.get_polars_validator()

    def test_dsl_result_to_pydantic(self):
        """DSL expression compiles to Pydantic validator."""
        age = FieldRef("age")
        result = ValidatorResult(age > 18)

        validator = result.get_pydantic_validator()
        assert validator is not None
        assert callable(validator)

        # Valid data passes
        class Data:
            age = 20

        assert validator(Data) == Data

        # Invalid data raises
        class InvalidData:
            age = 15

        with pytest.raises(ValueError, match="Validation failed"):
            validator(InvalidData)

    def test_dict_result_to_pydantic(self):
        """Dict format validator extracts Pydantic callable."""

        def custom_validator(v):
            if v.age < 18:
                raise ValueError("Too young")
            return v

        result = ValidatorResult(
            {"polars": (pl.col("age") > 18, "Age check"), "pydantic": custom_validator}
        )

        validator = result.get_pydantic_validator()
        assert validator is custom_validator

    def test_dict_result_polars_only(self):
        """Dict with only Polars returns None for Pydantic."""
        result = ValidatorResult({"polars": (pl.col("age") > 18, "Age check")})

        assert result.has_pydantic_validator() is False
        assert result.get_pydantic_validator() is None

    def test_invalid_result_type_raises(self):
        """Invalid result type raises error."""
        result = ValidatorResult("not a valid result")

        with pytest.raises(ValueError, match="Invalid validator result type"):
            result.get_polars_validator()


class TestColAlias:
    """Test convenience alias."""

    def test_col_alias_works(self):
        """col() is an alias for FieldRef."""

        assert col is FieldRef
        age = col("age")
        assert isinstance(age, FieldRef)
        assert age.name == "age"


class TestValidatorExecution:
    """Test actual validation execution."""

    def test_dsl_validator_in_polars_integration(self):
        """DSL validator works in actual Polars validation."""
        from flycatcher import Integer, Schema, model_validator

        class UserSchema(Schema):
            age = Integer()

            @model_validator
            def check_age():
                return FieldRef("age") > 18

        validator = UserSchema.to_polars_model()
        df = pl.DataFrame({"age": [20, 15, 25]})

        result = validator.validate(df, strict=False)
        assert result.height == 2  # Filters out age=15

    @pytest.mark.skipif(
        sys.version_info >= (3, 14),
        reason="Pydantic v2 compatibility issue with Python 3.14+",
    )
    def test_dsl_validator_in_pydantic_integration(self):
        """DSL validator works in actual Pydantic validation."""
        from flycatcher import Integer, Schema, model_validator

        class UserSchema(Schema):
            age = Integer()

            @model_validator
            def check_age():
                return FieldRef("age") > 18

        UserModel = UserSchema.to_pydantic()

        # Valid
        user = UserModel(age=20)
        assert user.age == 20

        # Invalid
        with pytest.raises(ValidationError):
            UserModel(age=15)
