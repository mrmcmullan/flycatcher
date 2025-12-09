"""Tests for validator DSL and validation execution."""

import sys
from datetime import datetime

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

    def test_is_null_polars(self):
        """is_null() compiles to Polars."""
        is_null = FieldRef("is_null")

        df = pl.DataFrame({"is_null": [None, True, False]})
        result = df.filter(is_null.is_null().to_polars())
        assert result.height == 1
        assert result["is_null"][0] is None

    def test_is_not_null_polars(self):
        """is_not_null() compiles to Polars."""
        is_not_null = FieldRef("is_not_null")
        df = pl.DataFrame({"is_not_null": [None, True, False]})
        result = df.filter(is_not_null.is_not_null().to_polars())
        assert result.height == 2
        assert result["is_not_null"][0] is True
        assert result["is_not_null"][1] is False

    def test_is_null_python(self):
        """is_null() evaluates in Python."""
        is_null = FieldRef("is_null")
        expr = is_null.is_null()
        assert expr.to_python({"is_null": None}) is True
        assert expr.to_python({"is_null": True}) is False
        assert expr.to_python({"is_null": False}) is False

    def test_is_not_null_python(self):
        """is_not_null() evaluates in Python."""
        is_not_null = FieldRef("is_not_null")
        expr = is_not_null.is_not_null()
        assert expr.to_python({"is_not_null": True}) is True
        assert expr.to_python({"is_not_null": None}) is False


class TestMembershipOperations:
    """Test membership helpers like is_in and is_between."""

    def test_is_in_polars(self):
        """is_in() compiles to Polars and handles null propagation."""
        country = col("country")
        expr = country.is_in(["US", "CA"])

        df = pl.DataFrame({"country": ["US", "MX", None, "CA"]})
        result = df.filter(expr.to_polars())
        assert result["country"].to_list() == ["US", "CA"]

    def test_is_in_nulls_equal_polars(self):
        """nulls_equal=True treats None as a distinct, matchable value."""
        country = col("country")
        expr = country.is_in([None, "CA"], nulls_equal=True)

        df = pl.DataFrame({"country": ["US", None, "CA"]})
        matches = df.select(expr.to_polars().alias("match"))["match"].to_list()
        assert matches == [False, True, True]

    def test_is_in_python(self):
        """is_in() evaluates in Python with null handling."""
        country = col("country")
        expr = country.is_in(["US", "CA"])

        assert expr.to_python({"country": "US"}) is True
        assert expr.to_python({"country": "MX"}) is False
        assert expr.to_python({"country": None}) is None

        null_expr = country.is_in([None], nulls_equal=True)
        assert null_expr.to_python({"country": None}) is True

    def test_is_between_polars_closed_variants(self):
        """is_between() supports closed intervals."""
        age = col("age")

        df = pl.DataFrame({"age": [18, 19, 30, 31]})
        assert df.filter(age.is_between(18, 30).to_polars())["age"].to_list() == [
            18,
            19,
            30,
        ]
        assert df.filter(age.is_between(18, 30, closed="left").to_polars())[
            "age"
        ].to_list() == [18, 19]
        assert df.filter(age.is_between(18, 30, closed="right").to_polars())[
            "age"
        ].to_list() == [19, 30]
        assert df.filter(age.is_between(18, 30, closed="none").to_polars())[
            "age"
        ].to_list() == [19]

    def test_is_between_polars_column_bounds(self):
        """String bounds are parsed as column references."""
        value = col("value")
        expr = value.is_between("low", "high", closed="right")

        df = pl.DataFrame(
            {
                "value": [5, 10, 15],
                "low": [0, 10, 20],
                "high": [10, 15, 30],
            }
        )

        filtered = df.filter(expr.to_polars())
        assert filtered["value"].to_list() == [5]

    def test_is_between_python(self):
        """is_between() evaluates bounds in Python."""
        age = col("age")

        assert age.is_between(18, 30).to_python({"age": 30}) is True
        assert age.is_between(18, 30).to_python({"age": 17}) is False
        assert age.is_between(18, 30, closed="none").to_python({"age": 18}) is False

        values = {"age": 10, "low": 5, "high": 10}
        assert age.is_between("low", "high", closed="right").to_python(values) is True


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
        """col() is a convenience function that creates FieldRef instances."""

        # col is a function, not a direct alias
        assert callable(col)
        assert col is not FieldRef

        # But it creates FieldRef instances
        age = col("age")
        assert isinstance(age, FieldRef)
        assert age.name == "age"

        # Test it works the same way
        price = col("price")
        assert isinstance(price, FieldRef)
        assert price.name == "price"


class TestValidatorExecution:
    """Test actual validation execution."""

    def test_dsl_validator_in_polars_integration(self):
        """DSL validator works in actual Polars validation."""
        from flycatcher import Schema, model_validator

        class UserSchema(Schema):
            age: int

            @model_validator
            def check_age():
                return FieldRef("age") > 18

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"age": [20, 15, 25]})

        result = validator.validate(df, strict=False)
        assert result.height == 2  # Filters out age=15

    @pytest.mark.skipif(
        sys.version_info >= (3, 14),
        reason="Pydantic v2 compatibility issue with Python 3.14+",
    )
    def test_dsl_validator_in_pydantic_integration(self):
        """DSL validator works in actual Pydantic validation."""
        from flycatcher import Schema, model_validator

        class UserSchema(Schema):
            age: int

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


class TestStringOperations:
    """Test string operations on FieldRef."""

    def test_contains_polars(self):
        """contains() compiles to Polars and works correctly."""
        email = col("email")
        expr = email.str.contains("@")

        df = pl.DataFrame({"email": ["user@example.com", "invalid", "test@test.com"]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # Both emails pass

    def test_contains_python(self):
        """contains() evaluates in Python."""
        email = col("email")
        expr = email.str.contains("@")

        assert expr.to_python({"email": "user@example.com"}) is True
        assert expr.to_python({"email": "invalid"}) is False
        assert expr.to_python({"email": None}) is False

    def test_contains_regex_polars(self):
        """contains() supports regex patterns in Polars."""
        phone = col("phone")
        expr = phone.str.contains(r"^\d{3}-\d{3}-\d{4}$")

        df = pl.DataFrame({"phone": ["123-456-7890", "invalid", "555-123-4567"]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # Both valid phones pass

    def test_starts_with_polars(self):
        """starts_with() compiles to Polars."""
        name = col("name")
        expr = name.str.starts_with("Dr.")

        df = pl.DataFrame({"name": ["Dr. Smith", "Mr. Jones", "Dr. Brown"]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # Both "Dr." names pass

    def test_starts_with_python(self):
        """starts_with() evaluates in Python."""
        name = col("name")
        expr = name.str.starts_with("Dr.")

        assert expr.to_python({"name": "Dr. Smith"}) is True
        assert expr.to_python({"name": "Mr. Jones"}) is False
        assert expr.to_python({"name": None}) is False

    def test_ends_with_polars(self):
        """ends_with() compiles to Polars."""
        email = col("email")
        expr = email.str.ends_with(".com")

        df = pl.DataFrame(
            {"email": ["test@example.com", "test@example.org", "user@test.com"]}
        )
        result = df.filter(expr.to_polars())
        assert result.height == 2  # Both .com emails pass

    def test_ends_with_python(self):
        """ends_with() evaluates in Python."""
        email = col("email")
        expr = email.str.ends_with(".com")

        assert expr.to_python({"email": "test@example.com"}) is True
        assert expr.to_python({"email": "test@example.org"}) is False
        assert expr.to_python({"email": None}) is False

    def test_len_chars_polars(self):
        """len_chars() compiles to Polars."""
        tag = col("tag")
        expr = tag.str.len_chars() <= 5

        df = pl.DataFrame({"tag": ["short", "toolong", "ok"]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # "short" and "ok" pass

    def test_len_chars_python(self):
        """len_chars() evaluates in Python."""
        tag = col("tag")
        expr = tag.str.len_chars()

        assert expr.to_python({"tag": "hello"}) == 5
        assert expr.to_python({"tag": "test"}) == 4
        assert expr.to_python({"tag": None}) == 0

    def test_strip_chars_polars(self):
        """strip_chars() compiles to Polars."""
        name = col("name")
        expr = name.str.strip_chars()

        df = pl.DataFrame({"name": ["  hello  ", "world", "  test  "]})
        result = df.select(expr.to_polars().alias("stripped"))
        assert result["stripped"][0] == "hello"
        assert result["stripped"][1] == "world"
        assert result["stripped"][2] == "test"

    def test_strip_chars_python(self):
        """strip_chars() evaluates in Python."""
        name = col("name")
        expr = name.str.strip_chars()

        assert expr.to_python({"name": "  hello  "}) == "hello"
        assert expr.to_python({"name": "world"}) == "world"
        assert expr.to_python({"name": None}) is None

    def test_to_lowercase_polars(self):
        """to_lowercase() compiles to Polars."""
        code = col("code")
        expr = code.str.to_lowercase()

        df = pl.DataFrame({"code": ["HELLO", "World", "TEST"]})
        result = df.select(expr.to_polars().alias("lower"))
        assert result["lower"][0] == "hello"
        assert result["lower"][1] == "world"
        assert result["lower"][2] == "test"

    def test_to_lowercase_python(self):
        """to_lowercase() evaluates in Python."""
        code = col("code")
        expr = code.str.to_lowercase()

        assert expr.to_python({"code": "HELLO"}) == "hello"
        assert expr.to_python({"code": "World"}) == "world"
        assert expr.to_python({"code": None}) is None

    def test_to_uppercase_polars(self):
        """to_uppercase() compiles to Polars."""
        code = col("code")
        expr = code.str.to_uppercase()

        df = pl.DataFrame({"code": ["hello", "World", "test"]})
        result = df.select(expr.to_polars().alias("upper"))
        assert result["upper"][0] == "HELLO"
        assert result["upper"][1] == "WORLD"
        assert result["upper"][2] == "TEST"

    def test_to_uppercase_python(self):
        """to_uppercase() evaluates in Python."""
        code = col("code")
        expr = code.str.to_uppercase()

        assert expr.to_python({"code": "hello"}) == "HELLO"
        assert expr.to_python({"code": "World"}) == "WORLD"
        assert expr.to_python({"code": None}) is None

    def test_replace_polars(self):
        """replace() compiles to Polars."""
        phone = col("phone")
        expr = phone.str.replace(r"[^\d]", "")

        df = pl.DataFrame({"phone": ["123-456-7890", "(555) 123-4567"]})
        result = df.select(expr.to_polars().alias("cleaned"))
        assert result["cleaned"][0] == "1234567890"
        assert result["cleaned"][1] == "5551234567"

    def test_replace_python(self):
        """replace() evaluates in Python."""
        phone = col("phone")
        expr = phone.str.replace(r"[^\d]", "")

        assert expr.to_python({"phone": "123-456-7890"}) == "1234567890"
        assert expr.to_python({"phone": "(555) 123-4567"}) == "5551234567"
        assert expr.to_python({"phone": None}) is None

    def test_extract_polars(self):
        """extract() compiles to Polars."""
        email = col("email")
        expr = email.str.extract(r"@(.+)", 1)

        df = pl.DataFrame({"email": ["user@example.com", "test@test.org"]})
        result = df.select(expr.to_polars().alias("domain"))
        assert result["domain"][0] == "example.com"
        assert result["domain"][1] == "test.org"

    def test_extract_python(self):
        """extract() evaluates in Python."""
        email = col("email")
        expr = email.str.extract(r"@(.+)", 1)

        assert expr.to_python({"email": "user@example.com"}) == "example.com"
        assert expr.to_python({"email": "test@test.org"}) == "test.org"
        assert expr.to_python({"email": "invalid"}) is None
        assert expr.to_python({"email": None}) is None

    def test_slice_polars(self):
        """slice() compiles to Polars."""
        code = col("code")
        expr = code.str.slice(0, 3)

        df = pl.DataFrame({"code": ["HELLO", "WORLD", "TEST"]})
        result = df.select(expr.to_polars().alias("prefix"))
        assert result["prefix"][0] == "HEL"
        assert result["prefix"][1] == "WOR"
        assert result["prefix"][2] == "TES"

    def test_slice_python(self):
        """slice() evaluates in Python."""
        code = col("code")
        expr = code.str.slice(0, 3)

        assert expr.to_python({"code": "HELLO"}) == "HEL"
        assert expr.to_python({"code": "WORLD"}) == "WOR"
        assert expr.to_python({"code": None}) is None

    def test_slice_no_length_python(self):
        """slice() without length extracts to end."""
        code = col("code")
        expr = code.str.slice(2)

        assert expr.to_python({"code": "HELLO"}) == "LLO"
        assert expr.to_python({"code": "WORLD"}) == "RLD"

    def test_count_matches_polars(self):
        """count_matches() compiles to Polars."""
        text = col("text")
        expr = text.str.count_matches(r"\d+")

        df = pl.DataFrame({"text": ["abc123def456", "no numbers", "1 2 3"]})
        result = df.select(expr.to_polars().alias("count"))
        assert result["count"][0] == 2  # Two number sequences
        assert result["count"][1] == 0
        assert result["count"][2] == 3  # Three single digits

    def test_count_matches_python(self):
        """count_matches() evaluates in Python."""
        text = col("text")
        expr = text.str.count_matches(r"\d+")

        assert expr.to_python({"text": "abc123def456"}) == 2
        assert expr.to_python({"text": "no numbers"}) == 0
        assert expr.to_python({"text": "1 2 3"}) == 3
        assert expr.to_python({"text": None}) == 0

    def test_count_matches_comparison(self):
        """count_matches() can be used in comparisons."""
        text = col("text")
        expr = text.str.count_matches(r"\d+") >= 2

        df = pl.DataFrame({"text": ["abc123def456", "no numbers", "1 2 3"]})
        result = df.filter(expr.to_polars())
        assert result.height == 2  # First and third pass

    def test_string_chaining(self):
        """String operations can be chained."""
        name = col("name")
        expr = name.str.strip_chars().str.to_lowercase()

        df = pl.DataFrame({"name": ["  HELLO  ", "  WORLD  "]})
        result = df.select(expr.to_polars().alias("cleaned"))
        assert result["cleaned"][0] == "hello"
        assert result["cleaned"][1] == "world"

    def test_string_chaining_python(self):
        """String operations can be chained in Python."""
        name = col("name")
        expr = name.str.strip_chars().str.to_lowercase()

        assert expr.to_python({"name": "  HELLO  "}) == "hello"
        assert expr.to_python({"name": "  WORLD  "}) == "world"

    def test_string_operations_in_validator(self):
        """String operations work in model validators."""
        from flycatcher import Schema, model_validator

        class UserSchema(Schema):
            email: str

            @model_validator
            def check_email():
                return col("email").str.contains("@")

        validator = UserSchema.to_polars_validator()
        df = pl.DataFrame({"email": ["valid@example.com", "invalid", "test@test.com"]})

        result = validator.validate(df, strict=False)
        assert result.height == 2  # Filters out invalid email

    def test_string_operations_combined(self):
        """String operations can be combined with logical operators."""
        email = col("email")
        expr = email.str.contains("@") & email.str.ends_with(".com")

        df = pl.DataFrame(
            {
                "email": [
                    "user@example.com",
                    "user@example.org",
                    "invalid",
                    "test@test.com",
                ]
            }
        )
        result = df.filter(expr.to_polars())
        assert result.height == 2  # Only .com emails pass


class TestDateTimeOperations:
    """Test datetime operations on FieldRef."""

    def test_components_polars(self):
        """year/month/day/hour/minute/second compile to Polars."""
        ts = col("ts")
        exprs = {
            "year": ts.dt.year(),
            "month": ts.dt.month(),
            "day": ts.dt.day(),
            "hour": ts.dt.hour(),
            "minute": ts.dt.minute(),
            "second": ts.dt.second(),
        }

        df = pl.DataFrame(
            {
                "ts": [
                    datetime(2024, 1, 2, 3, 4, 5),
                    datetime(2023, 5, 6, 7, 8, 9),
                ]
            }
        )
        result = df.select(
            [expr.to_polars().alias(name) for name, expr in exprs.items()]
        )

        assert result["year"].to_list() == [2024, 2023]
        assert result["month"].to_list() == [1, 5]
        assert result["day"].to_list() == [2, 6]
        assert result["hour"].to_list() == [3, 7]
        assert result["minute"].to_list() == [4, 8]
        assert result["second"].to_list() == [5, 9]

    def test_components_python(self):
        """year/month/day/hour/minute/second evaluate in Python."""
        ts = col("ts")
        exprs = {
            "year": ts.dt.year(),
            "month": ts.dt.month(),
            "day": ts.dt.day(),
            "hour": ts.dt.hour(),
            "minute": ts.dt.minute(),
            "second": ts.dt.second(),
        }
        values = {"ts": datetime(2024, 2, 3, 4, 5, 6)}

        assert exprs["year"].to_python(values) == 2024
        assert exprs["month"].to_python(values) == 2
        assert exprs["day"].to_python(values) == 3
        assert exprs["hour"].to_python(values) == 4
        assert exprs["minute"].to_python(values) == 5
        assert exprs["second"].to_python(values) == 6

    def test_total_days_polars(self):
        """total_days compiles to Polars and returns total days."""
        ts = col("ts")
        anchor = datetime(2024, 1, 1)
        df = pl.DataFrame(
            {
                "ts": [
                    datetime(2024, 1, 2),
                    datetime(2024, 1, 3),
                ]
            }
        )
        expr = ts.dt.total_days(anchor)
        result = df.select(expr.to_polars().alias("diff"))
        assert result["diff"].to_list() == [
            1.0,
            2.0,
        ]

    def test_total_days_python(self):
        """total_days evaluates to total days (float) in Python."""
        ts = col("ts")
        anchor = datetime(2024, 1, 1)
        expr = ts.dt.total_days(anchor)

        delta = expr.to_python({"ts": datetime(2024, 1, 2)})
        assert delta == 1.0
