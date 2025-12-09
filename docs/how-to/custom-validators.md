# Custom Validators

This guide shows you how to implement **custom validation logic that goes beyond simple field constraints.** You'll learn to write cross-field validators, conditional logic, and complex business rules that work seamlessly across both Pydantic and Polars.

---

## 📝 Quick Reference

```python
from datetime import datetime
from flycatcher import Schema, Field, col, model_validator

class BookingSchema(Schema):
    check_in: datetime
    check_out: datetime
    guests: int = Field(ge=1)

    @model_validator
    def check_dates():
        return (
            col('check_out') > col('check_in'),
            "Check-out must be after check-in"
        )
```

**Key Functions:**

- **`col(field_name)`** - Reference a field in validation expressions
- **`@model_validator`** - Decorator for cross-field validation functions

---

## 🧠 Understanding the `col()` DSL

The `col()` function **creates field references that compile to both Polars expressions and Python callables.** This lets you write validation logic once and have it work in **both row-level (Pydantic) and bulk (Polars) contexts.**

### Basic Field References

```python
from flycatcher import col

# Reference fields by name
col('price')
col('email')
col('created_at')
```

### Comparison Operations

```python
# Numeric comparisons
col('age') >= 18
col('price') > 0
col('discount_price') < col('regular_price')

# Equality checks
col('status') == 'active'
col('email') != None

# Date/time comparisons
col('end_date') > col('start_date')
```

### String Operations

```python
# String methods
col('email').str.contains('@')
col('email').str.ends_with('.com')
col('name').str.starts_with('Dr.')
col('tag').str.len_chars() <= 20

# Pattern matching
col('phone').str.contains(r'^\d{3}-\d{3}-\d{4}$')
```

### Logical Operations

```python
# AND
(col('age') >= 18) & (col('age') <= 65)

# OR
(col('type') == 'premium') | (col('type') == 'enterprise')

# NOT
~(col('status') == 'deleted')
```

### Null Checks

```python
# Null / not-null helpers
col('discount').is_null()
col('discount').is_not_null()
```

### Membership Operations

```python
# Membership with optional null matching
col('country').is_in(['US', 'CA'])
col('country').is_in([None, 'CA'], nulls_equal=True)

# Between with configurable interval closure
col('age').is_between(18, 65)                  # inclusive on both sides
col('score').is_between('min_score', 'max_score', closed='right')
```

- `is_in` accepts a sequence or Series; set `nulls_equal=True` to treat `None`
  as a distinct value instead of propagating nulls.
- `is_between` accepts expressions for bounds (strings are parsed as column
  names) and supports `closed='both' | 'left' | 'right' | 'none'`.

### Date/Time Operations

```python
# Extract components
col('created_at').dt.year() == 2024
col('created_at').dt.month() == 6  # June
col('created_at').dt.day() >= 15
col('created_at').dt.hour() < 18  # Before 6 PM
col('created_at').dt.minute() == 0  # On the hour
col('created_at').dt.second() == 0  # On the minute

# Date difference (returns float days)
col('check_out').dt.total_days(col('check_in')) >= 2  # Minimum 2 nights
col('created_at').dt.total_days(datetime(2024, 1, 1)) > 0  # After Jan 1, 2024
```

---

## 🙅‍♂️ Cross-Field Validation with `@model_validator`

Use `@model_validator` to implement validation rules that depend on multiple fields.

### Basic Cross-Field Validation

**Example: Price Comparison**

```python
from flycatcher import Schema, Field, col, model_validator

class ProductSchema(Schema):
    regular_price: float = Field(gt=0)
    sale_price: float | None = Field(default=None, gt=0, nullable=True)

    @model_validator
    def check_sale_price():
        """Sale price must be less than regular price."""
        return (
            col('sale_price') < col('regular_price'),
            "Sale price must be less than regular price"
        )
```

**How it works:**

1. The function returns a tuple: `(condition_expression, error_message)`
2. The condition should evaluate to `True` for valid data
3. Flycatcher compiles this to both Pydantic validators and Polars expressions

### Date Range Validation

**Example: Event Booking**

```python
from datetime import datetime
from flycatcher import Schema, Field, col, model_validator

class EventSchema(Schema):
    start_time: datetime
    end_time: datetime
    duration_hours: int = Field(ge=1, le=12)

    @model_validator
    def check_end_after_start():
        """End time must be after start time."""
        return (
            col('end_time') > col('start_time'),
            "Event must end after it starts"
        )
```

### Conditional Validation

**Example: Discount Rules**

```python
from flycatcher import Schema, Field, col, model_validator

class OrderSchema(Schema):
    item_type: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)
    discount_percent: float = Field(ge=0, le=100, default=0)

    @model_validator
    def bulk_discount_rule():
        """Orders of 10+ items get at least 10% discount."""
        return (
            (col('quantity') < 10) | (col('discount_percent') >= 10),
            "Bulk orders (10+) must have at least 10% discount"
        )
```

**Understanding the logic:**

- `col('quantity') < 10` - Small orders can have any discount
- `|` (OR) - Either condition can be true
- `col('discount_percent') >= 10` - Large orders must have discount
- **Result:** Small orders pass automatically, large orders need the discount

### Multiple Validators

You can add multiple validators to a single schema:

```python
from datetime import datetime
from flycatcher import Schema, Field, col, model_validator

class BookingSchema(Schema):
    check_in: datetime
    check_out: datetime
    guests: int = Field(ge=1)
    room_type: str

    @model_validator
    def check_dates():
        """Check-out must be after check-in."""
        return (
            col('check_out') > col('check_in'),
            "Check-out date must be after check-in date"
        )

    @model_validator
    def check_guest_capacity():
        """Rooms have max 4 guests, suites have max 8."""
        return (
            (
                ((col('room_type') == 'room') & (col('guests') <= 4))
                | ((col('room_type') == 'suite') & (col('guests') <= 8))
            ),
            "Room capacity exceeded (rooms: 4, suites: 8)"
        )

    @model_validator
    def check_minimum_stay():
        """Weekend bookings require 2+ night minimum."""
        # For weekends (check-in on Fri/Sat), require 2+ nights
        # Note: weekday() not yet available in DSL, use explicit Polars for now
        return (
            col('check_out').dt.total_days(col('check_in')) >= 2,
            "Minimum stay is 2 nights"
        )
```

---

## 🔄 Handling Nullable Fields

When validating optional fields, use the `|` (OR) operator to skip validation when the field is `None`:

```python
from flycatcher import Schema, Field, col, model_validator

class ProductSchema(Schema):
    regular_price: float = Field(gt=0)
    sale_price: float | None = Field(default=None, gt=0, nullable=True)  # Optional field

    @model_validator
    def check_sale_price():
        """If sale price is provided, it must be less than regular price."""
        return (
            col('sale_price').is_null()  # Skip if not provided
            | (col('sale_price') < col('regular_price')),  # Validate if provided
            "Sale price must be less than regular price when provided"
        )
```

**Pattern:** `col('field').is_null() | (validation_condition)`

This ensures validation only applies when the optional field has a value.

---

## 🧪 Testing Your Validators

### Test with Pydantic (Row-Level)

```python
from datetime import datetime
import pytest
from pydantic import ValidationError

# Generate Pydantic model
Booking = BookingSchema.to_pydantic()

# Valid booking
valid_booking = Booking(
    check_in=datetime(2024, 6, 1),
    check_out=datetime(2024, 6, 5),
    guests=2
)
assert valid_booking.guests == 2

# Invalid booking (check-out before check-in)
with pytest.raises(ValidationError) as exc_info:
    invalid_booking = Booking(
        check_in=datetime(2024, 6, 5),
        check_out=datetime(2024, 6, 1),
        guests=2
    )

assert "Check-out date must be after check-in date" in str(exc_info.value)
```

### Test with Polars (Bulk)

```python
import polars as pl
from datetime import datetime

# Generate Polars validator
BookingValidator = BookingSchema.to_polars_validator()

# Valid data
valid_df = pl.DataFrame({
    "check_in": [datetime(2024, 6, 1), datetime(2024, 7, 1)],
    "check_out": [datetime(2024, 6, 5), datetime(2024, 7, 5)],
    "guests": [2, 4]
})

validated = BookingValidator.validate(valid_df, strict=True)
assert len(validated) == 2

# Invalid data
invalid_df = pl.DataFrame({
    "check_in": [datetime(2024, 6, 5)],
    "check_out": [datetime(2024, 6, 1)],  # Before check-in!
    "guests": [2]
})

with pytest.raises(Exception) as exc_info:
    BookingValidator.validate(invalid_df, strict=True)

assert "Check-out date must be after check-in date" in str(exc_info.value)
```

---

## 🔍 Troubleshooting

### Validator Not Running

**Problem:** Your validator doesn't seem to execute.

**Solution:** Ensure it's decorated with `@model_validator` and returns a tuple:

```python
# ❌ Missing decorator
def my_check():
    return (col('x') > 0, "Error")

# ✅ Correct
@model_validator
def my_check():
    return (col('x') > 0, "Error")
```

### Type Errors in Polars

**Problem:** `TypeError` when validating with Polars.

**Solution:** Ensure your DSL expressions use Polars-compatible operations:

```python
# ❌ Python string method (doesn't work in Polars)
col('email').contains('@')

# ✅ Polars string method
col('email').str.contains('@')
```

### Validation Too Strict

**Problem:** Valid data is being rejected.

**Solution:** Add logging to see which condition is failing:

```python
@model_validator
def check_prices():
    return (
        col('sale_price') < col('regular_price'),
        f"Sale price must be less than regular price"
    )

# Test with show_violations to see failing rows
validator.validate(df, strict=True, show_violations=True)
```

---

## 🐾 Next Steps

<!-- - **[API Reference - Validators](../api/validators.md)** - Complete DSL reference -->
- **[Tutorials - First Schema](../tutorials/first-schema.md)** - Learn the basics
<!-- - **[Explanations - The Three Outputs](../explanations/three-outputs.md)** - Understand how validation works -->

---

## Need Help?

- 💬 [Ask in GitHub Discussions](https://github.com/mrmcmullan/flycatcher/discussions)
- 🐛 [Report a Bug](https://github.com/mrmcmullan/flycatcher/issues)
- 📖 [Read the Full Documentation](../index.md)

