# Polars Generator

Generate Polars DataFrame validators from Flycatcher schemas.

::: flycatcher.generators.polars.create_polars_validator
    options:
      show_root_heading: true
      show_source: true
      heading_level: 2

::: flycatcher.generators.polars.PolarsValidator
    options:
      show_root_heading: true
      show_source: true
      heading_level: 2

## Usage

The `create_polars_validator` function is typically called via the `Schema.to_polars_model()` method:

```python
from flycatcher import Schema, Integer, String
import polars as pl

class UserSchema(Schema):
    id = Integer(primary_key=True)
    name = String(min_length=1, max_length=100)

# Generate Polars validator
validator = UserSchema.to_polars_model()

# Validate a DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})

validated_df = validator.validate(df, strict=True)
```

## Validation Modes

The `PolarsValidator.validate()` method supports different validation modes:

- **Strict mode** (`strict=True`): Raises exceptions on validation errors
- **Non-strict mode** (`strict=False`): Filters out invalid rows
- **Show violations** (`show_violations=True`): Prints violation details to console
- **Fill nulls** (`fill_nulls=True`): Replaces null values with field defaults

```python
# Non-strict validation (filter invalid rows)
valid_df = validator.validate(df, strict=False)

# Show violations in console
validator.validate(df, strict=True, show_violations=True)

# Fill nulls with defaults
filled_df = validator.validate(df, fill_nulls=True)
```

