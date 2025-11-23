# API Reference

Complete **API documentation** for Flycatcher, organized by module.

## 📖 Overview

Flycatcher's API is organized into four main areas:

- **[Schema](schema.md)** - Core schema definition and validation
- **[Fields](fields.md)** - Field types and constraints
- **[Validators](validators.md)** -  Custom field-level and cross-field validation DSL
- **[Generators](generators/)** - Framework-specific output generators

## 🔍 Quick Navigation

### Core Classes

- [`Schema`](schema.md#flycatcher.base.Schema) - Base class for defining schemas
- [`Field`](fields.md#flycatcher.fields.Field) - Base field class
- [`FieldRef`](validators.md#flycatcher.validators.FieldRef) - Field reference for validator DSL

### Field Types

- [`Integer`](fields.md#flycatcher.fields.Integer) - Integer field with numeric constraints
- [`Float`](fields.md#flycatcher.fields.Float) - Float field with numeric constraints
- [`String`](fields.md#flycatcher.fields.String) - String field with length and pattern constraints
- [`Boolean`](fields.md#flycatcher.fields.Boolean) - Boolean field
- [`Datetime`](fields.md#flycatcher.fields.Datetime) - Datetime field
- [`Date`](fields.md#flycatcher.fields.Date) - Date field

### Validators

- [`col()`](validators.md#flycatcher.validators.col) - Convenience alias for creating a field reference for validators
- [`model_validator`](schema.md#flycatcher.base.model_validator) - Decorator for cross-field validation

### Generators

- **[Pydantic Generator](generators/pydantic.md)** - Generate Pydantic models
- **[Polars Generator](generators/polars.md)** - Generate Polars validators
- **[SQLAlchemy Generator](generators/sqlalchemy.md)** - Generate SQLAlchemy tables

## 📝 Usage Pattern

The typical workflow is:

1. **Define a schema** using `Schema` and field types
2. **Add validators** using `@model_validator` and the `col()` DSL
3. **Generate outputs** using `.to_pydantic()`, `.to_polars_model()`, or `.to_sqlalchemy()`

```python
from flycatcher import Schema, Integer, String, col, model_validator

class UserSchema(Schema):
    id = Integer(primary_key=True)
    name = String(min_length=1, max_length=100)
    age = Integer(ge=0, le=120)

    @model_validator
    def check_age_name():
        return col('age') >= 18, "Must be 18 or older"

# Generate outputs
UserModel = UserSchema.to_pydantic()
UserValidator = UserSchema.to_polars_model()
UserTable = UserSchema.to_sqlalchemy()
```

