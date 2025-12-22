# Flycatcher vs Dataframely Comparison

## Overview

Both **Flycatcher** and **Dataframely** are Python packages designed for schema validation of Polars DataFrames, but they take different approaches and have distinct design philosophies.

## Core Philosophy

### Flycatcher
- **"One schema, three representations"** - Define your schema once, generate optimized outputs for Pydantic, Polars, and SQLAlchemy
- **Multi-target generation** - Single source of truth that generates different representations for different use cases
- **Separation of concerns** - Keeps runtimes separate (no ORM ↔ DataFrame conversions)
- **DataFrame-native** - Built for modern data workflows that need both row-level and bulk validation

### Dataframely
- **Polars-focused validation** - Specialized for Polars DataFrame schema and content validation
- **Declarative schemas** - Define validation rules declaratively using classes
- **Enhanced type hints** - Provides improved type hints for validated DataFrames
- **Test data generation** - Can generate test data that complies with schemas

## Feature Comparison

| Feature | Flycatcher | Dataframely |
|---------|-----------|-------------|
| **Polars Validation** | ✅ | ✅ |
| **Pydantic Support** | ✅ | ❌ |
| **SQLAlchemy Support** | ✅ | ✅ (Integration) |
| **Cross-field/Column Validation** | ✅ (DSL-based) | ✅ |
| **Group Validation** | ❌ | ✅ |
| **Primary Key Constraints** | ✅ | ✅ |
| **Soft Validation (Filter Invalid Rows)** | ✅ (`strict=False`) | ✅ |
| **Type Hints Enhancement** | ❌ | ✅ |
| **Test Data Generation** | ❌ | ✅ |
| **PyArrow Integration** | ❌ | ✅ |
| **Single Schema Definition** | ✅ (Multi-target) | ✅ (Polars-focused) |

## Schema Definition

### Flycatcher
```python
from flycatcher import Schema, Field, col, model_validator

class UserSchema(Schema):
    id: int = Field(primary_key=True)
    name: str = Field(min_length=3, max_length=100)
    email: str = Field(pattern=r'^[^@]+@[^@]+\.[^@]+$')
    age: int = Field(ge=13, le=120)

    @model_validator
    def check_age():
        return (col('age') >= 18, "Must be 18 or older")

# Generate three outputs
UserModel = UserSchema.to_pydantic()
UserValidator = UserSchema.to_polars_validator()
UserTable = UserSchema.to_sqlalchemy()
```

### Dataframely
```python
import dataframely as dy
import polars as pl

class HouseSchema(dy.Schema):
    zip_code = dy.String(nullable=False, min_length=3)
    num_bedrooms = dy.UInt8(nullable=False)
    num_bathrooms = dy.UInt8(nullable=False)
    price = dy.Float64(nullable=False)

    @dy.rule()
    def reasonable_bathroom_to_bedroom_ratio(cls) -> pl.Expr:
        ratio = pl.col("num_bathrooms") / pl.col("num_bedrooms")
        return (ratio >= 1 / 3) & (ratio <= 3)

    @dy.rule(group_by=["zip_code"])
    def minimum_zip_code_count(cls) -> pl.Expr:
        return pl.len() >= 2

# Validate with enhanced type hints
validated_df: dy.DataFrame[HouseSchema] = HouseSchema.validate(df, cast=True)
```

## Key Differences

### 1. **Target Audience & Use Cases**

**Flycatcher** is ideal when you need:
- Multiple validation contexts (APIs, ETL pipelines, databases)
- Single source of truth across Pydantic, Polars, and SQLAlchemy
- Full-stack data validation (from API to database)
- ETL pipelines that need both row-level and bulk validation

**Dataframely** is ideal when you need:
- Polars-focused validation workflows
- Enhanced type hints for validated DataFrames
- Test data generation for development/testing
- Group validation across collections of DataFrames
- PyArrow integration

### 2. **Validation Approach**

**Flycatcher:**
- DSL-based cross-field validation (`col()` DSL)
- Validators work in both Pydantic and Polars contexts
- Explicit separation between row-level (Pydantic) and bulk (Polars) validation

**Dataframely:**
- Cross-column and group validation rules
- Validation constraints across collections of interdependent DataFrames
- Focus on DataFrame-level validation

### 3. **Integration & Ecosystem**

**Flycatcher:**
- Deep integration with Pydantic (generates actual Pydantic models)
- Deep integration with SQLAlchemy (generates actual SQLAlchemy tables)
- Polars validation as one of three outputs

**Dataframely:**
- Integration with SQLAlchemy (likely for schema export/import)
- PyArrow integration
- Enhanced type hints for better IDE support

### 4. **Additional Features**

**Flycatcher:**
- Generates actual Pydantic BaseModel classes
- Generates actual SQLAlchemy Table objects
- Cross-field validation DSL that compiles to both Pydantic and Polars
- Validation modes: strict (raise) vs non-strict (filter)

**Dataframely:**
- Test data generation
- Enhanced type hints for validated DataFrames
- Group validation (validation across multiple related DataFrames)
- Introspection of validation failures

## When to Choose Which?

### Choose **Flycatcher** if:
- ✅ You need validation across multiple contexts (API, ETL, database)
- ✅ You want a single schema definition for Pydantic, Polars, and SQLAlchemy
- ✅ You're building full-stack applications with APIs and databases
- ✅ You need both row-level (Pydantic) and bulk (Polars) validation
- ✅ You want to avoid schema drift across different parts of your stack

### Choose **Dataframely** if:
- ✅ You're primarily working with Polars DataFrames
- ✅ You need enhanced type hints for better IDE support
- ✅ You need test data generation capabilities
- ✅ You need group validation across multiple DataFrames
- ✅ You need PyArrow integration
- ✅ You want introspection of validation failures

## Current Status

### Flycatcher
- **Version:** 0.1.0 (Alpha)
- **Python:** 3.12+
- **Status:** Active development, v0.2.0 in progress
- **Dependencies:** Pydantic ≥2.0, Polars ≥0.19, SQLAlchemy ≥2.0

### Dataframely
- **Version:** 2.3.0 (Latest, Dec 2024)
- **Python:** 3.x
- **Status:** Mature, actively maintained
- **Dependencies:** Polars (primary), PyArrow (integration)
- **Repository:** [Quantco/dataframely](https://github.com/Quantco/dataframely)
- **Stars:** 477, Contributors: 22, Releases: 30

## Summary

**Flycatcher** is a **multi-target schema layer** that generates optimized representations for Pydantic, Polars, and SQLAlchemy from a single schema definition. It's designed for projects that need validation across the entire stack.

**Dataframely** is a **specialized Polars validation library** with enhanced type hints, test data generation, and group validation capabilities. It's designed for Polars-focused data workflows.

Both packages solve similar problems but target different use cases. Flycatcher excels at multi-context validation and preventing schema drift across your stack, while Dataframely excels at Polars-specific validation with enhanced developer experience features.

---

## Additional Resources

- **Dataframely:** [GitHub](https://github.com/Quantco/dataframely) | [Documentation](https://dataframely.readthedocs.io/)
- **Flycatcher:** [GitHub](https://github.com/mrmcmullan/flycatcher) | [Documentation](https://mrmcmullan.github.io/flycatcher)

For a detailed competitive analysis with specific recommendations, see [COMPETITIVE_ANALYSIS.md](COMPETITIVE_ANALYSIS.md).

