<div align="center" style="line-height: 1.2;">

<img src="https://raw.githubusercontent.com/mrmcmullan/flycatcher/main/docs/assets/logo.png" alt="Flycatcher Logo" width="400" style="margin-bottom: 0.5em;"/>

<!-- <h1 style="margin: 0.3em 0; font-family: system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; font-weight: 600; color: #000;">🐦 Flycatcher</h1> -->
<p style="margin: 0.2em 0; font-size: 1.3em;"><strong>Define your schema once. Validate at scale. Stay columnar.</strong></p>
<p style="margin: 0.2em 0;"><em>Built for DataFrames, powered across Pydantic, Polars, and SQLAlchemy.</em></p>

<p>
  <a href="https://github.com/mrmcmullan/flycatcher/actions/workflows/ci.yml" title="CI Status">
    <img src="https://github.com/mrmcmullan/flycatcher/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://codecov.io/gh/mrmcmullan/flycatcher" title="Codecov">
    <img src="https://codecov.io/gh/mrmcmullan/flycatcher/branch/main/graph/badge.svg" alt="codecov">
  </a>
  <a href="https://badge.fury.io/py/flycatcher" title="PyPI Version">
    <img src="https://badge.fury.io/py/flycatcher.svg" alt="PyPI version">
  </a>
  <a href="https://www.python.org/downloads/" title="Python 3.12+">
    <img src="https://img.shields.io/badge/python-3.12+-blue.svg" alt="Python 3.12+">
  </a>
  <a href="https://opensource.org/licenses/MIT" title="License: MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
  <a href="https://mmcmullan.github.io/flycatcher" title="Documentation">
    <img src="https://img.shields.io/badge/docs-mkdocs-blue.svg" alt="Documentation">
  </a>
</p>

</div>

---

Flycatcher is a **DataFrame-native schema layer** for Python. Define your data model once and generate optimized representations for every part of your stack:

- 🎯 **Pydantic models** for API validation & serialization
- ⚡ **Polars validators** for blazing-fast bulk validation
- 🗄️ **SQLAlchemy tables** for typed database access

**Built for modern data workflows:** Validate millions of rows at high speed, keep schema drift at zero, and stay columnar end-to-end.

---

## ❓ Why Flycatcher?

Many data projects need **row-level validation** (Pydantic), **efficient bulk operations** (Polars), and **typed database queries** (SQLAlchemy). But maintaining multiple schemas across this stack can lead to duplication, drift, and manually juggling row-oriented and columnar paradigms.

**Flycatcher solves this:** One schema definition → three optimized outputs.

```python
from flycatcher import Schema, Integer, String, Float, col, model_validator

class ProductSchema(Schema):
    id = Integer(primary_key=True)
    name = String(min_length=3, max_length=100)
    price = Float(gt=0)
    discount_price = Float(gt=0, nullable=True)

    @model_validator
    def check_discount():
        # Cross-field validation with DSL
        return (
            col('discount_price') < col('price'),
            "Discount price must be less than regular price"
        )

# Generate three optimized representations
ProductModel = ProductSchema.to_pydantic()         # → Pydantic BaseModel
ProductValidator = ProductSchema.to_polars_model() # → Polars DataFrame validator
ProductTable = ProductSchema.to_sqlalchemy()       # → SQLAlchemy Table
```

**Flycatcher lets you stay DataFrame-native without giving up the speed of Polars, the ergonomic validation of Pydantic, or the Pythonic power of SQLAlchemy**.

---

## 🚀 Quick Start

### Installation

```bash
pip install flycatcher
# or
uv add flycatcher
```

### Define Your Schema

```python
from flycatcher import Schema, Integer, String, Boolean, Datetime

class UserSchema(Schema):
    id = Integer(primary_key=True)
    username = String(min_length=3, max_length=50, unique=True)
    email = String(pattern=r'^[^@]+@[^@]+\.[^@]+$', unique=True, index=True)
    age = Integer(ge=13, le=120)
    is_active = Boolean(default=True)
    created_at = Datetime()
```

### Use Pydantic for Row-Level Validation

Perfect for APIs, forms, and single-record validation:

```python
from datetime import datetime

User = UserSchema.to_pydantic()

# Validates constraints automatically
user = User(
    id=1,
    username="alice",
    email="alice@example.com",
    age=25,
    created_at=datetime.utcnow()
)

# Serialize to JSON/dict
print(user.model_dump_json())
```

### Use Polars for Bulk Validation

Perfect for ETL, large-scale data pipelines, and DataFrame-level validation:

```python
import polars as pl

UserValidator = UserSchema.to_polars_model()

# Validate 1M+ rows with blazing speed
df = pl.read_csv("users.csv")
validated_df = UserValidator.validate(df, strict=True)

validated_df.write_parquet("validated_users.parquet")
```

### Use SQLAlchemy for Database Operations

Perfect for typed queries and database interactions:

```python
from sqlalchemy import create_engine

UserTable = UserSchema.to_sqlalchemy(table_name="users")

engine = create_engine("postgresql://localhost/mydb")

# Type-safe queries
with engine.connect() as conn:
    result = conn.execute(
        UserTable.select()
        .where(UserTable.c.is_active == True)
        .where(UserTable.c.age >= 18)
    )
    for row in result:
        print(row)
```

---

## ✨ Key Features

### Rich Field Types & Constraints

| Field Type | Constraints | Example |
|------------|-------------|---------|
| `Integer()` | `ge`, `gt`, `le`, `lt`, `multiple_of` | `age = Integer(ge=0, le=120)` |
| `Float()` | `ge`, `gt`, `le`, `lt` | `price = Float(gt=0)` |
| `String()` | `min_length`, `max_length`, `pattern` | `email = String(pattern=r'^[^@]+@...')` |
| `Boolean()` | - | `is_active = Boolean(default=True)` |
| `Datetime()` | Coming soon! | `created_at = Datetime()` |
| `Date()` | Coming soon! | `birth_date = Date()` |

**All fields support (validation):** `nullable`, `default`, `description`

**SQLAlchemy-specific:** `primary_key`, `unique`, `index`, `autoincrement`

### Custom & Cross-Field Validation

Use the `col()` DSL for powerful field-level and cross-field validation that works across both Pydantic and Polars:

```python
from flycatcher import Schema, Integer, Datetime, col, field_validator, model_validator

class BookingSchema(Schema):
    check_in = Datetime()
    check_out = Datetime()
    nights = Integer(ge=1)

    @field_validator
    def check_nights():
        # Require at least 3 nights for bookings in July or August (peak season)
        return (
            (~(col('check_in').dt.month.is_in([7, 8]))) | (col('nights') >= 3),
            "Minimum stay in July and August is 3 nights"
        )

    @model_validator
    def check_dates():
        return (
            col('check_out') > col('check_in'),
            "Check-out must be after check-in"
        )

```

### Validation Modes

Polars validation supports flexible error handling:

```python
# Strict mode: Raise on validation errors (default)
validated_df = UserValidator.validate(df, strict=True)

# Non-strict mode: Filter out invalid rows
valid_df = UserValidator.validate(df, strict=False)

# Show violations for debugging
validated_df = UserValidator.validate(df, strict=True, show_violations=True)
```

---

## 🎯 Complete Example: ETL Pipeline

```python
import polars as pl
from flycatcher import Schema, Integer, Float, String, Datetime, col, model_validator
from sqlalchemy import create_engine, MetaData

# 1. Define schema once
class OrderSchema(Schema):
    order_id = Integer(primary_key=True)
    customer_email = String(pattern=r'^[^@]+@[^@]+\.[^@]+$', index=True)
    amount = Float(gt=0)
    tax = Float(ge=0)
    total = Float(gt=0)
    created_at = Datetime()

    @model_validator
    def check_total():
        return (
            col('total') == col('amount') + col('tax'),
            "Total must equal amount + tax"
        )

# 2. Extract & Validate with Polars (handles millions of rows)
OrderValidator = OrderSchema.to_polars_model()
df = pl.read_csv("orders.csv")
validated_df = OrderValidator.validate(df, strict=True)

# 3. Load to database with SQLAlchemy
OrderTable = OrderSchema.to_sqlalchemy(table_name="orders")
engine = create_engine("postgresql://localhost/analytics")

with engine.connect() as conn:
    conn.execute(OrderTable.insert(), validated_df.to_dicts())
    conn.commit()
```

✅ **Result:** Validated millions of rows, enforced business rules, and loaded to database — all from one schema definition.

---

## 🏗️ Design Philosophy

**One schema, three representations. Each optimized for its use case.**

```
        Schema Definition
               ↓
    ┌──────────┼──────────┐
    ↓          ↓          ↓
Pydantic    Polars    SQLAlchemy
   ↓          ↓          ↓
 APIs       ETL      Database
```

### What Flycatcher Does

✅ Single source of truth for schema definitions
<br>
✅ Generate optimized representations for different use cases
<br>
✅ Keep runtimes separate (no ORM ↔ DataFrame conversions)
<br>
✅ Use stable public APIs (Pydantic, Polars, SQLAlchemy)
<br>

### What Flycatcher Doesn't Do

❌ Mix row-oriented and columnar paradigms
<br>
❌ Create a "unified runtime" (that would be slow)
<br>
❌ Reinvent validation logic (delegates to proven libraries)
<br>
❌ Depend on internal APIs

---

## 📊 Comparison

| Feature | Flycatcher | SQLModel | Patito |
|---------|-----------|----------|--------|
| Pydantic support | ✅ | ✅ | ✅ |
| Polars support | ✅ | ❌ | ✅ |
| SQLAlchemy support | ✅ | ✅ | ❌ |
| DataFrame-level DB ops | 🚧 (v0.2) | ❌ | ❌ |
| Cross-field validation | ✅ | ⚠️ (Pydantic only) | ⚠️ (Polars only) |
| Single schema definition | ✅ | ⚠️ (Pydantic + ORM hybrid) | ⚠️ (Pydantic + Polars hybrid) |

**Flycatcher** is the only library that generates optimized representations for all three systems while keeping them properly separated.

---

## 📚 Documentation

- **[Getting Started](https://mmcmullan.github.io/flycatcher/)** - Installation and basics
- **[Tutorials](https://mmcmullan.github.io/flycatcher/tutorials/)** - Step-by-step guides
- **[How-To Guides](https://mmcmullan.github.io/flycatcher/how-to/)** - Solve specific problems
- **[API Reference](https://mmcmullan.github.io/flycatcher/api/)** - Complete API documentation
- **[Explanations](https://mmcmullan.github.io/flycatcher/explanations/)** - Deep dives and concepts

---

## 🛣️ Roadmap

### v0.1.0 (Current) 🚧

- [x] Core schema definition with metaclass
- [x] Field types with constraints (Integer, String, Float, Boolean, Datetime, Date)
- [x] Pydantic model generator
- [x] Polars DataFrame validator with bulk validation
- [x] SQLAlchemy table generator
- [x] Cross-field validators with DSL (`col()`)
- [x] Test suite with 70%+ coverage
- [ ] Complete documentation site
- [ ] PyPI publication

### v0.2.0 (Planned)

- [ ] `@field_validator` support in addition to existing `@model_validator`
- [ ] Port & properly delegate other Pydantic & SQLAlchemy features
- [ ] DataFrame-level queries (`Schema.query()`)
- [ ] Bulk write operations (`Schema.insert()`, `Schema.update()`, `Schema.upsert()`)
- [ ] Complete ETL loop staying columnar end-to-end

### v0.3.0+ (Future)

- [ ] Additional field types (Enum, UUID, JSON, Array)
- [ ] JOIN support in queries
- [ ] Aggregations (GROUP BY, COUNT, SUM)
- [ ] Foreign key relationships
- [ ] Schema migrations helper

<!-- See our [full roadmap](docs/dev/ROADMAP.md) for details. -->

## 🤝 Contributing

Contributions are welcome! Please see our <!-- [Contributing Guide](CONTRIBUTING.md) --> for details.

**Priority areas for v0.1.0:**
- Test coverage improvements
- Documentation enhancements
- Bug fixes and error message improvements

---

## 📄 License

MIT License - see <!-- [LICENSE](LICENSE) --> for details.

---

## 💬 Community

- **[GitHub Issues](https://github.com/mrmcmullan/flycatcher/issues)** - Bug reports and feature requests
- **[GitHub Discussions](https://github.com/mrmcmullan/flycatcher/discussions)** - Questions and community discussion
- **[Documentation](https://mrmcmullan.github.io/flycatcher)** - Full guides and API reference

---

<div align="center">



<p><strong>Built with ❤️ for the DataFrame generation</strong></p>

<p>
  <a href="https://github.com/mmcmullan/flycatcher">⭐ Star us on GitHub</a>
  &nbsp;|&nbsp;
  <a href="https://mmcmullan.github.io/flycatcher">📖 Read the docs</a>
  &nbsp;|&nbsp;
  <a href="https://github.com/mmcmullan/flycatcher/issues">🐛 Report a bug</a>
</p>

</div>
