# Unified Schema

**Define your data models once. Generate Pydantic, Polars, and SQLAlchemy representations.**

---

## 🎯 Problem

When building data pipelines and applications, you often need:

- ✅ **Fast bulk validation** with Polars/Arrow
- ✅ **Typed ORM queries** with SQLAlchemy
- ✅ **Row-level validation** with Pydantic
- ❌ **Without duplicating schema definitions**

Existing solutions force you to:
- Define schemas multiple times (Pydantic model, SQLAlchemy table, Polars dtypes)
- Risk schema drift between systems
- Choose between row-oriented (ORM) or columnar (Polars) systems

## 💡 Solution

**Unified Schema** provides a single source of truth for your data models, then generates optimized representations for each use case:

```python
from schema import Schema, Integer, String, Datetime

class PlayerSchema(Schema):
    id = Integer(primary_key=True)
    name = String(max_length=100)
    age = Integer(optional=True)
    created_at = Datetime()

# Generate representations
Player = PlayerSchema.to_pydantic()        # For APIs & row validation
PlayerValidator = PlayerSchema.to_polars_model()  # For ETL & bulk ops
PlayerTable = PlayerSchema.to_sqlalchemy()  # For ORM queries
```

---

## 🚀 Quick Start

### Installation

```bash
pip install unified-schema
```

### Basic Usage

#### 1. Define Your Schema Once

```python
# models/user.py
from schema import Schema, Integer, String, Boolean, Datetime

class UserSchema(Schema):
    id = Integer(primary_key=True)
    username = String(max_length=50, unique=True)
    email = String(unique=True, index=True)
    is_active = Boolean(default=True)
    created_at = Datetime()
```

#### 2. Use Pydantic for APIs

```python
User = UserSchema.to_pydantic()

# Validate API requests
user = User(
    id=1,
    username="alice",
    email="alice@example.com",
    created_at=datetime.utcnow()
)

# Serialize to JSON
print(user.model_dump_json())
```

#### 3. Use Polars for ETL

```python
import polars as pl

UserValidator = UserSchema.to_polars_model()

# Load CSV
df = pl.read_csv("users.csv")

# Validate & enforce schema
validated_df = UserValidator.validate(df)

# Write to Parquet
validated_df.write_parquet("users.parquet")
```

#### 4. Use SQLAlchemy for Queries

```python
from sqlalchemy import create_engine

UserTable = UserSchema.to_sqlalchemy(table_name="users")

engine = create_engine("postgresql://localhost/mydb")
metadata.create_all(engine)

# Type-safe queries
with engine.connect() as conn:
    result = conn.execute(
        UserTable.select().where(UserTable.c.is_active == True)
    )
```

---

## 📦 Project Structure

```
unified-schema/
├── schema/
│   ├── __init__.py
│   ├── base.py              # Core Schema class
│   ├── fields.py            # Field type definitions
│   └── generators/
│       ├── __init__.py
│       ├── pydantic.py      # Pydantic model generator
│       ├── polars.py        # Polars validator generator
│       └── sqlalchemy.py    # SQLAlchemy table generator
├── models/
│   └── player.py            # Your schema definitions
├── examples/
│   └── usage_examples.py    # Complete examples
├── tests/
├── pyproject.toml
└── README.md
```

---

## 🔧 Field Types

| Field Type | Python Type | Polars Type | SQLAlchemy Type |
|------------|-------------|-------------|-----------------|
| `Integer()` | `int` | `pl.Int64` | `Integer` |
| `String()` | `str` | `pl.Utf8` | `String/Text` |
| `Float()` | `float` | `pl.Float64` | `Float` |
| `Boolean()` | `bool` | `pl.Boolean` | `Boolean` |
| `Datetime()` | `datetime` | `pl.Datetime` | `DateTime` |
| `Date()` | `date` | `pl.Date` | `Date` |

### Field Options

All fields support:
- `primary_key=True` - Mark as primary key (SQLAlchemy)
- `optional=True` - Allow null values
- `default=value` - Set default value
- `description="..."` - Add documentation
- `unique=True` - Enforce uniqueness (SQLAlchemy)
- `index=True` - Create database index (SQLAlchemy)

String fields also support:
- `max_length=100` - Maximum string length

---

## 🏗️ Design Philosophy

### ✅ What This Library Does

- Provides a **single source of truth** for schema definitions
- Generates **optimized representations** for different use cases
- Keeps runtimes **separate** (no ORM ↔ DataFrame conversions)
- Uses **stable public APIs** (Pydantic, Polars, SQLAlchemy)

### ❌ What This Library Doesn't Do

- Mix row-oriented and columnar paradigms
- Create a "unified runtime" (that would be slow)
- Reinvent validation logic (delegates to Polars/Pydantic)
- Depend on internal APIs of other libraries

### 🎯 The Sweet Spot

```
One Schema Definition
        ↓
    ┌───┴───┬────────┐
    ↓       ↓        ↓
Pydantic  Polars  SQLAlchemy
    ↓       ↓        ↓
  APIs    ETL     Queries
```

Each tool does what it's best at:
- **Polars**: Fast bulk validation & columnar I/O
- **Pydantic**: Row-level validation & serialization
- **SQLAlchemy**: Type-safe database queries

---

## 📊 Complete ETL Example

```python
import polars as pl
from sqlalchemy import create_engine

# 1. Define schema once
class OrderSchema(Schema):
    order_id = Integer(primary_key=True)
    customer_id = Integer(index=True)
    amount = Float()
    created_at = Datetime()

# 2. Extract: Load from CSV with Polars
OrderValidator = OrderSchema.to_polars_model()
df = pl.read_csv("orders.csv")

# 3. Transform: Validate schema
validated_df = OrderValidator.validate(df)

# 4. Load: Write to data lake
validated_df.write_parquet("orders.parquet")

# 5. Query: Use ORM for analytics
OrderTable = OrderSchema.to_sqlalchemy()
engine = create_engine("postgresql://localhost/analytics")

with engine.connect() as conn:
    # Load from parquet to database
    conn.execute(OrderTable.insert(), validated_df.to_dicts())

    # Query with type safety
    high_value = conn.execute(
        OrderTable.select().where(OrderTable.c.amount > 1000)
    )
```

---

## 🧪 Testing

```bash
# Run tests
pytest

# With coverage
pytest --cov=schema --cov-report=html

# Type checking
mypy schema/

# Code formatting
black schema/
ruff check schema/
```

---

## 🛣️ Roadmap

- [ ] Foreign key support
- [ ] Custom validators (e.g., regex, ranges)
- [ ] Enum field types
- [ ] JSON/Array field types
- [ ] Schema migrations
- [ ] CLI code generation tool
- [ ] Integration with Patito for richer Polars validation

---

## 📄 License

MIT License - see LICENSE file for details

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

## 💬 Feedback

Have questions or suggestions? Open an issue on GitHub!
