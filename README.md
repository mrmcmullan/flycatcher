# 🐦 Flycatcher

**Define your schema once & for all — built for DataFrames, powered across Pydantic, Polars, and SQLAlchemy.**

Flycatcher is a **DataFrame-native schema layer** for Python data systems.
Define your data model once and generate optimized representations for every part of your stack:

- **Pydantic models** for API validation & serialization
- **Polars validators** for blazing-fast columnar validation
- **SQLAlchemy tables** for typed database access

**Flycatcher is built for modern data workflows:** validate millions of rows at high speed, keep schema drift at zero, and interact with databases efficiently — **all without leaving the DataFrame world.**

---

## 🎯 Problem

When building data pipelines and applications, you often need:

- ✅ **Fast bulk validation** with Polars
- ✅ **Typed ORM queries** with SQLAlchemy
- ✅ **Row-level validation** with Pydantic
- ❌ **Without duplicating schema definitions**

Existing solutions force you to:

- Manage multiple schema definitions (Pydantic model, SQLAlchemy table, Polars dtypes)
- Risk schema drift between systems
- Choose between row-oriented (Pydantic/SQLAlchemy) or columnar (Polars) paradigms

## 💡 Solution

**Flycatcher** provides a single source of truth for your data models,
then generates optimized representations for each use case:

```python
from flycatcher import Schema, Integer, String, Datetime

class PlayerSchema(Schema):
    id = Integer(primary_key=True)
    name = String(max_length=100)
    age = Integer(nullable=True)
    created_at = Datetime()

# Generate representations
PlayerModel = PlayerSchema.to_pydantic()        # For APIs & row validation
PlayerValidator = PlayerSchema.to_polars_model()  # For ETL & bulk ops
PlayerTable = PlayerSchema.to_sqlalchemy()  # For ORM queries
```

## 🍎 Benefits

You get:

- **Columnar-first performance** for validation and bulk DB writes
- **Zero schema duplication** across Pydantic, Polars, and SQLAlchemy
- **Type safety** across your entire stack
- **Cleaner, more maintainable codebases**
- **Fast DataFrame-oriented workflows** without mixing row-based and columnar paradigms

**Flycatcher lets you stay DataFrame-native without giving up the speed of Polars, the ergonomics of Pydantic, or the power of SQLAlchemy.**

---

## 🚀 Quick Start

### Installation

```bash
pip install flycatcher
```

### 1. Define Your Schema Once

```python title="models/user.py"
from flycatcher import Schema, Integer, String, Boolean, Datetime

class UserSchema(Schema):
    id = Integer(primary_key=True)
    username = String(max_length=50, unique=True)
    email = String(unique=True, index=True)
    is_active = Boolean(default=True)
    created_at = Datetime()
```

#### 2. Validate row-level data with Pydantic

```python
User = UserSchema.to_pydantic()

user = User(
    id=1,
    username="alice",
    email="alice@example.com",
    created_at=datetime.utcnow()
)

print(user.model_dump())
```

#### 3. Validate entire dataframes with Polars

```python
import polars as pl

UserValidator = UserSchema.to_polars_model()

df = pl.read_csv("users.csv")
validated_df = UserValidator.validate(df, strict=True)
```

#### 4. Use SQLAlchemy for Typed Queries

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
