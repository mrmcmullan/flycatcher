# SQLAlchemy Generator

Generate SQLAlchemy Table objects from Flycatcher schemas.

::: flycatcher.generators.sqlalchemy.create_sqlalchemy_table
    options:
      show_root_heading: true
      show_source: true
      heading_level: 2

## Usage

The `create_sqlalchemy_table` function is typically called via the `Schema.to_sqlalchemy()` method:

```python
from flycatcher import Schema, Integer, String
from sqlalchemy import create_engine, MetaData

class UserSchema(Schema):
    id = Integer(primary_key=True)
    name = String(min_length=1, max_length=100)

# Generate SQLAlchemy table
metadata = MetaData()
users_table = UserSchema.to_sqlalchemy(table_name="users", metadata=metadata)

# Use with SQLAlchemy
engine = create_engine("sqlite:///example.db")
metadata.create_all(engine)
```

## Table Naming

If `table_name` is not provided, the table name is automatically generated from the schema class name:

- Removes "Schema" suffix if present
- Converts to lowercase
- Adds "s" for pluralization (simple: `UserSchema` → `users`)

```python
# Automatic naming
users_table = UserSchema.to_sqlalchemy()  # Table name: "users"

# Custom naming
users_table = UserSchema.to_sqlalchemy(table_name="app_users")
```

## Metadata Management

You can use a shared `MetaData` instance to manage multiple tables:

```python
from sqlalchemy import MetaData

metadata = MetaData()

users_table = UserSchema.to_sqlalchemy(metadata=metadata)
posts_table = PostSchema.to_sqlalchemy(metadata=metadata)

# Create all tables at once
metadata.create_all(engine)
```

