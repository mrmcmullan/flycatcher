# 🐣 Building Your First Schema

In this tutorial, **you'll learn the fundamentals of Flycatcher** by building a complete schema for a simple blog system. By the end, you'll understand how to:

- **Define a schema** with multiple field types
- **Add constraints** to validate data
- Generate **Pydantic** models for API validation
- Generate **Polars** validators for bulk operations
- Generate **SQLAlchemy** tables for database access

**Time to complete:** ~15 minutes

---

## 🔨 What We're Building

We'll create a schema for blog posts with the following requirements:

- **Unique** post IDs
- Titles between 5-200 characters
- Content with **minimum length**
- **Optional** tags
- Author **emails** (validated format)
- View counts (non-negative)
- Publication **timestamps**

This is a realistic example that demonstrates most of Flycatcher's core features.

---

## 📦 Prerequisites

Make sure you have Flycatcher installed:

```bash
pip install flycatcher
# or
uv add flycatcher
```

---

## 1️⃣ Step 1: Create Your First Schema

Let's start with a minimal schema. Create a new Python file called `blog.py`:

```python
from flycatcher import Schema, Integer, String

class PostSchema(Schema):
    id = Integer(primary_key=True)
    title = String()
    content = String()
```

That's it! You've defined your first schema with three fields.

### Understanding the Code

- **`Schema`** - Base class for all schemas. Uses metaclass magic to collect fields.
- **`Integer(primary_key=True)`** - Creates an integer field marked as the primary key (useful for databases).
- **`String()`** - Creates a string field with no constraints.

---

## 2️⃣ Step 2: Add Field Constraints

Let's make our schema more robust by adding validation constraints:

```python
from flycatcher import Schema, Integer, String, Float, Datetime

class PostSchema(Schema):
    id = Integer(primary_key=True)
    title = String(min_length=5, max_length=200)
    content = String(min_length=100)
    author_email = String(pattern=r'^[^@]+@[^@]+\.[^@]+$', index=True)
    view_count = Integer(ge=0, default=0)
    published_at = Datetime()
    tags = String(nullable=True)
```

### What Changed?

- **`min_length` / `max_length`** - Validates string length (titles must be 5-200 chars)
- **`pattern`** - Regex validation for email format
- **`ge=0`** - "Greater than or equal" constraint (view count can't be negative)
- **`default=0`** - Default value when none is provided
- **`nullable=True`** - Allows null/None values (tags are optional)
- **`index=True`** - Creates a database index on this field (for faster queries)

!!! tip "Field Constraints"
    All constraints work across **both** Pydantic and Polars! Define once, validate everywhere.

---

## 3️⃣ Step 3: Generate a Pydantic Model

Now let's use our schema to validate individual blog posts (perfect for APIs):

```python
from datetime import datetime

# Generate Pydantic model
Post = PostSchema.to_pydantic()

# Create and validate a post
post = Post(
    id=1,
    title="My First Blog Post",
    content="This is the content of my blog post. " * 10,  # Make it long enough
    author_email="alice@example.com",
    view_count=42,
    published_at=datetime(2024, 1, 15, 10, 30),
    tags="python, tutorial"
)

# Access fields
print(f"Post ID: {post.id}")
print(f"Title: {post.title}")

# Serialize to JSON
print(post.model_dump_json(indent=2))
```

### What Happens Here?

1. **`to_pydantic()`** generates a Pydantic `BaseModel` from your schema
2. All field constraints are translated to Pydantic validators
3. You get **full Pydantic functionality:** validation, serialization, type hints

### Try Breaking It!

What happens if you violate a constraint?

```python
# This will raise a ValidationError
invalid_post = Post(
    id=1,
    title="Hi",  # Too short! (min_length=5)
    content="Short",  # Too short! (min_length=100)
    author_email="not-an-email",  # Invalid format
    view_count=-5,  # Negative! (ge=0)
    published_at=datetime.now(),
)
```

Pydantic will tell you exactly what's wrong with helpful error messages!

For example, running the above code will print an error like:

```text
pydantic_core._pydantic_core.ValidationError: 4 validation errors for Post
title
  String should have at least 5 characters [input_value='Hi', input_type=str]
content
  String should have at least 100 characters [input_value='Short', input_type=str]
author_email
  Invalid email address: value is not a valid email address [input_value='not-an-email', input_type=str]
view_count
  Input should be greater than or equal to 0 [input_value=-5, input_type=int]
```

You'll get a detailed list of exactly which fields failed and why!

---

## 4️⃣ Step 4: Generate a Polars Validator

For bulk operations (like validating 10,000 blog posts from a CSV), use the Polars validator:

```python
import polars as pl

# Generate Polars validator
PostValidator = PostSchema.to_polars_model()

# Create sample data (imagine this came from a CSV)
df = pl.DataFrame({
    "id": [1, 2, 3],
    "title": ["First Post", "Second Post", "Another Great Article"],
    "content": ["This is long enough content for validation. " * 10] * 3,
    "author_email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "view_count": [10, 25, 100],
    "published_at": [datetime(2024, 1, i) for i in range(1, 4)],
    "tags": ["python", "rust", None]  # Third post has no tags
})

# Validate the entire DataFrame
validated_df = PostValidator.validate(df, strict=True)

print(f"✓ Validated {len(validated_df)} posts")
print(validated_df)
```

### What's Happening?

1. **`to_polars_model()`** generates a validator optimized for DataFrames
2. **`validate()`** checks all constraints in bulk (much faster than row-by-row!)
3. **`strict=True`** raises an error if any row fails validation

### Validation Modes

```python
# Strict mode (default): Raise on errors
validated_df = PostValidator.validate(df, strict=True)

# Non-strict mode: Filter out invalid rows
valid_df = PostValidator.validate(df, strict=False)

# Show violations for debugging
validated_df = PostValidator.validate(df, strict=True, show_violations=True)
```

!!! tip "Performance"
    Polars validation is **orders of magnitude faster** than validating row-by-row with Pydantic. Use it for large datasets!

---

## 5️⃣ Step 5: Generate a SQLAlchemy Table

Finally, let's create a database table for storing our posts:

```python
from sqlalchemy import create_engine, MetaData

# Generate SQLAlchemy table
metadata = MetaData()
PostTable = PostSchema.to_sqlalchemy(table_name="posts", metadata=metadata)

# Create an in-memory SQLite database
engine = create_engine("sqlite:///:memory:", echo=True)

# Create the table
metadata.create_all(engine)

# Insert data
with engine.connect() as conn:
    # Insert from our validated DataFrame
    conn.execute(PostTable.insert(), validated_df.to_dicts())
    conn.commit()

    # Query posts by author
    result = conn.execute(
        PostTable.select()
        .where(PostTable.c.author_email == "alice@example.com")
    )

    for row in result:
        print(f"Found post: {row.title}")
```

### What's Happening?

1. **`to_sqlalchemy()`** generates a SQLAlchemy `Table` object
2. All relevant field attributes (**primary keys, indexes, types**) are translated
3. You get type-safe database operations with SQLAlchemy Core

### Field Attributes in SQL

The schema's field attributes map to SQL features:

- **`primary_key=True`** → `PRIMARY KEY`
- **`unique=True`** → `UNIQUE` constraint
- **`index=True`** → Creates an index
- **`nullable=True`** → Allows `NULL` values
- **`default=X`** → `DEFAULT X`

---

## 6️⃣ Step 6: Putting It All Together

Here's a complete example showing all three outputs in action:

```python
from datetime import datetime
import polars as pl
from sqlalchemy import create_engine, MetaData
from flycatcher import Schema, Integer, String, Float, Datetime

# 1. Define schema once
class PostSchema(Schema):
    id = Integer(primary_key=True)
    title = String(min_length=5, max_length=200)
    content = String(min_length=100)
    author_email = String(pattern=r'^[^@]+@[^@]+\.[^@]+$', index=True)
    view_count = Integer(ge=0, default=0)
    published_at = Datetime()
    tags = String(nullable=True)

# 2. Validate single record with Pydantic
Post = PostSchema.to_pydantic()
post = Post(
    id=1,
    title="Understanding Flycatcher",
    content="Flycatcher makes schema management easy. " * 10,
    author_email="author@example.com",
    published_at=datetime.now()
)
print(f"✓ Validated single post: {post.title}")

# 3. Validate bulk data with Polars
PostValidator = PostSchema.to_polars_model()
df = pl.read_csv("posts.csv")  # Imagine you have this file
validated_df = PostValidator.validate(df, strict=True)
print(f"✓ Validated {len(validated_df)} posts from CSV")

# 4. Store in database with SQLAlchemy
metadata = MetaData()
PostTable = PostSchema.to_sqlalchemy(table_name="posts", metadata=metadata)
engine = create_engine("sqlite:///blog.db")
metadata.create_all(engine)

with engine.connect() as conn:
    conn.execute(PostTable.insert(), validated_df.to_dicts())
    conn.commit()
    print("✓ Stored posts in database")
```

✅ **Result:** One schema definition → Three optimized outputs → Complete data pipeline!

---

## 🐾 Next Steps

Congratulations! 🎉 You've built your first Flycatcher schema and learned how to use all three outputs.

### Go Deeper

<!-- - **[Custom Validators](../how-to/custom-validators.md)** - Add cross-field validation -->
<!-- - **[Field Types Reference](../api/fields.md)** - Explore all available field types -->
<!-- - **[Why Flycatcher?](../explanations/comparison.md)** - Understand how it compares to alternatives -->

### Try These Exercises

1. **Add a rating field** (Integer, 1-5 stars)
2. **Make title unique** (`unique=True`)
3. **Add a slug field** (URL-safe version of title, with pattern validation)
4. **Create a CommentSchema** that references PostSchema

### Get Help

<!-- - 📖 [API Reference](../api/index.md) -->
- 💬 [GitHub Discussions](https://github.com/mrmcmullan/flycatcher/discussions)
- 🐛 [Report Issues](https://github.com/mrmcmullan/flycatcher/issues)

---

## Full Example Code

Here's the complete working example from this tutorial:

```python title="blog.py"
"""Complete blog schema example."""
from datetime import datetime
import polars as pl
from sqlalchemy import create_engine, MetaData
from flycatcher import Schema, Integer, String, Datetime


class PostSchema(Schema):
    """Schema for blog posts."""

    id = Integer(
        primary_key=True,
        description="Unique identifier for the post"
    )
    title = String(
        min_length=5,
        max_length=200,
        description="Post title"
    )
    content = String(
        min_length=100,
        description="Post content body"
    )
    author_email = String(
        pattern=r'^[^@]+@[^@]+\.[^@]+$',
        index=True,
        description="Author's email address"
    )
    view_count = Integer(
        ge=0,
        default=0,
        description="Number of views"
    )
    published_at = Datetime(
        description="Publication timestamp"
    )
    tags = String(
        nullable=True,
        description="Comma-separated tags"
    )


def main():
    """Demonstrate all three outputs."""

    # 1. Pydantic for single record validation
    Post = PostSchema.to_pydantic()
    post = Post(
        id=1,
        title="My First Post",
        content="This is a great blog post! " * 15,
        author_email="author@example.com",
        published_at=datetime.now(),
        tags="python, tutorial"
    )
    print(f"✓ Created post: {post.title}")

    # 2. Polars for bulk validation
    PostValidator = PostSchema.to_polars_model()
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "title": ["First Post", "Second Post", "Third Post"],
        "content": ["Long content here! " * 15] * 3,
        "author_email": ["a@ex.com", "b@ex.com", "c@ex.com"],
        "view_count": [10, 25, 50],
        "published_at": [datetime.now()] * 3,
        "tags": ["python", "rust", None]
    })
    validated_df = PostValidator.validate(df, strict=True)
    print(f"✓ Validated {len(validated_df)} posts")

    # 3. SQLAlchemy for database operations
    metadata = MetaData()
    PostTable = PostSchema.to_sqlalchemy(table_name="posts", metadata=metadata)
    engine = create_engine("sqlite:///blog.db")
    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(PostTable.insert(), validated_df.to_dicts())
        conn.commit()
        print("✓ Stored posts in database")

        # Query example
        result = conn.execute(
            PostTable.select().where(PostTable.c.view_count > 20)
        )
        print(f"✓ Found {len(result.fetchall())} posts with >20 views")


if __name__ == "__main__":
    main()
```

Run it with:

```bash
python blog.py
```

Happy schema building! 🚀

