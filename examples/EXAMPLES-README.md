# Flycatcher Examples

This directory contains runnable examples demonstrating various features and use cases of Flycatcher.

## Examples

### [`basic_usage.py`](basic_usage.py)

**What it demonstrates:**

- Core Flycatcher workflow: define schema once, generate three outputs
- Pydantic model generation for row-level validation
- Polars validator generation for bulk DataFrame validation
- SQLAlchemy table generation for database operations

**Run it:**
```bash
python examples/basic_usage.py
```

**Use case:** Perfect starting point for understanding how Flycatcher works across different validation contexts.

---

### [`validation_modes.py`](validation_modes.py)

**What it demonstrates:**
- Strict validation mode (raises exceptions on errors)
- Non-strict validation mode (filters invalid rows)
- Violation reporting for debugging

**Run it:**
```bash
python examples/validation_modes.py
```

**Use case:** Learn how to handle validation errors in different scenarios, especially useful for ETL pipelines where you want to filter bad data rather than fail completely.

---

### [`etl_pipeline.py`](etl_pipeline.py)

**What it demonstrates:**
- Complete ETL workflow (Extract, Transform, Load)
- Using Polars for fast bulk validation
- Using SQLAlchemy for database operations
- Maintaining schema consistency across pipeline stages

**Run it:**
```bash
python examples/etl_pipeline.py
```

**Use case:** Real-world data pipeline where you need to validate large datasets before loading into a database. Shows how Flycatcher keeps your schema consistent from extraction to storage.

---

### [`cross_field_validators.py`](cross_field_validators.py)

**What it demonstrates:**
- Cross-field validation using the `col()` DSL
- Complex validation logic with multiple conditions
- Nullable field handling in validators
- Same validators working in both Pydantic and Polars contexts

**Run it:**
```bash
python examples/cross_field_validators.py
```

**Use case:** Enforce business rules that involve multiple fields, such as ensuring dates are in order, prices are consistent, or conditional logic based on field combinations.

---

## Running All Examples

To run all examples at once:

```bash
# From the project root
python examples/basic_usage.py
python examples/validation_modes.py
python examples/etl_pipeline.py
python examples/cross_field_validators.py
```

## Requirements

All examples require the following dependencies (already in `pyproject.toml`):

- `flycatcher` (the package itself)
- `polars` (for DataFrame validation)
- `pydantic` (for row-level validation)
- `sqlalchemy` (for database operations)

Install them with:

```bash
uv sync
# or
pip install -e .
```

## Notes

- Examples use sports-related data (basketball/player statistics) for consistency
- All examples are self-contained and runnable
- Examples demonstrate real-world patterns but keep complexity minimal
- SQLite is used for database examples (no external database required)

## Contributing

If you create additional examples, please:

1. Follow the existing naming convention (`snake_case.py`)
2. Include a docstring explaining what the example demonstrates
3. Add an entry to this README
4. Use type hints and grouped imports
5. Keep examples simple and focused on one concept



