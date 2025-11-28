# Examples

Flycatcher includes **several runnable examples** demonstrating different features and use cases. All examples are located in the [`examples/`](https://github.com/mrmcmullan/flycatcher/tree/main/examples) directory on GitHub.

## Available Examples

### [Basic Usage](https://github.com/mrmcmullan/flycatcher/blob/main/examples/basic_usage.py)

Demonstrates the **core Flycatcher workflow:**

- Define a schema once
- Generate Pydantic models for row-level validation
- Generate Polars validators for bulk DataFrame validation
- Generate SQLAlchemy tables for database operations

**Perfect for:** Understanding how Flycatcher works across different validation contexts.

---

### [Validation Modes](https://github.com/mrmcmullan/flycatcher/blob/main/examples/validation_modes.py)

Shows **different validation modes** available in Polars:

- **Strict mode**: Raises exceptions on validation errors (default)
- **Non-strict mode**: Filters out invalid rows instead of raising
- **Violation reporting**: Get detailed information about what failed

**Perfect for:** Learning how to handle validation errors in ETL pipelines where you want to filter bad data rather than fail completely.

---

### [ETL Pipeline](https://github.com/mrmcmullan/flycatcher/blob/main/examples/etl_pipeline.py)

Complete **ETL (Extract, Transform, Load)** workflow:

- Extract data from CSV
- Transform and validate using Polars
- Load validated data into a database using SQLAlchemy

**Perfect for:** Real-world data pipelines where you need to validate large datasets before loading into a database.

---

### [Cross-Field Validators](https://github.com/mrmcmullan/flycatcher/blob/main/examples/cross_field_validators.py)

Demonstrates **cross-field validation** using the `col()` DSL:

- Complex validation logic with multiple conditions
- Nullable field handling in validators
- Same validators working in both Pydantic and Polars contexts

**Perfect for:** Enforcing business rules that involve multiple fields, such as ensuring dates are in order or prices are consistent.

---

## Running Examples

All examples are **self-contained** and can be run directly:

```bash
# From the project root
python examples/basic_usage.py
python examples/validation_modes.py
python examples/etl_pipeline.py
python examples/cross_field_validators.py
```

## Requirements

Examples require the **standard Flycatcher dependencies:**

- `flycatcher`
- `polars`
- `pydantic`
- `sqlalchemy`

## Viewing Examples on GitHub

Browse all examples in the [examples directory](https://github.com/mrmcmullan/flycatcher/tree/main/examples) on GitHub. Each example includes:

- Detailed docstrings explaining what it demonstrates
- Inline comments explaining key steps
- Type hints for clarity
- Real-world sports-related data for consistency

## Contributing Examples

If you create additional examples, please follow these guidelines:

1. Follow the existing naming convention (`snake_case.py`)
2. Include a docstring explaining what the example demonstrates
3. Add an entry to [`examples/README.md`](https://github.com/mrmcmullan/flycatcher/blob/main/examples/README.md)
4. Use type hints and grouped imports
5. Keep examples simple and focused on one concept
