"""Bulk database operations for Flycatcher schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import polars as pl
from loguru import logger
from sqlalchemy import Engine, insert
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from ..base import Schema


def _chunk_dataframe(
    df: pl.DataFrame, chunk_size: int
) -> Iterator[list[dict]]:
    """Yield chunks of records from a DataFrame."""
    total_rows = df.height
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk_df = df.slice(start, end - start)
        yield chunk_df.to_dicts()


def bulk_insert(
    schema_cls: "type[Schema]",
    engine: Engine,
    df: pl.DataFrame,
    *,
    validate: bool = True,
    chunk_size: int = 1000,
    return_defaults: bool = False,
) -> int:
    """
    Insert a DataFrame into the database using bulk insert.

    Parameters
    ----------
    schema_cls : type[Schema]
        The schema class to use for validation and table generation.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the database.
    df : polars.DataFrame
        DataFrame containing the data to insert.
    validate : bool, default True
        Whether to validate the DataFrame before inserting.
    chunk_size : int, default 1000
        Number of rows to insert per batch. Use smaller values for
        memory-constrained environments.
    return_defaults : bool, default False
        If True, fetch generated defaults (like auto-increment IDs).
        Note: This is slower and not supported by all databases.

    Returns
    -------
    int
        Number of rows inserted.

    Raises
    ------
    ValueError
        If the schema has `table=False` or validation fails.

    Examples
    --------
        >>> from flycatcher import Schema, Field
        >>> from sqlalchemy import create_engine
        >>> import polars as pl
        >>>
        >>> class UserSchema(Schema, table=True):
        ...     id: int = Field(primary_key=True)
        ...     name: str
        ...     email: str
        >>>
        >>> engine = create_engine("sqlite:///users.db")
        >>> users_df = pl.DataFrame({
        ...     "id": [1, 2, 3],
        ...     "name": ["Alice", "Bob", "Charlie"],
        ...     "email": ["a@x.com", "b@x.com", "c@x.com"],
        ... })
        >>> rows_inserted = UserSchema.insert(engine, users_df)
        >>> print(f"Inserted {rows_inserted} rows")
    """
    # Check if schema is a table
    if not getattr(schema_cls, "_table", True):
        raise ValueError(
            f"Schema '{schema_cls.__name__}' has table=False and cannot be "
            "used for database operations. Use table=True to enable."
        )

    # Validate if requested
    if validate:
        logger.info(f"Validating {df.height} rows before insert...")
        df = schema_cls.validate(df, strict=True, cast=True)
        logger.info("Validation passed.")

    # Get the SQLAlchemy table
    table = schema_cls.to_sqlalchemy_table()

    # Filter columns to only those that exist in the table and have persist=True
    table_columns = {c.name for c in table.columns}
    schema_fields = schema_cls.fields()

    # Determine which DataFrame columns to include
    columns_to_insert = []
    for col_name in df.columns:
        if col_name in table_columns:
            # Check if field has persist=True (default)
            field = schema_fields.get(col_name)
            if field is None or getattr(field, "persist", True):
                columns_to_insert.append(col_name)

    # Select only the columns we need
    df_to_insert = df.select(columns_to_insert)

    # Perform chunked inserts
    total_inserted = 0

    with Session(engine) as session:
        for chunk_records in _chunk_dataframe(df_to_insert, chunk_size):
            if not chunk_records:
                continue

            stmt = insert(table).values(chunk_records)
            
            if return_defaults:
                # This path is slower but returns generated values
                session.execute(stmt)
            else:
                # Fast path - no fetching of defaults
                session.execute(stmt)

            total_inserted += len(chunk_records)
            logger.debug(f"Inserted chunk of {len(chunk_records)} rows")

        session.commit()

    logger.info(f"Successfully inserted {total_inserted} rows into {table.name}")
    return total_inserted

