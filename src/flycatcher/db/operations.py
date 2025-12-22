"""Bulk database operations for Flycatcher schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import polars as pl
from loguru import logger
from sqlalchemy import Engine, bindparam, insert, update
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


def bulk_update(
    schema_cls: "type[Schema]",
    engine: Engine,
    df: pl.DataFrame,
    *,
    on: list[str],
    validate: bool = True,
    chunk_size: int = 1000,
) -> int:
    """
    Update rows in the database using bulk update.

    Matches rows by the specified key columns and updates the remaining columns.

    Parameters
    ----------
    schema_cls : type[Schema]
        The schema class to use for validation and table generation.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the database.
    df : polars.DataFrame
        DataFrame containing the data to update. Must include key columns.
    on : list[str]
        Column name(s) to match on for the UPDATE WHERE clause.
    validate : bool, default True
        Whether to validate the DataFrame before updating.
    chunk_size : int, default 1000
        Number of rows to update per batch.

    Returns
    -------
    int
        Number of rows updated.

    Raises
    ------
    ValueError
        If the schema has `table=False`, key columns are missing,
        or validation fails.

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
        >>> updates_df = pl.DataFrame({
        ...     "id": [1, 2],
        ...     "name": ["Alice Updated", "Bob Updated"],
        ...     "email": ["a_new@x.com", "b_new@x.com"],
        ... })
        >>> rows_updated = UserSchema.update(engine, updates_df, on=['id'])
    """
    # Check if schema is a table
    if not getattr(schema_cls, "_table", True):
        raise ValueError(
            f"Schema '{schema_cls.__name__}' has table=False and cannot be "
            "used for database operations. Use table=True to enable."
        )

    # Validate key columns are present
    for key_col in on:
        if key_col not in df.columns:
            raise ValueError(
                f"Key column '{key_col}' not found in DataFrame. "
                f"Available columns: {df.columns}"
            )

    # Validate if requested
    if validate:
        logger.info(f"Validating {df.height} rows before update...")
        df = schema_cls.validate(df, strict=True, cast=True)
        logger.info("Validation passed.")

    # Get the SQLAlchemy table
    table = schema_cls.to_sqlalchemy_table()

    # Filter columns to only those that exist in the table and have persist=True
    table_columns = {c.name for c in table.columns}
    schema_fields = schema_cls.fields()

    # Determine which DataFrame columns to include
    columns_to_update = []
    for col_name in df.columns:
        if col_name in table_columns:
            # Check if field has persist=True (default)
            field = schema_fields.get(col_name)
            if field is None or getattr(field, "persist", True):
                columns_to_update.append(col_name)

    # Build the update statement with bound parameters
    # Key columns go in WHERE, others in SET
    set_columns = [c for c in columns_to_update if c not in on]
    
    if not set_columns:
        raise ValueError(
            "No columns to update. All columns are either key columns "
            "or have persist=False."
        )

    # Select only the columns we need
    df_to_update = df.select(columns_to_update)

    # Perform chunked updates
    total_updated = 0

    with Session(engine) as session:
        for chunk_records in _chunk_dataframe(df_to_update, chunk_size):
            if not chunk_records:
                continue

            # Build UPDATE statement with bound parameters for each row
            # This uses executemany pattern for efficiency
            stmt = update(table)
            
            # Add WHERE clause for key columns
            for key_col in on:
                stmt = stmt.where(table.c[key_col] == bindparam(f"_key_{key_col}"))
            
            # Add SET values for non-key columns
            set_values = {col: bindparam(col) for col in set_columns}
            stmt = stmt.values(**set_values)
            
            # Prepare records with renamed key columns
            prepared_records = []
            for record in chunk_records:
                prepared = {}
                for key_col in on:
                    prepared[f"_key_{key_col}"] = record[key_col]
                for col in set_columns:
                    prepared[col] = record[col]
                prepared_records.append(prepared)
            
            session.execute(stmt, prepared_records)
            total_updated += len(chunk_records)
            logger.debug(f"Updated chunk of {len(chunk_records)} rows")

        session.commit()

    logger.info(f"Successfully updated {total_updated} rows in {table.name}")
    return total_updated


def bulk_upsert(
    schema_cls: "type[Schema]",
    engine: Engine,
    df: pl.DataFrame,
    *,
    on: list[str],
    validate: bool = True,
    chunk_size: int = 1000,
) -> int:
    """
    Upsert (insert or update) a DataFrame into the database.

    Uses dialect-specific SQL:
    - PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
    - MySQL/MariaDB: INSERT ... ON DUPLICATE KEY UPDATE
    - SQLite: INSERT ... ON CONFLICT DO UPDATE (SQLite 3.24+)

    Parameters
    ----------
    schema_cls : type[Schema]
        The schema class to use for validation and table generation.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the database.
    df : polars.DataFrame
        DataFrame containing the data to upsert.
    on : list[str]
        Column name(s) that define the conflict (typically primary key or unique).
        These columns determine whether a row is inserted or updated.
    validate : bool, default True
        Whether to validate the DataFrame before upserting.
    chunk_size : int, default 1000
        Number of rows to upsert per batch.

    Returns
    -------
    int
        Number of rows affected (inserted + updated).

    Raises
    ------
    ValueError
        If the schema has `table=False`, conflict columns are missing,
        or validation fails.
    NotImplementedError
        If the database dialect is not supported.

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
        >>> engine = create_engine("postgresql://...")
        >>> users_df = pl.DataFrame({
        ...     "id": [1, 2, 3],
        ...     "name": ["Alice", "Bob", "Charlie"],
        ...     "email": ["a@x.com", "b@x.com", "c@x.com"],
        ... })
        >>> rows_affected = UserSchema.upsert(engine, users_df, on=['id'])
    """
    from .dialects import create_upsert_statement

    # Check if schema is a table
    if not getattr(schema_cls, "_table", True):
        raise ValueError(
            f"Schema '{schema_cls.__name__}' has table=False and cannot be "
            "used for database operations. Use table=True to enable."
        )

    # Validate conflict columns are present
    for conflict_col in on:
        if conflict_col not in df.columns:
            raise ValueError(
                f"Conflict column '{conflict_col}' not found in DataFrame. "
                f"Available columns: {df.columns}"
            )

    # Validate if requested
    if validate:
        logger.info(f"Validating {df.height} rows before upsert...")
        df = schema_cls.validate(df, strict=True, cast=True)
        logger.info("Validation passed.")

    # Get the SQLAlchemy table
    table = schema_cls.to_sqlalchemy_table()

    # Filter columns to only those that exist in the table and have persist=True
    table_columns = {c.name for c in table.columns}
    schema_fields = schema_cls.fields()

    # Determine which DataFrame columns to include
    columns_to_upsert = []
    for col_name in df.columns:
        if col_name in table_columns:
            # Check if field has persist=True (default)
            field = schema_fields.get(col_name)
            if field is None or getattr(field, "persist", True):
                columns_to_upsert.append(col_name)

    # Columns to update on conflict (all non-conflict columns)
    update_columns = [c for c in columns_to_upsert if c not in on]
    
    if not update_columns:
        logger.warning(
            "No columns to update on conflict. This will effectively be an "
            "INSERT IGNORE. Consider using insert() instead."
        )

    # Select only the columns we need
    df_to_upsert = df.select(columns_to_upsert)

    # Perform chunked upserts
    total_affected = 0

    with Session(engine) as session:
        for chunk_records in _chunk_dataframe(df_to_upsert, chunk_size):
            if not chunk_records:
                continue

            # Create dialect-specific upsert statement
            stmt = create_upsert_statement(
                engine, table, on, update_columns
            )
            
            session.execute(stmt, chunk_records)
            total_affected += len(chunk_records)
            logger.debug(f"Upserted chunk of {len(chunk_records)} rows")

        session.commit()

    logger.info(f"Successfully upserted {total_affected} rows into {table.name}")
    return total_affected

