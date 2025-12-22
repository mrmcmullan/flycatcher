"""Dialect-specific SQL generation for database operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import Table
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from sqlalchemy.sql import Insert


def get_dialect_name(engine: Engine) -> str:
    """Get the dialect name from a SQLAlchemy engine."""
    return engine.dialect.name


def create_upsert_statement(
    engine: Engine,
    table: Table,
    conflict_columns: list[str],
    update_columns: list[str],
) -> Any:
    """
    Create a dialect-specific upsert statement.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine to determine the dialect.
    table : Table
        SQLAlchemy table to upsert into.
    conflict_columns : list[str]
        Column names that define the conflict (typically primary key or unique).
    update_columns : list[str]
        Column names to update on conflict.

    Returns
    -------
    Insert statement with upsert clause for the specific dialect.
    """
    dialect = get_dialect_name(engine)
    logger.debug(f"Creating upsert statement for dialect: {dialect}")

    if dialect == "postgresql":
        return _postgresql_upsert(table, conflict_columns, update_columns)
    elif dialect == "mysql" or dialect == "mariadb":
        return _mysql_upsert(table, update_columns)
    elif dialect == "sqlite":
        return _sqlite_upsert(table, conflict_columns, update_columns)
    else:
        raise NotImplementedError(
            f"Upsert not implemented for dialect '{dialect}'. "
            f"Supported dialects: postgresql, mysql, mariadb, sqlite"
        )


def _postgresql_upsert(
    table: Table,
    conflict_columns: list[str],
    update_columns: list[str],
) -> "Insert":
    """
    PostgreSQL: INSERT ... ON CONFLICT DO UPDATE SET ...
    """
    stmt = postgresql.insert(table)
    
    # Build the ON CONFLICT DO UPDATE clause
    update_dict = {col: stmt.excluded[col] for col in update_columns}
    
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_dict,
    )
    
    return stmt


def _mysql_upsert(
    table: Table,
    update_columns: list[str],
) -> "Insert":
    """
    MySQL: INSERT ... ON DUPLICATE KEY UPDATE ...
    """
    stmt = mysql.insert(table)
    
    # Build the ON DUPLICATE KEY UPDATE clause
    update_dict = {col: stmt.inserted[col] for col in update_columns}
    
    stmt = stmt.on_duplicate_key_update(**update_dict)
    
    return stmt


def _sqlite_upsert(
    table: Table,
    conflict_columns: list[str],
    update_columns: list[str],
) -> "Insert":
    """
    SQLite: INSERT ... ON CONFLICT DO UPDATE SET ...
    (Requires SQLite 3.24.0+)
    """
    stmt = sqlite.insert(table)
    
    # Build the ON CONFLICT DO UPDATE clause
    update_dict = {col: stmt.excluded[col] for col in update_columns}
    
    stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_dict,
    )
    
    return stmt

