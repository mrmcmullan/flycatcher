"""SQLAlchemy table generator."""

from typing import TYPE_CHECKING

from sqlalchemy import Column, MetaData, Table

from ..fields import _MISSING

if TYPE_CHECKING:
    from ..base import Schema


def create_sqlalchemy_table(
    schema_cls: "type[Schema]",
    table_name: str | None = None,
    metadata: MetaData | None = None,
) -> Table:
    """
    Generate a SQLAlchemy Table from a Schema class.

    Parameters
    ----------
    schema_cls : type[Schema]
        A subclass of Schema used to define the table schema.
    table_name : str, optional
        The name of the SQL table to create. If not provided, defaults to
        the lowercase schema class name with trailing 's' added. Note: uses
        simple pluralization (e.g., 'person' -> 'persons').
    metadata : sqlalchemy.MetaData, optional
        An existing MetaData instance. If not provided, a new MetaData
        object is created.

    Returns
    -------
    sqlalchemy.Table
        An instance of SQLAlchemy Table corresponding to the schema.
    """
    if metadata is None:
        metadata = MetaData()

    if table_name is None:
        table_name = schema_cls.__name__.removesuffix("Schema").lower() + "s"

    fields = schema_cls.fields()
    columns = []

    for field_name, field in fields.items():
        sa_type = field.get_sqlalchemy_type()

        # Build column arguments
        column_kwargs = {}

        # Handle primary key
        if field.primary_key:
            column_kwargs["primary_key"] = True

        # Handle nullable
        column_kwargs["nullable"] = field.nullable

        # Handle autoincrement (explicit control over SQLAlchemy's behavior)
        if field.autoincrement is not None:
            column_kwargs["autoincrement"] = field.autoincrement

        # Handle unique
        if field.unique:
            column_kwargs["unique"] = True

        # Handle index
        if field.index:
            column_kwargs["index"] = True

        # Handle default
        if field.default is not _MISSING:
            column_kwargs["default"] = field.default

        # Create column
        # Column accepts dynamic kwargs that mypy can't verify statically
        col = Column(field_name, sa_type(), **column_kwargs)  # type: ignore[arg-type]
        columns.append(col)

    return Table(table_name, metadata, *columns)
