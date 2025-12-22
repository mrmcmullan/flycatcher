"""SQLAlchemy table generator."""

from typing import TYPE_CHECKING, Any, get_type_hints

from sqlalchemy import Column, Float, Integer, MetaData, String, Table

from ..fields import _MISSING

if TYPE_CHECKING:
    from ..base import Schema


def _get_sqlalchemy_type_for_python_type(python_type: Any) -> Any:
    """Map Python types to SQLAlchemy column types."""
    type_map = {
        int: Integer,
        float: Float,
        str: String,
        bool: Integer,  # SQLite doesn't have native bool
    }
    # Handle Optional types
    origin = getattr(python_type, "__origin__", None)
    if origin is not None:
        args = getattr(python_type, "__args__", ())
        # Get the non-None type from Optional
        for arg in args:
            if arg is not type(None):
                return type_map.get(arg, String)
    return type_map.get(python_type, String)


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
        # Skip fields marked as not persistent (virtual/computed fields)
        if not getattr(field, "persist", True):
            continue

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

        # Determine column name (use sa_column_name if set, otherwise field_name)
        col_name = getattr(field, "sa_column_name", None) or field_name

        # Create column
        # sa_type can be either a class (callable) or instance (when max_length is set)
        # Column accepts dynamic kwargs that mypy can't verify statically
        if callable(sa_type):
            col = Column(col_name, sa_type(), **column_kwargs)  # type: ignore[arg-type]
        else:
            col = Column(col_name, sa_type, **column_kwargs)  # type: ignore[arg-type]
        columns.append(col)

    # Add persisted computed fields
    computed_fields = schema_cls.computed_fields()
    for field_name, compute_fn in computed_fields.items():
        # Check if this computed field should be persisted
        persist = getattr(compute_fn, "_computed_field_persist", False)
        if not persist:
            continue

        # Try to get return type from function annotations
        try:
            hints = get_type_hints(compute_fn)
            return_type = hints.get("return", str)
        except Exception:
            return_type = str  # Default to string if can't determine type

        sa_type = _get_sqlalchemy_type_for_python_type(return_type)
        col_name = getattr(compute_fn, "_computed_field_name", field_name)

        col = Column(col_name, sa_type(), nullable=True)
        columns.append(col)

    return Table(table_name, metadata, *columns)
