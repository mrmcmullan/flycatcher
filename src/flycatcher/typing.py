"""Type hints for Flycatcher schemas.

This module provides generic type hints that enable IDE autocompletion
and static type checking for DataFrames validated by Flycatcher schemas.

Example:
    >>> from flycatcher import Schema, Field
    >>> from flycatcher.typing import DataFrame
    >>> import polars as pl
    >>>
    >>> class UserSchema(Schema):
    ...     id: int = Field(primary_key=True)
    ...     name: str
    >>>
    >>> def process_users(df: DataFrame[UserSchema]) -> DataFrame[UserSchema]:
    ...     # IDE knows df has 'id' and 'name' columns
    ...     return UserSchema.validate(df)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import polars as pl

if TYPE_CHECKING:
    from .base import Schema

# Type variable for Schema subclasses
SchemaT = TypeVar("SchemaT", bound="Schema")


class DataFrame(pl.DataFrame, Generic[SchemaT]):
    """
    A Polars DataFrame annotated with a Flycatcher Schema.

    This is a type hint helper that indicates a DataFrame has been validated
    against a specific Schema. At runtime, this is just a regular `pl.DataFrame`.

    The generic parameter provides IDE autocompletion and static type checking
    but does not affect runtime behavior.

    Examples
    --------
    Type annotation for function parameters and return values:

        >>> from flycatcher import Schema, Field
        >>> from flycatcher.typing import DataFrame
        >>>
        >>> class UserSchema(Schema):
        ...     id: int = Field(primary_key=True)
        ...     name: str
        >>>
        >>> def get_active_users(df: DataFrame[UserSchema]) -> DataFrame[UserSchema]:
        ...     '''Filter to active users.'''
        ...     return df.filter(pl.col('id') > 0)

    The annotation helps IDEs understand the DataFrame structure:

        >>> def process(df: DataFrame[UserSchema]) -> None:
        ...     # IDE knows df has 'id' and 'name' columns
        ...     ids = df['id']  # Autocomplete works!
        ...     names = df['name']
    """

    # This class exists purely for type hinting purposes.
    # At runtime, Schema.validate() returns a regular pl.DataFrame.
    # The type hint is a lie, but a useful one for IDE support.
    pass


# For convenience, also provide a type alias that works with type checkers
# that don't support Generic subclasses of concrete types
ValidatedDataFrame = DataFrame
"""Alias for DataFrame[SchemaT] for compatibility."""

