"""Naming convention helpers for Python/SQL field name conversion."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import SchemaMeta


def camel_to_snake(name: str) -> str:
    """
    Convert a camelCase or PascalCase string to snake_case.

    Examples
    --------
        >>> camel_to_snake("userId")
        'user_id'
        >>> camel_to_snake("firstName")
        'first_name'
        >>> camel_to_snake("HTTPResponse")
        'http_response'
        >>> camel_to_snake("getHTTPResponse")
        'get_http_response'
    """
    # Handle consecutive uppercase letters (acronyms like HTTP, ID)
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_camel(name: str, pascal: bool = False) -> str:
    """
    Convert a snake_case string to camelCase or PascalCase.

    Parameters
    ----------
    name : str
        The snake_case string to convert.
    pascal : bool, default False
        If True, use PascalCase (capitalize first letter).
        If False, use camelCase (lowercase first letter).

    Examples
    --------
        >>> snake_to_camel("user_id")
        'userId'
        >>> snake_to_camel("first_name")
        'firstName'
        >>> snake_to_camel("user_id", pascal=True)
        'UserId'
    """
    components = name.split("_")
    if pascal:
        return "".join(x.capitalize() for x in components)
    return components[0] + "".join(x.capitalize() for x in components[1:])


class PascalMeta(type):
    """
    Metaclass that auto-converts field names between camelCase (Python) and snake_case (SQL).

    Use this metaclass when you want to write schema fields in camelCase (matching
    JSON APIs) but store them in snake_case (matching SQL conventions).

    The metaclass automatically:
    - Sets SQL column names to snake_case versions of Python field names
    - Generates Pydantic aliases for camelCase access

    Examples
    --------
        >>> from flycatcher import Schema, Field
        >>> from flycatcher.naming import PascalMeta
        >>>
        >>> class UserSchema(Schema, metaclass=PascalMeta):
        ...     userId: int = Field(primary_key=True)
        ...     firstName: str
        ...     lastName: str
        ...     emailAddress: str
        >>>
        >>> # Fields are defined in camelCase but stored as snake_case in DB
        >>> table = UserSchema.to_sqlalchemy_table()
        >>> [c.name for c in table.columns]
        ['user_id', 'first_name', 'last_name', 'email_address']

    Notes
    -----
    This metaclass inherits from and extends `SchemaMeta`, so all standard
    Schema features (validators, field collection, etc.) work normally.

    If a field already has an explicit `alias` set, it will not be overwritten.
    If a field already has an explicit `sa_column_name` set, it will not be overwritten.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict,
    ):
        from .base import SchemaMeta

        # First, let SchemaMeta do its normal processing
        # We need to check if Schema is in bases (or any parent uses SchemaMeta)
        schema_meta = None
        for base in bases:
            if isinstance(base, SchemaMeta):
                schema_meta = SchemaMeta
                break

        if schema_meta is None:
            # No Schema in bases, just return a normal class
            return super().__new__(mcs, name, bases, namespace)

        # Create the class using SchemaMeta
        cls = schema_meta.__new__(schema_meta, name, bases, namespace)

        # Now apply naming conventions to fields
        if hasattr(cls, "_fields"):
            for field_name, field in cls._fields.items():
                snake_name = camel_to_snake(field_name)

                # Only set sql_column_name if not already set
                if not hasattr(field, "sa_column_name") or field.sa_column_name is None:
                    field.sa_column_name = snake_name

                # Only set alias if not already set
                if not hasattr(field, "alias") or field.alias is None:
                    # Keep the original camelCase name as the alias for Pydantic
                    # This allows JSON with camelCase to be deserialized
                    if snake_name != field_name:
                        field.alias = field_name

        return cls


# Also provide a simpler mixin approach
class NamingConventionMixin:
    """
    Mixin providing naming convention utilities.

    Add this mixin to your Schema to get access to naming conversion methods.

    Examples
    --------
        >>> from flycatcher import Schema
        >>> from flycatcher.naming import NamingConventionMixin
        >>>
        >>> class MySchema(NamingConventionMixin, Schema):
        ...     user_id: int
        ...
        >>> MySchema.to_camel_case_dict({"user_id": 1})
        {'userId': 1}
    """

    @classmethod
    def to_camel_case_dict(cls, data: dict) -> dict:
        """
        Convert a dict with snake_case keys to camelCase keys.

        Parameters
        ----------
        data : dict
            Dictionary with snake_case keys.

        Returns
        -------
        dict
            Dictionary with camelCase keys.
        """
        return {snake_to_camel(k): v for k, v in data.items()}

    @classmethod
    def to_snake_case_dict(cls, data: dict) -> dict:
        """
        Convert a dict with camelCase keys to snake_case keys.

        Parameters
        ----------
        data : dict
            Dictionary with camelCase keys.

        Returns
        -------
        dict
            Dictionary with snake_case keys.
        """
        return {camel_to_snake(k): v for k, v in data.items()}

