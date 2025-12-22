"""Query builder for database SELECT operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl
from loguru import logger
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from ..base import Schema


class QueryBuilder:
    """
    Fluent query builder for SELECT operations.

    Provides a DataFrame-oriented interface for querying databases,
    returning validated Polars DataFrames.

    Parameters
    ----------
    schema_cls : type[Schema]
        The schema class to query.
    engine : Engine
        SQLAlchemy engine connected to the database.

    Examples
    --------
        >>> from flycatcher import Schema, Field
        >>> from sqlalchemy import create_engine
        >>> import flycatcher as fl
        >>>
        >>> class UserSchema(Schema, table=True):
        ...     id: int = Field(primary_key=True)
        ...     name: str
        ...     age: int
        >>>
        >>> engine = create_engine("sqlite:///users.db")
        >>> users = (
        ...     UserSchema.query(engine)
        ...     .filter(fl.col('age') > 21)
        ...     .order_by('name')
        ...     .limit(100)
        ...     .execute()
        ... )
    """

    def __init__(self, schema_cls: "type[Schema]", engine: Engine):
        self.schema_cls = schema_cls
        self.engine = engine
        self.table = schema_cls.to_sqlalchemy_table()
        
        # Build initial SELECT statement
        self._stmt = select(self.table)
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._order_clauses: list = []

    def filter(self, *conditions: Any) -> "QueryBuilder":
        """
        Add filter conditions to the query.

        Accepts DSL expressions (fl.col() style) or raw SQLAlchemy conditions.

        Parameters
        ----------
        *conditions : Any
            One or more filter conditions. DSL expressions are automatically
            converted to SQLAlchemy WHERE clauses.

        Returns
        -------
        QueryBuilder
            Self for method chaining.

        Examples
        --------
            >>> query.filter(fl.col('age') > 21)
            >>> query.filter(fl.col('age') > 21, fl.col('is_active') == True)
        """
        for cond in conditions:
            sql_cond = self._to_sqlalchemy_condition(cond)
            self._stmt = self._stmt.where(sql_cond)
        return self

    def _to_sqlalchemy_condition(self, condition: Any) -> Any:
        """Convert a DSL expression to SQLAlchemy condition."""
        from sqlalchemy import and_, or_
        
        # If it has a to_polars method, it's a DSL expression
        if hasattr(condition, "to_polars"):
            return self._dsl_to_sql(condition)
        # Otherwise assume it's already a SQLAlchemy expression
        return condition

    def _dsl_to_sql(self, expr: Any) -> Any:
        """Convert a Flycatcher DSL expression to SQLAlchemy."""
        from ..validators.core import FieldRef, MultiColumnRef
        from ..validators.ops import BinaryOp, UnaryOp
        
        if isinstance(expr, FieldRef):
            return self.table.c[expr.name]
        
        if isinstance(expr, BinaryOp):
            left = self._dsl_to_sql(expr.left)
            right = self._dsl_to_sql(expr.right)
            
            op_map = {
                ">": lambda a, b: a > b,
                ">=": lambda a, b: a >= b,
                "<": lambda a, b: a < b,
                "<=": lambda a, b: a <= b,
                "==": lambda a, b: a == b,
                "!=": lambda a, b: a != b,
                "&": lambda a, b: a & b,
                "|": lambda a, b: a | b,
            }
            
            if expr.op in op_map:
                return op_map[expr.op](left, right)
            raise ValueError(f"Unsupported binary operator for SQL: {expr.op}")
        
        if isinstance(expr, UnaryOp):
            operand = self._dsl_to_sql(expr.operand)
            
            if expr.op == "is_null":
                return operand.is_(None)
            elif expr.op == "is_not_null":
                return operand.isnot(None)
            elif expr.op == "~":
                return ~operand
            raise ValueError(f"Unsupported unary operator for SQL: {expr.op}")
        
        # Literal values
        return expr

    def order_by(self, *columns: str, desc: bool = False) -> "QueryBuilder":
        """
        Add ORDER BY clause to the query.

        Parameters
        ----------
        *columns : str
            Column names to order by.
        desc : bool, default False
            If True, order descending.

        Returns
        -------
        QueryBuilder
            Self for method chaining.
        """
        for col_name in columns:
            col = self.table.c[col_name]
            if desc:
                self._order_clauses.append(col.desc())
            else:
                self._order_clauses.append(col.asc())
        return self

    def limit(self, n: int) -> "QueryBuilder":
        """
        Limit the number of rows returned.

        Parameters
        ----------
        n : int
            Maximum number of rows to return.

        Returns
        -------
        QueryBuilder
            Self for method chaining.
        """
        self._limit_value = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        """
        Skip the first n rows.

        Parameters
        ----------
        n : int
            Number of rows to skip.

        Returns
        -------
        QueryBuilder
            Self for method chaining.
        """
        self._offset_value = n
        return self

    def execute(
        self,
        *,
        validate: bool = True,
        cast: bool = True,
    ) -> pl.DataFrame:
        """
        Execute the query and return results as a Polars DataFrame.

        Parameters
        ----------
        validate : bool, default True
            Whether to validate the result against the schema.
        cast : bool, default True
            Whether to cast columns to schema types during validation.

        Returns
        -------
        polars.DataFrame
            Query results as a validated DataFrame.
        """
        # Build final statement
        stmt = self._stmt
        
        if self._order_clauses:
            stmt = stmt.order_by(*self._order_clauses)
        if self._limit_value is not None:
            stmt = stmt.limit(self._limit_value)
        if self._offset_value is not None:
            stmt = stmt.offset(self._offset_value)

        # Execute query
        logger.debug(f"Executing query: {stmt}")
        
        with Session(self.engine) as session:
            result = session.execute(stmt)
            rows = result.fetchall()
            columns = result.keys()

        # Convert to DataFrame
        if not rows:
            # Empty result - create empty DataFrame with schema columns
            schema_dict = {}
            for field_name, field in self.schema_cls.fields().items():
                schema_dict[field_name] = field.get_polars_dtype()
            df = pl.DataFrame(schema=schema_dict)
        else:
            # Convert rows to dict format
            data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            df = pl.DataFrame(data)

        logger.info(f"Query returned {df.height} rows")

        # Validate if requested
        if validate:
            df = self.schema_cls.validate(df, strict=True, cast=cast)

        return df

    def count(self) -> int:
        """
        Return the count of rows matching the query.

        Returns
        -------
        int
            Number of matching rows.
        """
        from sqlalchemy import func

        # Build count query
        count_stmt = select(func.count()).select_from(self._stmt.subquery())
        
        with Session(self.engine) as session:
            result = session.execute(count_stmt)
            count = result.scalar()

        return count or 0

    def exists(self) -> bool:
        """
        Check if any rows match the query.

        Returns
        -------
        bool
            True if at least one row matches.
        """
        from sqlalchemy import exists as sql_exists

        exists_stmt = select(sql_exists(self._stmt))
        
        with Session(self.engine) as session:
            result = session.execute(exists_stmt)
            return result.scalar() or False

