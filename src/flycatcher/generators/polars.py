"""Polars validation model generator with dataframe-level constraint checking."""

from typing import TYPE_CHECKING, Dict, List, Tuple

import polars as pl
from loguru import logger

from ..fields import _MISSING
from ..validators import ValidatorResult

if TYPE_CHECKING:
    from ..base import Schema


class PolarsValidator:
    """A validator for Polars DataFrames based on schema definition."""

    def __init__(self, schema_cls: type[Schema]) -> None:
        self.schema_cls = schema_cls
        self.fields = schema_cls.fields()
        self._polars_schema = self._build_polars_schema()
        self._constraints = self._build_constraints()

    def _build_polars_schema(self) -> Dict[str, pl.DataType]:
        """Build Polars schema dict from fields."""
        schema = {}
        for field_name, field in self.fields.items():
            dtype = field.get_polars_dtype()
            schema[field_name] = dtype
        return schema

    def _build_constraints(self) -> List[Tuple[pl.Expr, str]]:
        """
        Build list of constraint expressions from fields.

        Note: Constraints are evaluated after null-checking, so they
        don't need to handle null values explicitly.
        """
        constraints = []

        # Field-level constraints
        for _field_name, field in self.fields.items():
            field_constraints = field.get_polars_constraints()
            constraints.extend(field_constraints)

        # Model-level validators (cross-field)
        for validator in self.schema_cls.model_validators():
            result = ValidatorResult(validator(self.schema_cls))
            polars_validator = result.get_polars_validator()
            if polars_validator:
                expr, msg = polars_validator
                constraints.append((expr, msg))

        return constraints

    def validate(
        self,
        df: pl.DataFrame,
        strict: bool = True,
        show_violations: bool = False,
        fill_nulls: bool = False,
    ) -> pl.DataFrame:
        """
        Validate and coerce a DataFrame to match the schema.

        Parameters
        ----------
        df : pl.DataFrame
            Input Polars DataFrame.
        strict : bool, default True
            If True, raise on validation errors. If False, filter invalid rows.
        show_violations : bool, default False
            If True, show violations in the console.
        fill_nulls : bool, default False
            If True, replace null values with field defaults (if specified).
            Note: This is a transformation step. Defaults only apply to missing
            columns by default. Enable this to also fill existing null values.

        Returns
        -------
        pl.DataFrame
            Validated DataFrame with correct types. If fill_nulls=True, null
            values will be replaced with defaults where applicable.

        Raises
        ------
        ValueError
            If validation fails and strict=True.

        Notes
        -----
        Behavior of defaults:
        - Missing columns with defaults are always added to the DataFrame
        - Existing null values are filled with defaults only if fill_nulls=True
        - If a field is nullable without a default, nulls are preserved
        """
        # Check for missing required columns (no default value = required)
        required_cols = {
            name for name, field in self.fields.items() if field.default is _MISSING
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Add missing columns with default values
        missing_with_defaults = []
        for field_name, field in self.fields.items():
            if field_name not in df.columns and field.default is not _MISSING:
                missing_with_defaults.append((field_name, field.default, field))

        if missing_with_defaults:
            for field_name, default_value, field in missing_with_defaults:
                dtype = field.get_polars_dtype()
                df = df.with_columns(
                    pl.lit(default_value).cast(dtype).alias(field_name)
                )
                logger.info(
                    f"Added column '{field_name}' with default value: {default_value}"
                )

        # Cast to correct types and ensure column order matches schema
        cast_exprs = []
        for col_name, dtype in self._polars_schema.items():
            if col_name in df.columns:
                cast_exprs.append(pl.col(col_name).cast(dtype, strict=False))

        if cast_exprs:
            df = df.select(cast_exprs)

        # Handle nulls based on configuration
        for field_name, field in self.fields.items():
            if field_name not in df.columns:
                continue

            null_count = df[field_name].null_count()
            if null_count == 0:
                continue  # No nulls, nothing to do

            # Option 1: Fill nulls with defaults (if enabled and default exists)
            if fill_nulls and field.default is not _MISSING:
                dtype = field.get_polars_dtype()
                df = df.with_columns(
                    pl.col(field_name)
                    .fill_null(pl.lit(field.default).cast(dtype))
                    .alias(field_name)
                )
                logger.info(
                    f"Filled {null_count} null values in '{field_name}' "
                    f"with default: {field.default}"
                )
                continue

            # Option 2: Validate nulls against nullable constraint
            if not field.nullable:
                if strict:
                    raise ValueError(
                        f"Column '{field_name}' has {null_count} null values "
                        f"but is not nullable"
                    )
                else:
                    # Filter out null rows
                    df = df.filter(pl.col(field_name).is_not_null())
            # If nullable=True, nulls are allowed and preserved

        # Apply custom constraints
        violations = []
        for constraint_expr, error_msg in self._constraints:
            # Get rows that violate the constraint
            try:
                invalid_mask = ~constraint_expr
                violation_count = df.filter(invalid_mask).height

                if violation_count > 0:
                    if strict:
                        # Show sample of violations
                        sample_violations = df.filter(invalid_mask).head(5)
                        raise ValueError(
                            f"Constraint violation: {error_msg}\n"
                            f"Found {violation_count} violations.\n"
                            f"Sample violations:\n{sample_violations}"
                        )
                    else:
                        # Filter out invalid rows
                        violations.append(
                            {
                                "constraint": error_msg,
                                "count": violation_count,
                                "rows": df.filter(invalid_mask).head(10),
                            }
                        )
                        df = df.filter(constraint_expr)
            except Exception as e:
                # Handle cases where constraint can't be evaluated
                logger.warning(
                    f"Could not evaluate constraint '{error_msg}': {e}",
                    exc_info=True,
                )

        if show_violations:
            for violation in violations:
                logger.warning(f"Constraint violation: {violation['constraint']}")
                logger.warning(f"Count: {violation['count']}")
                logger.warning(f"Rows: {violation['rows']}")
                logger.warning("-" * 80)
        return df

    @property
    def schema(self) -> Dict[str, pl.DataType]:
        """Return the Polars schema dict."""
        return self._polars_schema.copy()

    def describe_constraints(self) -> List[str]:
        """Return human-readable list of constraints."""
        return [msg for _, msg in self._constraints]


def create_polars_validator(schema_cls: type[Schema]) -> PolarsValidator:
    """
    Create a Polars validator from a Schema class.

    Parameters
    ----------
    schema_cls : type
        A subclass of Schema.

    Returns
    -------
    PolarsValidator
        An instance of PolarsValidator for the given schema.
    """
    return PolarsValidator(schema_cls)
