"""Pydantic model generator with constraint support."""

import inspect
from typing import Any, Union

from loguru import logger
from pydantic import BaseModel, ConfigDict, create_model, model_validator
from pydantic import Field as PydanticField

from ..base import Schema
from ..fields import _MISSING
from ..validators import ValidatorResult


def create_pydantic_model(schema_cls: "type[Schema]") -> type[BaseModel]:
    """
    Generate a Pydantic BaseModel from a Schema class.

    Parameters
    ----------
    schema_cls : type[Schema]
        A subclass of Schema.

    Returns
    -------
    type[BaseModel]
        A dynamically created Pydantic BaseModel class.
    """
    fields = schema_cls.fields()
    pydantic_fields = {}

    for field_name, field in fields.items():
        python_type: type | type[None] = field.get_python_type()

        # Handle nullable fields (can be None)
        if field.nullable:
            # Create Union type for nullable fields
            # Using tuple form for Union to avoid mypy assignment error
            python_type = Union[python_type, None]  # type: ignore[assignment]

        # Create Pydantic Field with metadata
        field_kwargs: dict[str, Any] = {}

        # Basic metadata
        if field.description:
            field_kwargs["description"] = field.description

        # Handle default values (including explicit None)
        if field.default is not _MISSING:
            field_kwargs["default"] = field.default

        # Handle alias
        if field.alias:
            field_kwargs["alias"] = field.alias

        # Get constraint kwargs from field (gt, le, pattern, etc.)
        get_kwargs = getattr(field, "get_pydantic_field_kwargs", None)
        if get_kwargs is not None:
            constraint_kwargs = get_kwargs()
            field_kwargs.update(constraint_kwargs)

        # Add to fields dict
        if field_kwargs:
            pydantic_fields[field_name] = (python_type, PydanticField(**field_kwargs))
        else:
            pydantic_fields[field_name] = (python_type, ...)

    # Check if any fields have aliases
    has_aliases = any(field.alias for field in fields.values())

    # Create the model dynamically
    model_name = schema_cls.__name__.removesuffix("Schema") + "Model"

    # If there are aliases, configure the model to accept both field names and aliases
    if has_aliases:
        # Create a base class with the config
        class AliasConfigBase(BaseModel):
            model_config = ConfigDict(populate_by_name=True)

        base_model: type[BaseModel] = create_model(
            model_name, __base__=AliasConfigBase, **pydantic_fields
        )  # type: ignore[assignment, call-overload]
    else:
        # Pydantic's create_model is dynamically typed - returns type[BaseModel] at runtime
        base_model = create_model(model_name, **pydantic_fields)  # type: ignore[assignment, call-overload]

    # Collect model validators
    model_validators_to_add = []
    for validator_func in schema_cls.model_validators():
        # Handle both regular functions and classmethod descriptors
        if isinstance(validator_func, classmethod):
            # For classmethod descriptors, access the underlying function
            func = validator_func.__func__
        else:
            func = validator_func

        # Check if function accepts cls parameter - make it optional for ergonomics
        sig = inspect.signature(func)
        if len(sig.parameters) > 0:
            # Function accepts at least one parameter, pass cls
            validator_result = func(schema_cls)  # type: ignore[call-arg]
        else:
            # Function takes no parameters, call without args
            validator_result = func()  # type: ignore[call-arg]
        result = ValidatorResult(validator_result)
        if result.has_pydantic_validator():
            pydantic_val = result.get_pydantic_validator()
            model_validators_to_add.append(pydantic_val)

    # Collect field validators by mode
    field_validators_before: dict[str, list[tuple[Any, str | None]]] = {}
    field_validators_after: dict[str, list[tuple[Any, str | None]]] = {}

    for field_name, validators in schema_cls.field_validators().items():
        for validator_func in validators:
            # Handle both regular functions and classmethod descriptors
            if isinstance(validator_func, classmethod):
                func = validator_func.__func__
            else:
                func = validator_func

            # Get the mode (default: after)
            mode = getattr(func, "_field_validator_mode", "after")

            # Call the validator to get the DSL expression
            sig = inspect.signature(func)
            if len(sig.parameters) > 0:
                validator_result = func(schema_cls)  # type: ignore[call-arg]
            else:
                validator_result = func()  # type: ignore[call-arg]

            # Extract expression and optional error message
            if isinstance(validator_result, tuple) and len(validator_result) == 2:
                expr, msg = validator_result
            else:
                expr = validator_result
                msg = None

            # Add to appropriate collection based on mode
            if mode == "before":
                if field_name not in field_validators_before:
                    field_validators_before[field_name] = []
                field_validators_before[field_name].append((expr, msg))
            else:
                if field_name not in field_validators_after:
                    field_validators_after[field_name] = []
                field_validators_after[field_name].append((expr, msg))

    # If we have validators, create a new class with them
    has_validators = (
        model_validators_to_add or field_validators_before or field_validators_after
    )

    if has_validators:
        # Build namespace for the new class with validators
        namespace: dict[str, Any] = {}

        # Add before validator if any
        if field_validators_before:

            def _make_before_validator(
                fv_before: dict[str, list[tuple[Any, str | None]]]
            ) -> Any:
                @model_validator(mode="before")
                @classmethod
                def validate_before(cls: Any, data: Any) -> Any:
                    """Execute before validators on raw input."""
                    if not isinstance(data, dict):
                        return data

                    for field_name, validators in fv_before.items():
                        if field_name not in data:
                            continue
                        for expr, msg in validators:
                            if hasattr(expr, "to_python"):
                                try:
                                    result = expr.to_python(data)
                                    if isinstance(result, bool):
                                        if not result:
                                            error_msg = (
                                                msg
                                                or f"Field '{field_name}' validation failed"
                                            )
                                            raise ValueError(error_msg)
                                    else:
                                        data[field_name] = result
                                except ValueError:
                                    raise
                                except Exception as e:
                                    error_msg = (
                                        msg or f"Field '{field_name}' validation failed"
                                    )
                                    raise ValueError(f"{error_msg}: {e}") from e
                    return data

                return validate_before

            namespace["validate_before"] = _make_before_validator(field_validators_before)

        # Add after validator if any
        if field_validators_after or model_validators_to_add:

            def _make_after_validator(
                fv_after: dict[str, list[tuple[Any, str | None]]],
                mv_to_add: list[Any],
            ) -> Any:
                @model_validator(mode="after")
                def validate_after(self: Any) -> Any:
                    """Execute after validators on validated model."""
                    for field_name, validators in fv_after.items():
                        for expr, msg in validators:
                            if hasattr(expr, "to_python"):
                                try:
                                    result = expr.to_python(self)
                                    if isinstance(result, bool):
                                        if not result:
                                            error_msg = (
                                                msg
                                                or f"Field '{field_name}' validation failed"
                                            )
                                            raise ValueError(error_msg)
                                    else:
                                        setattr(self, field_name, result)
                                except ValueError:
                                    raise
                                except Exception as e:
                                    error_msg = (
                                        msg or f"Field '{field_name}' validation failed"
                                    )
                                    raise ValueError(f"{error_msg}: {e}") from e

                    for validator in mv_to_add:
                        try:
                            validator(self)
                        except Exception as e:
                            logger.warning(
                                f"Model validator failed: {e}",
                                exc_info=True,
                            )
                            raise
                    return self

                return validate_after

            namespace["validate_after"] = _make_after_validator(
                field_validators_after, model_validators_to_add
            )

        # Create the model class with validators in the namespace
        ModelWithValidators = type(model_name, (base_model,), namespace)
        return ModelWithValidators  # type: ignore[return-value]

    return base_model  # type: ignore[no-any-return]
