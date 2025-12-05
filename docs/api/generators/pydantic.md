# Pydantic Generator

Generate Pydantic models from Flycatcher schemas.

::: flycatcher.generators.pydantic.create_pydantic_model
    options:
      show_root_heading: true
      show_source: true
      heading_level: 2

## Usage

The `create_pydantic_model` function is typically called via the `Schema.to_pydantic()` method:

```python
from flycatcher import Schema, Field

class UserSchema(Schema):
    id: int = Field(primary_key=True)
    name: str = Field(min_length=1, max_length=100)

# Generate Pydantic model
UserModel = UserSchema.to_pydantic()

# Use as a regular Pydantic model
user = UserModel(id=1, name="Alice")
print(user.model_dump())  # {'id': 1, 'name': 'Alice'}
```

## Direct Usage

You can also call the generator function directly:

```python
from flycatcher.generators.pydantic import create_pydantic_model

UserModel = create_pydantic_model(UserSchema)
```

