"""
Basic Usage Example: Player Statistics Schema

This example demonstrates the core Flycatcher workflow:
1. Define a schema with field types and constraints
2. Generate Pydantic models for row-level validation
3. Generate Polars validators for bulk DataFrame validation
4. Generate SQLAlchemy tables for database operations
"""

from datetime import datetime

import polars as pl

from flycatcher import Field, Schema


# Define a schema for player statistics
class PlayerSchema(Schema):
    """Schema for tracking player statistics in a sports league."""

    # Primary key field
    player_id: int = Field(primary_key=True, description="Unique player identifier")

    # String fields with constraints
    name: str = Field(min_length=1, max_length=100, description="Player full name")
    team: str = Field(min_length=2, max_length=50, description="Team abbreviation")
    position: str = Field(
        min_length=1,
        max_length=20,
        description="Player position (e.g., 'PG', 'C', 'F')",
    )

    # Numeric fields with constraints
    age: int = Field(ge=18, le=50, description="Player age in years")
    points_per_game: float = Field(ge=0.0, description="Average points scored per game")
    games_played: int = Field(ge=0, description="Total games played this season")

    # Boolean field with default
    is_active: bool = True  # description="Whether player is currently active"

    # Datetime field
    created_at: datetime  # description="Record creation timestamp"


def main() -> None:
    """Demonstrate basic schema usage across all three generators."""

    # 1. Generate Pydantic model for row-level validation
    # Perfect for API endpoints, form validation, or single-record operations
    Player = PlayerSchema.to_pydantic()
    print("[OK] Generated Pydantic model")

    # Create and validate a single player record
    player = Player(
        player_id=1,
        name="LeBron James",
        team="LAL",
        position="F",
        age=39,
        points_per_game=25.2,
        games_played=71,
        is_active=True,
        created_at=datetime.now(),
    )
    print(f"[OK] Created Pydantic instance: {player.name} ({player.team})")

    # Serialize to dict/JSON
    player_dict = player.model_dump()
    print(f"[OK] Serialized to dict: {player_dict['name']}")

    # 2. Generate Polars validator for bulk DataFrame validation
    # Perfect for ETL pipelines, data processing, and large-scale validation
    PlayerValidator = PlayerSchema.to_polars_validator()
    print("[OK] Generated Polars validator")

    # Create a DataFrame with multiple players
    players_df = pl.DataFrame(
        {
            "player_id": [1, 2, 3],
            "name": ["LeBron James", "Stephen Curry", "Kevin Durant"],
            "team": ["LAL", "GSW", "PHX"],
            "position": ["F", "PG", "F"],
            "age": [39, 36, 35],
            "points_per_game": [25.2, 26.4, 27.1],
            "games_played": [71, 74, 75],
            "is_active": [True, True, True],
            "created_at": [datetime.now()] * 3,
        }
    )

    # Validate the entire DataFrame at once
    validated_df = PlayerValidator.validate(players_df, strict=True)
    print(f"[OK] Validated DataFrame: {validated_df.height} rows")

    # 3. Generate SQLAlchemy table for database operations
    # Perfect for typed database queries and ORM operations
    PlayerTable = PlayerSchema.to_sqlalchemy(table_name="players")
    print(f"[OK] Generated SQLAlchemy table: {PlayerTable.name}")

    # Example: Create table in database (commented out - requires actual DB)
    # engine = create_engine("sqlite:///example.db")
    # PlayerTable.metadata.create_all(engine)
    # print("✓ Created database table")

    print("\n[SUCCESS] All three generators working correctly!")


if __name__ == "__main__":
    main()
