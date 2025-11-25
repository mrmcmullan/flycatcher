"""
ETL Pipeline Example: Processing Season Statistics

This example demonstrates a complete ETL (Extract, Transform, Load) workflow:
1. Extract: Read raw game data from CSV
2. Transform: Validate and clean data using Polars
3. Load: Store validated data in a database using SQLAlchemy

This showcases Flycatcher's strength in maintaining schema consistency
across different stages of a data pipeline.
"""

import polars as pl
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from flycatcher import Float, Integer, Schema, String


class SeasonStatsSchema(Schema):
    """Schema for season-level player statistics."""

    player_id = Integer(primary_key=True, description="Unique player identifier")
    season = String(min_length=4, max_length=7, description="Season (e.g., '2023-24')")
    team = String(min_length=2, max_length=50, description="Team abbreviation")
    games_played = Integer(ge=0, le=82, description="Games played (max 82 for NBA)")
    points_per_game = Float(ge=0.0, le=50.0, description="Average points per game")
    rebounds_per_game = Float(ge=0.0, le=25.0, description="Average rebounds per game")
    assists_per_game = Float(ge=0.0, le=15.0, description="Average assists per game")
    field_goal_percentage = Float(
        ge=0.0, le=1.0, description="Field goal percentage (0-1)"
    )


def extract_data(file_path: str) -> pl.DataFrame:
    """
    Extract: Read raw data from CSV file.

    In a real scenario, this might read from:
    - CSV files
    - Parquet files
    - API endpoints
    - Database queries
    """
    print("[EXTRACT] Reading raw data from CSV...")

    # For this example, we'll create sample data
    # In production, you'd use: df = pl.read_csv(file_path)
    raw_data = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5],
            "season": ["2023-24", "2023-24", "2023-24", "2023-24", "2023-24"],
            "team": ["LAL", "GSW", "BOS", "MIA", "PHX"],
            "games_played": [71, 74, 82, 68, 75],
            "points_per_game": [25.2, 26.4, 23.8, 20.1, 27.1],
            "rebounds_per_game": [7.3, 4.5, 8.1, 6.2, 6.7],
            "assists_per_game": [8.2, 5.1, 5.2, 4.8, 4.9],
            "field_goal_percentage": [0.540, 0.452, 0.471, 0.463, 0.521],
        }
    )

    print(f"   [OK] Loaded {raw_data.height} rows")
    return raw_data


def transform_data(raw_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform: Validate and clean data using Polars validator.

    This step ensures:
    - All data types are correct
    - All constraints are satisfied
    - Invalid rows are filtered out
    """
    print("\n[TRANSFORM] Validating data with Flycatcher schema...")

    # Generate Polars validator from schema
    StatsValidator = SeasonStatsSchema.to_polars_model()

    # Validate the data
    # Non-strict mode filters invalid rows instead of raising
    valid_df = StatsValidator.validate(raw_df, strict=False)

    print(f"   [OK] Validated {valid_df.height} rows")
    if valid_df.height < raw_df.height:
        print(f"   [WARNING] Filtered {raw_df.height - valid_df.height} invalid rows")

    # Additional transformations can happen here
    # For example: calculating derived fields, aggregations, etc.
    transformed_df = valid_df.with_columns(
        # Calculate total points (points_per_game * games_played)
        (pl.col("points_per_game") * pl.col("games_played")).alias("total_points")
    )

    print("   [OK] Applied transformations")
    return transformed_df


def load_data(validated_df: pl.DataFrame, database_url: str) -> None:
    """
    Load: Store validated data in database using SQLAlchemy.

    The validated DataFrame is converted to dictionaries and inserted
    into the database table generated from the same schema.
    """
    print("\n[LOAD] Storing data in database...")

    # Generate SQLAlchemy table from schema
    StatsTable = SeasonStatsSchema.to_sqlalchemy(table_name="season_stats")

    # Create database engine
    engine = create_engine(database_url, echo=False)

    # Drop and recreate table for demo purposes (in production, use migrations)
    StatsTable.metadata.drop_all(engine)
    StatsTable.metadata.create_all(engine)
    print("   [OK] Created/verified database table")

    # Convert DataFrame to list of dictionaries
    # Note: We exclude the derived 'total_points' column for insertion
    records = validated_df.drop("total_points").to_dicts()

    # Insert data
    with Session(engine) as session:
        stmt = insert(StatsTable).values(records)
        session.execute(stmt)
        session.commit()

    print(f"   [OK] Inserted {len(records)} records into database")


def run_etl_pipeline() -> None:
    """Run the complete ETL pipeline."""
    print("\n" + "=" * 60)
    print("FLYCATCHER ETL PIPELINE DEMONSTRATION")
    print("=" * 60 + "\n")

    # Step 1: Extract
    raw_data = extract_data("season_stats.csv")

    # Step 2: Transform
    validated_data = transform_data(raw_data)

    # Step 3: Load
    # Using SQLite for this example (in-memory database)
    # In production, you'd use: "postgresql://user:pass@host/db"
    database_url = "sqlite:///example.db"
    load_data(validated_data, database_url)

    print("\n" + "=" * 60)
    print("[SUCCESS] ETL PIPELINE COMPLETE")
    print("=" * 60)
    print("\nKey benefits demonstrated:")
    print("  • Single schema definition used across all stages")
    print("  • Type-safe validation with Polars (fast, columnar)")
    print("  • Type-safe database operations with SQLAlchemy")
    print("  • Consistent constraints enforced everywhere")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_etl_pipeline()
