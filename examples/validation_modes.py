"""
Validation Modes Example: Game Statistics with Flexible Error Handling

This example demonstrates different validation modes available in Polars:
- Strict mode: Raises exceptions on validation errors (default)
- Non-strict mode: Filters out invalid rows and optionally returns violations
- Violation reporting: Get detailed information about what failed
"""

from datetime import date

import polars as pl

from flycatcher import Date, Integer, Schema, String


class GameStatsSchema(Schema):
    """Schema for tracking individual game statistics."""

    game_id = Integer(primary_key=True, description="Unique game identifier")
    date = Date(description="Date the game was played")
    home_team = String(
        min_length=2, max_length=50, description="Home team abbreviation"
    )
    away_team = String(
        min_length=2, max_length=50, description="Away team abbreviation"
    )
    home_score = Integer(ge=0, description="Home team final score")
    away_score = Integer(ge=0, description="Away team final score")
    attendance = Integer(
        ge=0, nullable=True, description="Game attendance (if available)"
    )


def demonstrate_strict_mode() -> None:
    """Strict mode: Raises exceptions when validation fails."""
    print("=" * 60)
    print("STRICT MODE: Raises exceptions on validation errors")
    print("=" * 60)

    GameValidator = GameStatsSchema.to_polars_validator()

    # Valid data - passes without issues
    valid_data = pl.DataFrame(
        {
            "game_id": [1, 2],
            "date": [date(2024, 1, 15), date(2024, 1, 16)],
            "home_team": ["LAL", "GSW"],
            "away_team": ["BOS", "MIA"],
            "home_score": [108, 120],
            "away_score": [105, 115],
            "attendance": [18000, 19500],
        }
    )

    validated = GameValidator.validate(valid_data, strict=True)
    print(f"[OK] Valid data passed: {validated.height} rows validated\n")

    # Invalid data - will raise an exception
    invalid_data = pl.DataFrame(
        {
            "game_id": [3],
            "date": [date(2024, 1, 17)],
            "home_team": ["PHX"],
            "away_team": ["DEN"],
            "home_score": [-5],  # Invalid: negative score
            "away_score": [110],
            "attendance": [None],
        }
    )

    try:
        GameValidator.validate(invalid_data, strict=True)
        print("[ERROR] Should have raised an exception!")
    except Exception as e:
        print(f"[OK] Exception raised as expected: {type(e).__name__}")
        # Truncate error message to avoid Unicode issues on Windows
        error_msg = str(e).split("\n")[0]  # Just get first line
        print(f"  Error: {error_msg[:80]}...\n")


def demonstrate_non_strict_mode() -> None:
    """Non-strict mode: Filters invalid rows instead of raising."""
    print("=" * 60)
    print("NON-STRICT MODE: Filters invalid rows")
    print("=" * 60)

    GameValidator = GameStatsSchema.to_polars_validator()

    # Mixed valid and invalid data
    mixed_data = pl.DataFrame(
        {
            "game_id": [1, 2, 3, 4],
            "date": [
                date(2024, 1, 15),
                date(2024, 1, 16),
                date(2024, 1, 17),
                date(2024, 1, 18),
            ],
            "home_team": ["LAL", "GSW", "PHX", "BOS"],
            "away_team": ["BOS", "MIA", "DEN", "MIA"],
            "home_score": [108, 120, -5, 95],  # Row 3 has invalid negative score
            "away_score": [105, 115, 110, 98],
            "attendance": [18000, 19500, None, 20000],
        }
    )

    print(f"Original data: {mixed_data.height} rows")

    # Non-strict mode: Returns only valid rows (filtered DataFrame)
    valid_df = GameValidator.validate(mixed_data, strict=False)

    print(f"[OK] Valid rows after filtering: {valid_df.height} rows")
    print(f"[OK] Rows filtered out: {mixed_data.height - valid_df.height}\n")

    # Show summary of validated data
    print("Validated data summary:")
    for row in valid_df.select(
        ["game_id", "home_team", "away_team", "home_score", "away_score"]
    ).iter_rows(named=True):
        print(
            f"  Game {row['game_id']}: {row['home_team']} {row['home_score']} - "
            f"{row['away_score']} {row['away_team']}"
        )


def demonstrate_violation_reporting() -> None:
    """Show detailed violation information for debugging."""
    print("=" * 60)
    print("VIOLATION REPORTING: Detailed error information")
    print("=" * 60)

    GameValidator = GameStatsSchema.to_polars_validator()

    # Data with multiple validation issues
    problematic_data = pl.DataFrame(
        {
            "game_id": [1, 2, 3],
            "date": [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)],
            "home_team": ["LAL", "GSW", "PHX"],
            "away_team": ["BOS", "MIA", "DEN"],
            "home_score": [108, -10, 120],  # Row 2: negative score
            "away_score": [105, 115, -5],  # Row 3: negative score
            "attendance": [18000, None, 19500],
        }
    )

    # Non-strict mode: Filter invalid rows
    valid_df = GameValidator.validate(problematic_data, strict=False)

    print(f"[OK] Valid rows kept: {valid_df.height}")
    print(f"[OK] Rows filtered out: {problematic_data.height - valid_df.height}")


def main() -> None:
    """Run all validation mode demonstrations."""
    print("\n" + "=" * 60)
    print("FLYCATCHER VALIDATION MODES DEMONSTRATION")
    print("=" * 60 + "\n")

    demonstrate_strict_mode()
    demonstrate_non_strict_mode()
    demonstrate_violation_reporting()

    print("\n" + "=" * 60)
    print("[OK] All validation modes demonstrated successfully!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
