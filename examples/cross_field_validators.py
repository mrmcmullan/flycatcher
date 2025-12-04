"""
Cross-Field Validators Example: Sports League Rules

This example demonstrates cross-field validation using the col() DSL.
Cross-field validators allow you to enforce business rules that involve
multiple fields, such as:
- Ensuring discount prices are less than regular prices
- Validating that end dates are after start dates
- Enforcing complex conditional logic

These validators work across both Pydantic (row-level) and Polars (bulk) validation.
"""

from datetime import date

import polars as pl

from flycatcher import Field, Schema, col, model_validator


class PlayerContractSchema(Schema):
    """Schema for player contracts with cross-field validation."""

    contract_id: int = Field(primary_key=True, description="Unique contract identifier")
    player_id: int  # description="Player identifier"
    team: str = Field(min_length=2, max_length=50, description="Team abbreviation")
    start_date: date  # description="Contract start date"
    end_date: date  # description="Contract end date"
    base_salary: float = Field(gt=0, description="Base annual salary")
    bonus: float | None = Field(
        default=None, ge=0, description="Performance bonus (optional)"
    )

    @model_validator
    def check_contract_dates():
        """
        Validate that end date is after start date.

        This validator uses the col() DSL to reference fields and create
        expressions that work in both Pydantic and Polars contexts.
        """
        return col("end_date") > col("start_date")

    @model_validator
    def check_bonus_reasonable():
        """
        Validate that bonus doesn't exceed 50% of base salary.

        This demonstrates more complex validation logic with nullable fields.
        Note: When bonus is None, the validation passes (nullable fields are optional).
        """
        # Use explicit format for better control over None handling
        import polars as pl

        def pydantic_validator(v):
            if v.bonus is not None and v.bonus > v.base_salary * 0.5:
                raise ValueError("Bonus cannot exceed 50% of base salary")
            return v

        return {
            "polars": (
                pl.col("bonus").is_null()
                | (pl.col("bonus") <= pl.col("base_salary") * 0.5),
                "Bonus cannot exceed 50% of base salary",
            ),
            "pydantic": pydantic_validator,
        }


class GameResultSchema(Schema):
    """Schema for game results with score validation."""

    game_id: int = Field(primary_key=True, description="Unique game identifier")
    home_team: str = Field(min_length=2, max_length=50, description="Home team")
    away_team: str = Field(min_length=2, max_length=50, description="Away team")
    home_score: int = Field(ge=0, description="Home team score")
    away_score: int = Field(ge=0, description="Away team score")
    overtime: str | None = None  # description="Overtime indicator (e.g., 'OT', '2OT')"

    @model_validator
    def check_teams_different():
        """Ensure home and away teams are different."""
        return col("home_team") != col("away_team")

    @model_validator
    def check_overtime_logic():
        """
        Validate overtime logic: if scores are equal, overtime must be specified.

        This demonstrates conditional validation using logical operators.
        """
        return (col("home_score") != col("away_score")) | (col("overtime") != None)  # noqa: E711


def demonstrate_pydantic_validation() -> None:
    """Show cross-field validation in Pydantic (row-level)."""
    print("=" * 60)
    print("PYDANTIC CROSS-FIELD VALIDATION (Row-Level)")
    print("=" * 60)

    Contract = PlayerContractSchema.to_pydantic()

    # Valid contract
    print("\n[OK] Valid contract:")
    valid_contract = Contract(
        contract_id=1,
        player_id=23,
        team="LAL",
        start_date=date(2023, 7, 1),
        end_date=date(2026, 6, 30),
        base_salary=50000000.0,
        bonus=10000000.0,
    )
    print(
        f"   Player {valid_contract.player_id}: ${valid_contract.base_salary:,.0f}/year"
    )

    # Invalid contract: end date before start date
    print("\n[ERROR] Invalid contract (end date before start):")
    try:
        _invalid_contract = Contract(
            contract_id=2,
            player_id=30,
            team="GSW",
            start_date=date(2024, 7, 1),
            end_date=date(2023, 6, 30),  # Invalid!
            base_salary=40000000.0,
            bonus=None,
        )
        print("   Should have raised validation error!")
    except Exception as e:
        print(f"   [OK] Validation error caught: {type(e).__name__}")

    # Invalid contract: bonus too high
    print("\n[ERROR] Invalid contract (bonus exceeds 50% of base):")
    try:
        invalid = Contract(
            contract_id=3,
            player_id=35,
            team="BOS",
            start_date=date(2023, 7, 1),
            end_date=date(2025, 6, 30),
            base_salary=30000000.0,
            bonus=20000000.0,  # Invalid: 66% of base
        )
        print(f"   [ERROR] Should have raised validation error! Got: {invalid}")
    except Exception as e:
        print(f"   [OK] Validation error caught: {type(e).__name__}")


def demonstrate_polars_validation() -> None:
    """Show cross-field validation in Polars (bulk validation)."""
    print("\n" + "=" * 60)
    print("POLARS CROSS-FIELD VALIDATION (Bulk)")
    print("=" * 60)

    GameValidator = GameResultSchema.to_polars_validator()

    # Mixed valid and invalid games
    games_df = pl.DataFrame(
        {
            "game_id": [1, 2, 3, 4, 5],
            "home_team": ["LAL", "GSW", "BOS", "MIA", "PHX"],
            "away_team": [
                "BOS",
                "MIA",
                "LAL",
                "PHX",
                "PHX",
            ],  # Row 5: same team (invalid)
            "home_score": [108, 120, 95, 110, 105],
            "away_score": [105, 115, 98, 108, 105],  # Row 5: tied score
            "overtime": [
                None,
                None,
                None,
                None,
                None,
            ],  # Row 5: tied but no overtime (invalid)
        }
    )

    print(f"\nOriginal data: {games_df.height} games")

    # Validate with non-strict mode to see what gets filtered
    valid_games = GameValidator.validate(games_df, strict=False)

    print(f"[OK] Valid games after validation: {valid_games.height}")
    print(f"[OK] Games filtered out: {games_df.height - valid_games.height}")

    print("\nValid games:")
    for row in valid_games.select(
        ["game_id", "home_team", "away_team", "home_score", "away_score"]
    ).iter_rows(named=True):
        print(
            f"  Game {row['game_id']}: {row['home_team']} {row['home_score']} - "
            f"{row['away_score']} {row['away_team']}"
        )


def demonstrate_complex_validation() -> None:
    """Show more complex validation scenarios."""
    print("\n" + "=" * 60)
    print("COMPLEX VALIDATION SCENARIOS")
    print("=" * 60)

    Contract = PlayerContractSchema.to_pydantic()

    # Edge case: Nullable bonus with valid value
    print("\n[OK] Contract with valid bonus:")
    contract_with_bonus = Contract(
        contract_id=4,
        player_id=7,
        team="DEN",
        start_date=date(2023, 7, 1),
        end_date=date(2027, 6, 30),
        base_salary=40000000.0,
        bonus=15000000.0,  # 37.5% of base - valid
    )
    print(
        f"   Bonus: ${contract_with_bonus.bonus:,.0f} "
        f"({contract_with_bonus.bonus / contract_with_bonus.base_salary * 100:.1f}% "
        f"of base)"
    )

    # Edge case: No bonus (nullable field)
    print("\n[OK] Contract with no bonus:")
    contract_no_bonus = Contract(
        contract_id=5,
        player_id=13,
        team="LAC",
        start_date=date(2023, 7, 1),
        end_date=date(2025, 6, 30),
        base_salary=35000000.0,
        bonus=None,  # Valid: bonus is nullable
    )
    print(f"   Base salary only: ${contract_no_bonus.base_salary:,.0f}")


def main() -> None:
    """Run all cross-field validator demonstrations."""
    print("\n" + "=" * 60)
    print("FLYCATCHER CROSS-FIELD VALIDATORS DEMONSTRATION")
    print("=" * 60)

    demonstrate_pydantic_validation()
    demonstrate_polars_validation()
    demonstrate_complex_validation()

    print("\n" + "=" * 60)
    print("[SUCCESS] CROSS-FIELD VALIDATION DEMONSTRATED")
    print("=" * 60)
    print("\nKey features:")
    print("  • col() DSL works in both Pydantic and Polars")
    print("  • Complex logical expressions (|, &, ~)")
    print("  • Nullable field handling")
    print("  • Custom error messages")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
