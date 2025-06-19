#!/usr/bin/env python3
"""
Simplified Presidential Polling Data Processor for Tableau

This script cleans and prepares polling data for Tableau analysis, keeping individual
poll records to allow flexible filtering and real-time calculations in Tableau.

Tableau will handle:
- Daily averages per candidate per day (after filtering)
- Rolling averages of those daily averages

Author: [Your Name]
Date: 2024
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SimplifiedPollingProcessor:
    """Simplified processor for presidential polling data analysis."""

    SWING_STATES = [
        "Arizona",
        "Georgia",
        "Michigan",
        "Nevada",
        "North Carolina",
        "Pennsylvania",
        "Wisconsin",
    ]

    MAIN_CANDIDATES = ["Donald Trump", "Joe Biden", "Kamala Harris"]

    POPULATION_MAPPING = {
        "v": "Registered voters",
        "rv": "Registered voters",
        "lv": "Likely voters",
        "a": "All adults",
    }

    # Election date for calculations
    ELECTION_DATE = pd.to_datetime("2024-11-05")

    def __init__(
        self,
        input_path: str = "president_polls.csv",
        output_path: str = "cleaned_polling_data.csv",
    ):
        """Initialize with input and output file paths."""
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)

    def load_and_parse_data(self) -> pd.DataFrame:
        """Load the raw polling data and parse dates."""
        logger.info(f"Loading data from {self.input_path}...")

        df = pd.read_csv(self.input_path)

        # Parse dates
        df["end_date"] = pd.to_datetime(df["end_date"], format="mixed")
        df["start_date"] = pd.to_datetime(
            df["start_date"], format="mixed", errors="coerce"
        )
        df["created_at"] = pd.to_datetime(
            df["created_at"], format="mixed", errors="coerce"
        )

        logger.info(f"Loaded {len(df)} total polling records")
        logger.info(f"Date range: {df['end_date'].min()} to {df['end_date'].max()}")
        logger.info(f"Candidates: {df['candidate_name'].unique()}")

        return df

    def filter_and_clean_core_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to relevant data and clean core fields."""

        # Filter to main candidates and remove missing percentages
        df_filtered = df[
            (df["candidate_name"].isin(self.MAIN_CANDIDATES))
            & (df["pct"].notna())
            & (df["pct"] >= 0)  # Remove negative percentages
            & (df["pct"] <= 100)  # Remove percentages over 100
        ].copy()

        # Clean and standardize population values (v and rv both become "Registered voters")
        df_filtered["population_clean"] = (
            df_filtered["population"].map(self.POPULATION_MAPPING).fillna("Other")
        )

        # Create geographic categorization
        df_filtered["geographic_scope"] = df_filtered["state"].apply(
            lambda x: (
                "National"
                if pd.isna(x)
                else ("Swing State" if x in self.SWING_STATES else "Other State")
            )
        )

        # Clean sample sizes
        df_filtered["sample_size"] = df_filtered["sample_size"].fillna(1000)
        df_filtered["sample_size"] = df_filtered["sample_size"].clip(
            lower=50, upper=10000
        )

        # Clean pollster ratings
        df_filtered["numeric_grade"] = df_filtered["numeric_grade"].fillna(2.5)
        df_filtered["pollscore"] = df_filtered["pollscore"].fillna(1.0)

        # Keep hypothetical field as-is from raw data, just fill nulls with False
        df_filtered["hypothetical"] = df_filtered["hypothetical"].fillna(False)

        logger.info(f"Filtered to {len(df_filtered)} relevant records")
        return df_filtered

    def create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create comprehensive temporal analysis features."""

        # Basic temporal features
        df["year"] = df["end_date"].dt.year
        df["month"] = df["end_date"].dt.month
        df["quarter"] = df["end_date"].dt.quarter
        df["week_of_year"] = df["end_date"].dt.isocalendar().week
        df["month_year"] = df["end_date"].dt.to_period("M").astype(str)
        df["year_quarter"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)

        # Election-relative features
        df["days_until_election"] = (self.ELECTION_DATE - df["end_date"]).dt.days
        df["weeks_until_election"] = df["days_until_election"] / 7
        df["months_until_election"] = df["days_until_election"] / 30.44

        # Polling period analysis
        df["polling_period_days"] = (df["end_date"] - df["start_date"]).dt.days
        df["polling_period_days"] = (
            df["polling_period_days"].fillna(1).clip(lower=1, upper=30)
        )

        # Campaign phase categorization
        def categorize_campaign_phase(days_until):
            if pd.isna(days_until):
                return "Unknown"
            elif days_until < 0:
                return "Post-Election"
            elif days_until <= 30:
                return "Final Month"
            elif days_until <= 100:
                return "Final Stretch"
            elif days_until <= 200:
                return "Fall Campaign"
            elif days_until <= 365:
                return "Primary Season"
            else:
                return "Early Campaign"

        df["campaign_phase"] = df["days_until_election"].apply(
            categorize_campaign_phase
        )

        logger.info("Created temporal features")
        return df

    def create_pollster_quality_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create pollster quality and methodology features."""

        # Pollster quality categories
        df["pollster_grade_category"] = pd.cut(
            df["numeric_grade"],
            bins=[0, 1.5, 2.5, 3.5, 5.0],
            labels=["A-grade", "B-grade", "C-grade", "D/F-grade"],
            include_lowest=True,
        )

        # Poll score categories
        df["pollscore_category"] = pd.cut(
            df["pollscore"],
            bins=[0, 1.5, 2.0, 2.5, 5.0],
            labels=["Excellent", "Good", "Fair", "Poor"],
            include_lowest=True,
        )

        # Sample size categories
        df["sample_size_category"] = pd.cut(
            df["sample_size"],
            bins=[0, 500, 1000, 2000, float("inf")],
            labels=[
                "Small (≤500)",
                "Medium (501-1000)",
                "Large (1001-2000)",
                "Very Large (>2000)",
            ],
            include_lowest=True,
        )

        # Methodology standardization
        df["methodology_clean"] = df["methodology"].fillna("Unknown").str.title()

        # Poll characteristics
        df["is_tracking_poll"] = df["tracking"].notna() & (df["tracking"] != "")
        df["is_internal_poll"] = df["internal"].notna() & (df["internal"] == "TRUE")
        df["is_partisan_poll"] = df["partisan"].notna() & (df["partisan"] != "")

        logger.info("Created pollster quality and methodology features")
        return df

    def calculate_statistical_measures(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate statistical confidence measures and poll quality indicators."""

        # Margin of error based on sample size (simplified formula)
        df["margin_of_error"] = 1.96 * np.sqrt(
            (df["pct"] * (100 - df["pct"])) / df["sample_size"]
        )

        # Confidence intervals
        df["ci_lower"] = df["pct"] - df["margin_of_error"]
        df["ci_upper"] = df["pct"] + df["margin_of_error"]

        # Poll quality score (composite of various factors)
        df["poll_quality_score"] = (
            (5 - df["numeric_grade"]) * 0.3  # Grade (inverted)
            + (3 - df["pollscore"]) * 0.2  # Poll score (inverted)
            + np.log10(df["sample_size"] / 100) * 0.3  # Sample size (log scaled)
            + (1 / (df["polling_period_days"] + 1)) * 0.2  # Recency
        )

        # Normalize poll quality score to 0-100
        df["poll_quality_score"] = (
            (df["poll_quality_score"] - df["poll_quality_score"].min())
            / (df["poll_quality_score"].max() - df["poll_quality_score"].min())
            * 100
        )

        logger.info("Calculated statistical confidence measures")
        return df

    def create_key_events_markers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add markers for key campaign events."""

        # Define key dates
        key_events = {
            "2024-07-13": "Trump Assassination Attempt",
            "2024-07-21": "Biden Withdraws/Harris Enters",
            "2024-09-10": "Presidential Debate #1",
            "2024-10-01": "VP Debate",
            "2024-08-19": "Democratic Convention",
            "2024-07-15": "Republican Convention",
            "2024-11-05": "Election Day",
        }

        # Add event markers
        df["key_event"] = df["end_date"].dt.strftime("%Y-%m-%d").map(key_events)
        df["has_key_event"] = df["key_event"].notna()

        # Days since/until key events
        biden_withdrawal = pd.to_datetime("2024-07-21")
        df["days_since_harris_entry"] = (df["end_date"] - biden_withdrawal).dt.days

        first_debate = pd.to_datetime("2024-09-10")
        df["days_from_first_debate"] = (df["end_date"] - first_debate).dt.days

        logger.info("Added key campaign event markers")
        return df

    def select_final_columns_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select final columns and perform final cleaning."""

        final_columns = [
            # Core identification
            "poll_id",
            "end_date",
            "start_date",
            "candidate_name",
            "pct",
            # Geographic and demographic filters
            "state",
            "geographic_scope",
            "population_clean",
            "hypothetical",
            # Poll characteristics
            "pollster",
            "sample_size",
            "sample_size_category",
            "methodology_clean",
            "polling_period_days",
            "is_tracking_poll",
            "is_internal_poll",
            "is_partisan_poll",
            # Quality measures
            "numeric_grade",
            "pollscore",
            "pollster_grade_category",
            "pollscore_category",
            "poll_quality_score",
            "margin_of_error",
            "ci_lower",
            "ci_upper",
            # Temporal features
            "year",
            "month",
            "quarter",
            "month_year",
            "year_quarter",
            "week_of_year",
            "days_until_election",
            "weeks_until_election",
            "campaign_phase",
            # Event markers
            "key_event",
            "has_key_event",
            "days_since_harris_entry",
            "days_from_first_debate",
        ]

        # Only include columns that exist in the dataframe
        available_columns = [col for col in final_columns if col in df.columns]

        df_final = df[available_columns].copy()

        # Final sort and cleanup
        df_final = df_final.sort_values(
            ["end_date", "candidate_name"], ascending=[False, True]
        )

        # Remove any remaining duplicates
        df_final = df_final.drop_duplicates()

        logger.info(f"Selected {len(available_columns)} columns for final dataset")
        return df_final

    def export_with_summary(self, df: pd.DataFrame) -> None:
        """Export the data with summary statistics."""

        df.to_csv(self.output_path, index=False)

        logger.info(f"Exported {len(df)} records to {self.output_path}")

        # Summary
        print("\n" + "=" * 80)
        print("CLEANED POLLING DATASET SUMMARY")
        print("=" * 80)

        print(f"📊 BASIC STATISTICS:")
        print(f"   Total poll records: {len(df):,}")
        print(
            f"   Date range: {df['end_date'].min().date()} to {df['end_date'].max().date()}"
        )
        print(f"   Unique polls: {df['poll_id'].nunique():,}")
        print(f"   Polling organizations: {df['pollster'].nunique()}")

        print(f"\n🏛️ GEOGRAPHIC BREAKDOWN:")
        geo_counts = df["geographic_scope"].value_counts()
        for scope, count in geo_counts.items():
            print(f"   {scope}: {count:,} ({count/len(df)*100:.1f}%)")

        print(f"\n👥 POPULATION BREAKDOWN:")
        pop_counts = df["population_clean"].value_counts()
        for pop, count in pop_counts.items():
            print(f"   {pop}: {count:,} ({count/len(df)*100:.1f}%)")

        print(f"\n🗳️ CANDIDATE BREAKDOWN:")
        cand_counts = df["candidate_name"].value_counts()
        for cand, count in cand_counts.items():
            print(f"   {cand}: {count:,} ({count/len(df)*100:.1f}%)")

        print(f"\n🔬 HYPOTHETICAL POLLS (AS PROVIDED IN RAW DATA):")
        hyp_counts = df["hypothetical"].value_counts()
        for hyp, count in hyp_counts.items():
            label = "Hypothetical" if hyp else "Real"
            print(f"   {label}: {count:,} ({count/len(df)*100:.1f}%)")

        print(f"\n⭐ POLL QUALITY DISTRIBUTION:")
        quality_dist = df["pollster_grade_category"].value_counts()
        for grade, count in quality_dist.items():
            print(f"   {grade}: {count:,} ({count/len(df)*100:.1f}%)")

        print(f"\n📊 TABLEAU CALCULATIONS TO CREATE:")
        print(
            f"   ✅ Daily Average: {{FIXED [Candidate Name], [End Date] : AVG([Pct])}}"
        )
        print(f"   ✅ 7-Day Rolling: WINDOW_AVG([Daily Average], -6, 0)")
        print(f"   ✅ Filter by: Geographic Scope, Population Clean")

        print("=" * 80)
        print("🚀 Ready for Tableau analysis with flexible filtering!")
        print("=" * 80)

    def process(self) -> None:
        """Execute the simplified data processing pipeline."""
        logger.info("Starting simplified polling data processing...")
        start_time = datetime.now()

        try:
            # Load and process data through the simplified pipeline
            df = self.load_and_parse_data()
            df = self.filter_and_clean_core_data(df)
            df = self.create_temporal_features(df)
            df = self.create_pollster_quality_features(df)
            df = self.calculate_statistical_measures(df)
            df = self.create_key_events_markers(df)
            df = self.select_final_columns_and_clean(df)

            # Export with summary
            self.export_with_summary(df)

            elapsed_time = datetime.now() - start_time
            logger.info(
                f"Simplified processing completed successfully in {elapsed_time}"
            )

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise


def main():
    """Main execution function."""
    processor = SimplifiedPollingProcessor(
        input_path="president_polls.csv", output_path="cleaned_polling_data.csv"
    )

    processor.process()


if __name__ == "__main__":
    main()
