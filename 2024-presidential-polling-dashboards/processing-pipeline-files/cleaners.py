"""
Simplified Data Cleaning Functions
=================================

Minimal cleaning focused on actual needs:
- Parse dates properly
- Optionally filter to main candidates
- Keep it simple and extensible

"""

import pandas as pd
import numpy as np
import logging
import warnings
from config import Config

logger = logging.getLogger(__name__)


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and parse date columns with better format handling."""
    logger.info("Parsing date columns")

    df = df.copy()

    for col in Config.DATE_COLUMNS:
        if col in df.columns:
            logger.info(f"Processing date column: {col}")

            parsed_successfully = False

            # Try each date format
            for date_format in Config.DATE_FORMATS:
                try:
                    df[col] = pd.to_datetime(
                        df[col], format=date_format, errors="raise"
                    )
                    logger.info(
                        f"Successfully parsed {col} using format: {date_format}"
                    )
                    parsed_successfully = True
                    break
                except (ValueError, TypeError):
                    continue

            # If no format worked, fall back to automatic parsing (with warning suppression)
            if not parsed_successfully:
                logger.info(
                    f"No single format worked for {col}, using flexible parsing"
                )
                # Suppress the warning since we're intentionally using flexible parsing
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def filter_main_candidates(df: pd.DataFrame, apply_filter: bool = True) -> pd.DataFrame:
    """
    Optionally filter to main candidates.

    Args:
        df: DataFrame with polling data
        apply_filter: Whether to actually apply the filter (default: True)

    Returns:
        Filtered DataFrame (or original if apply_filter=False)
    """
    if not apply_filter:
        logger.info("Skipping candidate filtering")
        return df

    logger.info("Filtering to main candidates")

    df = df.copy()
    initial_rows = len(df)

    if "candidate_name" in df.columns:
        # Clean candidate names first (remove extra whitespace)
        df["candidate_name"] = df["candidate_name"].str.strip()

        # Filter to main candidates
        df = df[df["candidate_name"].isin(Config.MAIN_CANDIDATES)]

    kept_rows = len(df)
    logger.info(f"Candidate filtering: kept {kept_rows:,} of {initial_rows:,} rows")

    # Show candidate distribution
    if "candidate_name" in df.columns and len(df) > 0:
        candidate_counts = df["candidate_name"].value_counts()
        logger.info("Final candidate distribution:")
        for candidate, count in candidate_counts.items():
            logger.info(f"  {candidate}: {count:,} polls")

    return df


def basic_data_quality_check(df: pd.DataFrame) -> None:
    """
    Basic data quality reporting without changing the data.

    Args:
        df: DataFrame to analyze
    """
    logger.info("Running basic data quality check")

    for col in Config.REQUIRED_COLUMNS:
        if col in df.columns:
            total = len(df)
            missing = df[col].isnull().sum()
            valid = total - missing

            logger.info(f"{col}: {valid:,}/{total:,} valid ({valid/total*100:.1f}%)")

            # Additional checks for specific columns
            if col == "pct":
                invalid_pct = df[(df[col] < 0) | (df[col] > 100)].shape[0]
                if invalid_pct > 0:
                    logger.warning(
                        f"  {invalid_pct} invalid percentages (outside 0-100%)"
                    )


def simple_cleaning_pipeline(
    df: pd.DataFrame, filter_candidates: bool = True
) -> pd.DataFrame:
    """
    Args:
        df: Raw DataFrame
        filter_candidates: Whether to filter to main candidates

    Returns:
        Minimally cleaned DataFrame
    """
    logger.info("Starting simple cleaning pipeline")

    initial_rows = len(df)

    # Step 1: Parse dates (always needed)
    df = clean_dates(df)

    # Step 2: Basic quality check (informational only)
    basic_data_quality_check(df)

    # Step 3: Optionally filter candidates
    df = filter_main_candidates(df, apply_filter=filter_candidates)

    final_rows = len(df)

    logger.info("Simple cleaning complete:")
    logger.info(f"  Input: {initial_rows:,} rows")
    logger.info(f"  Output: {final_rows:,} rows")
    logger.info(f"  Data quality: Clean and ready for analysis")

    return df


# For future extensibility - placeholder functions
def advanced_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder for future advanced cleaning if needed.

    Currently just calls simple_cleaning_pipeline, but can be
    extended later based on actual data quality issues found.
    """
    logger.info("Advanced cleaning not needed - using simple pipeline")
    return simple_cleaning_pipeline(df)


if __name__ == "__main__":
    print("Simple cleaners module - focused on actual needs")
    print("Available functions:")
    print("  - clean_dates()")
    print("  - filter_main_candidates()")
    print("  - basic_data_quality_check()")
    print("  - simple_cleaning_pipeline()")
