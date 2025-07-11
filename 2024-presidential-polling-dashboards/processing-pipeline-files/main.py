# =============================================================================
# main.py - Complete Pipeline with Entry Point
# =============================================================================

import logging
import sys
import pandas as pd

# Import from our modules
import cleaners as clean
import data_loader as loader
import feature_engineering as features


def setup_logging(debug=False):
    """Configure logging for debugging."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("polling_data_pipeline.log"),
        ],
    )


def print_data_summary(df: pd.DataFrame, stage: str):
    """Print summary statistics for debugging."""
    print(f"\n{'='*20}")
    print(f"DATA SUMMARY - {stage.upper()}")
    print(f"{'='*20}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")


def process_polling_data(
    input_file: str, output_file: str, debug_mode: bool = False
) -> pd.DataFrame:
    """
    Complete processing pipeline from raw data to analysis-ready format.

    Args:
        input_file: Path to raw CSV
        output_file: Path for cleaned CSV output
        debug_mode: Whether to show detailed summaries

    Returns:
        Processed DataFrame
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting complete polling data pipeline")

    # Step 1: Load data
    if debug_mode:
        print("Step 1: Loading data...")
    df = loader.load_polling_data(input_file)
    if debug_mode:
        print_data_summary(df, "Raw Data")

    # Step 2: Clean data
    if debug_mode:
        print("\nStep 2: Cleaning data...")
    df = clean.simple_cleaning_pipeline(
        df, filter_candidates=True
    )  # Change boolean to False to include all candidates
    if debug_mode:
        print_data_summary(df, "Cleaned Data")

    # Step 3: Add features for analysis
    if debug_mode:
        print("\nStep 3: Adding features...")

    # Add all features
    df = features.add_geographic_features(df)
    df = features.add_temporal_features(df)
    df = features.add_methodology_features(df)
    df = features.add_quality_metrics(df)

    # Create visualization-ready dataset
    viz_columns = [
        "candidate_name",
        "pct",
        "end_date",
        "pollster",
        "sample_size",
        "geographic_scope",
        "campaign_phase",
        "population_clean",
        "methodology_clean",
        "margin_of_error",
        "ci_lower",
        "ci_upper",
        "sample_size_category",
        "pollster_grade_category",
        "has_key_event",
        "is_tracking_poll",
        "polling_period_days",
    ]

    # Create streamlined version
    df_viz = df[viz_columns]

    # Calculate optimization metrics
    original_memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    optimized_memory_mb = df_viz.memory_usage(deep=True).sum() / 1024**2
    reduction_percent = (
        (original_memory_mb - optimized_memory_mb) / original_memory_mb
    ) * 100

    # Log the optimization
    logger.info("Creating visualization-ready dataset")
    logger.info(f"Optimized from {len(df.columns)} to {len(df_viz.columns)} columns")
    logger.info(
        f"Memory reduced from {original_memory_mb:.1f}MB to {optimized_memory_mb:.1f}MB ({reduction_percent:.1f}% smaller)"
    )

    if debug_mode:
        print(f"\n{'='*20}")
        print(f"DATASET OPTIMIZATION")
        print(f"{'='*20}")
        print(
            f"Columns: {len(df.columns)} → {len(df_viz.columns)} ({len(df.columns) - len(df_viz.columns)} removed)"
        )
        print(f"Memory: {original_memory_mb:.1f}MB → {optimized_memory_mb:.1f}MB")
        print(f"Reduction: {reduction_percent:.1f}% smaller")

    # Save the streamlined dataset
    df_viz.to_csv(output_file, index=False)
    logger.info(f"Tableau-ready dataset saved to {output_file}")

    if debug_mode:
        print(f"\nTableau-ready dataset saved: {output_file}")
        print(f"Ready for dashboard creation!")

    return df_viz


def main():
    """Main entry point."""

    # Parse command line arguments more robustly
    if len(sys.argv) == 1:
        # No additional arguments - use defaults
        input_file = "../data/president_polls.csv"  # Default
        debug_mode = False
        print("Using default settings...")
    elif len(sys.argv) == 2:
        if sys.argv[1] == "--debug":
            # Just --debug flag, use default file
            input_file = "../data/president_polls.csv"
            debug_mode = True
        else:
            # Just filename, normal mode
            input_file = sys.argv[1]
            debug_mode = False
    elif len(sys.argv) == 3:
        # Filename + flag
        input_file = sys.argv[1]
        if sys.argv[2] == "--debug":
            debug_mode = True
        else:
            print(f"Unknown flag: {sys.argv[2]}")
            sys.exit(1)
    else:
        print("Usage: python main.py [input_file] [--debug]")
        sys.exit(1)

    output_file = "../data/cleaned_polling_data.csv"  # Can change for different vizzes

    # Setup logging
    setup_logging(debug=debug_mode)

    # Show what we're doing
    print("POLLING DATA PROCESSING PIPELINE")
    print("=" * 50)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(
        f"Mode:   {'Debug (detailed summaries)' if debug_mode else 'Production (streamlined)'}"
    )
    print()

    try:
        # Run the complete pipeline
        result_df = process_polling_data(input_file, output_file, debug_mode)

        # Success summary
        print(f"\nSUCCESS!")
        print(f"Processed {len(result_df):,} rows")

        return result_df

    except FileNotFoundError:
        print(f"ERROR: Could not find input file '{input_file}'")
        print("Check the file path and try again")
        sys.exit(1)

    except Exception as e:
        print(f"ERROR: Pipeline failed")
        print(f"{type(e).__name__}: {e}")
        print("Check the log file for details: polling_data_pipeline.log")
        sys.exit(1)


if __name__ == "__main__":
    result_df = main()
