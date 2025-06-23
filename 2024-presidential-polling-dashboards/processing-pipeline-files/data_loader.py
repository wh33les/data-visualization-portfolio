"""
Data Loading and Basic Validation
=================================
"""

import pandas as pd
import logging
from config import Config

logger = logging.getLogger(__name__)


def load_polling_data(file_path: str) -> pd.DataFrame:
    """
    Load raw polling data with basic validation.

    Args:
        file_path: Path to CSV file

    Returns:
        Raw DataFrame

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If data format is invalid
    """
    logger.info(f"Loading data from {file_path} ")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")

        # Basic validation
        validate_raw_data(df)
        return df

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise ValueError(f"Invalid data format: {e}")


def validate_raw_data(df: pd.DataFrame) -> None:
    """
    Validate that raw data has required columns and reasonable values.

    Args:
        df: Raw DataFrame to validate

    Raises:
        ValueError: If validation fails
    """
    missing_columns = [col for col in Config.REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if len(df) == 0:
        raise ValueError("DataFrame is empty")

    logger.info("Data validation passed")
