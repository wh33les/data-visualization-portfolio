"""
Configuration and Constants for Polling Data Pipeline
====================================================
"""

import pandas as pd
from typing import Dict, List, Tuple


class Config:
    """Configuration constants for the polling data pipeline."""

    # Try common date formats first (faster and more reliable)
    DATE_FORMATS = [
        "%Y-%m-%d",  # 2024-01-15
        "%m/%d/%Y",  # 1/15/2024
        "%Y-%m-%d %H:%M:%S",  # 2024-01-15 14:30:00
        "%m-%d-%Y",  # 01-15-2024
    ]

    # Data schema configuration
    DATE_COLUMNS = ["start_date", "end_date", "election_date"]
    REQUIRED_COLUMNS = ["candidate_name", "pct", "end_date"]

    # Geographic configuration
    SWING_STATES = [
        "Arizona",
        "Georgia",
        "Michigan",
        "Nevada",
        "North Carolina",
        "Pennsylvania",
        "Wisconsin",
    ]

    # Main candidates for analysis
    MAIN_CANDIDATES = ["Donald Trump", "Joe Biden", "Kamala Harris"]

    # Key campaign dates
    ELECTION_DATE = pd.Timestamp("2024-11-05")
    HARRIS_ENTRY_DATE = pd.Timestamp("2024-07-21")
    FIRST_DEBATE_DATE = pd.Timestamp("2024-09-10")

    # Campaign phases
    CAMPAIGN_PHASES = {
        "Early Primary": ("2023-01-01", "2024-03-01"),
        "Primary Season": ("2024-03-01", "2024-07-21"),
        "Harris Entry": ("2024-07-21", "2024-09-01"),
        "General Election": ("2024-09-01", "2024-11-05"),
        "Post-Election": ("2024-11-05", "2025-01-20"),
    }

    # Key events for annotation
    KEY_EVENTS = {
        "2024-07-21": "Biden Withdraws, Harris Enters",
        "2024-08-19": "Democratic Convention",
        "2024-09-10": "First Presidential Debate",
        "2024-10-01": "VP Debate",
        "2024-11-05": "Election Day",
    }

    # Confidence levels for MOE calculations
    CONFIDENCE_LEVEL = 0.95
