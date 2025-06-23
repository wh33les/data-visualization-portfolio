"""
Feature Engineering Functions
============================
"""

import pandas as pd
import numpy as np
import scipy.stats as stats
import logging
from config import Config

logger = logging.getLogger(__name__)

_logged_methodologies = set()


def add_geographic_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add geographic classification features."""
    logger.info("Adding geographic features")
    df = df.copy()

    # Debug: Show data quality
    missing_states = df["state"].isnull().sum()
    logger.debug(
        f"Missing state values: {missing_states:,} ({missing_states/len(df)*100:.1f}%)"
    )

    df["geographic_scope"] = df["state"].apply(_classify_geographic_scope)

    # Debug: Show categorization results
    if logger.isEnabledFor(logging.DEBUG):
        geo_counts = df["geographic_scope"].value_counts()
        logger.debug("Geographic scope distribution:")
        for scope, count in geo_counts.items():
            logger.debug(f"  {scope}: {count:,} polls")

    return df


def _classify_geographic_scope(state_value):
    if pd.isna(state_value):
        return "National"
    elif state_value in Config.SWING_STATES:
        return "Swing State"
    else:
        return "Other State"


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add comprehensive temporal features."""
    logger.info("Adding temporal features")

    df = df.copy()

    # Basic date components
    df["year"] = df["end_date"].dt.year
    df["month"] = df["end_date"].dt.month
    df["quarter"] = df["end_date"].dt.quarter
    df["week_of_year"] = df["end_date"].dt.isocalendar().week

    # Date strings for Tableau
    df["month_year"] = df["end_date"].dt.to_period("M").astype(str)
    df["year_quarter"] = df["end_date"].dt.to_period("Q").astype(str)

    # Campaign-specific metrics
    # More explicit for Pylance
    end_date_dt = pd.to_datetime(df["end_date"])
    df["days_until_election"] = (Config.ELECTION_DATE - end_date_dt).dt.days
    df["weeks_until_election"] = df["days_until_election"] / 7.0
    df["days_since_harris_entry"] = (end_date_dt - Config.HARRIS_ENTRY_DATE).dt.days
    df["days_from_first_debate"] = (end_date_dt - Config.FIRST_DEBATE_DATE).dt.days

    # Campaign phases
    df["campaign_phase"] = df["end_date"].apply(_get_campaign_phase)

    # Key events
    df["key_event"] = df["end_date"].dt.strftime("%Y-%m-%d").map(Config.KEY_EVENTS)
    df["has_key_event"] = df["key_event"].notna()

    return df


def _get_campaign_phase(date):
    """Helper function to determine campaign phase."""
    for phase, (start, end) in Config.CAMPAIGN_PHASES.items():
        if pd.Timestamp(start) <= date <= pd.Timestamp(end):
            return phase
    return "Other"


def add_methodology_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add methodology and poll type features."""
    logger.info("Adding methodology features")

    df = df.copy()

    # Population mapping
    population_mapping = {
        "a": "(All)",
        "all": "(All)",
        "rv": "Registered voters",
        "lv": "Likely voters",
        "v": "(All)",
    }

    df["population_clean"] = df["population"].str.lower().map(population_mapping)
    df["population_clean"] = df["population_clean"].fillna("All adults")

    # Methodology cleaning
    df["methodology_clean"] = df["methodology"].apply(_clean_methodology)

    # Polling period
    df["polling_period_days"] = (df["end_date"] - df["start_date"]).dt.days
    df["polling_period_days"] = df["polling_period_days"].fillna(1).astype(int)

    # Boolean flags
    df["is_tracking_poll"] = (
        df["tracking"].fillna("").str.lower().isin(["yes", "true", "1"])
    )
    df["is_internal_poll"] = (
        df["internal"].fillna("").str.lower().isin(["yes", "true", "1"])
    )
    df["is_partisan_poll"] = (
        df["partisan"].fillna("").str.lower().isin(["yes", "true", "1"])
    )

    return df


def _clean_methodology(method):
    """Helper function to standardize methodology."""
    if pd.isna(method):
        return "Unknown"

    method_str = str(method).lower()

    # Keyword lists
    phone_keywords = ["live", "phone", "telephone", "landline", "cell", "mobile"]
    online_keywords = [
        "online",
        "web",
        "internet",
        "digital",
        "panel",
        "probability",
        "app",
        "email",
    ]
    ivr_keywords = ["ivr", "robo", "automated", "auto", "interactive"]
    text_keywords = ["text", "sms"]

    # Check if unrecognized and log once
    all_keywords = phone_keywords + online_keywords + ivr_keywords + text_keywords
    if not any(keyword in method_str for keyword in all_keywords):
        if method not in _logged_methodologies:
            logger.debug(f"Unrecognized methodology: '{method}' -> 'Mixed/Other'")
            _logged_methodologies.add(method)

    if "live" in method or "phone" in method:
        return "Live Phone"
    elif "online" in method or "web" in method:
        return "Online"
    elif "ivr" in method or "robo" in method:
        return "IVR/Robocall"
    elif "text" in method or "sms" in method:
        return "Text/SMS"
    else:
        return "Mixed/Other"


def add_quality_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add comprehensive quality metrics."""
    logger.info("Adding quality metrics")

    df = df.copy()

    # Debug: Sample size insights
    logger.debug(
        f"Sample size range: {df['sample_size'].min():,.0f} to {df['sample_size'].max():,.0f}"
    )

    # Sample size categories
    df["sample_size_category"] = df["sample_size"].apply(_categorize_sample_size)

    # Debug: Show sample size distribution
    if logger.isEnabledFor(logging.DEBUG):
        size_counts = df["sample_size_category"].value_counts()
        logger.debug("Sample size categories:")
        for category, count in size_counts.items():
            logger.debug(f"  {category}: {count:,} polls")

    # Grade categories
    df["pollster_grade_category"] = df["numeric_grade"].apply(_categorize_grade)
    df["pollscore_category"] = df["pollscore"].apply(_categorize_pollscore)

    # Default worst-case MOE
    df["margin_of_error"] = df["sample_size"].apply(_calculate_margin_of_error)

    # Candidate-specific MOE
    df["candidate_specific_moe"] = df.apply(
        lambda row: _calculate_margin_of_error(row["sample_size"], p=row["pct"] / 100),
        axis=1,
    )

    # Confidence intervals
    df["ci_lower"], df["ci_upper"] = _calculate_confidence_interval(
        df["pct"], df["margin_of_error"]
    )
    df["ci_cs_lower"], df["ci_cs_upper"] = _calculate_confidence_interval(
        df["pct"], df["candidate_specific_moe"]
    )

    return df


def _categorize_sample_size(size):
    """Categorize sample sizes."""
    if pd.isna(size):
        return "Unknown"
    elif size < 500:
        return "Small (<500)"
    elif size < 1000:
        return "Medium (500-999)"
    elif size < 2000:
        return "Large (1000-1999)"
    else:
        return "Very Large (2000+)"


def _categorize_grade(grade):
    """Categorize pollster grades."""
    if pd.isna(grade):
        return "Unrated"
    elif grade >= 3.0:
        return "A-grade"
    elif grade >= 2.0:
        return "B-grade"
    elif grade >= 1.0:
        return "C-grade"
    else:
        return "D/F-grade"


def _categorize_pollscore(score):
    """Categorize pollster scores."""
    if pd.isna(score):
        return "Unrated"

    # Debug: Log unusual scores
    if score < -3 or score > 3:
        logger.debug(f"Unusual pollscore detected: {score}")

    elif score <= -1:
        return "High Quality"
    elif score >= 0:
        return "Good Quality"
    elif score >= 1:
        return "Lower Quality"
    else:
        return "Very Low Quality"


def _calculate_margin_of_error(sample_size, confidence_level=None, p=0.5):
    """Calculate margin of error.

    Args:
        sample_size: Number of respondents
        confidence_level: Confidence level (default 0.95)
        p: Proportion (default 0.5 for worst-case scenario)
    """
    if pd.isna(sample_size) or sample_size <= 0:
        return np.nan

    if confidence_level is None:
        confidence_level = Config.CONFIDENCE_LEVEL

    # Calculate critical value for any confidence level
    alpha = 1 - confidence_level
    critical_value = stats.norm.ppf(1 - alpha / 2)

    # Calculate p*(1-p) - defaults to 0.5*0.5 = 0.25
    variance = p * (1 - p)

    return critical_value * np.sqrt(variance / sample_size) * 100


def _calculate_confidence_interval(pct, moe):
    """Calculate confidence interval bounds clipped to valid percentage range."""
    lower = (pct - moe).clip(lower=0)
    upper = (pct + moe).clip(upper=100)
    return lower, upper
