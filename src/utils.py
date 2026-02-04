"""Utility helpers for the ETL pipeline.

All public functions have English docstrings. Values returned for categorization are localized to Portuguese to support downstream dashboards.
"""
from datetime import time, datetime
from typing import Union


def classify_time_of_day(value: Union[time, datetime, str]) -> str:
    """Classify a time value into a period of day in Portuguese.

    Periods:
    - 'Almoço' for 11:00-14:59
    - 'Tarde' for 15:00-17:59
    - 'Jantar' for 18:00-22:59
    - 'Madrugada' for all other times

    Accepts a datetime.time, datetime.datetime, or a string parseable by the time constructor.
    """
    if isinstance(value, datetime):
        hour = value.time().hour
    elif isinstance(value, time):
        hour = value.hour
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            hour = parsed.time().hour
        except Exception:
            # fallback: try HH:MM
            parts = value.split(":")
            hour = int(parts[0]) if parts and parts[0].isdigit() else 0
    else:
        hour = 0

    if 10 <= hour <= 14:
        return "Almoço"
    if 15 <= hour <= 17:
        return "Tarde"
    if 18 <= hour <= 23:
        return "Jantar"
    return "Jantar"
