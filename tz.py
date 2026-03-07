# forexlab/tz.py
import pytz
import pandas as pd

BROKER_TZ = "Europe/Helsinki"          # UTC+2 / UTC+3 DST typical MT5 broker time
DISPLAY_TZ = "America/New_York"        # you want to view everything in EST/EDT

def broker_to_utc(dt_series: pd.Series) -> pd.Series:
    """Treat incoming timestamps as broker/server time, convert to UTC naive."""
    broker = pytz.timezone(BROKER_TZ)
    utc = pytz.UTC

    # localize naive -> broker TZ (handles DST)
    localized = dt_series.dt.tz_localize(broker, ambiguous="NaT", nonexistent="shift_forward")
    converted = localized.dt.tz_convert(utc).dt.tz_localize(None)  # store naive UTC
    return converted

def utc_to_display(dt_series_utc_naive: pd.Series) -> pd.Series:
    """Convert stored UTC-naive timestamps to America/New_York for display."""
    utc = pytz.UTC
    disp = pytz.timezone(DISPLAY_TZ)
    localized = dt_series_utc_naive.dt.tz_localize(utc)
    return localized.dt.tz_convert(disp)