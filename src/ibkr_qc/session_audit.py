"""Exchange-calendar-aware diagnostics, never infer trades from absent bars."""
from __future__ import annotations

import pandas as pd

from .validate import QCError, market_labels


def exchange_schedule(frame: pd.DataFrame, *, start_ny: str | None = None,
                      end_ny: str | None = None, calendar_name: str = "NYSE") -> pd.DataFrame:
    import pandas_market_calendars as mcal
    if frame.empty:
        raise QCError("NO_VALID_BARS")
    observed = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert("America/New_York")
    first = str(observed.dt.date.min())
    last = str(observed.dt.date.max())
    first = start_ny or first
    last = end_ny or last
    if pd.Timestamp(first).date() > pd.Timestamp(last).date():
        raise ValueError("AUDIT_DATE_BOUNDS_INVERTED")
    schedule = mcal.get_calendar(calendar_name).schedule(start_date=first, end_date=last)
    if schedule.empty:
        raise QCError("NO_CALENDAR_SESSIONS")
    return schedule


def trading_day_gaps(frame: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    """Flag whole NYSE sessions with zero observed RTH, including calendar-only dates."""
    labels = frame if "date_ny" in frame else market_labels(frame)
    times = labels["timestamp_utc"]
    first_observed = times.min()
    last_observed = times.max()
    results = []
    grouped = {date: pd.DatetimeIndex(g["timestamp_utc"]) for date, g in labels.groupby("date_ny")}
    for date, s in schedule.iterrows():
        op = pd.Timestamp(s["market_open"]).tz_convert("UTC")
        close = pd.Timestamp(s["market_close"]).tz_convert("UTC")
        label = date.strftime("%Y-%m-%d")
        seen = grouped.get(label, pd.DatetimeIndex([], tz="UTC"))
        rth = int(((seen >= op) & (seen < close)).sum())
        if rth:
            continue
        any_bars = len(seen)
        boundary = close <= first_observed or op > last_observed
        state = "OUTSIDE_OBSERVED_SPAN_REVIEW" if boundary else (
            "NO_RTH_BARS_BUT_OTHER_BARS" if any_bars else "NO_BARS_ON_TRADING_DAY"
        )
        results.append({"date_ny": label, "session_open_utc": op.isoformat(),
                        "session_close_utc": close.isoformat(), "all_session_observed_bars": any_bars,
                        "rth_observed_bars": 0, "state": state})
    return pd.DataFrame(results, columns=["date_ny", "session_open_utc", "session_close_utc",
                                          "all_session_observed_bars", "rth_observed_bars", "state"])


def extended_hours_daily(frame: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    """Observed-only PRE/POST diagnostics: absence is not proof of lost trades.

    Windows: PRE 04:00 to scheduled open, POST scheduled close to 20:00 NY.
    On early-close dates the post window begins at the actual exchange close.
    No expectation of a bar at every wall-clock minute is asserted.
    """
    labels = frame if "date_ny" in frame else market_labels(frame)
    by_date = {date: g.sort_values("timestamp_utc") for date, g in labels.groupby("date_ny")}
    results = []
    for date, s in schedule.iterrows():
        day = date.strftime("%Y-%m-%d")
        ny_day = pd.Timestamp(day).tz_localize("America/New_York")
        op = pd.Timestamp(s["market_open"]).tz_convert("UTC")
        close = pd.Timestamp(s["market_close"]).tz_convert("UTC")
        for session, low, high in (("PRE", ny_day + pd.Timedelta(hours=4), op),
                                   ("POST", close, ny_day + pd.Timedelta(hours=20))):
            low_utc = low.tz_convert("UTC")
            high_utc = high.tz_convert("UTC")
            bars = by_date.get(day)
            if bars is None:
                bars = labels.iloc[:0]
            sub = bars.loc[(bars["timestamp_utc"] >= low_utc) &
                           (bars["timestamp_utc"] < high_utc)].copy()
            count = len(sub)
            grid = max(0, int((high_utc - low_utc) / pd.Timedelta(minutes=1)))
            flat = ((sub["open"] == sub["high"]) & (sub["high"] == sub["low"]) &
                    (sub["low"] == sub["close"])).sum() if count else 0
            if count:
                consecutive = ((sub["timestamp_utc"].diff() == pd.Timedelta(minutes=1)) &
                               (sub["close"].diff() == 0)).sum()
                positive = int((sub["volume"] > 0).sum())
                zero = int((sub["volume"] == 0).sum())
            else:
                consecutive = positive = zero = 0
            results.append({"date_ny": day, "session": session, "window_start_utc": low_utc.isoformat(),
                            "window_end_utc": high_utc.isoformat(), "diagnostic_grid_minutes": grid,
                            "observed_minutes": count, "observed_positive_volume_minutes": positive,
                            "observed_zero_volume_minutes": zero, "flat_ohlc_minutes": int(flat),
                            "consecutive_same_close_minutes": int(consecutive),
                            "no_bar_observed_minutes": max(0, grid - count),
                            "interpretation": "OBSERVATIONS_ONLY_NO_ABSENT_BAR_CAUSE_INFERRED"})
    return pd.DataFrame(results)
