"""Pure/mostly-vectorized SOXX bar validation. Never edits source files."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

EXPECTED_SYMBOL = "SOXX"
EXPECTED_CONID = 12658194
OHLCV = ("open", "high", "low", "close", "volume")
TIME_CANDIDATES = ("timestamp_utc", "datetime_utc", "datetime", "timestamp", "date", "time")


class QCError(ValueError):
    """Fail-closed for ambiguous or contradictory input."""


@dataclass(frozen=True)
class Normalization:
    frame: pd.DataFrame
    original_rows: int
    exact_duplicates: int
    time_field: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _timestamp(raw: pd.Series, *, assume_naive_utc: bool) -> pd.Series:
    """Do not silently reinterpret timezone-naive IBKR timestamps as UTC."""
    if isinstance(raw.dtype, pd.DatetimeTZDtype):
        out = pd.to_datetime(raw, errors="coerce", utc=True)
    elif pd.api.types.is_datetime64_dtype(raw.dtype):
        if not assume_naive_utc:
            raise QCError("TIMEZONE_NAIVE: explicit --assume-naive-utc required")
        out = pd.to_datetime(raw, errors="coerce").dt.tz_localize("UTC")
    elif pd.api.types.is_numeric_dtype(raw.dtype):
        raise QCError("NUMERIC_TIMESTAMP_UNIT_AMBIGUOUS: provide normalized ISO timestamps")
    else:
        # ISO string timestamps must include explicit Z or +/-HH:MM, except with explicit opt-in.
        text = raw.astype("string").str.strip()
        has_tz = text.str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", case=False, regex=True, na=False)
        if (~has_tz).any() and not assume_naive_utc:
            raise QCError("TIMEZONE_NAIVE_STRING: explicit --assume-naive-utc required")
        out = pd.to_datetime(text, utc=True, errors="coerce")
    if out.isna().any():
        raise QCError("INVALID_TIMESTAMP")
    return out


def normalize_soxx(frame: pd.DataFrame, *, assume_naive_utc: bool = False) -> Normalization:
    if frame.empty:
        raise QCError("EMPTY_CHUNK")
    work = frame.copy()
    # Preserve actual column spellings as an explicit schema audit, while accepting a few known timestamp aliases.
    work.columns = [str(c).strip() for c in work.columns]
    time_field = next((t for t in TIME_CANDIDATES if t in work), None)
    if time_field is None and isinstance(work.index, pd.DatetimeIndex):
        index_name = work.index.name or "index"
        work = work.reset_index().rename(columns={index_name: "timestamp_utc"})
        time_field = "timestamp_utc"
    if time_field is None:
        raise QCError("TIMESTAMP_COLUMN_UNKNOWN: inspect source Parquet schema")
    if not set(OHLCV).issubset(work.columns):
        missing = sorted(set(OHLCV).difference(work.columns))
        raise QCError(f"MISSING_OHLCV_COLUMNS: {missing}")
    if "symbol" in work.columns and not work["symbol"].astype(str).eq(EXPECTED_SYMBOL).all():
        raise QCError("SYMBOL_MISMATCH: expected SOXX; SOX is a different instrument")
    if "conId" in work.columns and not pd.to_numeric(work["conId"], errors="coerce").eq(EXPECTED_CONID).all():
        raise QCError("CONID_MISMATCH")
    work["timestamp_utc"] = _timestamp(work[time_field], assume_naive_utc=assume_naive_utc)
    if not work["timestamp_utc"].dt.second.eq(0).all() or not work["timestamp_utc"].dt.microsecond.eq(0).all():
        raise QCError("NOT_MINUTE_ALIGNED")
    for c in OHLCV:
        work[c] = pd.to_numeric(work[c], errors="coerce")
    if work[list(OHLCV)].isna().any().any():
        raise QCError("NULL_OR_NON_NUMERIC_OHLCV")
    o, h, l, c, v = (work[k] for k in OHLCV)
    invalid = ((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0) | (v < 0)
               | (h < pd.concat([o, l, c], axis=1).max(axis=1))
               | (l > pd.concat([o, h, c], axis=1).min(axis=1)))
    if invalid.any():
        raise QCError(f"INVALID_OHLCV: {int(invalid.sum())} bars")
    clean = work[["timestamp_utc", *OHLCV]].copy()
    # Fail on same-timestamp conflicts even across chunks; never arbitrary keep='last'.
    conflicts = clean.groupby("timestamp_utc", sort=False, dropna=False)[list(OHLCV)].nunique(dropna=False).gt(1).any(axis=1)
    if conflicts.any():
        raise QCError(f"CONFLICTING_DUPLICATE_BARS: {int(conflicts.sum())} timestamps")
    before = len(clean)
    clean = clean.drop_duplicates(subset=["timestamp_utc", *OHLCV]).sort_values("timestamp_utc").reset_index(drop=True)
    return Normalization(clean, before, before - len(clean), time_field)


def merge_exact(frames: Sequence[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    if not frames:
        return pd.DataFrame(columns=["timestamp_utc", *OHLCV]), 0
    merged = pd.concat(frames, ignore_index=True)
    conflicts = merged.groupby("timestamp_utc", sort=False)[list(OHLCV)].nunique(dropna=False).gt(1).any(axis=1)
    if conflicts.any():
        raise QCError(f"CROSS_CHUNK_CONFLICT: {int(conflicts.sum())} timestamps")
    dup_count = len(merged) - len(merged.drop_duplicates(subset=["timestamp_utc", *OHLCV]))
    return merged.drop_duplicates(subset=["timestamp_utc", *OHLCV]).sort_values("timestamp_utc").reset_index(drop=True), dup_count


def market_labels(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach exchange-local wall time; do not infer a data-provider RTH setting."""
    out = frame.copy()
    ny = out["timestamp_utc"].dt.tz_convert("America/New_York")
    out["date_ny"] = ny.dt.date.astype("string")
    out["time_ny"] = ny.dt.strftime("%H:%M")
    out["utc_offset"] = ny.dt.strftime("%z")
    t = ny.dt.hour * 60 + ny.dt.minute
    out["session_type"] = np.select(
        [(t >= 4*60) & (t < 9*60+30), (t >= 9*60+30) & (t < 16*60), (t >= 16*60) & (t < 20*60)],
        ["PRE", "REGULAR_CANDIDATE", "POST"],
        default="OUTSIDE_EXTENDED_WINDOW",
    )
    return out


def daily_coverage(frame: pd.DataFrame, *, calendar_name: str = "NYSE") -> pd.DataFrame:
    """Calendar-aware 1-minute regular-session coverage, including known half-days.

    Assumes timestamps label bar starts. This is a provisional QA hypothesis,
    NOT validation of IBKR timestamp labels or useRTH setting.
    """
    import pandas_market_calendars as mcal
    if frame.empty:
        raise QCError("NO_VALID_BARS")
    if "date_ny" not in frame:
        frame = market_labels(frame)
    calendar = mcal.get_calendar(calendar_name)
    dates = pd.to_datetime(frame["date_ny"])
    schedule = calendar.schedule(start_date=dates.min().date(), end_date=dates.max().date())
    if schedule.empty:
        raise QCError("NO_CALENDAR_SESSIONS")
    observed_by_date = {key: pd.DatetimeIndex(group["timestamp_utc"].drop_duplicates()) for key, group in frame.groupby("date_ny", sort=False)}
    rows = []
    for session_date, day in schedule.iterrows():
        start = pd.Timestamp(day["market_open"]).tz_convert("UTC")
        end = pd.Timestamp(day["market_close"]).tz_convert("UTC")
        expected = pd.date_range(start=start, end=end, freq="min", inclusive="left")
        observed_unique = observed_by_date.get(session_date.strftime("%Y-%m-%d"), pd.DatetimeIndex([], tz="UTC"))
        observed_unique = observed_unique[(observed_unique >= start) & (observed_unique < end)]
        # This uses actual bars, not row-counts from downloader status.
        missing = expected.difference(observed_unique)
        extra = observed_unique.difference(expected)
        date_str = session_date.strftime("%Y-%m-%d")
        rows.append({
            "date_ny": date_str, "session_open_utc": start.isoformat(), "session_close_utc": end.isoformat(),
            "expected_rth_minutes": len(expected), "observed_rth_minutes": len(expected) - len(missing),
            "missing_rth_minutes": len(missing), "outside_grid_rth_minutes": len(extra),
            "coverage_rth": round((len(expected)-len(missing))/len(expected), 6),
            "status": "COVERAGE_OK_LABEL_UNVERIFIED" if not len(missing) else "PARTIAL_COVERAGE",
            "first_missing_utc": missing[0].isoformat() if len(missing) else "",
            "last_missing_utc": missing[-1].isoformat() if len(missing) else "",
        })
    return pd.DataFrame(rows)


def coverage_gaps(frame: pd.DataFrame, *, calendar_name: str = "NYSE") -> pd.DataFrame:
    """Emit missing minutes for RTH only; never forward-fill and never infer RTH from total daily count."""
    import pandas_market_calendars as mcal
    cal = mcal.get_calendar(calendar_name)
    start_date = frame["timestamp_utc"].min().tz_convert("America/New_York").date()
    end_date = frame["timestamp_utc"].max().tz_convert("America/New_York").date()
    sched = cal.schedule(start_date=start_date,end_date=end_date)
    missing = []
    if "date_ny" not in frame:
        frame = market_labels(frame)
    by_date = {key: pd.DatetimeIndex(group["timestamp_utc"]) for key, group in frame.groupby("date_ny", sort=False)}
    for d, row in sched.iterrows():
        expected = pd.date_range(pd.Timestamp(row["market_open"]),pd.Timestamp(row["market_close"]),freq="min",inclusive="left")
        missed = expected.difference(by_date.get(d.strftime("%Y-%m-%d"), pd.DatetimeIndex([], tz="UTC")))
        if len(missed):
            # RLE contiguous missing timestamps, preserving exact UTC intervals.
            ts = pd.Series(missed)
            group = ts.diff().ne(pd.Timedelta(minutes=1)).cumsum()
            for _, seg in ts.groupby(group):
                missing.append({"date_ny": d.strftime("%Y-%m-%d"), "first_missing_utc": seg.iloc[0].isoformat(),
                                "last_missing_utc": seg.iloc[-1].isoformat(), "missing_minutes": len(seg),
                                "status": "RTH_MISSING_UNEXPLAINED"})
    return pd.DataFrame(missing,columns=["date_ny","first_missing_utc","last_missing_utc","missing_minutes","status"])


def corporate_action_candidates(frame: pd.DataFrame, *, threshold: float = 0.40) -> pd.DataFrame:
    """Flag (do not fix) large inter-bar jumps; these do not establish splits."""
    if frame.empty:
        return pd.DataFrame(columns=["timestamp_utc", "abs_return", "classification"])
    data=frame.sort_values("timestamp_utc")
    change=data["close"].pct_change(fill_method=None).abs()
    result=data.loc[change.ge(threshold),["timestamp_utc"]].copy()
    result["abs_return"]=change[change.ge(threshold)].to_numpy()
    result["classification"]="LARGE_MOVE_REVIEW_CORPORATE_ACTION_OR_DATA_ERROR"
    return result.reset_index(drop=True)
