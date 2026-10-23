"""NQ Globex 1-minute validation. NO roll stitching and NO data imputation.

Weekday grid follows CME's published *standard* NQ Globex hours
(Sunday 18:00 through Friday 17:00 America/New_York, with daily
17:00-18:00 maintenance). Special holidays/early closures require
external calendar reconciliation and are never asserted as data loss.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo
import re

import numpy as np
import pandas as pd

from .validate import QCError, OHLCV, _timestamp

CONTRACT_RE = re.compile(r"^(NQ[HMUZ]\d{1,2})_(\d+)$")
TIMEZONE_NY = "America/New_York"
TIMEZONE_TPE = "Asia/Taipei"
BAR_LABEL_POLICY = "BAR_START_PROVISIONAL"
# Fixed, UTC-absolute time grid; conversion to NY handles DST accurately.


@dataclass(frozen=True)
class NQContract:
    directory: str
    local_symbol: str
    con_id: int


def parse_contract_folder(name: str) -> NQContract:
    match = CONTRACT_RE.fullmatch(name)
    if not match:
        raise QCError(f"NQ_CONTRACT_FOLDER_UNEXPECTED: {name}")
    return NQContract(name, match.group(1), int(match.group(2)))


def normalize_nq(frame: pd.DataFrame, spec: NQContract) -> tuple[pd.DataFrame, dict]:
    """Reject incompatible identity, malformed OHLCV, or conflicting duplicates."""
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise QCError("EMPTY_NQ_CONTRACT")
    work = frame.copy()
    work.columns = [str(c).strip() for c in work.columns]
    required = set(OHLCV) | {
        "timestamp_utc", "symbol", "localSymbol", "conId", "expiry",
        "secType", "bar_size", "what_to_show", "use_rth",
    }
    missing = sorted(required - set(work.columns))
    if missing:
        raise QCError(f"NQ_REQUIRED_METADATA_MISSING: {missing}")
    for col, expected in (("symbol", "NQ"), ("localSymbol", spec.local_symbol),
                          ("secType", "FUT"), ("what_to_show", "TRADES")):
        values = work[col].dropna().astype(str).unique().tolist()
        if (len(values) != 1 or values[0].upper() not in ({expected.upper(), spec.local_symbol.upper()} if col == "symbol" else {expected.upper()}) or work[col].isna().any()):
            raise QCError(f"NQ_METADATA_CONFLICT_{col}: {values[:4]}")
    if not pd.to_numeric(work.conId, errors="coerce").eq(spec.con_id).all():
        raise QCError("NQ_CONID_MISMATCH")
    if work["expiry"].isna().any() or work["expiry"].astype(str).nunique() != 1:
        raise QCError("NQ_EXPIRY_MISMATCH")
    month={"H":3,"M":6,"U":9,"Z":12}[spec.local_symbol[2]]
    year=2000+int(spec.local_symbol[3:])
    expected_ym=f"{year:04d}{month:02d}"
    if not str(work.expiry.iloc[0]).startswith(expected_ym):
        raise QCError(f"NQ_EXPIRY_NOT_MATCHED_TO_CONTRACT: {work.expiry.iloc[0]} expected {expected_ym}")
    if not work.bar_size.astype(str).isin(["1 min", "1min", "1 mins", "1"]).all():
        raise QCError("NQ_BAR_SIZE_NOT_1MIN")
    if work.use_rth.isna().any() or not work.use_rth.eq(False).all():
        raise QCError("NQ_USE_RTH_TRUE_OR_UNKNOWN_NOT_FULL_GLOBEX")
    work["timestamp_utc"] = _timestamp(work.timestamp_utc, assume_naive_utc=False)
    ts = work.timestamp_utc
    if (not ts.dt.second.eq(0).all() or not ts.dt.microsecond.eq(0).all()
            or not ts.dt.nanosecond.eq(0).all()):
        raise QCError("NQ_BAR_NOT_MINUTE_ALIGNED")
    for col in OHLCV:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    if (work[list(OHLCV)].isna().any().any()
            or not np.isfinite(work[list(OHLCV)].to_numpy(dtype=float)).all()):
        raise QCError("NQ_INVALID_NUMERIC_OHLCV")
    o,h,l,c,v=(work[col] for col in OHLCV)
    fail=(o.le(0)|h.le(0)|l.le(0)|c.le(0)|v.lt(0)|
          h.lt(pd.concat([o,l,c],axis=1).max(axis=1))|
          l.gt(pd.concat([o,h,c],axis=1).min(axis=1)))
    if fail.any():
        raise QCError(f"NQ_INVALID_OHLCV: {int(fail.sum())}")
    clean=work[["timestamp_utc",*OHLCV]].copy()
    conflicts=clean.groupby("timestamp_utc")[list(OHLCV)].nunique(dropna=False).gt(1).any(axis=1)
    if conflicts.any():
        raise QCError(f"NQ_CONFLICTING_SAME_TIMESTAMP: {int(conflicts.sum())}")
    original=len(clean)
    clean=(clean.drop_duplicates(subset=["timestamp_utc",*OHLCV])
           .sort_values("timestamp_utc").reset_index(drop=True))
    meta={"contract_folder":spec.directory, "local_symbol":spec.local_symbol,
          "con_id":spec.con_id, "expiry":str(work.expiry.iloc[0]),
          "exchange_values":sorted(map(str,work.exchange.dropna().unique())) if "exchange" in work else [],
          "original_rows":original,"valid_rows":len(clean),
          "exact_duplicates_removed":original-len(clean),
          "start_utc":clean.timestamp_utc.iloc[0].isoformat(),
          "end_utc":clean.timestamp_utc.iloc[-1].isoformat(),
          "bar_label_policy":BAR_LABEL_POLICY,
          "what_to_show":"TRADES", "use_rth":False}
    return clean,meta


def standard_globex_open(utc_index: pd.DatetimeIndex) -> np.ndarray:
    """Standard session mask, without claiming knowledge of special holidays."""
    if utc_index.tz is None:
        raise QCError("NAIVE_UTC_GRID")
    ny = utc_index.tz_convert(TIMEZONE_NY)
    weekdays=np.asarray(ny.weekday)
    mins=np.asarray(ny.hour)*60+np.asarray(ny.minute)
    # Sun >=18, Mon-Thu <17 or >=18, Fri <17; Saturday closed.
    return (((weekdays>=0)&(weekdays<=3)&(mins<17*60)) |
            ((weekdays>=0)&(weekdays<=3)&(mins>=18*60)) |
            ((weekdays==4)&(mins<17*60)) |
            ((weekdays==6)&(mins>=18*60)))


def globex_trade_dates(utc_index: pd.DatetimeIndex) -> np.ndarray:
    """Exchange trade date: Sunday 18:00 ET belongs to Monday."""
    ny=utc_index.tz_convert(TIMEZONE_NY)
    local=pd.DatetimeIndex(ny.tz_localize(None).normalize())
    overnight=(ny.hour>=18)
    days=local + pd.to_timedelta(np.asarray(overnight,dtype=int),unit="D")
    return days.strftime("%Y-%m-%d").to_numpy()


def _missing_runs(missing: pd.DatetimeIndex, contract: str) -> list[dict]:
    if len(missing)==0:
        return []
    arr=pd.Series(missing)
    group=arr.diff().ne(pd.Timedelta(minutes=1)).cumsum()
    out=[]
    for _,sub in arr.groupby(group):
        out.append({"contract":contract, "start_utc":sub.iloc[0].isoformat(),
                    "end_utc":sub.iloc[-1].isoformat(),"missing_minutes":len(sub),
                    "classification":"POSSIBLE_GLOBEX_GAP_OR_SPECIAL_SESSION_REVIEW"})
    return out


def audit_standard_globex(frame: pd.DataFrame, spec: NQContract) -> tuple[pd.DataFrame,pd.DataFrame]:
    """Minute-level observed/expected coverage within EACH contract's actual span.

    Standard CME regular/maintenance hours only. Holiday/halts/early closes
    are not independently verified. Do not regard every missing minute as
    proof of bad vendor data.
    """
    if frame.empty:
        raise QCError("NO_NORMALIZED_NQ_BARS")
    observed=pd.DatetimeIndex(frame.timestamp_utc).tz_convert("UTC")
    grid=pd.date_range(observed[0], observed[-1],freq="min",tz="UTC")
    expected=grid[standard_globex_open(grid)]
    present=expected.intersection(observed)
    missing=expected.difference(observed)
    # Bars during standard CLOSED hours are separate metadata/clock QA candidates.
    closed_observed=observed[~standard_globex_open(observed)]
    closed_dates=pd.Series(globex_trade_dates(closed_observed)).value_counts()
    expected_dates=pd.Series(globex_trade_dates(expected),index=expected)
    present_dates=pd.Series(globex_trade_dates(present),index=present)
    missing_dates=pd.Series(globex_trade_dates(missing),index=missing)
    n_expected=expected_dates.value_counts()
    n_present=present_dates.value_counts()
    n_missing=missing_dates.value_counts()
    coverage=[]
    for trade_date in sorted(n_expected.index):
        total=int(n_expected[trade_date])
        have=int(n_present.get(trade_date,0))
        absent=int(n_missing.get(trade_date,0))
        coverage.append({
            "contract":spec.local_symbol,"trade_date_ny":trade_date,
            "expected_standard_minutes":total,
            "observed_standard_minutes":have,
            "missing_standard_minutes":absent,
            "coverage_fraction":have/total,
            "observed_during_standard_closed_minutes":int(closed_dates.get(trade_date,0)),
            "status":"PASS_PROVISIONAL_HOLIDAY_UNVERIFIED" if absent==0 else
                     ("NO_BARS_SPECIAL_CALENDAR_REVIEW" if have==0 else "PARTIAL_SPECIAL_CALENDAR_REVIEW"),
        })
    summary=pd.DataFrame(coverage)
    gap_frame=pd.DataFrame(_missing_runs(missing,spec.local_symbol),
                           columns=["contract","start_utc","end_utc","missing_minutes","classification"])
    return summary,gap_frame


def anchor_prices(frame: pd.DataFrame, spec: NQContract, *, tolerance_minutes: int=2) -> pd.DataFrame:
    """Find closest OBSERVED bar at/before specified Taipei clock anchors.

    Separate contract outputs; never calculate cross-contract returns. No
    extrapolation, forward-fill or stitching. For summer-time 05:00 Taipei
    Globex maintenance, 04:59 bar may serve as the 05:00 close proxy, but
    bar-start/end semantics remain unverified.
    """
    if tolerance_minutes<0 or tolerance_minutes>5:
        raise ValueError("anchor tolerance must be between 0 and 5 minutes")
    src=frame.set_index("timestamp_utc").sort_index()
    idx=pd.DatetimeIndex(src.index).tz_convert("UTC")
    start=idx[0].tz_convert(TIMEZONE_TPE).normalize()
    end=idx[-1].tz_convert(TIMEZONE_TPE).normalize()
    dates=pd.date_range(start,end,freq="D")
    data=[]
    for day in dates:
        if day.weekday()>=5:  # Taiwan holidays require TAIFEX calendar join downstream
            continue
        labels=(
            ("day_close_1345",day+pd.Timedelta(hours=13,minutes=45)),
            ("night_open_1500",day+pd.Timedelta(hours=15)),
            ("next_0500",day+pd.Timedelta(days=1,hours=5)),
            ("next_0845",day+pd.Timedelta(days=1,hours=8,minutes=45)),
        )
        row={"contract":spec.local_symbol,"con_id":spec.con_id,
             "candidate_taipei_weekday":day.date().isoformat(),
             "roll_policy":"UNDEFINED_DO_NOT_STITCH"}
        for name,clock in labels:
            stamp=clock.tz_convert("UTC")
            loc=idx.searchsorted(stamp,side="right")-1
            if loc>=0 and 0 <= (stamp-idx[loc]).total_seconds() <= tolerance_minutes*60:
                row[name+"_close"]=float(src.iloc[loc]["close"])
                row[name+"_observed_utc"]=idx[loc].isoformat()
                row[name+"_distance_min"]=(stamp-idx[loc]).total_seconds()/60.0
            else:
                row[name+"_close"]=np.nan
                row[name+"_observed_utc"]=""
                row[name+"_distance_min"]=np.nan
        row["day_close_to_night_open_ok"]=pd.notna(row["day_close_1345_close"]) and pd.notna(row["night_open_1500_close"])
        row["night_open_to_next_0500_ok"]=pd.notna(row["night_open_1500_close"]) and pd.notna(row["next_0500_close"])
        row["next_0500_to_0845_ok"]=pd.notna(row["next_0500_close"]) and pd.notna(row["next_0845_close"])
        # Explicitly same-contract price ratios, not a continuous NQ series.
        for new,left,right in (
            ("return_1345_1500","day_close_1345_close","night_open_1500_close"),
            ("return_1500_next0500","night_open_1500_close","next_0500_close"),
            ("return_next0500_0845","next_0500_close","next_0845_close"),
        ):
            row[new] = row[right]/row[left]-1 if pd.notna(row[left]) and pd.notna(row[right]) else np.nan
        data.append(row)
    return pd.DataFrame(data)


def jump_candidates(frame: pd.DataFrame,spec: NQContract, threshold: float=0.05) -> pd.DataFrame:
    """Within 1-minute adjacent bars ONLY; never treat nontrading gaps as jumps."""
    x=frame.sort_values("timestamp_utc")
    ret=x.close.pct_change(fill_method=None)
    adjacency=x.timestamp_utc.diff().eq(pd.Timedelta(minutes=1))
    mask=adjacency & ret.abs().ge(threshold)
    return pd.DataFrame({
        "contract":spec.local_symbol,
        "timestamp_utc":x.loc[mask,"timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "abs_return":ret.loc[mask].abs().to_numpy(),
        "classification":"NQ_LARGE_1MIN_MOVE_REVIEW_NOT_AUTOMATIC_ERROR",
    })
