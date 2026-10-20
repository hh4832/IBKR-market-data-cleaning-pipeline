from datetime import timezone
import pandas as pd
import pytest

from ibkr_qc.validate import QCError, normalize_soxx, merge_exact, market_labels, corporate_action_candidates


def bars(n=3, start="2026-10-19T13:30:00Z"):
    ts=pd.date_range(start=start, periods=n, freq="min", tz="UTC")
    return pd.DataFrame({"timestamp_utc":ts,"open":[100.0]*n,"high":[101.0]*n,
                         "low":[99.0]*n,"close":[100.5]*n,"volume":[100]*n,
                         "symbol":["SOXX"]*n,"conId":[12658194]*n})


def test_normalization_success():
    result=normalize_soxx(bars())
    assert len(result.frame)==3 and result.exact_duplicates==0
    assert str(result.frame.timestamp_utc.dt.tz)=="UTC"


def test_exact_duplicate_within_chunk():
    x=bars()
    x=pd.concat([x,x.iloc[[0]]],ignore_index=True)
    r=normalize_soxx(x)
    assert r.original_rows==4 and r.exact_duplicates==1


def test_same_timestamp_ohlcv_conflict_fails():
    x=bars()
    duplicate=x.iloc[[0]].copy()
    duplicate["close"]=100.7
    with pytest.raises(QCError,match="CONFLICTING_DUPLICATE_BARS"):
        normalize_soxx(pd.concat([x,duplicate],ignore_index=True))


def test_cross_chunk_exact_only():
    x=normalize_soxx(bars()).frame
    merged,dupes=merge_exact([x,x.iloc[[0]]])
    assert len(merged)==3 and dupes==1
    y=x.iloc[[0]].copy()
    y["volume"]=101
    with pytest.raises(QCError,match="CROSS_CHUNK_CONFLICT"):
        merge_exact([x,y])


def test_no_naive_timezone_guess():
    x=bars()
    x["timestamp_utc"]=x["timestamp_utc"].dt.tz_localize(None)
    with pytest.raises(QCError,match="TIMEZONE_NAIVE"):
        normalize_soxx(x)
    assert len(normalize_soxx(x,assume_naive_utc=True).frame)==3


def test_numeric_timestamp_no_inference():
    x=bars()
    x["timestamp_utc"]=x["timestamp_utc"].map(lambda v:int(v.timestamp()))
    with pytest.raises(QCError,match="NUMERIC_TIMESTAMP"):
        normalize_soxx(x)


def test_sox_symbol_rejected():
    x=bars()
    x["symbol"]="SOX"
    with pytest.raises(QCError,match="SYMBOL_MISMATCH"):
        normalize_soxx(x)


def test_invalid_ohlcv_and_null():
    x=bars()
    x.loc[0,"high"]=90
    with pytest.raises(QCError,match="INVALID_OHLCV"):
        normalize_soxx(x)
    x=bars()
    x.loc[0,"volume"]=None
    with pytest.raises(QCError,match="NULL_OR_NON_NUMERIC"):
        normalize_soxx(x)


def test_minute_alignment():
    x=bars()
    x.loc[0,"timestamp_utc"]+=pd.Timedelta(seconds=30)
    with pytest.raises(QCError,match="NOT_MINUTE_ALIGNED"):
        normalize_soxx(x)


def test_different_ny_dst_utc_offsets():
    x=pd.concat([bars(1,"2026-03-06T14:30:00Z"),bars(1,"2026-03-09T13:30:00Z")],ignore_index=True)
    labeled=market_labels(normalize_soxx(x).frame)
    assert list(labeled.time_ny)==["09:30","09:30"]
    assert list(labeled.utc_offset)==["-0500","-0400"]


def test_split_candidate_flagged_not_modified():
    x=bars(2)
    x.loc[1,"open"]=50
    x.loc[1,"high"]=51
    x.loc[1,"low"]=49
    x.loc[1,"close"]=50.0
    clean=normalize_soxx(x).frame
    flags=corporate_action_candidates(clean)
    assert len(flags)==1
    assert clean.loc[1,"close"]==50.0


def test_missing_expected_390_minutes(monkeypatch):
    from ibkr_qc.validate import daily_coverage, coverage_gaps
    # Fake calendar explicit one full regular trading day, no external dependency required.
    class MockCal:
        def schedule(self,start_date,end_date):
            d=pd.Timestamp("2026-10-19")
            return pd.DataFrame({"market_open":[pd.Timestamp("2026-10-19T13:30:00Z")],
                "market_close":[pd.Timestamp("2026-10-19T20:00:00Z")]},index=pd.DatetimeIndex([d]))
    import sys, types
    monkeypatch.setitem(sys.modules,"pandas_market_calendars",types.SimpleNamespace(get_calendar=lambda name:MockCal()))
    x=market_labels(normalize_soxx(bars(30)).frame)
    coverage=daily_coverage(x)
    assert coverage.loc[0,"expected_rth_minutes"]==390
    assert coverage.loc[0,"observed_rth_minutes"]==30
    assert coverage.loc[0,"missing_rth_minutes"]==360
    assert coverage.loc[0,"status"]=="PARTIAL_COVERAGE"
    gaps=coverage_gaps(x)
    assert gaps["missing_minutes"].sum()==360


def test_early_close_210_minutes(monkeypatch):
    from ibkr_qc.validate import daily_coverage
    class MockCal:
        def schedule(self,start_date,end_date):
            return pd.DataFrame({"market_open":[pd.Timestamp("2026-11-27T14:30:00Z")],
                "market_close":[pd.Timestamp("2026-11-27T18:00:00Z")]},index=pd.DatetimeIndex([pd.Timestamp("2026-11-27")]))
    import sys, types
    monkeypatch.setitem(sys.modules,"pandas_market_calendars",types.SimpleNamespace(get_calendar=lambda name:MockCal()))
    x=market_labels(normalize_soxx(bars(210,"2026-11-27T14:30:00Z")).frame)
    coverage=daily_coverage(x)
    assert coverage.loc[0,"expected_rth_minutes"]==210
    assert coverage.loc[0,"missing_rth_minutes"]==0
