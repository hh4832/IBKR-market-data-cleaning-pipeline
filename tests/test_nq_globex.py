"""NQ-specific regression tests. SOXX/NYSE logic must remain untouched."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from ibkr_qc.nq_validate import (
    anchor_prices, audit_standard_globex, globex_trade_dates,
    normalize_nq, parse_contract_folder, standard_globex_open,
)
from ibkr_qc.nq_pipeline import NQRunConfig
from ibkr_qc.validate import QCError

SPEC=parse_contract_folder("NQZ6_563947726")


def sample(when=None):
    when=when if when is not None else pd.date_range("2026-10-13T01:00:00Z",periods=5,freq="min")
    return pd.DataFrame({
        "timestamp_utc":when,"open":[101.] * len(when),
        "high":[102.] * len(when),"low":[99.] * len(when),
        "close":[100.+i for i in range(len(when))],
        "volume":[10.] * len(when),"symbol":["NQ"] * len(when),
        "localSymbol":["NQZ6"] * len(when),"conId":[563947726]*len(when),
        "expiry":["20261218"]*len(when),"exchange":["CME"]*len(when),
        "secType":["FUT"]*len(when),"bar_size":["1 min"]*len(when),
        "what_to_show":["TRADES"]*len(when),"use_rth":[False]*len(when),
    }).assign(high=lambda x:x[["open","close"]].max(axis=1)+1.)


def test_parse_contract_and_identity_fail_closed():
    assert (SPEC.local_symbol,SPEC.con_id)==("NQZ6",563947726)
    with pytest.raises(QCError,match="CONTRACT_FOLDER"):
        parse_contract_folder("NQ_MAIN_CONTINUOUS")
    with pytest.raises(QCError,match="CONID"):
        normalize_nq(sample().assign(conId=123),SPEC)
    with pytest.raises(QCError,match="USE_RTH"):
        normalize_nq(sample().assign(use_rth=True),SPEC)
    with pytest.raises(QCError,match="NQ_BAR_SIZE"):
        normalize_nq(sample().assign(bar_size="5 mins"),SPEC)


def test_exact_duplicate_only_no_conflict_and_naive_timezone_rejected():
    f=sample()
    norm,meta=normalize_nq(pd.concat([f,f.iloc[:1]],ignore_index=True),SPEC)
    assert len(norm)==5 and meta["exact_duplicates_removed"]==1
    bad=pd.concat([f,f.iloc[:1].assign(close=75.,low=74.)],ignore_index=True)
    with pytest.raises(QCError,match="CONFLICTING"):
        normalize_nq(bad,SPEC)
    naive=f.assign(timestamp_utc=f.timestamp_utc.dt.tz_localize(None))
    with pytest.raises(QCError,match="NAIVE"):
        normalize_nq(naive,SPEC)


def test_standard_globex_hours_and_maintenance_daylight_savings():
    # Globex NQ: Sunday 18 ET to Friday 17 ET, daily 17-18 ET maintenance.
    events=[
        ("2026-10-18 17:59","America/New_York",False),
        ("2026-10-18 18:00","America/New_York",True),
        ("2026-10-19 16:59","America/New_York",True),
        ("2026-10-19 17:00","America/New_York",False),
        ("2026-10-19 18:00","America/New_York",True),
        ("2026-10-23 16:59","America/New_York",True),
        ("2026-10-23 18:00","America/New_York",False),
        ("2026-10-24 09:00","America/New_York",False),
    ]
    for local,tz,open_ in events:
        stamp=pd.DatetimeIndex([pd.Timestamp(local,tz=tz).tz_convert("UTC")])
        assert bool(standard_globex_open(stamp)[0]) is open_
    summer=pd.Timestamp("2026-07-14 17:00",tz="America/New_York").tz_convert("Asia/Taipei")
    winter=pd.Timestamp("2026-01-13 17:00",tz="America/New_York").tz_convert("Asia/Taipei")
    assert (summer.hour,winter.hour)==(5,6)
    su=pd.DatetimeIndex([pd.Timestamp("2026-10-18 18:00",tz="America/New_York").tz_convert("UTC")])
    assert globex_trade_dates(su).tolist()==["2026-10-19"]


def test_gap_grid_excludes_regular_maintenance_weekend():
    # 1m missing inside an open session must be flagged as candidate, never imputed.
    t=pd.date_range("2026-10-13 01:00",periods=12,freq="min",tz="UTC")
    frame=sample(t.delete(5))
    cleaned,_=normalize_nq(frame,SPEC)
    days,gaps=audit_standard_globex(cleaned,SPEC)
    assert days.missing_standard_minutes.sum()==1
    assert len(gaps)==1 and gaps.iloc[0].missing_minutes==1
    assert "REVIEW" in days.iloc[0]["status"]


def test_taipei_anchors_require_same_contract_and_max_two_minutes():
    tz="Asia/Taipei"
    times=[
        pd.Timestamp("2026-10-13 13:44",tz=tz),
        pd.Timestamp("2026-10-13 14:59",tz=tz),
        pd.Timestamp("2026-10-14 04:59",tz=tz),
        pd.Timestamp("2026-10-14 08:44",tz=tz),
    ]
    frame=sample(pd.DatetimeIndex(times).tz_convert("UTC"))
    norm,_=normalize_nq(frame,SPEC)
    result=anchor_prices(norm,SPEC,tolerance_minutes=2)
    day=result.set_index("candidate_taipei_weekday").loc["2026-10-13"]
    assert bool(day["day_close_to_night_open_ok"])
    assert bool(day["night_open_to_next_0500_ok"])
    assert bool(day["next_0500_to_0845_ok"])
    assert day["return_1345_1500"]==pytest.approx(101/100-1)
    assert day["return_1500_next0500"]==pytest.approx(102/101-1)
    assert day["return_next0500_0845"]==pytest.approx(103/102-1)
    assert day["roll_policy"]=="UNDEFINED_DO_NOT_STITCH"
    # More than two minutes away: MUST NOT silently forward-fill.
    tight=anchor_prices(norm,SPEC,tolerance_minutes=0)
    fail=tight.set_index("candidate_taipei_weekday").loc["2026-10-13"]
    assert pd.isna(fail["return_1345_1500"])


def test_validation_config_source_and_output_distinct():
    NQRunConfig().validate()
    with pytest.raises(ValueError,match="OVERLAP"):
        NQRunConfig(source_folder_id="same",output_parent_id="same").validate()


def test_source_contract_metadata_is_not_available_as_research_ready():
    # The validation output can be provisionally clean, but roll/calendar and
    # IBKR bar-start semantics still require confirmation before backtesting.
    assert NQRunConfig().max_contracts == 0


def test_exact_boundary_bar_not_used_if_start_label():
    times = pd.DatetimeIndex([
        pd.Timestamp("2026-10-13 13:44", tz="Asia/Taipei"),
        pd.Timestamp("2026-10-13 13:45", tz="Asia/Taipei"),
        pd.Timestamp("2026-10-13 14:59", tz="Asia/Taipei"),
    ]).tz_convert("UTC")
    norm,_=normalize_nq(sample(times),SPEC)
    out=anchor_prices(norm,SPEC,tolerance_minutes=2)
    day=out.set_index("candidate_taipei_weekday").loc["2026-10-13"]
    assert day["day_close_1345_observed_utc"] == times[0].isoformat()
    assert day["day_close_1345_close"] == 100.0
