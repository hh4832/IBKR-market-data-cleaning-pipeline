"""Synthetic v0.1.1 checkpoints and NYSE PRE/POST diagnostics."""
import json
from pathlib import Path
import sys
import types

import pandas as pd
import pytest

from ibkr_qc.checkpoint import source_key, saved_normalization, save_normalization
from ibkr_qc.session_audit import exchange_schedule, trading_day_gaps, extended_hours_daily
from ibkr_qc.validate import market_labels, daily_coverage, coverage_gaps


def test_checkpoint_uses_file_identity_metadata_and_algorithm(tmp_path):
    one = {"name": "same.parquet", "id": "A", "size": "100", "md5Checksum": "aa", "modifiedTime": "2026-01-01"}
    other = {**one, "id": "B"}
    key=source_key(one, assume_naive_utc=False, validator_sha256="abc")
    assert key != source_key(other, assume_naive_utc=False, validator_sha256="abc")
    assert key != source_key(one, assume_naive_utc=True, validator_sha256="abc")
    assert key != source_key(one, assume_naive_utc=False, validator_sha256="new_version")
    assert key != source_key({**one,"md5Checksum":"bb"}, assume_naive_utc=False, validator_sha256="abc")


def test_checkpoint_completed_entry_verified_then_corruption_rejected(monkeypatch, tmp_path):
    def fake_to_parquet(self, dest, index=False):
        Path(dest).write_bytes(b"NORMALIZED_SAMPLE")
    monkeypatch.setattr(pd.DataFrame,"to_parquet",fake_to_parquet)
    root=tmp_path/"ckpt"
    assert saved_normalization(root,"KEY") is None
    path,_=save_normalization(root,"KEY",pd.DataFrame({"x":[1]}),
                              quality={"source_name":"x","source_metadata":"{}"},
                              manifest={"sha256":"rawsha","source_metadata":{}})
    assert saved_normalization(root,"KEY") is not None
    path.write_bytes(b"TRUNCATED")
    assert saved_normalization(root,"KEY") is None
    assert saved_normalization(root,"OTHER") is None


def fake_calendar(monkeypatch):
    # Three NYSE dates, no bars on 10/20. Last session has 13:00 early close.
    sessions = pd.DataFrame({
        "market_open": pd.to_datetime(["2026-10-19T13:30Z","2026-10-20T13:30Z","2026-10-21T13:30Z"]),
        "market_close": pd.to_datetime(["2026-10-19T20:00Z","2026-10-20T20:00Z","2026-10-21T17:00Z"]),
    },index=pd.DatetimeIndex(["2026-10-19","2026-10-20","2026-10-21"]))
    class MockCal:
        def schedule(self, start_date, end_date):
            return sessions.loc[str(start_date):str(end_date)]
    monkeypatch.setitem(sys.modules,"pandas_market_calendars",types.SimpleNamespace(get_calendar=lambda name:MockCal()))
    return sessions


def sample_bars():
    # 2026-10-19 has only 04:00 PRE, 09:30 RTH and 16:00 POST observed.
    # 2026-10-21 early-close day has a flat, zero-volume PRE bar.
    times=pd.to_datetime(["2026-10-19T08:00Z","2026-10-19T13:30Z","2026-10-19T20:00Z",
                          "2026-10-21T08:00Z","2026-10-21T13:30Z","2026-10-21T17:00Z"])
    return market_labels(pd.DataFrame({"timestamp_utc":times,"open":[10]*6,"high":[10]*6,
                                       "low":[10]*6,"close":[10]*6,"volume":[0,5,5,0,5,5]}))


def test_full_session_missing_detected_even_with_other_dates(monkeypatch):
    fake_calendar(monkeypatch)
    bars=sample_bars()
    schedule=exchange_schedule(bars)
    missing=trading_day_gaps(bars,schedule)
    assert "2026-10-20" in missing.date_ny.to_list()
    assert missing.loc[missing.date_ny.eq("2026-10-20"),"state"].iloc[0]=="NO_BARS_ON_TRADING_DAY"
    report=daily_coverage(bars,schedule=schedule)
    x=report.set_index("date_ny").loc["2026-10-20"]
    assert x["expected_rth_minutes"]==390 and x["missing_rth_minutes"]==390
    missing_intervals=coverage_gaps(bars,schedule=schedule)
    assert int(missing_intervals.loc[missing_intervals.date_ny.eq("2026-10-20"),"missing_minutes"].sum())==390


def test_extended_observed_only_and_early_close(monkeypatch):
    fake_calendar(monkeypatch)
    ext=extended_hours_daily(sample_bars(),exchange_schedule(sample_bars()))
    first=ext.set_index(["date_ny","session"]).loc[("2026-10-19","PRE")]
    assert first["diagnostic_grid_minutes"]==330
    assert first["observed_minutes"]==1
    assert first["observed_zero_volume_minutes"]==1
    assert first["flat_ohlc_minutes"]==1
    second=ext.set_index(["date_ny","session"]).loc[("2026-10-20","PRE")]
    assert second["observed_minutes"]==0
    assert second["interpretation"]=="OBSERVATIONS_ONLY_NO_ABSENT_BAR_CAUSE_INFERRED"
    early=ext.set_index(["date_ny","session"]).loc[("2026-10-21","POST")]
    assert early["diagnostic_grid_minutes"]==420  # 13:00 to 20:00 NY
    assert early["observed_minutes"]==1


def test_explicit_calendar_boundaries_classified_as_unverified(monkeypatch):
    fake_calendar(monkeypatch)
    bars=sample_bars()
    bars=bars[bars.date_ny=="2026-10-21"]
    schedule=exchange_schedule(bars,start_ny="2026-10-19",end_ny="2026-10-21")
    flags=trading_day_gaps(bars,schedule).set_index("date_ny")
    assert flags.loc["2026-10-19","state"]=="OUTSIDE_OBSERVED_SPAN_REVIEW"
    assert flags.loc["2026-10-20","state"]=="OUTSIDE_OBSERVED_SPAN_REVIEW"


def test_pipeline_resumes_without_reprocessing_normalized_source(monkeypatch,tmp_path):
    from ibkr_qc.pipeline import Config, run
    src = tmp_path / "raw"
    src.mkdir()
    raw = src / "end_20261020T000000Z.parquet"
    raw.write_bytes(b"SOURCE_DATA")
    fake_files = {}
    read_raw = []
    source_frame = pd.DataFrame({"timestamp_utc":pd.date_range("2026-10-19T13:30:00Z", periods=3, freq="min"),
         "open":[10.] * 3,"high":[11.] * 3,"low":[9.] * 3,"close":[10.] * 3,
         "volume":[5] * 3,"symbol":["SOXX"] * 3,"conId":[12658194] * 3,
         "what_to_show":["TRADES"]*3,"use_rth":[False]*3,
         "bar_size":["1 min"]*3,"secType":["STK"]*3})

    def mock_write(self,p,**kw):
        fake_files[str(p)] = self.copy()
        Path(p).write_bytes(b"PARQUET_"+str(len(fake_files)).encode())

    def mock_read(p,**kw):
        if Path(p)==raw:
            read_raw.append(True)
            return source_frame.copy()
        key=str(p)
        # The test double stores its in-memory table at the .part path;
        # atomic os.replace moves the real output file to its final path.
        if key not in fake_files and Path(key+".part").exists() is False:
            key=key+".part"
        return fake_files[key].copy()
    monkeypatch.setattr(pd.DataFrame,"to_parquet",mock_write)
    monkeypatch.setattr(pd,"read_parquet",mock_read)
    fake_calendar(monkeypatch)
    shared_cache = tmp_path / "persistent"
    run(Config(output_dir=tmp_path/"first",source_dir=src,cache_dir=shared_cache,max_chunks=1))
    out = run(Config(output_dir=tmp_path/"second",source_dir=src,cache_dir=shared_cache,max_chunks=1))
    details = json.loads((out/"run_info.json").read_text())
    assert len(read_raw)==1
    assert details["checkpoint_reused_chunks"]==1
    assert details["new_normalized_chunks"]==0
    assert details["research_ready"] is False
    assert (out/"extended_hours_daily.csv").exists()
    assert (out/"trading_day_gaps.csv").exists()
    assert (out/"run_status.json").exists()
