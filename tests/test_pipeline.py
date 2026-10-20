from pathlib import Path
import json
import pandas as pd
import pytest
from ibkr_qc.pipeline import Config, run


def test_requires_one_source(tmp_path):
    with pytest.raises(ValueError, match="exactly one"):
        run(Config(output_dir=tmp_path))


def test_no_parquet_input(tmp_path):
    src=tmp_path/"empty"
    src.mkdir()
    with pytest.raises(ValueError,match="NO_PARQUET"):
        run(Config(output_dir=tmp_path/"out",source_dir=src))


def test_pipeline_file_immutable_and_review_state(monkeypatch,tmp_path):
    src=tmp_path/"raw"
    src.mkdir()
    raw=src/"end_20261020T000000Z.parquet"
    raw.write_bytes(b"FAKE")
    original=raw.read_bytes()
    stored={}
    def mockread(p,*args,**kwargs):
        p=str(p)
        if p.endswith("end_20261020T000000Z.parquet"):
            return pd.DataFrame({"timestamp_utc":pd.date_range("2026-10-19T13:30Z",periods=2,freq="min"),
                                "open":[10.,10.],"high":[11.,11.],"low":[9.,9.],"close":[10.,10.],"volume":[10,10],
                                "symbol":["SOXX"]*2,"conId":[12658194]*2, "what_to_show":["TRADES"]*2,
                                "use_rth":[False]*2, "bar_size":["1 min"]*2, "secType":["STK"]*2})
        return stored[p].copy()
    def mockwrite(self,path,*args,**kwargs):
        path=str(path)
        stored[path]=self.copy()
        Path(path).write_bytes(b"FAKE_OUTPUT")
    # Use a stable single-session calendar fixture, no runtime calendar package required.
    class MockCal:
        def schedule(self,start_date,end_date):
            return pd.DataFrame({"market_open":[pd.Timestamp("2026-10-19T13:30:00Z")],
                "market_close":[pd.Timestamp("2026-10-19T20:00:00Z")]},
                index=pd.DatetimeIndex([pd.Timestamp("2026-10-19")]))
    import sys,types
    monkeypatch.setitem(sys.modules,"pandas_market_calendars",types.SimpleNamespace(get_calendar=lambda name:MockCal()))
    monkeypatch.setattr(pd,"read_parquet",mockread)
    monkeypatch.setattr(pd.DataFrame,"to_parquet",mockwrite)
    output=run(Config(output_dir=tmp_path/"out",source_dir=src,max_chunks=1,git_sha="TEST"))
    assert raw.read_bytes()==original
    info=json.loads((output/"run_info.json").read_text())
    assert info["research_ready"] is False
    assert info["status"] == "REVIEW_REQUIRED"
    assert info["rth_missing_minutes"] == 388
    assert info["source_metadata_values"]["what_to_show"] == ["TRADES"]
    assert info["source_metadata_values"]["use_rth"] == ["False"]
    assert not (output/"staging").exists()
    assert (output/"normalized_by_year/SOXX_2026.parquet").exists()
    assert (output/"source_manifest.json").exists()
