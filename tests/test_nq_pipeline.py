"""NQ pipeline end-to-end tests with an offline original-style Parquet file."""
import json
import pandas as pd
import pytest

from ibkr_qc.nq_pipeline import (
    NQRunConfig, run_nq_qc, source_contract_manifest,
)
from ibkr_qc.nq_validate import parse_contract_folder
from ibkr_qc.validate import QCError


class FakeFiles:
    def __init__(self, folders):
        self.folders=folders
        self.request=None
    def list(self,**kwargs):
        assert "pageSize" in kwargs and "page_size" not in kwargs
        parent=kwargs["q"].split("'")[1]
        values=self.folders.get(parent,[])
        class Response:
            def execute(self_inner):
                return {"files":values}
        return Response()


class Service:
    def __init__(self,items):
        self._files=FakeFiles(items)
    def files(self):
        return self._files


def test_manifest_enumerates_contracts_without_hidden_duplicates():
    source="root"
    items={source:[{"name":"NQZ6_563947726","id":"folder","mimeType":"application/vnd.google-apps.folder"}],
           "folder":[{"name":"consolidated.parquet","id":"parquet-1","mimeType":"application/octet-stream","size":"100","md5Checksum":"a"*32}]}
    manifest=source_contract_manifest(Service(items),source)
    assert len(manifest)==1
    assert manifest[0]["contract"].local_symbol=="NQZ6"
    assert manifest[0]["file"]["id"]=="parquet-1"
    bad=dict(items)
    bad["folder"] += [{"name":"second.parquet","id":"parquet-2","mimeType":"application/octet-stream"}]
    with pytest.raises(QCError,match="ONE_CONSOLIDATED_PARQUET"):
        source_contract_manifest(Service(bad),source)


def test_end_to_end_pilot_never_updates_source_or_marks_research_ready(tmp_path,monkeypatch):
    pytest.importorskip("pyarrow")
    n=12
    f=pd.DataFrame({
        "timestamp_utc":pd.date_range("2026-10-14 01:00",periods=n,freq="min",tz="UTC"),
        "open":[21000.]*n,"high":[21002.]*n,"low":[20998.]*n,"close":[21001.]*n,
        "volume":[12.]*n,"symbol":["NQ"]*n,
        "localSymbol":["NQZ6"]*n,"conId":[563947726]*n,
        "expiry":["20261218"]*n,"exchange":["CME"]*n,
        "bar_size":["1 min"]*n,"what_to_show":["TRADES"]*n,
        "use_rth":[False]*n,"secType":["FUT"]*n,
    })
    raw=tmp_path/"source.parquet"
    f.to_parquet(raw,index=False)
    import ibkr_qc.nq_pipeline as pipe
    def manifest(_service,_root):
        return [{"contract":parse_contract_folder("NQZ6_563947726"),"folder_id":"source-folder",
                 "file":{"id":"source-id","name":"consolidated.parquet","size":str(raw.stat().st_size)}}]
    monkeypatch.setattr(pipe,"source_contract_manifest",manifest)
    monkeypatch.setattr(pipe,"cached_download",lambda *_args,**_kw:raw)
    out=run_nq_qc(None,NQRunConfig(max_contracts=1,upload=False),
                  cache_root=tmp_path/"cache",output_root=tmp_path/"out")
    info=json.loads((out/"run_info.json").read_text())
    assert info["mode"]=="PILOT"
    assert info["research_ready"] is False
    assert info["source_files_modified"] is False
    assert info["raw_data_redownloaded_from_ibkr"] is False
    assert info["selected_contracts"]==1
    assert info["successful_contracts"]==1
    assert (out/"normalized_by_contract"/"NQZ6.parquet").exists()
    assert (out/"daily_globex_coverage.csv").exists()
    assert (out/"taipei_candidate_anchor_prices.csv").exists()
    assert (out/"source_manifest.json").exists()
    assert pd.read_parquet(raw).equals(f)
