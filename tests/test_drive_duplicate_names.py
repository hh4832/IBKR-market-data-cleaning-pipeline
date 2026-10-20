"""Regression tests for distinct Google Drive files with the same filename."""
from __future__ import annotations
import json
from pathlib import Path
import pytest
from ibkr_qc.drive_io import filename_collision_audit, list_parquet_files

def item(file_id, name, md5="abc", size="100"):
    return {"id":file_id, "name":name, "md5Checksum":md5, "size":size,
            "mimeType":"application/octet-stream"}

class Service:
    def __init__(self, pages):
        self.pages=pages
        self.calls=[]
    def files(self):
        return self
    def list(self, **kwargs):
        self.calls.append(kwargs)
        return self
    def execute(self):
        return self.pages[0] if "pageToken" not in self.calls[-1] else self.pages[1]

def test_same_name_distinct_ids_across_pages_retained():
    svc=Service([{"nextPageToken":"next","files":[item("2","a.parquet"),item("x","skip.txt")]},
                 {"files":[item("1","a.parquet"),item("3","b.parquet")]}])
    result=list_parquet_files(svc,"folder")
    assert [(f["name"],f["id"]) for f in result] == [
        ("a.parquet","1"),("a.parquet","2"),("b.parquet","3")]
    assert len(svc.calls)==2

def test_same_id_repeated_across_pages_rejected():
    svc=Service([{"nextPageToken":"next","files":[item("1","a.parquet")]},
                 {"files":[item("1","a.parquet")]}])
    with pytest.raises(ValueError,match="DRIVE_PAGINATION_REPEATED_FILE_ID"):
        list_parquet_files(svc,"folder")

def test_collisions_are_classified_without_deletion():
    src=[item("1","a.parquet","abc","100"),item("2","a.parquet","abc","100"),
         item("3","b.parquet","abc","100"),item("4","b.parquet","def","100"),
         item("5","c.parquet",None,"100"),item("6","c.parquet",None,"100"),
         item("7","d.parquet","abc","100"),item("8","d.parquet","abc","101")]
    d={v["source_name"]:v for v in filename_collision_audit(src)}
    assert d["a.parquet"]["state"]=="BYTE_IDENTICAL_BY_DRIVE_MD5"
    assert d["b.parquet"]["state"]=="DIFFERENT_METADATA_REVIEW_REQUIRED"
    assert d["c.parquet"]["state"]=="UNVERIFIED_CONTENT_REVIEW_REQUIRED"
    assert d["d.parquet"]["state"]=="DIFFERENT_METADATA_REVIEW_REQUIRED"
    assert d["a.parquet"]["drive_file_ids"]=="1|2"
    assert len(src)==8

def test_notebook_exposes_same_name_diagnostic():
    nb=json.loads((Path(__file__).resolve().parents[1]/
        "notebooks/run_soxx_qc.ipynb").read_text())
    content="".join(nb["cells"][3]["source"])
    assert "filename_collision_audit" in content
    assert "unresolved" in content
