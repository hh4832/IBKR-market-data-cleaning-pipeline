"""SOXX backfill audit: source is immutable; every run is independently archived."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
from zoneinfo import ZoneInfo

import pandas as pd
from tqdm.auto import tqdm

from .checkpoint import source_key, saved_normalization, save_normalization
from .session_audit import exchange_schedule, extended_hours_daily, trading_day_gaps
from .validate import (EXPECTED_SYMBOL, EXPECTED_CONID, QCError, normalize_soxx,
                       merge_exact, market_labels, daily_coverage, coverage_gaps,
                       corporate_action_candidates, sha256_file)


@dataclass
class Config:
    output_dir: Path
    source_dir: Path | None = None
    drive_source_id: str | None = None
    drive_output_id: str | None = None
    cache_dir: Path | None = None
    max_chunks: int = 5
    assume_naive_utc: bool = False
    git_sha: str = "UNKNOWN"
    audit_start_ny: str | None = None
    audit_end_ny: str | None = None


def git_sha() -> str:
    try:
        return subprocess.check_output(["git","rev-parse","HEAD"],stderr=subprocess.DEVNULL,text=True,timeout=3).strip()
    except Exception:
        return "UNKNOWN"


def _write_json(path:Path, obj:dict):
    path.write_text(json.dumps(obj,indent=2,ensure_ascii=False,default=str)+"\n",encoding="utf-8")


def _csv(path:Path, frame:pd.DataFrame):
    frame.to_csv(path,index=False,encoding="utf-8-sig")


def run(config: Config, *, service=None) -> Path:
    if bool(config.source_dir) == bool(config.drive_source_id):
        raise ValueError("Specify exactly one of source_dir or drive_source_id")
    if config.max_chunks < 0:
        raise ValueError("max_chunks must be >= 0; zero means full audit")
    if config.drive_source_id and service is None:
        raise ValueError("Drive source needs authorized OAuth service")
    if config.drive_output_id and service is None:
        raise ValueError("Drive output needs authorized OAuth service")
    if config.source_dir:
        if not config.source_dir.is_dir():
            raise FileNotFoundError(str(config.source_dir))
        sources=[{"path":str(p),"id":None,"name":p.name,"size":p.stat().st_size} for p in sorted(config.source_dir.glob("*.parquet"))]
    else:
        from .drive_io import list_parquet_files
        sources=list_parquet_files(service,config.drive_source_id)
    if not sources:
        raise ValueError("NO_PARQUET_FILES_FOUND_IN_SOXX_CHUNKS")
    from .drive_io import filename_collision_audit
    collisions=filename_collision_audit(sources)
    total_count=len(sources)
    selected=sources if config.max_chunks == 0 else sources[:config.max_chunks]
    base=config.output_dir
    base.mkdir(parents=True,exist_ok=True)
    suffix="FULL" if config.max_chunks == 0 else f"PILOT_{len(selected)}"
    name=datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y%m%d_%H%M%S") + "_"+config.git_sha[:10]+"_SOXX_"+suffix
    out=base/name
    if out.exists():
        raise FileExistsError(f"OUTPUT_RUN_ALREADY_EXISTS {out}")
    out.mkdir()
    stage=out / "staging"
    stage.mkdir()
    cache=config.cache_dir or (base / "download_cache")
    # A caller-supplied --cache-dir alone does NOT establish persistence.
    path_hint=str(cache)
    if path_hint.startswith("/content/drive/MyDrive/"):
        persistence_hint="COLAB_MYDRIVE_MOUNT_EXPECTED"
    elif path_hint.startswith("/content/"):
        persistence_hint="COLAB_EPHEMERAL"
    else:
        persistence_hint="CALLER_FILESYSTEM_UNVERIFIED"
    checkpoint_root=cache / "soxx_checkpoint_v011"
    validator_sha256=sha256_file(Path(__file__).with_name("validate.py"))
    resumed=0
    processed=0
    _write_json(out/"run_status.json",{"state":"RUNNING", "git_sha":config.git_sha,
                                         "checkpoint_root":str(checkpoint_root),
                                         "checkpoint_persistence_hint":persistence_hint})
    collision_columns=["source_name","file_count","state","drive_file_ids","sizes","md5_checksums"]
    _csv(out / "duplicate_source_filenames.csv",
         pd.DataFrame(collisions,columns=collision_columns))
    quality=[]
    manifests=[]
    years=set()
    ibkr_source_metadata={}
    for i, source in enumerate(tqdm(selected,desc="SOXX: inspect chunk",unit="chunk")):
        source_info={"source_name":source["name"],"source_id":source.get("id"),
                     "remote_size":source.get("size"),"remote_md5":source.get("md5Checksum")}
        key=source_key(source, assume_naive_utc=config.assume_naive_utc,
                       validator_sha256=validator_sha256)
        try:
            previous=saved_normalization(checkpoint_root, key)
            if previous is not None:
                normalized_path,record=previous
                row={**record["quality"],"state":"RESUMED_CHUNK"}
                manifest={**record["manifest"],"checkpoint_reused":True}
                normalized=pd.read_parquet(normalized_path)
                resumed+=1
            else:
                if source.get("id"):
                    from .drive_io import cached_download
                    path=cached_download(service,source,cache)
                else:
                    path=Path(source["path"])
                digest=sha256_file(path)
                chunk=pd.read_parquet(path)
                chunk_metadata={}
                for field in ("what_to_show","use_rth","bar_size","secType","exchange"):
                    if field in chunk.columns:
                        nonnull=chunk[field].dropna()
                        vals=nonnull.unique().tolist()
                        if len(vals)!=1:
                            raise QCError(f"INCONSISTENT_SOURCE_METADATA_{field}: {vals[:6]}")
                        value=vals[0]
                        chunk_metadata[field]=value.item() if hasattr(value,"item") else value
                if chunk_metadata.get("secType") not in (None,"STK"):
                    raise QCError("SECTYPE_MISMATCH_NOT_STK")
                if chunk_metadata.get("bar_size") not in (None,"1 min", "1min", "1 mins", "1"):
                    raise QCError(f"BAR_SIZE_UNEXPECTED: {chunk_metadata['bar_size']}")
                norm=normalize_soxx(chunk, assume_naive_utc=config.assume_naive_utc)
                normalized=norm.frame
                row={"source_name":source["name"],"source_id":source.get("id"),"state":"PASS_CHUNK",
                     "original_rows":norm.original_rows,"clean_rows":len(norm.frame),
                     "exact_duplicate_rows":norm.exact_duplicates,"timestamp_field":norm.time_field,
                     "first_utc":norm.frame["timestamp_utc"].min().isoformat(),
                     "last_utc":norm.frame["timestamp_utc"].max().isoformat(),"error":"",
                     "source_metadata":json.dumps(chunk_metadata,default=str)}
                manifest={**source_info,"sha256":digest,"source_metadata":chunk_metadata,
                          "state":"NORMALIZED", "checkpoint_reused":False}
                # Atomically commit a normalized chunk and its metadata; failures
                # are retried, not mistakenly marked checkpoint-complete.
                save_normalization(checkpoint_root, key, normalized, quality=row, manifest=manifest)
                processed+=1
            for k,v in manifest.get("source_metadata",{}).items():
                ibkr_source_metadata.setdefault(k,set()).add(str(v))
            labeled=market_labels(normalized)
            for year, group in labeled.groupby(labeled["timestamp_utc"].dt.tz_convert("America/New_York").dt.year):
                year=int(year)
                years.add(year)
                folder=stage / str(year)
                folder.mkdir(exist_ok=True)
                group.to_parquet(folder / f"{i:05d}.parquet", index=False)
            quality.append(row)
            manifests.append(manifest)
        except Exception as exc:
            quality.append({"source_name":source["name"],"source_id":source.get("id"),"state":"INVALID_CHUNK",
                            "original_rows":None,"clean_rows":None,"exact_duplicate_rows":None,
                            "timestamp_field":None,"first_utc":None,"last_utc":None,
                            "error":f"{type(exc).__name__}: {str(exc)[:300]}","source_metadata":""})
            manifests.append({**source_info,"sha256":None,"state":"FAILED"})
    _csv(out / "chunk_quality.csv",pd.DataFrame(quality))
    _write_json(out / "source_manifest.json",{"symbol":EXPECTED_SYMBOL,"conId":EXPECTED_CONID,
                                                 "filename_collisions":collisions,"files":manifests})
    errors=[v for v in quality if v["state"]=="INVALID_CHUNK"]
    for collision in collisions:
        if collision["state"] != "BYTE_IDENTICAL_BY_DRIVE_MD5":
            errors.append({"source_name":collision["source_name"],
                           "error":f"SAME_NAME_SOURCE_AMBIGUITY: {collision['state']}",
                           "source_ids":collision["drive_file_ids"]})
    duplicate_cross=0
    all_timestamp=[]
    action_flags=[]
    cleaned=out / "normalized_by_year"
    cleaned.mkdir()
    for year in tqdm(sorted(years),desc="SOXX: merge NY years",unit="year"):
        try:
            sub=[pd.read_parquet(p) for p in sorted((stage/str(year)).glob("*.parquet"))]
            combined,removed=merge_exact(sub)
            duplicate_cross+=removed
            combined.to_parquet(cleaned/f"SOXX_{year}.parquet",index=False)
            all_timestamp.append(combined[["timestamp_utc","open","high","low","close","volume"]].copy())
            events=corporate_action_candidates(combined)
            if not events.empty:
                action_flags.append(events)
        except Exception as exc:
            errors.append({"source_name":f"NY_YEAR_{year}","error":f"{type(exc).__name__}: {str(exc)[:300]}"})
    if not all_timestamp:
        _write_json(out/"run_info.json",{"status":"FAILED_NO_VALID_OUTPUT","errors":errors,"source_count":len(selected)})
        raise QCError(f"No normalizable SOXX data. QA outputs: {out}")
    times=pd.concat(all_timestamp,ignore_index=True).drop_duplicates("timestamp_utc").sort_values("timestamp_utc")
    labeled=market_labels(times)
    schedule=exchange_schedule(labeled,start_ny=config.audit_start_ny,end_ny=config.audit_end_ny)
    # Explicit schedule bounds catch entirely absent trading dates, not merely holes
    # within dates where other bars were returned. Default scope is observed span.
    daily=daily_coverage(labeled, schedule=schedule)
    gaps=coverage_gaps(labeled, schedule=schedule)
    days=trading_day_gaps(labeled,schedule)
    extended=extended_hours_daily(labeled,schedule)
    _csv(out/"trading_day_gaps.csv",days)
    _csv(out/"extended_hours_daily.csv",extended)
    _csv(out/"daily_coverage.csv",daily)
    _csv(out/"rth_missing_intervals.csv",gaps)
    action=pd.concat(action_flags,ignore_index=True) if action_flags else pd.DataFrame(columns=["timestamp_utc","abs_return","classification"])
    _csv(out/"large_move_candidates.csv",action)
    provisional_missing=int(daily["missing_rth_minutes"].sum())
    metadata_conflicts={k:sorted(v) for k,v in ibkr_source_metadata.items() if len(v)>1}
    if metadata_conflicts:
        errors.append({"source_name":"SOURCE_METADATA","error":f"INCONSISTENT_CROSS_CHUNK_METADATA: {metadata_conflicts}"})
    status=("REVIEW_REQUIRED" if errors or provisional_missing or len(action) else "COVERAGE_OK_LABEL_UNVERIFIED")
    # Even full coverage does not prove corporate-action adjustment or vendor bar-label semantics.
    info={
        "pipeline":"ibkr-soxx-qc","version":"0.1.1","symbol":"SOXX","conId":EXPECTED_CONID,
        "mode":"FULL" if config.max_chunks == 0 else "PILOT", "source_kind":"GOOGLE_DRIVE" if config.drive_source_id else "LOCAL",
        "source_folder_id":config.drive_source_id,"total_input_files_listed":total_count,"files_selected":len(selected),
        "duplicate_filename_groups":len(collisions),
        "unresolved_duplicate_filename_groups":sum(v["state"] != "BYTE_IDENTICAL_BY_DRIVE_MD5" for v in collisions),
        "file_failures":errors,"cross_chunk_duplicates_removed":duplicate_cross,
        "checkpoint_reused_chunks":resumed,"new_normalized_chunks":processed,
        "checkpoint_root":str(checkpoint_root), "checkpoint_persistence_hint":persistence_hint,
        "audit_window_ny":{"start":str(schedule.index.min().date()),"end":str(schedule.index.max().date()),
                           "explicit_bounds":bool(config.audit_start_ny or config.audit_end_ny)},
        "trading_days_with_no_rth_bars":len(days),
        "trading_days_with_no_bars_at_all":int((days["state"]=="NO_BARS_ON_TRADING_DAY").sum()) if not days.empty else 0,
        "extended_sessions_with_zero_observed_bars":int((extended["observed_minutes"]==0).sum()),
        "rth_missing_minutes":provisional_missing,"missing_intervals":len(gaps),"large_move_candidates":len(action),
        "start_utc":times["timestamp_utc"].min().isoformat(),"end_utc":times["timestamp_utc"].max().isoformat(),
        "git_sha":config.git_sha,"run_time_taipei":datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "bar_timestamp_semantics":"START_LABEL_PROVISIONAL_NOT_VENDOR_VERIFIED",
        "source_metadata_values":{k:sorted(v) for k,v in ibkr_source_metadata.items()},
        "source_metadata_consistent":all(len(v)==1 for v in ibkr_source_metadata.values()),
        "useRTH":"READ_SOURCE_METADATA_VALUES", "whatToShow":"READ_SOURCE_METADATA_VALUES",
        "corporate_action_adjustment":"RAW_UNADJUSTED_UNVERIFIED",
        "head_timestamp_soxx":"FALLBACK_FLOOR_UNVERIFIED",
        "research_ready":False,
        "status":status,
    }
    _write_json(out/"run_info.json",info)
    (out/"README_RESULT.txt").write_text(
        "SOXX v0.1.1 audit outputs are NOT research-ready. Verify IBKR bar-labeling, useRTH, whatToShow, corporate actions and all QA exclusions before statistical use.\n"
        "No forward-fill, price correction or source overwrite has been performed.\n",encoding="utf-8")
    # Never upload staging fragments; checkpoint remains outside immutable run archive.
    shutil.rmtree(stage)
    _write_json(out/"run_status.json",{"state":"COMPLETED_QA_NOT_RESEARCH_READY", "git_sha":config.git_sha,
                                        "checkpoint_reused_chunks":resumed,"new_normalized_chunks":processed})
    if config.drive_output_id:
        from .drive_io import upload_run
        info["drive_run_url"]=upload_run(service,out,config.drive_output_id)
        _write_json(out/"run_info.json",info)
    return out
