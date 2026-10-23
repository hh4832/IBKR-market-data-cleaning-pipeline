"""Read-only Drive validation of existing IBKR NQ consolidated Parquet contracts.

Every actual source parquet stays in the original Google Drive folder.
Run outputs are new immutable run folders. This pipeline does NOT download
IBKR market data, synthesize bars, select a front contract, or join TXF data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

import pandas as pd
from tqdm.auto import tqdm

from .drive_io import cached_download, list_parquet_files, upload_run
from .nq_validate import (
    NQContract, anchor_prices, audit_standard_globex, jump_candidates,
    normalize_nq, parse_contract_folder,
)
from .validate import QCError, sha256_file

NQ_SOURCE_ROOT_ID = "1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w"
NQ_OUTPUT_PARENT_ID = "1Kl7s_3Y02tGeEbLbzF-LhoyEMyCtG129"
EXPECTED_CONTRACT_COUNT = 9


@dataclass(frozen=True)
class NQRunConfig:
    source_folder_id: str = NQ_SOURCE_ROOT_ID
    output_parent_id: str = NQ_OUTPUT_PARENT_ID
    max_contracts: int = 0  # 0 means all; positive values are PILOT only
    tolerance_minutes: int = 2
    upload: bool = True

    def validate(self):
        if not self.source_folder_id or not self.output_parent_id:
            raise ValueError("NQ_SOURCE_OR_OUTPUT_FOLDER_ID_EMPTY")
        if self.source_folder_id == self.output_parent_id:
            raise ValueError("NQ_OUTPUT_CANNOT_OVERLAP_SOURCE")
        if self.max_contracts < 0 or not (0<=self.tolerance_minutes<=5):
            raise ValueError("NQ_INVALID_CONFIG")
        return self


def _list_contract_folders(service, root_id: str) -> list[tuple[NQContract,str]]:
    results=[]
    page=None
    while True:
        kwargs={"q":f"'{root_id}' in parents and trashed = false",
                "fields":"nextPageToken,files(id,name,mimeType)",
                "pageSize":1000,"supportsAllDrives":True,"includeItemsFromAllDrives":True}
        if page:
            kwargs["pageToken"]=page
        answer=service.files().list(**kwargs).execute()
        for entry in answer.get("files", []):
            if entry.get("mimeType")!="application/vnd.google-apps.folder":
                raise QCError("NQ_ROOT_CONTAINS_UNEXPECTED_NONFOLDER")
            spec=parse_contract_folder(entry["name"])
            results.append((spec,entry["id"]))
        page=answer.get("nextPageToken")
        if not page:
            break
    names=[s.directory for s,_ in results]
    ids=[folder for _,folder in results]
    if len(names)!=len(set(names)) or len(ids)!=len(set(ids)):
        raise QCError("NQ_DUPLICATE_CONTRACT_FOLDER")
    if not results:
        raise QCError("NQ_CONTRACT_FOLDERS_EMPTY")
    return sorted(results,key=lambda p:p[0].directory)


def source_contract_manifest(service, source_id: str) -> list[dict]:
    contracts=_list_contract_folders(service,source_id)
    selected=[]
    for spec,folder_id in contracts:
        candidates=list_parquet_files(service,folder_id)
        if len(candidates)!=1 or candidates[0]["name"]!="consolidated.parquet":
            raise QCError(
                f"NQ_CONTRACT_REQUIRES_ONE_CONSOLIDATED_PARQUET: {spec.directory}, "
                f"found={[x['name'] for x in candidates]}"
            )
        selected.append({"contract":spec,"folder_id":folder_id,"file":candidates[0]})
    return selected


def run_nq_qc(service, config: NQRunConfig, *, cache_root: Path, output_root: Path) -> Path:
    config.validate()
    source_manifest=source_contract_manifest(service,config.source_folder_id)
    selected=(source_manifest if config.max_contracts==0 else source_manifest[:config.max_contracts])
    now=datetime.now(ZoneInfo("Asia/Taipei"))
    try:
        sha=subprocess.check_output(
            ["git","rev-parse","--short=12","HEAD"],stderr=subprocess.DEVNULL,text=True
        ).strip()
    except (OSError,subprocess.CalledProcessError):
        sha="UNKNOWN"
    mode="FULL" if config.max_contracts==0 else "PILOT"
    run_dir=output_root/f"{now:%Y%m%d_%H%M%S}_{sha}_NQ_GLOBEX_{mode}"
    if run_dir.exists():
        raise FileExistsError(f"NQ immutable run already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    cache_root.mkdir(parents=True,exist_ok=True)
    (run_dir/"normalized_by_contract").mkdir()
    daily_frames=[]
    gaps_frames=[]
    anchors_frames=[]
    jumps_frames=[]
    successes=[]
    errors=[]

    for entry in tqdm(selected,desc="IBKR NQ Globex QA",unit="contract"):
        spec: NQContract=entry["contract"]
        file=entry["file"]
        try:
            local=cached_download(service,file,cache_root)
            raw=pd.read_parquet(local)
            normalized,stats=normalize_nq(raw,spec)
            normalized_path=run_dir/"normalized_by_contract"/(spec.local_symbol+".parquet")
            normalized.to_parquet(normalized_path,index=False)
            daily,gaps=audit_standard_globex(normalized,spec)
            anchors=anchor_prices(normalized,spec,tolerance_minutes=config.tolerance_minutes)
            jumps=jump_candidates(normalized,spec)
            daily_frames.append(daily)
            gaps_frames.append(gaps)
            anchors_frames.append(anchors)
            jumps_frames.append(jumps)
            successes.append({
                **stats,"source_folder_id":entry["folder_id"],
                "source_file_id":file["id"],"source_file_name":file["name"],
                "source_file_size":file.get("size"),"source_drive_md5":file.get("md5Checksum"),
                "source_sha256":sha256_file(local),
                "normalized_sha256":sha256_file(normalized_path),
                "daily_trade_dates_audited":int(len(daily)),
                "days_with_possible_gaps":int(daily.missing_standard_minutes.gt(0).sum()),
                "total_possible_missing_minutes":int(daily.missing_standard_minutes.sum()),
                "anchor_candidate_days":len(anchors),"large_move_flags":len(jumps),
            })
        except Exception as exc:
            errors.append({"contract":spec.local_symbol, "folder_id":entry["folder_id"],
                           "source_file_id":file.get("id"),
                           "error_type":type(exc).__name__, "error":str(exc)[:350]})
    def write_csv(name,parts,columns):
        data=pd.concat(parts,ignore_index=True) if parts else pd.DataFrame(columns=columns)
        data.to_csv(run_dir/name,index=False,encoding="utf-8-sig")
        return data

    daily=write_csv("daily_globex_coverage.csv",daily_frames,
                    ["contract","trade_date_ny","expected_standard_minutes",
                     "observed_standard_minutes","missing_standard_minutes","coverage_fraction","status"])
    gaps=write_csv("possible_missing_intervals.csv",gaps_frames,
                   ["contract","start_utc","end_utc","missing_minutes","classification"])
    anchors=write_csv("taipei_candidate_anchor_prices.csv",anchors_frames,
                      ["contract","candidate_taipei_weekday"])
    jumps=write_csv("large_one_minute_moves.csv",jumps_frames,
                    ["contract","timestamp_utc","abs_return","classification"])
    (run_dir/"source_manifest.json").write_text(
        json.dumps({"expected_contract_count":EXPECTED_CONTRACT_COUNT,
                    "selected_contract_count":len(selected),
                    "listed_contract_count":len(source_manifest),
                    "successful_contracts":successes,"errors":errors},
                   ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    missing=int(daily.missing_standard_minutes.sum()) if not daily.empty else 0
    coverage=int(daily.expected_standard_minutes.sum()) if not daily.empty else 0
    valid_anchor_count=int(anchors.night_open_to_next_0500_ok.sum()) if (
        not anchors.empty and "night_open_to_next_0500_ok" in anchors) else 0
    has_all_nine=(len(source_manifest)==EXPECTED_CONTRACT_COUNT and
                  len(successes)==EXPECTED_CONTRACT_COUNT)
    status=("FAILED_SOURCE_CONTRACTS" if errors else
            "REVIEW_REQUIRED_MISSING_STANDARD_SESSION_MINUTES" if missing>0 else
            "PROVISIONAL_STANDARD_GRID_NO_GAPS")
    if not has_all_nine:
        status="REVIEW_REQUIRED_CONTRACT_SCOPE" if not errors else status
    info={
        "pipeline":"ibkr-nq-globex-qc","version":"0.2.0","source":"IBKR_EXISTING_PARQUET",
        "mode":mode,"git_sha":sha,"run_time_taipei":now.isoformat(),
        "raw_source_nq_folder_id":config.source_folder_id,
        "output_parent_folder_id":config.output_parent_id,
        "total_contract_folders":len(source_manifest),
        "selected_contracts":len(selected),"successful_contracts":len(successes),
        "source_errors":errors,"status":status,
        "globex_expected_standard_minutes":coverage,
        "globex_observed_standard_minutes":coverage-missing,
        "possible_missing_standard_minutes":missing,
        "possible_gap_intervals":len(gaps),
        "taipei_candidate_anchor_rows":len(anchors),
        "candidate_night_1500_to_next0500_eligible_rows":valid_anchor_count,
        "large_move_candidates":len(jumps),
        "bar_label_convention":"START_LABEL_PROVISIONAL_NOT_INDEPENDENTLY_VERIFIED",
        "special_cme_holidays_early_closes":"NOT_VALIDATED_ALL_MISSING_ARE_POSSIBLE_GAPS",
        "taiwan_trading_calendar":"NOT_JOINED_TPE_WEEKDAY_CANDIDATES_ONLY",
        "roll_policy":"UNDEFINED_CANNOT_FORM_CONTINUOUS_CONTRACT_SERIES",
        "research_ready":False,
        "source_files_modified":False,
        "raw_data_redownloaded_from_ibkr":False,
        "outputs_are_new_immutable_folder":True,
    }
    (run_dir/"run_info.json").write_text(json.dumps(info,ensure_ascii=False,indent=2)+"\n",
                                           encoding="utf-8")
    (run_dir/"README_RESULT.txt").write_text(
        "NQ Globex provisional QA of EXISTING IBKR per-contract Parquet files.\n"
        "Missing standard Globex minutes are candidates: holiday, halts or early close unverified.\n"
        "No minute fill, no forward/nearest timestamp extrapolation outside 2 min, no cross-contract roll.\n"
        "Taipei weekday anchors are not TAIFEX verified session dates. No research-ready claim.\n"
        "Review session gaps, bar-label semantics and roll calendar before predicting TXF flows.\n",
        encoding="utf-8")
    if config.upload:
        url=upload_run(service,run_dir,config.output_parent_id)
        print("Archived NQ QA run:",url)
    return run_dir
