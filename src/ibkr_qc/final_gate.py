"""One-shot IBKR data FINAL GATE for NQ, ES, TSM and SOXX.

Raw Drive objects are read only.  Every run creates a new local directory and,
when requested, a new Drive archive.  The gate consolidates existing futures
recovery bars, audits the full equity chunk sets and emits a finite verdict.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from .drive_io import cached_download, upload_run
from .validate import (
    OHLCV, QCError, _timestamp, corporate_action_candidates,
    coverage_gaps, daily_coverage, market_labels,
)
from .session_audit import exchange_schedule, trading_day_gaps


FOLDER_MIME = "application/vnd.google-apps.folder"
DEFAULT_IDS = {
    "NQ": "1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w",
    "ES": "18Gla7newPUwuvXDze3kaXE_rxkDqFVhl",
    "NQ_RECOVERY": "10p2pdI47zfgg3BYkLhsQYkY4G_1yO7NM",
    "ES_RECOVERY": "1PuuKVR9k1f5I75vU9QMZ54MiMp4bfXwD",
    "TSM": "1g5Rmcg_79uO55KtoDfIA61__k-JnSe3c",
    "SOXX": "1lIq9fWpZHy9RR2xkTIOeEAcmPniwXlNK",
    "OUTPUT": "1CIlmaQv6BsAnMlNHxDTanJB4JttqYbs9",
}
EQUITY_CONIDS = {"TSM": 6223250, "SOXX": 12658194}


@dataclass(frozen=True)
class FinalGateConfig:
    folder_ids: dict[str, str] | None = None
    upload: bool = True
    max_equity_chunks: int = 0
    max_futures_contracts: int = 0

    @property
    def ids(self) -> dict[str, str]:
        return {**DEFAULT_IDS, **(self.folder_ids or {})}

    def validate(self) -> None:
        missing = sorted(k for k, value in self.ids.items() if not str(value).strip())
        if missing:
            raise ValueError(f"FINAL_GATE_FOLDER_IDS_EMPTY: {missing}")
        if self.max_equity_chunks < 0 or self.max_futures_contracts < 0:
            raise ValueError("FINAL_GATE_LIMIT_NEGATIVE")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _children(service, folder_id: str) -> list[dict]:
    fields = "nextPageToken,files(id,name,mimeType,size,md5Checksum,modifiedTime,parents)"
    result: list[dict] = []
    page = None
    while True:
        args = {
            "q": f"'{folder_id}' in parents and trashed = false",
            "fields": fields,
            "pageSize": 1000,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if page:
            args["pageToken"] = page
        response = service.files().list(**args).execute()
        result.extend(response.get("files", []))
        page = response.get("nextPageToken")
        if not page:
            break
    ids = [x["id"] for x in result]
    if len(ids) != len(set(ids)):
        raise QCError("DRIVE_PAGINATION_REPEATED_FILE_ID")
    return sorted(result, key=lambda x: (x.get("name", ""), x["id"]))


def _walk(service, root_id: str) -> list[dict]:
    files: list[dict] = []
    pending = [(root_id, "")]
    seen = set()
    while pending:
        folder, relative = pending.pop()
        if folder in seen:
            raise QCError("DRIVE_FOLDER_CYCLE_OR_DUPLICATE_ID")
        seen.add(folder)
        for item in _children(service, folder):
            item = {**item, "parent_folder_id": folder}
            if item.get("mimeType") == FOLDER_MIME:
                child_relative = f"{relative}/{item['name']}".strip("/")
                pending.append((item["id"], child_relative))
            else:
                item["relative_path"] = f"{relative}/{item['name']}".strip("/")
                files.append(item)
    return sorted(files, key=lambda x: (x.get("name", ""), x["id"]))


def _download_json(service, item: dict, cache: Path):
    path = cached_download(service, item, cache)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _time_column(frame: pd.DataFrame) -> str:
    for name in ("timestamp_utc", "datetime_utc", "datetime", "timestamp", "date", "time"):
        if name in frame.columns:
            return name
    if isinstance(frame.index, pd.DatetimeIndex):
        return "__index__"
    raise QCError("TIMESTAMP_COLUMN_UNKNOWN")


def _normalize(frame: pd.DataFrame, *, symbol: str, local_symbol: str | None = None,
               con_id: int | None = None) -> tuple[pd.DataFrame, dict]:
    if frame.empty:
        raise QCError("EMPTY_SOURCE_FRAME")
    work = frame.copy()
    work.columns = [str(c).strip() for c in work.columns]
    time_col = _time_column(work)
    if time_col == "__index__":
        work = work.reset_index().rename(columns={work.index.name or "index": "timestamp_utc"})
        time_col = "timestamp_utc"
    missing = sorted(set(OHLCV) - set(work.columns))
    if missing:
        raise QCError(f"MISSING_OHLCV_COLUMNS: {missing}")
    for column, expected in (("symbol", symbol), ("localSymbol", local_symbol)):
        if expected and column in work:
            values = work[column].dropna().astype(str).str.upper()
            if values.empty or not values.eq(str(expected).upper()).all():
                raise QCError(f"IDENTITY_MISMATCH_{column}: expected {expected}")
    if con_id is not None and "conId" in work:
        if not pd.to_numeric(work["conId"], errors="coerce").eq(con_id).all():
            raise QCError("IDENTITY_MISMATCH_conId")
    work["timestamp_utc"] = _timestamp(work[time_col], assume_naive_utc=False)
    ts = work["timestamp_utc"]
    if not (ts.dt.second.eq(0) & ts.dt.microsecond.eq(0) & ts.dt.nanosecond.eq(0)).all():
        raise QCError("NOT_MINUTE_ALIGNED")
    for column in OHLCV:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    values = work[list(OHLCV)].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise QCError("NULL_OR_NONFINITE_OHLCV")
    o, h, l, c, v = (work[x] for x in OHLCV)
    invalid = (o.le(0) | h.le(0) | l.le(0) | c.le(0) | v.lt(0)
               | h.lt(pd.concat([o, l, c], axis=1).max(axis=1))
               | l.gt(pd.concat([o, h, c], axis=1).min(axis=1)))
    if invalid.any():
        raise QCError(f"INVALID_OHLCV: {int(invalid.sum())}")
    clean = work[["timestamp_utc", *OHLCV]].copy()
    conflicts = clean.groupby("timestamp_utc")[list(OHLCV)].nunique(dropna=False).gt(1).any(axis=1)
    if conflicts.any():
        raise QCError(f"CONFLICTING_SAME_TIMESTAMP: {int(conflicts.sum())}")
    before = len(clean)
    clean = clean.drop_duplicates().sort_values("timestamp_utc").reset_index(drop=True)
    return clean, {"source_rows": before, "normalized_rows": len(clean),
                   "exact_duplicates_removed": before - len(clean)}


def _merge_exact(frames: Iterable[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    merged = pd.concat(list(frames), ignore_index=True)
    conflicts = merged.groupby("timestamp_utc")[list(OHLCV)].nunique(dropna=False).gt(1).any(axis=1)
    if conflicts.any():
        raise QCError(f"CROSS_SOURCE_CONFLICT: {int(conflicts.sum())}")
    before = len(merged)
    merged = merged.drop_duplicates().sort_values("timestamp_utc").reset_index(drop=True)
    return merged, before - len(merged)


def _select_recovery_attempts(attempts: dict[str, list[dict]]) -> pd.DataFrame:
    rows = []
    status_rank = {
        "OBSERVED_ALL_CANDIDATE_MINUTES": 3,
        "PARTIAL_UNRESOLVED": 2,
        "FAILED_UNRESOLVED": 1,
    }
    for window_id, history in attempts.items():
        # Recovery runs are append-only attempts. A newer run may improve an
        # earlier partial/failed result, so differing manifests are not raw-bar
        # conflicts. Prefer the least unresolved result, then the strongest
        # status, most returned rows and latest manifest. Actual OHLCV conflicts
        # still fail closed later in _merge_exact.
        best = max(
            history,
            key=lambda x: (
                -x["unresolved_minutes"],
                status_rank.get(str(x["recovery_status"]), 0),
                x["rows"],
                str(x.get("result_file_modified_time") or ""),
            ),
        ).copy()
        best["attempt_count"] = len(history)
        best["reconciliation"] = (
            "SINGLE_ATTEMPT" if len(history) == 1
            else "BEST_AVAILABLE_ATTEMPT_SELECTED_RAW_BARS_STILL_CONFLICT_CHECKED"
        )
        best["status_history"] = "|".join(sorted({
            str(x.get("recovery_status") or "") for x in history
        }))
        best["unresolved_history"] = "|".join(map(str, sorted({
            int(x.get("unresolved_minutes") or 0) for x in history
        })))
        best["saved_sha256_history"] = "|".join(sorted({
            str(x.get("saved_sha256") or "") for x in history
        }))
        rows.append(best)
    frame = pd.DataFrame(sorted(rows, key=lambda x: x["window_id"]))
    return frame


def _recovery_summary(service, root_id: str, cache: Path) -> tuple[pd.DataFrame, list[dict]]:
    result_files = [x for x in _walk(service, root_id) if x["name"] == "results.json"]
    attempts: dict[str, list[dict]] = {}
    for item in tqdm(result_files, desc="Recovery result manifests", unit="file"):
        for result in _download_json(service, item, cache):
            window = result.get("window") or {}
            window_id = str(window.get("id") or "")
            if not window_id:
                raise QCError("RECOVERY_WINDOW_ID_MISSING")
            compact = {
                "window_id": window_id,
                "contract": window.get("localSymbol"),
                "start_utc": window.get("start_utc"),
                "end_utc": window.get("end_utc"),
                "window_classification": window.get("status"),
                "recovery_status": result.get("status"),
                "unresolved_minutes": int(result.get("unresolved_minutes") or 0),
                "rows": int(result.get("rows") or 0),
                "saved_sha256": result.get("saved_sha256"),
                "result_file_id": item["id"],
                "result_file_modified_time": item.get("modifiedTime"),
            }
            attempts.setdefault(window_id, []).append(compact)
    frame = _select_recovery_attempts(attempts)
    return frame, result_files


def _futures_gate(service, symbol: str, source_id: str, recovery_id: str,
                  cache: Path, output: Path, limit: int) -> dict:
    contracts = [x for x in _children(service, source_id) if x.get("mimeType") == FOLDER_MIME]
    contracts = sorted(contracts, key=lambda x: x["name"])
    if limit:
        contracts = contracts[-limit:]
    recovery_files = _walk(service, recovery_id)
    recovery_parquets = [x for x in recovery_files if x["name"].lower().endswith(".parquet")]
    recovery, result_files = _recovery_summary(service, recovery_id, cache / "result_json")
    exclusions = recovery.loc[recovery["unresolved_minutes"].gt(0)].copy()
    exclusions.to_csv(output / f"{symbol.lower()}_unresolved_exclusions.csv", index=False)
    recovery.to_csv(output / f"{symbol.lower()}_recovery_windows.csv", index=False)
    contract_rows = []
    errors = []
    normalized_dir = output / "normalized_futures" / symbol
    normalized_dir.mkdir(parents=True, exist_ok=True)
    for contract in tqdm(contracts, desc=f"{symbol} contracts", unit="contract"):
        local_symbol = contract["name"].split("_", 1)[0]
        con_id = int(contract["name"].split("_", 1)[1])
        base = [x for x in _children(service, contract["id"]) if x["name"] == "consolidated.parquet"]
        if len(base) != 1:
            errors.append({"contract": local_symbol, "error": "BASE_CONSOLIDATED_COUNT", "count": len(base)})
            continue
        related = [x for x in recovery_parquets
                   if local_symbol.upper() in x.get("relative_path", x["name"]).upper()]
        try:
            frames = []
            sources = []
            for item in [base[0], *related]:
                path = cached_download(service, item, cache / symbol / local_symbol)
                data, stats = _normalize(pd.read_parquet(path), symbol=symbol,
                                         local_symbol=local_symbol, con_id=con_id)
                frames.append(data)
                sources.append({"drive_file_id": item["id"], "name": item["name"],
                                "sha256": _sha256(path), **stats})
            merged, duplicate_rows = _merge_exact(frames)
            target = normalized_dir / f"{local_symbol}.parquet"
            merged.to_parquet(target, index=False)
            contract_rows.append({"contract": local_symbol, "con_id": con_id,
                                  "base_rows": len(frames[0]), "recovery_files": len(related),
                                  "published_rows": len(merged), "exact_duplicate_rows": duplicate_rows,
                                  "start_utc": merged.timestamp_utc.min().isoformat(),
                                  "end_utc": merged.timestamp_utc.max().isoformat(),
                                  "published_sha256": _sha256(target), "sources": sources})
        except Exception as exc:
            errors.append({"contract": local_symbol, "error": type(exc).__name__,
                           "detail": str(exc)[:500]})
    expected_contracts = 9 if limit == 0 else limit
    unresolved = int(exclusions["unresolved_minutes"].sum()) if not exclusions.empty else 0
    verdict = "NO_GO" if errors or len(contract_rows) != expected_contracts else (
        "SCOPED_GO" if unresolved else "GO")
    return {"symbol": symbol, "verdict": verdict, "contracts_expected": expected_contracts,
            "contracts_published": len(contract_rows), "recovery_result_files": len(result_files),
            "recovery_parquet_files": len(recovery_parquets),
            "unresolved_minutes": unresolved, "excluded_windows": len(exclusions),
            "contracts": contract_rows, "errors": errors}


def _equity_gate(service, symbol: str, folder_id: str, cache: Path,
                 output: Path, limit: int) -> dict:
    items = [x for x in _children(service, folder_id) if x["name"].lower().endswith(".parquet")]
    selected = items[:limit] if limit else items
    frames = []
    sources = []
    errors = []
    for item in tqdm(selected, desc=f"{symbol} chunks", unit="chunk"):
        try:
            path = cached_download(service, item, cache / symbol)
            frame, stats = _normalize(pd.read_parquet(path), symbol=symbol,
                                      con_id=EQUITY_CONIDS[symbol])
            frames.append(frame)
            sources.append({"drive_file_id": item["id"], "name": item["name"],
                            "sha256": _sha256(path), **stats})
        except Exception as exc:
            errors.append({"drive_file_id": item["id"], "name": item["name"],
                           "error": type(exc).__name__, "detail": str(exc)[:500]})
    if not frames:
        return {"symbol": symbol, "verdict": "NO_GO", "errors": errors or [{"error": "NO_VALID_CHUNKS"}]}
    try:
        merged, duplicates = _merge_exact(frames)
        target = output / f"{symbol.lower()}_normalized.parquet"
        merged.to_parquet(target, index=False)
        schedule = exchange_schedule(merged)
        day_gaps = trading_day_gaps(merged, schedule)
        labelled = market_labels(merged)
        daily = daily_coverage(labelled, schedule=schedule)
        minute_gaps = coverage_gaps(labelled, schedule=schedule)
        actions = corporate_action_candidates(merged)
        daily.to_csv(output / f"{symbol.lower()}_daily_rth_coverage.csv", index=False)
        minute_gaps.to_csv(output / f"{symbol.lower()}_rth_missing_intervals.csv", index=False)
        day_gaps.to_csv(output / f"{symbol.lower()}_trading_day_gaps.csv", index=False)
        actions.to_csv(output / f"{symbol.lower()}_corporate_action_candidates.csv", index=False)
        full_scope = limit == 0 and len(selected) == len(items)
        verdict = "NO_GO" if errors else (
            "SCOPED_GO" if len(minute_gaps) or len(actions) else "GO")
        if not full_scope:
            verdict = "PILOT_ONLY"
        return {"symbol": symbol, "verdict": verdict, "chunks_listed": len(items),
                "chunks_scanned": len(selected), "rows": len(merged),
                "exact_duplicate_rows": duplicates, "start_utc": merged.timestamp_utc.min().isoformat(),
                "end_utc": merged.timestamp_utc.max().isoformat(),
                "rth_missing_minutes": int(daily["missing_rth_minutes"].sum()),
                "rth_missing_intervals": len(minute_gaps),
                "trading_day_gap_candidates": len(day_gaps),
                "corporate_action_candidates": len(actions),
                "published_sha256": _sha256(target), "sources": sources, "errors": errors}
    except Exception as exc:
        return {"symbol": symbol, "verdict": "NO_GO", "errors": [*errors, {
            "error": type(exc).__name__, "detail": str(exc)[:500]}]}


def run_final_gate(service, config: FinalGateConfig, *, cache_root: Path,
                   output_root: Path) -> Path:
    config.validate()
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    try:
        repository = Path(__file__).resolve().parents[2]
        sha = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "--short=12", "HEAD"],
                                      text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        sha = "UNKNOWN"
    mode = "FULL" if not config.max_equity_chunks and not config.max_futures_contracts else "PILOT"
    run_dir = output_root / f"{now:%Y%m%d_%H%M%S}_{sha}_IBKR_FINAL_GATE_{mode}"
    run_dir.mkdir(parents=True, exist_ok=False)
    cache_root.mkdir(parents=True, exist_ok=True)
    ids = config.ids
    results = []
    for symbol in ("NQ", "ES"):
        results.append(_futures_gate(service, symbol, ids[symbol], ids[f"{symbol}_RECOVERY"],
                                     cache_root, run_dir, config.max_futures_contracts))
    for symbol in ("TSM", "SOXX"):
        results.append(_equity_gate(service, symbol, ids[symbol], cache_root,
                                    run_dir, config.max_equity_chunks))
    verdicts = {x["symbol"]: x["verdict"] for x in results}
    overall = "NO_GO" if "NO_GO" in verdicts.values() else (
        "PILOT_ONLY" if "PILOT_ONLY" in verdicts.values() else
        "SCOPED_GO" if "SCOPED_GO" in verdicts.values() else "GO")
    manifest = {
        "gate": "IBKR_FINAL_GATE", "schema_version": "1.0.0", "mode": mode,
        "run_time_taipei": now.isoformat(), "git_sha": sha, "overall_verdict": overall,
        "instrument_verdicts": verdicts, "folder_ids": ids, "source_files_modified": False,
        "raw_data_redownloaded_from_ibkr": False, "historical_runs_overwritten": False,
        "rules": {"conflicting_timestamp": "NO_GO", "unresolved_recovery": "SCOPED_GO_EXCLUDE",
                  "equity_calendar_or_action_candidate": "SCOPED_GO_REVIEW_OR_EXCLUDE",
                  "pilot": "NEVER_PUBLISH"},
        "results": results,
    }
    manifest_path = run_dir / "final_gate_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
                             encoding="utf-8")
    summary = pd.DataFrame([{"instrument": x["symbol"], "verdict": x["verdict"],
                             "rows": x.get("rows") or sum(c.get("published_rows", 0) for c in x.get("contracts", [])),
                             "errors": len(x.get("errors", [])),
                             "unresolved_minutes": x.get("unresolved_minutes", 0),
                             "excluded_or_review_candidates": x.get("excluded_windows", 0)
                             + x.get("trading_day_gap_candidates", 0)
                             + x.get("corporate_action_candidates", 0)} for x in results])
    summary.to_csv(run_dir / "final_gate_summary.csv", index=False)
    (run_dir / "FINAL_VERDICT.txt").write_text(overall + "\n", encoding="utf-8")
    manifest["manifest_sha256_before_archive_url"] = _sha256(manifest_path)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
                             encoding="utf-8")
    if config.upload:
        url = upload_run(service, run_dir, ids["OUTPUT"])
        print("Archived IBKR FINAL GATE:", url)
    return run_dir
