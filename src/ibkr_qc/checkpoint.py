"""Crash-safe, content-addressed normalization checkpoint (never raw replacement)."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

SCHEMA = "soxx-normalized-v0.1.1"


def source_key(source: dict, *, assume_naive_utc: bool, validator_sha256: str) -> str:
    """Invalidate on source metadata, local mtime, timestamp policy or algorithm changes."""
    path = source.get("path")
    local = Path(path) if path else None
    fingerprint = {
        "schema": SCHEMA, "name": source["name"], "id": source.get("id"),
        "size": str(source.get("size")), "remote_md5": source.get("md5Checksum"),
        "remote_modified": source.get("modifiedTime"),
        "local_mtime_ns": local.stat().st_mtime_ns if local else None,
        "assume_naive_utc": assume_naive_utc,
        "validator_sha256": validator_sha256,
    }
    return hashlib.sha256(json.dumps(fingerprint, sort_keys=True, default=str).encode()).hexdigest()


def _hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def saved_normalization(cache_root: Path, key: str) -> tuple[Path, dict] | None:
    """Reuse only an atomic completed entry whose normalized parquet hash still matches."""
    record = cache_root / "records" / f"{key}.json"
    parquet = cache_root / "normalized" / f"{key}.parquet"
    if not (record.is_file() and parquet.is_file()):
        return None
    try:
        data = json.loads(record.read_text(encoding="utf-8"))
        if (data.get("key") != key or data.get("schema") != SCHEMA
                or data.get("parquet_sha256") != _hash(parquet)):
            return None
        return parquet, data
    except (OSError, ValueError, TypeError):
        return None


def save_normalization(cache_root: Path, key: str, frame: Any,
                       *, quality: dict, manifest: dict) -> tuple[Path, dict]:
    normalized_dir = cache_root / "normalized"
    record_dir = cache_root / "records"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    record_dir.mkdir(parents=True, exist_ok=True)
    dest = normalized_dir / f"{key}.parquet"
    temp = normalized_dir / f"{key}.parquet.part"
    meta = record_dir / f"{key}.json"
    tmpmeta = record_dir / f"{key}.json.part"
    try:
        frame.to_parquet(temp, index=False)
        # The normalized bytes must be persisted before the 'completed' record.
        os.replace(temp, dest)
        entry = {"schema": SCHEMA, "key": key, "parquet_sha256": _hash(dest),
                 "quality": quality, "manifest": manifest}
        tmpmeta.write_text(json.dumps(entry, ensure_ascii=False, default=str, sort_keys=True), encoding="utf-8")
        os.replace(tmpmeta, meta)
    finally:
        temp.unlink(missing_ok=True)
        tmpmeta.unlink(missing_ok=True)
    return dest, entry
