"""Drive folder-ID I/O through Colab OAuth; never uses a public-link scraper."""
from __future__ import annotations

from collections import defaultdict
import hashlib
import io
from pathlib import Path
import time
from typing import Iterator

from tqdm.auto import tqdm


def authorized_drive():
    try:
        from google.colab import auth
        import google.auth
        from googleapiclient.discovery import build
    except ImportError as ex:
        raise RuntimeError("Google Drive API requires Colab / project[colab] dependencies") from ex
    auth.authenticate_user()
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    # The output folder must be writable for this OAuth user.
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_parquet_files(service, folder_id: str) -> list[dict]:
    if not folder_id.strip():
        raise ValueError("SOURCE_FOLDER_ID_EMPTY")
    result=[]
    page=None
    while True:
        query={"q": f"'{folder_id}' in parents and trashed = false", "fields": "nextPageToken,files(id,name,mimeType,size,md5Checksum,modifiedTime)",
               "pageSize": 1000, "supportsAllDrives": True, "includeItemsFromAllDrives": True}
        if page:
            query["pageToken"]=page
        response=service.files().list(**query).execute()
        result.extend(f for f in response.get("files",[]) if f.get("name", "").lower().endswith(".parquet") and f.get("mimeType") != "application/vnd.google-apps.folder")
        page=response.get("nextPageToken")
        if not page:
            break
    # Google Drive permits distinct file IDs to share the same file name.
    # Keep every ID: downstream caching is ID-keyed and bar duplicates are
    # resolved only after exact OHLCV checks. Never arbitrarily choose a copy.
    ids = [f["id"] for f in result]
    if len(set(ids)) != len(ids):
        raise ValueError("DRIVE_PAGINATION_REPEATED_FILE_ID")
    return sorted(result, key=lambda f: (f["name"], f["id"]))


def filename_collision_audit(sources: list[dict]) -> list[dict]:
    """Record same-name Drive objects without discarding any of their IDs.

    Matching non-empty Drive MD5+size provides evidence of byte-identical
    uploads; any mismatch or unknown digest requires manual review.
    """
    groups: dict[str, list[dict]] = defaultdict(list)
    for source in sources:
        groups[source["name"]].append(source)

    rows = []
    for name, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        sizes = [str(m.get("size") or "") for m in members]
        md5s = [str(m.get("md5Checksum") or "").lower() for m in members]
        all_md5_known = all(md5s)
        if len(set(sizes)) > 1 or (all_md5_known and len(set(md5s)) > 1):
            state = "DIFFERENT_METADATA_REVIEW_REQUIRED"
        elif all_md5_known and len(set(md5s)) == 1 and len(set(sizes)) == 1 and sizes[0]:
            state = "BYTE_IDENTICAL_BY_DRIVE_MD5"
        else:
            state = "UNVERIFIED_CONTENT_REVIEW_REQUIRED"
        rows.append({
            "source_name": name,
            "file_count": len(members),
            "state": state,
            "drive_file_ids": "|".join(str(m.get("id") or "") for m in members),
            "sizes": "|".join(sizes),
            "md5_checksums": "|".join(md5s),
        })
    return rows


def cached_download(service, item: dict, cache_dir: Path, *, retries: int=3) -> Path:
    from googleapiclient.http import MediaIoBaseDownload
    cache_dir.mkdir(parents=True,exist_ok=True)
    # Key by immutable-ish Drive file ID; revalidate remote byte size and md5 before reuse.
    target=cache_dir / f"{item['id']}_{item['name']}"
    if target.exists():
        if target.stat().st_size == int(item.get("size", -1)):
            md5=item.get("md5Checksum")
            # Without a remote content hash, same-size data could have changed;
            # never trust a persistent cache based on size alone.
            if md5 and hashlib.md5(target.read_bytes()).hexdigest().lower()==md5.lower():
                return target
        target.unlink()
    for attempt in range(retries):
        temp=target.with_suffix(target.suffix+".part")
        try:
            request=service.files().get_media(fileId=item["id"],supportsAllDrives=True)
            with temp.open("wb") as fp:
                downloader=MediaIoBaseDownload(fp,request,chunksize=4*1024*1024)
                done=False
                while not done:
                    _,done=downloader.next_chunk(num_retries=2)
            if temp.stat().st_size != int(item["size"]):
                raise IOError("REMOTE_SIZE_MISMATCH")
            if item.get("md5Checksum") and hashlib.md5(temp.read_bytes()).hexdigest().lower()!=item["md5Checksum"].lower():
                raise IOError("REMOTE_MD5_MISMATCH")
            temp.replace(target)
            return target
        except Exception:
            temp.unlink(missing_ok=True)
            if attempt + 1 == retries:
                raise
            time.sleep(min(4 * (attempt + 1), 12))
    raise RuntimeError("UNREACHABLE")


def upload_run(service, local_run: Path, target_parent_id: str) -> str:
    """Always creates a new folder, never overwrites existing user data."""
    from googleapiclient.http import MediaFileUpload
    folder=service.files().create(body={"name":local_run.name,"mimeType":"application/vnd.google-apps.folder", "parents":[target_parent_id]},
                                  fields="id,webViewLink",supportsAllDrives=True).execute()
    run_id=folder["id"]
    run_url="https://drive.google.com/drive/folders/"+run_id
    # Embed the actual created Drive location before archiving run_info.json.
    # Otherwise Drive's archived run_info differs from the local copy.
    run_info_path=local_run / "run_info.json"
    if run_info_path.is_file():
        import json
        details=json.loads(run_info_path.read_text(encoding="utf-8"))
        details["drive_run_url"]=run_url
        run_info_path.write_text(json.dumps(details,indent=2,ensure_ascii=False,default=str)+"\n",encoding="utf-8")
    for path in tqdm(sorted(local_run.rglob("*")),desc="Upload run results to Drive"):
        if not path.is_file():
            continue
        # Mirror subfolders lazily with isolated output-only IDs.
        parent=run_id
        parts=path.relative_to(local_run).parts[:-1]
        for component in parts:
            # O(total dirs) is tiny in this output format.
            folder_key=(parent,component)
            # Resolve existing folder by listing it, avoiding stale parallel state.
            q=f"'{parent}' in parents and name = '{component}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            found=service.files().list(q=q,fields="files(id)",pageSize=10).execute().get("files",[])
            if found:
                parent=found[0]["id"]
            else:
                parent=service.files().create(body={"name":component,"mimeType":"application/vnd.google-apps.folder","parents":[parent]},
                                              fields="id",supportsAllDrives=True).execute()["id"]
        media=MediaFileUpload(str(path),mimetype="application/octet-stream",resumable=True)
        service.files().create(body={"name":path.name,"parents":[parent]},media_body=media,fields="id,size",supportsAllDrives=True).execute()
    return "https://drive.google.com/drive/folders/"+run_id
