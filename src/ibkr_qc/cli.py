"""CLI for SOXX historical minute QA."""
from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import Config, git_sha, run


def parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description="SOXX 1-minute QA, does not edit source")
    source=p.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-dir",type=Path,help="Local folder containing SOXX *.parquet chunks")
    source.add_argument("--drive-source-id",help="Existing SOXX/.../chunks Drive folder ID")
    p.add_argument("--output-dir",type=Path,default=Path("./outputs"))
    p.add_argument("--cache-dir",type=Path,default=None)
    p.add_argument("--drive-output-id",help="Target Drive parent folder ID for timestamped run archive")
    p.add_argument("--max-chunks",type=int,default=5,help="Pilot size (default 5). Zero = full source scan")
    p.add_argument("--assume-naive-utc",action="store_true",help="Explicitly allow timezone-naive input as UTC after verifying provider semantics")
    p.add_argument("--git-sha",default=None)
    return p


def main(argv=None):
    opts=parser().parse_args(argv)
    service=None
    if opts.drive_source_id or opts.drive_output_id:
        from .drive_io import authorized_drive
        service=authorized_drive()
    cfg=Config(source_dir=opts.source_dir,drive_source_id=opts.drive_source_id,
               output_dir=opts.output_dir,cache_dir=opts.cache_dir,
               drive_output_id=opts.drive_output_id,max_chunks=opts.max_chunks,
               assume_naive_utc=opts.assume_naive_utc,git_sha=opts.git_sha or git_sha())
    out=run(cfg,service=service)
    print("Audit results:",out)
    print((out/"run_info.json").read_text(encoding="utf-8"))

if __name__ == "__main__":
    main()
