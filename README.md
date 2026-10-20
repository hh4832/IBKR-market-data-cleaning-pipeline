# IBKR-market-data-cleaning-pipeline

**v0.1.0 | SOXX first | independent quality-control only, not a price repair tool.**

[Original data folder](https://drive.google.com/drive/folders/1jLOYnUhWx6coNV6eqYnjUAeeHZPP6aMh) → `IKBR` (spelling in Drive) → `raw/SOXX/SOXX_12658194/chunks`.

- **SOXX** = iShares Semiconductor ETF, IBKR `secType=STK`, `conId=12658194`. **Not SOX** (PHLX semiconductor index).
- Existing Downloader reports 2,699 SOXX Parquet chunks; `full_audit_required=True` and `fallback_floor_UNVERIFIED` for earliest head timestamp. Those are source reports, **not** independently validated observations.
- v0.1.0 does **not** download from IBKR, modify existing Downloader/TSM/NQ/ES/TXF, or apply split adjustments. It only audits already-downloaded raw SOXX Parquet chunks. TSM, NQ and ES are explicitly **out of scope** for this version; do not silently apply equity NYSE calendar logic to Globex futures.

## Use in Colab (recommended)

Open [`notebooks/run_soxx_qc.ipynb`](notebooks/run_soxx_qc.ipynb) on Google Colab. Runtime **must** install a compatible Python and `pyarrow`. Authenticate with Google OAuth, review the **hardcoded public folder IDs** shown below, and run the pilot first. The Notebook uses Google's authenticated Drive API directly, so it does not depend on the `IKBR` folder being mounted in `My Drive` or knowing its full filesystem path. OAuth permission can see and create files in Drive; the pipeline only creates a **new timestamped child folder** in the designated output parent, never deletes or modifies sources.

```python
SOURCE_FOLDER_ID = "1lIq9fWpZHy9RR2xkTIOeEAcmPniwXlNK"
OUTPUT_PARENT_ID = "1k4kZ6kVjkfn99FW7AgRC6e9MDk848oVt"
MAX_CHUNKS = 5  # pilot: 5 earliest chunks. For full scan explicitly set to 0.
```

The Notebook mounts Drive **optionally** for persistent local caching, but source and output are accessed by folder IDs through Drive API. Users with no access to the source folder will receive an authorization error, not a silently empty dataset.

## Local usage

```bash
python -m pip install -e '.[test]'
ibkr-soxx-qc --source-dir '/path/to/SOXX_12658194/chunks' --output-dir ./outputs --max-chunks 5
# After reviewing pilot and source timestamp semantics:
ibkr-soxx-qc --source-dir '/path/to/SOXX_12658194/chunks' --output-dir ./outputs --max-chunks 0
pytest -q
```

- If input timestamps have **no timezone**, abort by default. Only use `--assume-naive-utc` after verifying they represent UTC. The pipeline never guesses a numeric timestamp unit.
- Input must have `open/high/low/close/volume`, plus UTC-aware `timestamp_utc` or recognized timestamp alias / DatetimeIndex. Unknown source schema fails closed and is documented per chunk.
- `--max-chunks=5` is only a pilot. Full historical coverage cannot be asserted from a pilot.

## Validation semantics

1. Strict contract provenance: named source folder `SOXX_12658194`, optional per-row symbol and conId validation; keep every input Drive ID and SHA256 in `source_manifest.json`.
2. Reject timezone-ambiguous, malformed or off-minute timestamps; reject null/non-numeric/invalid OHLCV. Zero volume is **not** assumed to indicate a missing candle.
3. Drive can contain different file IDs using the same filename. Retain every ID in stable (filename, file ID) order, cache/download by ID and record all collisions in `duplicate_source_filenames.csv`. Matching MD5 and size indicates byte-identical files; missing or mismatched metadata remains `REVIEW_REQUIRED`. After reading bars, consolidate **only exact timestamp + OHLCV duplicates**; same timestamp with conflicting data is never silently resolved.
4. Convert UTC to `America/New_York` (DST handled by IANA timezone). Compute expected regular trading minutes from the **NYSE historical session calendar**, including known early closes. Expected minutes are conventionally **bar-start labelled**; that label convention is **not independently verified** for current IBKR source.
5. Save `daily_coverage.csv`, `rth_missing_intervals.csv`, `large_move_candidates.csv`, `chunk_quality.csv`, `duplicate_source_filenames.csv`, yearly Parquet, `source_manifest.json`, `run_info.json` and run notices. No forward-fill, retroactive price correction, resampling, synthetic bars or inferred split factors.
6. Flag large price changes for **review**. Do not automatically attribute them to splits; historical adjusted/unadjusted sources have not been reconciled.
7. **`research_ready=false` in all v0.1.0 runs**, including those with 100% observed RTH coverage: source metadata may identify `use_rth`/`what_to_show` (explicitly audited from source), but corporate-action adjustment policy and IBKR bar-label convention remain unverified. A complete downloader checkpoint or successful API request is not proof of completeness.

### Quality gates and limitations

- `COVERAGE_OK_LABEL_UNVERIFIED`: 100% *provisionally expected* RTH timestamps within scanned interval and no other detected issues, **not** certified to study returns.
- `PARTIAL_COVERAGE`: missing RTH bars, without claiming whether IBKR omitted trades / data or the exchange was closed.
- `REVIEW_REQUIRED`: one or more invalid chunks, calendar gaps, cross-chunk conflicts, price-jump candidates or unverified fundamental semantics.
- A pilot may include only the earliest files; it cannot certify full period or missing dates **outside the observed date bounds**. If a whole calendar year lacks input files, further manifest-level coverage audit is required.
- Extended-hours observed bars are labelled `PRE`/`POST` but extended-hours completeness is not asserted (sparse trading is legitimate). Do **not** apply a 390-bar requirement to pre/post-market.

## Architecture

```text
src/ibkr_qc/validate.py      Pure OHLCV/time checks, exact dedup, NYSE calendar QA
src/ibkr_qc/drive_io.py      OAuth Drive list/download/archive, paginated >1000 files
src/ibkr_qc/pipeline.py      Stream chunks → NY year partitions → QC reports
src/ibkr_qc/cli.py           Bounded pilot or explicit full source scan
notebooks/run_soxx_qc.ipynb  Colab thin runner + tqdm progress + output readback
.github/workflows/tests.yml GitHub Actions unit tests, no access to private data
```

Outputs are stored in a fresh folder `YYYYMMDD_HHMMSS_<GitSHA>_SOXX_<PILOT|FULL>` (Asia/Taipei). **No old data or research runs are overwritten**. Downloaded local chunk cache is reused only after verifying size and optionally remote MD5. Remote output is uploaded to a new child folder under the requested Drive parent ID. GitHub contains source, tests and docs only.

## Security and deployment

- Local data, Drive OAuth tokens, IBKR credentials, API keys and raw Parquet are **never committed**. `Colab` uses the user's OAuth permissions and does not need IBKR login for this QC job.
- The GitHub Actions workflow runs synthetic tests only; it does not download personal market data or touch Drive.
- All failures remain visible; `run_info.json` and `chunk_quality.csv` are forensic reports, not a certification.
- If schema mismatch prevents processing the real sample, inspect the source schema and issue a narrowly scoped compatibility fix with a unit test; do not auto-convert ambiguous timestamps.

## Future milestones (not implemented)

- v0.2: SOXX source metadata reconciliation (`whatToShow`, `useRTH`, IBKR bar-label semantics, corporate action raw/adjusted mapping, NYSE event-window QA).
- v0.3: Separate TSM stock adapter, regression fixtures and checks.
- v0.4: CME Globex ES/NQ futures adapter (quarterly contracts, expiry policies, 1-minute session completeness, roll provenance), distinct from equity calendar logic.

See [Development Prompt](docs/DEVELOPMENT_PROMPT_v0.1.0.md) and [AGENTS.md](AGENTS.md).

## v0.1.1 — Google Drive same-name files hotfix

Google Drive permits distinct file IDs with the same `end_YYYYMMDD...parquet` filename. Formerly the listing halted with `AMBIGUOUS_DUPLICATE_DRIVE_FILENAMES`. Now the paginator retains **all distinct IDs**, sorted by (name, ID), and records the complete collision list with file ID, size and Drive MD5. Source files are never renamed, deleted or overwritten. Verified matching MD5+size is labelled byte-identical by Drive metadata; conflicting or unknown metadata is marked `REVIEW_REQUIRED`. The pipeline still processes each distinct ID and merges only exact matching OHLCV records, preserving disagreement diagnostics. If the same ID is emitted twice during pagination, it raises `DRIVE_PAGINATION_REPEATED_FILE_ID` rather than double counting. The Colab notebook prints the first same-name groups before Pilot mode. This does not certify source data as research-ready.

`google_auth_httplib2 ... per-request timeout` is a transport warning, not the cause of the previous duplicate-filename ValueError. Reopen the latest notebook from GitHub to use this fix.
