# IBKR-market-data-cleaning-pipeline

**v0.1.1 | SOXX first | immutable-source quality audit with persistent checkpoint/resume, extended-hours diagnostics and full-session calendar gaps.**

[Original data folder](https://drive.google.com/drive/folders/1jLOYnUhWx6coNV6eqYnjUAeeHZPP6aMh) → `IKBR` (spelling in Drive) → `raw/SOXX/SOXX_12658194/chunks`.

- **SOXX** = iShares Semiconductor ETF, IBKR `secType=STK`, `conId=12658194`. **Not SOX** (PHLX semiconductor index).
- Existing Downloader reports 2,699 SOXX Parquet chunks; `full_audit_required=True` and `fallback_floor_UNVERIFIED` for earliest head timestamp. Those are source reports, **not** independently validated observations.
- v0.1.0 does **not** download from IBKR, modify existing Downloader/TSM/NQ/ES/TXF, or apply split adjustments. It only audits already-downloaded raw SOXX Parquet chunks. TSM, NQ and ES are explicitly **out of scope** for this version; do not silently apply equity NYSE calendar logic to Globex futures.

## Use in Colab (recommended)

### Colab running-kernel module cache (v0.1.1 hotfix)

If the Notebook prints the newest Git SHA but raises `TypeError: Config.__init__() got an unexpected keyword argument 'audit_start_ny'`, the Colab **kernel still holds an old `ibkr_qc.pipeline.Config` Python class**. Git pull/pip install update disk, not already imported Python modules. The first code cell now removes stale `ibkr_qc.*` entries from `sys.modules` **before** importing the package; the execution cell reloads `ibkr_qc.pipeline` and checks for the required constructor arguments. Reopen the latest Notebook and select **Runtime → Restart session → Run all**. This issue is unrelated to market data or checkpoint quality.


Open [`notebooks/run_soxx_qc.ipynb`](notebooks/run_soxx_qc.ipynb) on Google Colab. The v0.1.1 Notebook explicitly mounts **My Drive** and uses `/content/drive/MyDrive/IBKR_SOXX_QC_PERSISTENT_CACHE_V011` as a persistent checkpoint: this is separate from both original source and archived QA results. Re-running after interruption verifies each cached normalized chunk's SHA256 and original Drive file identity/metadata before reuse. Failed/incomplete chunks are never marked complete. A later FULL run can reuse the earlier five PILOT checkpoints if no source or normalization policy has changed. The checkpoint folder may grow large (~thousands of parquet and JSON files); do not delete it before FULL completes.

The Colab runtime **must** install a compatible Python and `pyarrow`. Authenticate with Google OAuth, review the **hardcoded public folder IDs** shown below, and run the pilot first. The Notebook uses Google's authenticated Drive API directly, so it does not depend on the `IKBR` folder being mounted in `My Drive` or knowing its full filesystem path. OAuth permission can see and create files in Drive; the pipeline only creates a **new timestamped child folder** in the designated output parent, never deletes or modifies sources.

```python
SOURCE_FOLDER_ID = "1lIq9fWpZHy9RR2xkTIOeEAcmPniwXlNK"
OUTPUT_PARENT_ID = "1k4kZ6kVjkfn99FW7AgRC6e9MDk848oVt"
MAX_CHUNKS = 5  # pilot: 5 earliest chunks. For full scan explicitly set to 0.
AUDIT_START_NY = None  # defaults to first observed NY date
AUDIT_END_NY = None    # defaults to last observed NY date
```

The v0.1.1 Notebook mounts My Drive for persistent local caching, but source and output are accessed by folder IDs through Drive API. Users with no access to the source folder will receive an authorization error, not a silently empty dataset.

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
- For CLI calendar boundaries, optionally use `--audit-start-ny YYYY-MM-DD --audit-end-ny YYYY-MM-DD`. Dates outside the observed source span are labelled for review, not silently passed or called provider failures.
- `--cache-dir` must point to persistent storage for restart-surviving checkpoint. The default output-local cache is **ephemeral**; it can be lost if Colab restarts. The Notebook uses mounted My Drive by default.


## Validation semantics

1. Strict contract provenance: named source folder `SOXX_12658194`, optional per-row symbol and conId validation; keep every input Drive ID and SHA256 in `source_manifest.json`.
2. Reject timezone-ambiguous, malformed or off-minute timestamps; reject null/non-numeric/invalid OHLCV. Zero volume is **not** assumed to indicate a missing candle.
3. Consolidate **only exact timestamp + OHLCV duplicates**, including across chunks and within NY calendar year. Conflicting same-timestamp OHLCV causes `REVIEW_REQUIRED`; never pick the first/last conflicting observation.
4. Convert UTC to `America/New_York` (DST handled by IANA timezone). Compute expected regular trading minutes from the **NYSE historical session calendar**, including known early closes. Expected minutes are conventionally **bar-start labelled**; that label convention is **not independently verified** for current IBKR source.
5. Save `daily_coverage.csv`, `rth_missing_intervals.csv`, `trading_day_gaps.csv`, `extended_hours_daily.csv`, `large_move_candidates.csv`, `chunk_quality.csv`, yearly Parquet, `source_manifest.json`, `run_info.json` and run notices. No forward-fill, retroactive price correction, resampling, synthetic bars or inferred split factors.
6. Flag large price changes for **review**. Do not automatically attribute them to splits; historical adjusted/unadjusted sources have not been reconciled.
7. **`research_ready=false` in all v0.1.0 runs**, including those with 100% observed RTH coverage: source metadata may identify `use_rth`/`what_to_show` (explicitly audited from source), but corporate-action adjustment policy and IBKR bar-label convention remain unverified. A complete downloader checkpoint or successful API request is not proof of completeness.

### Quality gates and limitations

- `COVERAGE_OK_LABEL_UNVERIFIED`: 100% *provisionally expected* RTH timestamps within scanned interval and no other detected issues, **not** certified to study returns.
- `PARTIAL_COVERAGE`: missing RTH bars, without claiming whether IBKR omitted trades / data or the exchange was closed.
- `REVIEW_REQUIRED`: one or more invalid chunks, calendar gaps, cross-chunk conflicts, price-jump candidates or unverified fundamental semantics.
- A pilot may include only the earliest files; it cannot certify full period or missing dates **outside the observed date bounds**. If a whole calendar year lacks input files, further manifest-level coverage audit is required.
- **v0.1.1 PRE/POST**: `extended_hours_daily.csv` uses observed minute bars only, includes observed positive/zero-volume bars, flat OHLC bars and successive unchanged closes. PRE diagnostic grid = 04:00 ET to actual NYSE open; POST = actual exchange close to 20:00 ET, with early closes reflected. Unobserved minutes are not proof of API data loss or zero trades. Do **not** apply a 390-bar requirement to pre/post-market.
- **v0.1.1 full absent sessions**: `trading_day_gaps.csv` explicitly enumerates exchange trading days with **zero RTH observed bars**, distinguishing those with some extended-hours observations and those outside the observed source span. Calendar range defaults to first/last observed dates; explicit bounds may expand the scope. The same calendar schedule feeds RTH missing intervals so days without any input file appear in QA.
- **Checkpoint**: per-source normalization uses a metadata/code-version hash and a verified on-disk normalized SHA256. Resume retains metadata/provenance and reruns aggregate QA over the selected chunks. `run_info.json` includes `checkpoint_reused_chunks`, `new_normalized_chunks`, `checkpoint_persistence_hint`, the date scope and absent-day counts. Existing immutable output run folders and source Parquet are never modified.


## Architecture

```text
src/ibkr_qc/validate.py      Pure OHLCV/time checks, exact dedup, NYSE calendar QA
src/ibkr_qc/drive_io.py      OAuth Drive list/download/archive, paginated >1000 files
src/ibkr_qc/checkpoint.py     Atomic verified per-source normalization cache / resume
src/ibkr_qc/session_audit.py PRE/POST observed diagnostics, complete missing session flags
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

## What the 5-file pilot proves—and does not prove

The [2026-10-20 SOXX pilot](https://drive.google.com/drive/folders/1-vouLVPwmZil7FZOGaYBqf3kEHZtMwr7) validated 5 of 2,700 Drive file IDs, with 390/390 observed 1-minute RTH bars on five October 2016 NYSE sessions, no missing RTH minute, and no invalid chunk. Four source chunks begin at 09:30 ET; one begins at 04:00 ET. This asymmetry is now visible in PRE/POST QA but **does not establish that IBKR omitted trades**. One globally duplicated filename in Feb 2023 has matching Drive MD5/size and is not part of the pilot. Full chronological data, corporate actions, timestamp semantics and any absence outside observed dates remain unverified. The implementation's synthetic tests are separate from real full-history QA.

### v0.1.1 integration notes

- The run folder is immutable and archived in Drive. The mounted My Drive normalization checkpoint is mutable operational state and is not a published dataset.
- The first execution may process chunks again because v0.1.1 checkpoint layout/algorithm differs from v0.1.0. Subsequent executions reuse successful normalized checkpoints unless the underlying source or policy changes.
- The PRE/POST count fields represent **observed bars**, not guaranteed traded minutes. Even 100% RTH timestamp coverage is not proof of adjusted-price validity or a tradable backtest.

## Future milestones (not implemented)

- v0.2: SOXX source metadata reconciliation (`whatToShow`, `useRTH`, IBKR bar-label semantics, corporate action raw/adjusted mapping, NYSE event-window QA).
- v0.3: Separate TSM stock adapter, regression fixtures and checks.
- v0.4: CME Globex ES/NQ futures adapter (quarterly contracts, expiry policies, 1-minute session completeness, roll provenance), distinct from equity calendar logic.

See [v0.1.1 Development Prompt](docs/DEVELOPMENT_PROMPT_v0.1.1.md), [v0.1.0 prompt](docs/DEVELOPMENT_PROMPT_v0.1.0.md) and [AGENTS.md](AGENTS.md).


## v0.2.0 — NQ futures Globex quality validation

This is a SEPARATE NQ validation workflow in the original repository; the
SOXX v0.1.1 workflow and its NYSE calendar are unchanged.

- [Development prompt](docs/DEVELOPMENT_PROMPT_NQ_v0.2.0.md)
- [NQ Colab runner](notebooks/run_nq_qc.ipynb)
- [Existing NQ raw data](https://drive.google.com/drive/folders/1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w) — nine quarterly-contract subfolders, original consolidated.parquet is read only
- [New NQ QC outputs](https://drive.google.com/drive/folders/1Kl7s_3Y02tGeEbLbzF-LhoyEMyCtG129) — new immutable run subfolder for each execution

Launch the NQ Colab notebook, authorize Google Drive and mount My Drive for
persistent, remote-MD5-checked cache. Start with MAX_CONTRACTS=1 (pilot).
Change it to 0 only after reviewing a pilot, to audit all nine contracts.
No IBKR login or new IBKR download is needed.

NQ v0.2.0 validates UTC timestamp semantics, NQ/FUT/conId/expiry identity,
1-minute OHLCV, exact duplicates, CME weekly Globex standard open hours
(Sunday 18:00 to Friday 17:00 New York, with weekday 17:00–18:00 maintenance),
DST conversion, candidate missing intervals and Taipei session anchor quotes.
CME special-holiday / early-close / halt exceptions are NOT independently
reconciled. Missing minutes are review candidates, not proven vendor errors.

Per-contract output: normalized_by_contract, source_manifest.json,
daily_globex_coverage.csv, possible_missing_intervals.csv,
taipei_candidate_anchor_prices.csv, large_one_minute_moves.csv, run_info.json.

IMPORTANT: No cross-contract stitching or roll selection, no 0050/TXF research
in the QC pipeline, no interpolation and no lookahead beyond anchor timestamps.
Taipei anchors are weekday candidates only, not confirmed TAIFEX trading days.
IBKR bar start/end labeling remains unverified, therefore research_ready=false
even for apparently complete contracts. CI tests are synthetic/offline; the real
IBKR Drive files require the user's Colab OAuth validation.
