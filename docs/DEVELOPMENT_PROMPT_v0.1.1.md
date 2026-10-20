# Development Prompt — IBKR SOXX Minute-Data QC v0.1.1

## Authority, scope and non-goals

Target: `hh4832/IBKR-market-data-cleaning-pipeline`, branch `main`, Python >=3.11.
The canonical research subject is **SOXX**, iShares Semiconductor ETF / IBKR `STK` `conId=12658194`, **not SOX**. The inputs are existing raw IBKR 1-minute `TRADES` Parquet files. The SOXX Drive `chunks` source ID is `1lIq9fWpZHy9RR2xkTIOeEAcmPniwXlNK`; publish completed immutable results beneath Google Drive parent ID `1k4kZ6kVjkfn99FW7AgRC6e9MDk848oVt`.

Perform a read-only *quality audit*. Do not call IBKR to refetch history, trade, infer corporate actions, split-adjust prices, forward-fill absent bars, edit raw files, overwrite research results, or modify the TSM, NQ, ES or TXF datasets/logic. Always set `research_ready=false` pending source-specific semantics and corporate-action verification.

## Source-evidence baseline (2026-10-20 pilot)

- v0.1.0 pilot: **5 of 2,700** Drive parquet file IDs processed; 2016-10-18,19,20,21,24 all showed 390/390 regular-session minutes under provisional *bar-start* timestamp labeling; 0 RTH gaps, 0 chunk errors, 0 candidate large jumps.
- Metadata in these 5 chunks: `symbol=SOXX`, `conId=12658194`, `what_to_show=TRADES`, `use_rth=false`, `bar_size=1 min`, `secType=STK`, `exchange=SMART`.
- Among the five source chunks, four begin at 09:30 ET and one at 04:00 ET. **Missing observed PRE bars are not evidence of an API error**; stock premarket trading can be sparse or vendor-dependent. The five had observed POST bars through 19:59 ET.
- The *full folder* contains two different Google Drive IDs sharing the same filename `end_20230201T000000Z.parquet`, with matching Drive MD5 and size. Keep distinct file IDs and audit their contents; do not silently discard either one or conflate a full-folder collision with the 5-chunk pilot.
- Downloader originally recorded SOXX head timestamp as `fallback_floor_UNVERIFIED`. A downloader “complete” status never substitutes for bar-level or full-calendar QA.

## v0.1.1 deliverables and acceptance gates

### A. Crash-safe checkpoint / resume

1. Make full runs resumable in Colab across runtime restarts using an **explicitly mounted persistent My Drive cache**. For local runs use a caller-specified persistent `--cache-dir`; when cache is ephemeral, label it as ephemeral and never promise cross-session resume.
2. Separate raw cached files from normalized, validated per-source chunks. Keep original raw source unmodified. Do not place incomplete cache fragments in archived outputs.
3. Compute a deterministic checkpoint key from Google Drive **file ID**, filename, size, MD5, modified timestamp, the normalization code hash, timezone override flag and checkpoint schema version. For local paths also include mtime/size. Never key by filename alone.
4. A checkpoint is complete only after normalized parquet is written atomically and a separate atomic JSON success record is written; store SHA256 for the normalized parquet and original input SHA256. On resume, accept only fully present, matching entries. Corrupted entries or source/parameter/schema changes must reprocess. Never cache a failed validation as successful.
5. Emit resumed/newly-normalized counts, per-source audit manifest and clear progress via `tqdm`. A pilot can seed checkpoints that later get reused by a full run. All final reports must be recomputed over the full selected input set rather than trusting older aggregates.
6. Archive each run under a fresh `YYYYMMDD_HHMMSS_<git_sha>_SOXX_<PILOT|FULL>` folder. Persistent checkpoint/cache are mutable working state *outside* those immutable result folders. Do not silently overwrite an existing run.

### B. PRE / POST observed-only diagnostics

1. Use NYSE calendar times in `America/New_York` with DST handled by IANA tzdata. PRE diagnostic interval: 04:00 ET until actual session open. POST: actual session close until 20:00 ET; allow early closes. Document that exchange-trading/halt and IBKR market-data permissions may impose additional limitations.
2. Output `extended_hours_daily.csv` with NY date, window timestamps, diagnostic minute grid size, **observed** distinct minutes, observed positive-volume and zero-volume bars, OHLC flat bars, consecutive same-close minutes and unobserved minute count.
3. Missing PRE/POST minute bars are **not labeled as confirmed data loss or zero trades**. Do not fill absent minute bars, assign zero volume to missing bars, assert 330/240 expected *traded* candles, or infer volume from unchanged prices. For actual observed zero-volume bars, report them without claiming any particular underlying vendor behavior.
4. Maintain separate normal-session coverage calculations. Do not apply 09:30–16:00 fixed RTH assumptions on early-close days or automatically to CME futures.

### C. Completely absent trading-session detection

1. Compare actual normalized SOXX bars to the NYSE trading calendar for the chosen **audit scope**, not only dates present in the input files.
2. Default calendar range = first/last observed New York date, hence only the internal missing sessions are fully assessable. Allow explicit `AUDIT_START_NY` and `AUDIT_END_NY` overrides for intentional coverage checks. Label missing leading/trailing source data as `OUTSIDE_OBSERVED_SPAN_REVIEW`, not “provider failure”.
3. Output `trading_day_gaps.csv`, distinguishing no bars all day vs observed extended-hours but no RTH vs outside observed data span. Extend RTH daily report / missing-interval report using the same explicit calendar schedule so full missing days appear, including 390/390 missing on a normal full session or correct early-close totals.
4. Do not classify exchange holidays as missing, nor assume dates without input files were genuine holidays. Preserve a clear exclusion/review state.

### D. Testing and reproducibility

- Keep Python code modular within `src/ibkr_qc/`, Notebook as thin runner. Maintain `pyproject.toml`, `README.md`, `AGENTS.md`, `tests/` and GitHub CI. Prefer vectorized pandas checks; explicitly bounded loops for Drive pagination, partitions and calendar days are acceptable.
- Add synthetic tests for checkpoint reuse/invalidation/corruption; a trading day with zero source bars; PRE with zero observed bars and an actual zero-volume bar; NYSE early close; DSV/UTC handling; unchanged historical duplicate-file semantics. Confirm previous tests remain green.
- Verify with synthetic pytest locally and GitHub Actions on push. **Do not claim all real SOXX files were validated** without a Colab full run, and do not claim full stock/corporate-action semantic validity from tests.
- Update Colab bootstrap so `git clone` is idempotent, `pip install -e` uses current kernel Python, and `src/` is on `sys.path`. Explicitly mount Drive and use a persistent checkpoint folder. Default remains a 5-file Pilot. Provide visible pilot/full mode and QA readback, including the new CSV files.
- Preserve Drive source IDs, Git SHA, file hashes, timestamp policy, source metadata and failure reasons. Never commit secrets or raw/derived Parquet to GitHub. No `force-push`.

## Done definition

All changed Python sources compile; `pytest -q` passes; GitHub Actions verifies after push; the new Development Prompt and README are committed. Report commit SHA, CI result, what is implemented and what still requires a full authenticated Colab run. Do not silently advance `research_ready` to true.
