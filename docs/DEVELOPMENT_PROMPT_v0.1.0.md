# Development Prompt — IBKR SOXX Historical 1-Minute Cleaning & QA v0.1.0

## Scope / authorization
Implement on **hh4832/IBKR-market-data-cleaning-pipeline**, `main` (Python 3.11). Keep the original IBKR Downloader and the TXF processing repo untouched. Target instrument is **SOXX ETF**, `STK`/SMART `conId=12658194`, **not SOX index**. TSM, NQ, ES are future milestones. Source Drive chunk folder is `1lIq9fWpZHy9RR2xkTIOeEAcmPniwXlNK`; output parent is `1k4kZ6kVjkfn99FW7AgRC6e9MDk848oVt`. These folders are distinct.

## Data handling
1. Source is immutable. Use Google Drive authenticated files.list pagination, source IDs, bytes, checksums, and optionally a local path. No downloader/API calls to IBKR.
2. Run `pilot` on 5 earliest chunks then explicit `full` over all ~2,699 chunks; tqdm progress, stages, elapsed logs, bounded retry and cache reuse. Implement safe on-disk partition processing rather than requiring all chunks in memory.
3. Detect source Parquet timestamp schema; require UTC-aware time or explicit opt-in for naive UTC. Normalize OHLCV numeric types; preserve `timestamp_utc`, `America/New_York` date/time, DST UTC offset and source provenance.
4. Check strict positivity, high/low envelopes, duplicates and conflicts; deduplicate only exact same OHLCV/timestamp; conflicting duplicates fail closed. Validate NYSE session 09:30–16:00 Eastern and official early closes; no assumptions that early closes always 390 bars.
5. For each session produce observed/expected/minutes missing; bounded missing-minute windows; separately tag pre/regular/post, zero-volume observed bars, unusual price jumps. Do not forward-fill or infer IBKR split-adjustment policy. Don't mistake API status=`ok` for full coverage.
6. Preserve unknowns: actual `use_rth`, `what_to_show` and `bar_size` values from Parquet metadata (verify internal consistency), actual minute bar labeling, SOXX split/corporate action method and IBKR head timestamp fallback status are initially unverified. No `research_ready` certificate without external evidence.
7. Archive deterministic QA outputs with Git SHA, Asia/Taipei timestamp, source hashes, config, errors, quality status. Create a **new** Drive output child folder per run; never overwrite a previous run or source.
8. Repo layout: README, AGENTS, pyproject, `src/`, `tests/`, Colab Runner and GitHub Actions pytest. Test synthetic DST, holidays/half-days, exact duplicates, conflicts, invalid OHLCV, naive times, partial coverage, and no data loss on runs.

## Deliverables and acceptance gates
- Repo source and Colab thin runner, operational CLI and smoke/pilot/full controls.
- Unit tests and bounded synthetic smoke. `pytest -q` passes on Python 3.11 with dependencies.
- Git diff reviewed: no raw data, tokens, secrets or cross-repo mutation.
- Real Drive full audit may only be declared after execution with user OAuth and successful readback; **do not invent a successful 2,699-chunk backfill audit**.
- User has authorized modifying the indicated Repo in this task. Commit/push only to that Repo with no force-push after tests; provide commit SHA and exact blockers.
