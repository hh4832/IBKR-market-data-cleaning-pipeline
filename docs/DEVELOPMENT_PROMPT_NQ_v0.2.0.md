# Development Prompt — IBKR NQ Globex Validation v0.2.0

## Scope and repositories
- Extend **hh4832/IBKR-market-data-cleaning-pipeline**, branch main. Do NOT create another repository.
- Implement a separate NQ-specific futures QA module; preserve SOXX v0.1.1 behavior.
- Python >=3.11, source code/test-first development, Colab runner, GitHub Actions offline tests.
- No IBKR downloads, credential commits, source mutation, SOXX edits, trade execution, price prediction or market-data fabrication.

## Source and results
- Existing original raw NQ root: Drive 1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w.
- Contains nine quarterly-contract folders NQH/M/U/Z<year>_<conId>. Each has an original consolidated.parquet plus raw chunks/reports.
- Read original consolidated files only; do not delete, move or rewrite any original file.
- New results parent: Drive 1Kl7s_3Y02tGeEbLbzF-LhoyEMyCtG129, under 00Quant_Research/IBKR_NQ_Globex_QC.
- Every output is a separate timestamp+Git SHA folder. Persistent Colab cache must verify original file IDs, sizes, MD5 when available, plus local SHA256 reporting.

## Data contract
- Each original Parquet must include timezone-aware UTC timestamp_utc, open/high/low/close/volume, NQ symbol, quarterly localSymbol, conId, expiry, FUT secType, 1 min bar_size, TRADES what_to_show and use_rth=False.
- Reject ambiguous/missing/contradictory identity, malformed timestamps or OHLC, off-minute bars and inconsistent same-timestamp OHLCV.
- Collapse exact-identical duplicates only; no other repair.
- Distinguish observed source-quality defects from provisional calendar anomalies.

## Futures session validation
- Do not use SOXX or NYSE market calendar for NQ.
- Standard NQ Globex weekly schedule (America/New_York): Sunday 18:00–Friday 17:00 ET, maintenance 17:00–18:00 ET Monday–Thursday.
- DST conversions through IANA timezone; do not hardcode UTC/Taipei offsets.
- Detect potential missing standard-session minutes, completely absent sessions, unexpected closed-time observations, suspect intra-minute price jumps.
- Standard weekly hours do not fully encode special CME holidays, early closes or market halts; mark suspected gaps for review rather than confirm provider failure.
- Existing backfill status=already_complete is NOT evidence of full-time coverage.

## Trading study candidate anchors
- For each independent quarterly contract, compute observed-only as-of close candidates at Taipei 13:45, 15:00, next calendar day 05:00, next calendar day 08:45, max stale tolerance 2 minutes.
- Never use a bar after the requested timestamp, interpolate, forward-fill outside tolerance, backadjust a futures roll or combine two different conIds in one return.
- Emit separate same-contract interval-return candidates only. Candidate Taipei weekdays are NOT equivalent to the verified TAIFEX trading calendar; the research repository must join its dates explicitly later.
- Label Globex 05:00 summer-time boundary as a special timing case if appropriate; bar timestamp/start convention is only provisional.

## Required outputs
- source_manifest.json with original Drive IDs, sizes, hashes, symbol/conId/expiry and errors.
- daily_globex_coverage.csv and possible_missing_intervals.csv.
- taipei_candidate_anchor_prices.csv.
- large_one_minute_moves.csv.
- normalized_by_contract/<contract>.parquet (a NEW output copy, never overwrites original).
- run_info.json and README_RESULT.txt, status/QA/research limitations.
- Always research_ready=false until source timestamps, CME holiday exceptions, coverage and explicit rollover selection independently reviewed.

## Execution and QA
- Colab PILOT: MAX_CONTRACTS=1; FULL: MAX_CONTRACTS=0 means all nine.
- GitHub Actions runs synthetic fixtures and offline smoke tests (no access to Drive market files).
- Tests must cover invalid source contracts, duplicates, DST, Friday close, Sunday opening, maintenance, true open-session gap, Taipei anchor as-of tolerance and original-file immutability.
- Run original SOXX tests unchanged and require the full test suite green.
- Commit and push code/documents to main after tests; preserve Git history, never force-push.
- Do not claim the actual historical NQ data passed QA until the user authenticates Google Drive and runs the Colab notebook.
