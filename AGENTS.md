# Engineering contract / source of truth

- Scope of v0.1.0: **SOXX**, not SOX, not TSM, ES, NQ or TXF. IBKR `STK` `conId=12658194`. Do not edit unrelated repos, raw datasets, existing Drive folders, downloader scripts or credentials.
- Python 3.11, GitHub main is canonical source. `src/` owns algorithms; Colab notebook is a thin runner. Keep pytest and CI green.
- Explicit local or authenticated Google Drive source folder **ID**; do not assume folder name `IBKR` is correctly spelled in Drive (old folder is named `IKBR`).
- Read-only raw chunks, SHA256 per chunk, exact-only duplicate removal; conflicting timestamps are errors, not silently repaired.
- NYSE official session dates and early closes for provisional bar-start labelled regular market minutes, timezone `America/New_York`; DST via IANA. Do not infer RTH/extended useRTH or IBKR timestamp label from observations.
- No silent forward-fill, interpolation, fake zero-volume rows, resampling to a trading day, inferred corporate action/split adjustment, mixing SOXX ETF and SOX index or cross-contract futures roll.
- Report missing/invalid/chunk errors, coverage %, source metadata, Git SHA, user-specified flags, and explicit provisional states. No studies, regressions, alpha claims, or order execution here.
- Each run uses an immutable dated output folder; do not overwrite historical runs. Keep raw files and user output folders unchanged. Large source data stays out of Git.
- Use tqdm for chunk and year stages and vectorized pandas/NumPy for bar checks. API I/O pagination and bounded retries are allowed. Do not use accidental O(n^2) minute loops.
- Synthetic pytest pass is not proof of historical raw data correctness; real Colab Drive audit requires valid OAuth and source bytes.
- Changes to data semantics need regression tests, README update and review of the QA manifest. Never claim `research_ready=True` while source metadata and split adjustment are unverified.
- After testing, changes may be committed/pushed only as authorized by the current user task. Never use force push.

- Google Drive permits duplicate file *names* with distinct file IDs; preserve all IDs and audit collisions with Drive MD5, size, and per-file SHA256. Never arbitrarily choose a same-name file; resolve duplicate bars only by exact OHLCV; metadata mismatch or missing MD5 is `REVIEW_REQUIRED`. Do not modify original chunks.

## v0.1.1 — SOXX audit extension

- Use a persistent mounted My Drive checkpoint for Colab runs; caller-supplied arbitrary cache paths are NOT proof of persistence. Checkpoint files are operational working state, separate from immutable output archives.
- A completed source checkpoint requires a normalized parquet plus atomic completion metadata, bound to Drive file ID, remote metadata, timestamp override flag, validation-code checksum and normalized file SHA256. Never cache failed source validation as complete.
- PRE and POST coverage is observed-only: count available minute bars, zero/positive-volume observations, flat OHLC, consecutive unchanged closes. Never label absent bars as zero-volume or confirmed provider data loss. NYSE early closes and DST affect session windows.
- Missing entire trading days must appear in separate NYSE calendar QA and must distinguish internal absent sessions from dates outside the actual observed span.
- Preserve existing SOXX-specific RTH QA, source hashes, identical-only duplicate handling and `research_ready=false` until bar timestamp semantics and adjustment policies are independently verified. Never modify raw input.
- Preserve synthetic unit tests, GitHub CI and development prompt for changes; do not silently alter TSM/NQ/ES/TXF logic.


## v0.2.0 — separately authorized NQ validation scope

The v0.1.1 SOXX-only restrictions above refer to the existing SOXX module.
NQ is now authorized only through nq_validate.py, nq_pipeline.py and
notebooks/run_nq_qc.ipynb; preserve existing SOXX/NYSE logic and outputs.
Original NQ root 1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w is read-only.
Output parent 1Kl7s_3Y02tGeEbLbzF-LhoyEMyCtG129 receives new, unique QA runs.
All NQ QA uses per-contract identity and UTC-aware Globex (NOT NYSE/stock
sessions); flag exceptional holidays/halts instead of asserting missing data
is definitely corrupt. DST via IANA zones. Never stitch quarterly contracts,
guess bar-label convention, interpolate, or claim research_ready=true.
