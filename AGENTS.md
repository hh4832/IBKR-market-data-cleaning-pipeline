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
