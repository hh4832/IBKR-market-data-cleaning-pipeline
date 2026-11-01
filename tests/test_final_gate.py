import pandas as pd
import pytest

from ibkr_qc.final_gate import (
    FinalGateConfig, _merge_exact, _normalize, _select_recovery_attempts,
)
from ibkr_qc.validate import QCError


def bars():
    return pd.DataFrame({
        "timestamp_utc": ["2026-01-02T14:30:00+00:00", "2026-01-02T14:31:00+00:00"],
        "open": [100.0, 101.0], "high": [101.0, 102.0],
        "low": [99.0, 100.0], "close": [100.5, 101.5], "volume": [10, 12],
        "symbol": ["TSM", "TSM"], "conId": [6223250, 6223250],
    })


def test_normalize_and_exact_merge():
    clean, stats = _normalize(bars(), symbol="TSM", con_id=6223250)
    merged, duplicates = _merge_exact([clean, clean])
    assert len(merged) == 2
    assert duplicates == 2
    assert stats["normalized_rows"] == 2


def test_conflicting_recovery_fails_closed():
    clean, _ = _normalize(bars(), symbol="TSM", con_id=6223250)
    conflict = clean.copy()
    conflict.loc[0, "close"] = 100.75
    with pytest.raises(QCError, match="CROSS_SOURCE_CONFLICT"):
        _merge_exact([clean, conflict])


def test_negative_limits_rejected():
    with pytest.raises(ValueError, match="FINAL_GATE_LIMIT_NEGATIVE"):
        FinalGateConfig(max_equity_chunks=-1).validate()


def test_recovery_history_selects_best_attempt_without_hiding_history():
    base = {
        "window_id": "same-window", "contract": "NQZ4",
        "start_utc": "2024-10-15T00:00:00+00:00",
        "end_utc": "2024-10-15T06:00:00+00:00",
        "window_classification": "REVIEW",
    }
    frame = _select_recovery_attempts({"same-window": [
        {**base, "recovery_status": "FAILED_UNRESOLVED", "unresolved_minutes": 360,
         "rows": 0, "saved_sha256": None, "result_file_id": "old",
         "result_file_modified_time": "2026-10-24T01:00:00Z"},
        {**base, "recovery_status": "OBSERVED_ALL_CANDIDATE_MINUTES",
         "unresolved_minutes": 0, "rows": 360, "saved_sha256": "abc",
         "result_file_id": "new", "result_file_modified_time": "2026-10-25T01:00:00Z"},
    ]})
    assert frame.loc[0, "result_file_id"] == "new"
    assert frame.loc[0, "unresolved_minutes"] == 0
    assert frame.loc[0, "attempt_count"] == 2
    assert "FAILED_UNRESOLVED" in frame.loc[0, "status_history"]
