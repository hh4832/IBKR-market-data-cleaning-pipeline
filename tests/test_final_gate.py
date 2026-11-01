import pandas as pd
import pytest

from ibkr_qc.final_gate import FinalGateConfig, _merge_exact, _normalize
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
