import json
from pathlib import Path


def test_final_gate_notebook_is_thin_runner_with_full_toggle():
    path = Path(__file__).resolve().parents[1] / "notebooks" / "run_ibkr_final_gate.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    source = "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])
    assert "PILOT = True" in source
    assert "run_final_gate" in source
    assert "FinalGateConfig" in source
    assert "PILOT_ONLY" in source
    assert "1CIlmaQv6BsAnMlNHxDTanJB4JttqYbs9" in source
