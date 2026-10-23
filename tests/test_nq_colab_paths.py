"""Ensure the NQ Colab launcher keeps previously verified Drive paths."""
import json
from pathlib import Path


def notebook_cells():
    base = Path(__file__).resolve().parents[1]
    nb = json.loads((base / "notebooks" / "run_nq_qc.ipynb").read_text(encoding="utf-8"))
    return ["".join(c.get("source", [])) for c in nb["cells"] if c.get("cell_type") == "code"]


def test_verified_cache_path_is_existing_folder_not_recreated():
    cells = notebook_cells()
    assert 'CACHE_FOLDER_ID = "1ExEXrUdnO1mh-MTalmJ8n-fLa362SX91"' in cells[1]
    assert 'CACHE_PARENT_ID = "16RheGRQUz-VWhvQJA_2Be2o7BM0zBU1u"' in cells[1]
    assert 'QUANT_ROOT_ID = "1kRfLhTLdHevVuFkEzdZH9M5wEui7JuEg"' in cells[1]
    assert '/content/drive/MyDrive/00Quant_Research/00_Cache/IBKR_NQ_GLOBEX_QC_CACHE_V020' in cells[1]
    assert "if not CACHE_DIR.is_dir()" in cells[1]
    assert "if not EXPECTED_CACHED_PILOT.is_file()" in cells[1]
    assert "CACHE_DIR.mkdir" not in cells[1]


def test_new_outputs_archive_under_verified_parent_not_inside_pilot():
    cells = notebook_cells()
    assert 'OUTPUT_PARENT_ID = "1Kl7s_3Y02tGeEbLbzF-LhoyEMyCtG129"' in cells[1]
    assert 'SOURCE_FOLDER_ID = "1C0wfYpT8zioRmZxBnqO7d79-zJyD_S5w"' in cells[1]
    assert '(OUTPUT_PARENT_ID, "IBKR_NQ_Globex_QC", QUANT_ROOT_ID)' in cells[1]
    assert "OUTPUT_PARENT_ID = parents[0]" not in cells[1]
    assert 'OUTPUT_DIR = Path("/content/ibkr_nq_qc_outputs")' in cells[1]
    assert "output_parent_id=OUTPUT_PARENT_ID" in cells[2]
    assert "upload=True" in cells[2]
