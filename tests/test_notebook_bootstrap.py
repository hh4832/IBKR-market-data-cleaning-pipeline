"""Colab bootstrap is rerunnable and imports in the current kernel."""
import ast
import json
from pathlib import Path


def test_bootstrap_starts_and_imports_without_kernel_restart():
    nb=json.loads((Path(__file__).resolve().parents[1]/"notebooks/run_soxx_qc.ipynb").read_text())
    source="".join(nb["cells"][1]["source"])
    ast.parse(source)
    assert 'sys.path.insert(0, str(SRC))' in source
    assert source.index('sys.path.insert(0, str(SRC))') < source.index('import ibkr_qc')
    assert '"pull", "--ff-only"' in source
    assert '"remote", "get-url", "origin"' in source
    assert 'package_file.is_file()' in source


def test_full_has_persistent_cache_and_explicit_bounds():
    nb=json.loads((Path(__file__).resolve().parents[1]/"notebooks/run_soxx_qc.ipynb").read_text())
    config="".join(nb["cells"][2]["source"])
    assert 'drive.mount("/content/drive")' in config
    assert 'MyDrive/IBKR_SOXX_QC_PERSISTENT_CACHE_V011' in config
    assert 'AUDIT_START_NY' in config and 'AUDIT_END_NY' in config
    assert 'MAX_CHUNKS = 5' in config

def test_bootstrap_invalidates_stale_ibkr_modules_before_import():
    nb = json.loads((Path(__file__).resolve().parents[1] / "notebooks/run_soxx_qc.ipynb").read_text())
    source = "".join(nb["cells"][1]["source"])
    ast.parse(source)
    assert 'name == "ibkr_qc" or name.startswith("ibkr_qc.")' in source
    assert source.index('del sys.modules[name]') < source.index('import ibkr_qc\n')
    assert "STALE_IBKR_QC_CONFIG" in source


def test_run_cell_reload_config_before_constructing_dataclass():
    nb = json.loads((Path(__file__).resolve().parents[1] / "notebooks/run_soxx_qc.ipynb").read_text())
    source = "".join(nb["cells"][4]["source"])
    ast.parse(source)
    assert source.index("importlib.reload(_pipeline)") < source.index("config = Config(")
    assert "audit_start_ny" in source and "audit_end_ny" in source
