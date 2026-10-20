"""Prevent Colab kernel import regression after editable package installation."""
from __future__ import annotations

import ast
import json
from pathlib import Path


def test_colab_bootstrap_registers_src_in_live_kernel():
    path = Path(__file__).resolve().parents[1] / "notebooks" / "run_soxx_qc.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    source = "".join(notebook["cells"][1]["source"])
    ast.parse(source)
    # Editable install may not update sys.path of an already-running kernel.
    assert "sys.path.insert(0, str(SRC))" in source
    assert source.index("sys.path.insert(0, str(SRC))") < source.index("import ibkr_qc")
    assert "importlib.invalidate_caches()" in source
    assert "package_file.is_file()" in source


def test_colab_bootstrap_is_idempotent_and_checks_origin():
    path = Path(__file__).resolve().parents[1] / "notebooks" / "run_soxx_qc.ipynb"
    source = "".join(json.loads(path.read_text(encoding="utf-8"))["cells"][1]["source"])
    assert 'if (REPO / ".git").is_dir():' in source
    assert '"pull", "--ff-only"' in source
    assert '"remote", "get-url", "origin"' in source
    assert 'raise RuntimeError(' in source
