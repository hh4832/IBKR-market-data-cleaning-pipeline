"""Colab Git bootstrap must never erase a modified local checkout."""
import json
import subprocess
from pathlib import Path


def git(*args, cwd=None):
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def bootstrap_source():
    nb_file = Path(__file__).resolve().parents[1] / "notebooks" / "run_nq_qc.ipynb"
    nb = json.loads(nb_file.read_text(encoding="utf-8"))
    s = "".join(nb["cells"][1]["source"])
    return s.split('subprocess.run([sys.executable, "-m", "pip"', 1)[0]


def make_local_git_source(root):
    source = root / "origin"
    source.mkdir()
    subprocess.run(["git", "init", "-b", "main", str(source)], check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "ci@example.invalid"], cwd=source, check=True)
    subprocess.run(["git", "config", "user.name", "CI Runner"], cwd=source, check=True)
    (source / "README.md").write_text("canonical\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=source, check=True)
    subprocess.run(["git", "commit", "-m", "fixture"], cwd=source, check=True, capture_output=True)
    return source


def run_bootstrap_before_pip(source_path, preferred, root):
    s = bootstrap_source()
    s = s.replace(
        'PREFERRED_REPO = Path("/content/IBKR-market-data-cleaning-pipeline")',
        f"PREFERRED_REPO = Path({str(preferred)!r})",
    ).replace(
        'URL = "https://github.com/hh4832/IBKR-market-data-cleaning-pipeline.git"',
        f"URL = {str(source_path)!r}",
    ).replace(
        'dir="/content"',
        f"dir={str(root)!r}",
    )
    assert 'PREFERRED_REPO = Path("/content/' not in s
    assert 'URL = "https://' not in s
    env = {}
    exec(compile(s, "<nq-colab-bootstrap>", "exec"), env)
    return env["REPO"]


def test_dirty_checkout_kept_bit_identical(tmp_path):
    source = make_local_git_source(tmp_path)
    preferred = tmp_path / "user-existing-repo"
    subprocess.run(
        ["git", "clone", "--branch", "main", str(source), str(preferred)],
        check=True, capture_output=True,
    )
    original = preferred / "README.md"
    original.write_text("uncommitted important changes\n", encoding="utf-8")
    untracked = preferred / "my-local-work.txt"
    untracked.write_text("not committed\n", encoding="utf-8")
    status_before = git("status", "--porcelain", cwd=preferred)
    repo_used = run_bootstrap_before_pip(source, preferred, tmp_path)
    assert repo_used != preferred
    assert repo_used.joinpath(".git").is_dir()
    assert git("rev-parse", "HEAD", cwd=repo_used) == git("rev-parse", "HEAD", cwd=source)
    assert original.read_text(encoding="utf-8") == "uncommitted important changes\n"
    assert untracked.read_text(encoding="utf-8") == "not committed\n"
    assert git("status", "--porcelain", cwd=preferred) == status_before
    assert not git("status", "--porcelain", cwd=repo_used)


def test_clean_checkout_can_fast_forward_in_place(tmp_path):
    source = make_local_git_source(tmp_path)
    preferred = tmp_path / "user-existing-repo"
    subprocess.run(
        ["git", "clone", "--branch", "main", str(source), str(preferred)],
        check=True, capture_output=True,
    )
    repo_used = run_bootstrap_before_pip(source, preferred, tmp_path)
    assert repo_used == preferred
    assert not git("status", "--porcelain", cwd=preferred)


def test_colab_has_no_destructive_recovery_command():
    s = bootstrap_source()
    assert "fresh_isolated_clone" in s
    for forbidden in ["reset --hard", "git clean", "stash drop", "restore --staged"]:
        assert forbidden not in s
