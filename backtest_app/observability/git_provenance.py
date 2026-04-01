from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any


def _repo_root_from(repo_root: str | Path | None = None) -> Path:
    if repo_root is not None:
        return Path(repo_root).resolve()
    return Path(__file__).resolve().parents[2]


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return None


def _stdout_lines(result: subprocess.CompletedProcess[str] | None) -> list[str]:
    if result is None:
        return []
    return [line.strip() for line in str(result.stdout or "").splitlines() if line.strip()]


def collect_git_provenance(repo_root: str | Path | None = None) -> dict[str, Any]:
    resolved_root = _repo_root_from(repo_root)
    head = _run_git(resolved_root, "rev-parse", "HEAD")
    branch = _run_git(resolved_root, "branch", "--show-current")
    tracked_changes = _run_git(resolved_root, "diff", "--name-only", "HEAD", "--")
    untracked = _run_git(resolved_root, "ls-files", "--others", "--exclude-standard")
    diff_result = _run_git(resolved_root, "diff", "--binary", "HEAD", "--")

    changed_tracked_files = _stdout_lines(tracked_changes)
    untracked_files = _stdout_lines(untracked)
    diff_text = str(diff_result.stdout or "") if diff_result is not None else ""
    dirty_worktree = bool(changed_tracked_files or untracked_files)
    diff_fingerprint = hashlib.sha256(diff_text.encode("utf-8")).hexdigest() if diff_text else None

    return {
        "repo_root": str(resolved_root),
        "branch": (str(branch.stdout).strip() if branch is not None else "") or "DETACHED",
        "head_commit": str(head.stdout).strip() if head is not None else None,
        "dirty_worktree": dirty_worktree,
        "changed_tracked_files": changed_tracked_files,
        "untracked_files": untracked_files,
        "diff_fingerprint": diff_fingerprint,
        "authoritative": not dirty_worktree,
    }
