from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.harness

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS = REPO_ROOT / "AGENTS.md"
HARNESS_README = REPO_ROOT / "docs" / "harness" / "README.md"
OVERLAY_DOCS = [
    REPO_ROOT / "docs" / "harness" / "north-star.md",
    REPO_ROOT / "docs" / "harness" / "runtime-lanes.md",
    REPO_ROOT / "docs" / "harness" / "task-taxonomy.md",
    REPO_ROOT / "docs" / "harness" / "source-of-truth.md",
    REPO_ROOT / "docs" / "harness" / "evidence-contract.md",
    REPO_ROOT / "docs" / "harness" / "failure-attribution.md",
    REPO_ROOT / "docs" / "harness" / "promotion-gate.md",
    REPO_ROOT / "docs" / "harness" / "checklists.md",
    REPO_ROOT / "docs" / "harness" / "doc-gardening.md",
]
EXISTING_DOC_LINKS = {
    "docs/cutover-gates.md",
    "docs/cutover-summary.md",
    "docs/route-and-scheduler-parity.md",
    "docs/shadow-e2e-plan.md",
    "docs/shadow-run-result-report.md",
    "docs/runtime-replay-corpus.md",
    "docs/research_run_protocol.md",
    "docs/local-backtest-postgres.md",
    "docs/experiment-tracking.md",
    "docs/live-path-gap-list.md",
    "docs/recommended-next-steps.md",
    "docs/structured-logging-and-metrics.md",
}

LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def _resolved_links(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    links: set[str] = set()
    for raw_link in LINK_RE.findall(text):
        link = raw_link.split("#", 1)[0]
        if not link or "://" in link:
            continue
        resolved = (path.parent / link).resolve().relative_to(REPO_ROOT).as_posix()
        links.add(resolved)
    return links


def test_agents_links_to_harness_readme() -> None:
    links = _resolved_links(AGENTS)
    assert "docs/harness/README.md" in links


def test_harness_readme_indexes_every_overlay_doc() -> None:
    links = _resolved_links(HARNESS_README)
    indexed = {path.relative_to(REPO_ROOT).as_posix() for path in OVERLAY_DOCS}
    assert indexed.issubset(links)


def test_each_overlay_doc_links_existing_source_of_truth() -> None:
    for doc in OVERLAY_DOCS:
        links = _resolved_links(doc)
        assert links & EXISTING_DOC_LINKS, f"{doc.name} must link at least one existing source-of-truth doc"
