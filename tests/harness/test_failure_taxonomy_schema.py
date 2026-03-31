from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate

pytestmark = pytest.mark.harness

REPO_ROOT = Path(__file__).resolve().parents[2]
FAILURE_SCHEMA_PATH = REPO_ROOT / "evals" / "schemas" / "failure_attribution.schema.json"
SCORECARD_SCHEMA_PATH = REPO_ROOT / "evals" / "schemas" / "scorecard.schema.json"

LANES = {
    "live-cutover",
    "shadow-replay",
    "research-discovery",
    "promotion",
}
FAILURE_CLASSES = {
    "product_regression": "do_not_rerun_until_code_or_config_changes",
    "harness_bug": "fix_harness_then_rerun",
    "environment_issue": "fix_environment_then_rerun",
    "evidence_gap": "collect_evidence_then_rerun",
    "task_spec_gap": "clarify_task_then_rerun",
}


def _load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_schema_enums_match_expected_taxonomy() -> None:
    failure_schema = _load_schema(FAILURE_SCHEMA_PATH)
    scorecard_schema = _load_schema(SCORECARD_SCHEMA_PATH)

    assert set(failure_schema["properties"]["lane"]["enum"]) == LANES
    assert set(scorecard_schema["properties"]["lane"]["enum"]) == LANES
    assert set(failure_schema["properties"]["failure_class"]["enum"]) == set(FAILURE_CLASSES)


@pytest.mark.parametrize(
    ("failure_class", "lane"),
    [
        ("product_regression", "live-cutover"),
        ("harness_bug", "shadow-replay"),
        ("environment_issue", "shadow-replay"),
        ("evidence_gap", "promotion"),
        ("task_spec_gap", "research-discovery"),
    ],
)
def test_failure_attribution_schema_accepts_each_failure_class(failure_class: str, lane: str) -> None:
    schema = _load_schema(FAILURE_SCHEMA_PATH)
    payload = {
        "task_id": f"task-{failure_class}",
        "lane": lane,
        "failure_class": failure_class,
        "summary": f"{failure_class} example",
        "evidence_refs": ["docs/example.md"],
        "rerun_policy": FAILURE_CLASSES[failure_class],
        "remaining_risk": ["needs follow-up"],
        "next_smallest_step": "rerun the targeted check after the classified fix",
    }

    validate(instance=payload, schema=schema)


def test_scorecard_schema_accepts_minimal_payload() -> None:
    schema = _load_schema(SCORECARD_SCHEMA_PATH)
    payload = {
        "task_id": "task-scorecard",
        "lane": "research-discovery",
        "outcome": "pass",
        "tests_run": ["pytest -q tests/harness/test_failure_taxonomy_schema.py"],
        "artifacts": [{"name": "scorecard", "path": "runs/research_ledger/demo/scorecard.json"}],
        "next_gate": "promotion-review",
        "strategy_version": "research_similarity_v2",
        "feature_version": "baseline",
        "decision_engine_version": "shared-domain-v1",
        "parameter_hash": "abc123",
        "seed": 42,
        "replay_anchor_ids": ["us_open_20260324"],
    }

    validate(instance=payload, schema=schema)


def test_failure_attribution_schema_rejects_unknown_failure_class() -> None:
    schema = _load_schema(FAILURE_SCHEMA_PATH)
    payload = {
        "task_id": "task-bad-class",
        "lane": "live-cutover",
        "failure_class": "unknown_failure",
        "summary": "bad class",
        "evidence_refs": ["docs/example.md"],
        "rerun_policy": "fix_harness_then_rerun",
        "remaining_risk": ["unclassified"],
        "next_smallest_step": "fix it",
    }

    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_failure_attribution_schema_requires_rerun_policy() -> None:
    schema = _load_schema(FAILURE_SCHEMA_PATH)
    payload = {
        "task_id": "task-missing-rerun",
        "lane": "promotion",
        "failure_class": "evidence_gap",
        "summary": "missing rerun policy",
        "evidence_refs": ["docs/example.md"],
        "remaining_risk": ["cannot rerun safely"],
        "next_smallest_step": "collect the missing truth anchor",
    }

    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_failure_attribution_schema_requires_next_smallest_step() -> None:
    schema = _load_schema(FAILURE_SCHEMA_PATH)
    payload = {
        "task_id": "task-missing-next-step",
        "lane": "research-discovery",
        "failure_class": "task_spec_gap",
        "summary": "missing next step",
        "evidence_refs": ["docs/example.md"],
        "rerun_policy": "clarify_task_then_rerun",
        "remaining_risk": ["task still ambiguous"],
    }

    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)
