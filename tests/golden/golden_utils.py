from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures'
GOLDEN = ROOT / 'golden'


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding='utf-8'))


def canonicalize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: canonicalize(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [canonicalize(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 8)
    return obj


def assert_matches_golden(name: str, actual: Any) -> None:
    path = GOLDEN / name
    expected = json.loads(path.read_text(encoding='utf-8'))
    assert canonicalize(actual) == canonicalize(expected)
