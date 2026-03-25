from __future__ import annotations

from datetime import datetime
from pprint import pformat

from live_app.application.context import RunContext
from live_app.application.planning_commands import BuildOrderPlanCommand, LivePlanningInput
from tests.golden.golden_utils import load_fixture


def _make_plan(fixture_name: str):
    fixture = load_fixture(fixture_name)
    generated_at = datetime.fromisoformat(fixture["generated_at"])
    ctx = RunContext(actor="test", channel="parity", invoked_at=generated_at, metadata={"fixture": fixture_name})
    plan, skip = BuildOrderPlanCommand().execute(
        LivePlanningInput(
            market=fixture["market"],
            candidate=fixture["candidate"],
            tuning=fixture["tuning"],
            budget=float(fixture["budget"]),
            side=fixture["side"],
            generated_at=generated_at,
            rationale_prefix="replay-parity",
        ),
        ctx,
    )
    return fixture, plan, skip


def _pct_gap(current_price: float, limit_price: float) -> float:
    return abs((float(limit_price) / float(current_price)) - 1.0)


def _semantic_view(plan, skip, fixture):
    if plan is None:
        return {
            "skip": True,
            "skip_code": (skip or {}).get("code"),
            "skip_note": (skip or {}).get("note"),
        }
    current_price = float(fixture["candidate"]["current_price"])
    return {
        "skip": False,
        "symbol": plan.symbol,
        "side": plan.side.value,
        "policy_version": fixture["candidate"].get("policy_version"),
        "requested_quantity": plan.requested_quantity,
        "leg_count": len(plan.legs),
        "quantity_split": [leg.quantity for leg in plan.legs],
        "limit_prices": [leg.limit_price for leg in plan.legs],
        "first_leg_pct": _pct_gap(current_price, plan.legs[0].limit_price),
        "last_leg_pct": _pct_gap(current_price, plan.legs[-1].limit_price),
        "metadata": dict(plan.metadata),
        "risk_notes": list(plan.risk_notes),
        "skip_reason": plan.skip_reason,
    }


def _diff_against_expected(actual, expected):
    problems = []
    if expected.get("should_skip"):
        if not actual["skip"]:
            problems.append(f"expected skip but built plan: {pformat(actual)}")
        elif expected.get("skip_code") != actual.get("skip_code"):
            problems.append(f"skip_code mismatch expected={expected.get('skip_code')} actual={actual.get('skip_code')}")
        return problems

    if actual["skip"]:
        problems.append(f"unexpected skip: {pformat(actual)}")
        return problems

    exact_keys = ["symbol", "side", "policy_version", "requested_quantity", "leg_count", "quantity_split"]
    for key in exact_keys:
        if actual.get(key) != expected.get(key):
            problems.append(f"{key} mismatch expected={expected.get(key)} actual={actual.get(key)}")

    relation = expected.get("price_relation")
    prices = actual["limit_prices"]
    current_price = float(prices[0]) / (1.0 - actual["first_leg_pct"]) if relation == "strictly_below_current" else float(prices[0]) / (1.0 + actual["first_leg_pct"]) if relation == "strictly_above_current" else None
    if relation == "strictly_below_current" and not all(prices[i] > prices[i + 1] for i in range(len(prices) - 1)):
        problems.append(f"BUY ladder ordering drift: {prices}")
    if relation == "strictly_above_current" and not all(prices[i] < prices[i + 1] for i in range(len(prices) - 1)):
        problems.append(f"SELL ladder ordering drift: {prices}")

    lo, hi = expected.get("first_leg_pct_range", [None, None])
    if lo is not None and not (lo <= actual["first_leg_pct"] <= hi):
        problems.append(f"first_leg_pct out of range expected=[{lo}, {hi}] actual={actual['first_leg_pct']}")
    lo, hi = expected.get("last_leg_pct_range", [None, None])
    if lo is not None and not (lo <= actual["last_leg_pct"] <= hi):
        problems.append(f"last_leg_pct out of range expected=[{lo}, {hi}] actual={actual['last_leg_pct']}")

    for key, value in (expected.get("required_metadata") or {}).items():
        if actual["metadata"].get(key) != value:
            problems.append(f"metadata[{key}] mismatch expected={value} actual={actual['metadata'].get(key)}")
    return problems


def _assert_fixture_parity(fixture_name: str):
    fixture, plan, skip = _make_plan(fixture_name)
    actual = _semantic_view(plan, skip, fixture)
    expected = fixture["expected"]
    problems = _diff_against_expected(actual, expected)
    assert not problems, "Decision parity drift for " + fixture_name + "\n" + "\n".join(problems) + "\nACTUAL=\n" + pformat(actual) + "\nEXPECTED=\n" + pformat(expected)


def test_us_open_buy_decision_parity():
    _assert_fixture_parity("decision_parity_us_open_buy_fixture.json")


def test_kr_open_sell_decision_parity():
    _assert_fixture_parity("decision_parity_kr_open_sell_fixture.json")


def test_budget_skip_decision_parity():
    _assert_fixture_parity("decision_parity_budget_skip_fixture.json")
