from __future__ import annotations

from datetime import datetime
from pprint import pformat

from backtest_app.configs.models import BacktestConfig, BacktestScenario, RunnerRequest
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.runner.cli import run_backtest
from live_app.application.context import RunContext
from live_app.application.planning_commands import BuildOrderPlanCommand, LivePlanningInput, OutcomeLabelParityQuery
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate
from tests.golden.golden_utils import load_fixture


PM_BACKTEST_FIXTURE = r"A:\vargg-workspace\30_trading\auto-trader-bot\tests\fixtures\backtest_historical_fixture.json"


def _canonical_plan_view(plan):
    if plan is None:
        return None
    return {
        "symbol": plan.symbol,
        "side": plan.side.value,
        "policy_version": plan.metadata.get("policy_version"),
        "decision_metadata": {
            "anchor_date": plan.metadata.get("anchor_date"),
            "reverse_breach_day": plan.metadata.get("reverse_breach_day"),
            "source": plan.metadata.get("source"),
        },
        "requested_budget": plan.requested_budget,
        "requested_quantity": plan.requested_quantity,
        "legs": [
            {
                "leg_id": leg.leg_id,
                "quantity": leg.quantity,
                "limit_price": leg.limit_price,
                "order_type": leg.order_type.value,
            }
            for leg in plan.legs
        ],
        "risk_notes": plan.risk_notes,
        "skip_reason": plan.skip_reason,
    }


def _assert_same(lhs, rhs):
    assert lhs == rhs, "Parity drift detected\nLIVE:\n" + pformat(lhs) + "\nBACKTEST:\n" + pformat(rhs)


def _backtest_plan_from_fixture(fixture_name: str):
    fixture = load_fixture(fixture_name)
    c = fixture["candidate"]
    candidate = SignalCandidate(
        symbol=c["symbol"],
        ticker_id=c["ticker_id"],
        market=MarketCode(fixture["market"]),
        side_bias=Side(fixture["side"]),
        signal_strength=float(c["signal_strength"]),
        current_price=float(c["current_price"]),
        atr_pct=float(c["atr_pct"]),
        outcome_label=OutcomeLabel(c["tb_label"]),
        reverse_breach_day=c.get("reverse_breach_day"),
        provenance={"has_long_recommendation": c.get("has_long_recommendation", False), "policy_version": c.get("policy_version"), "source": c.get("source")},
        diagnostics={"iae_1_3": c.get("iae_1_3")},
    )
    generated_at = datetime.fromisoformat(fixture["generated_at"])
    return build_order_plan_from_candidate(
        candidate,
        generated_at=generated_at,
        market=fixture["market"],
        side=Side(fixture["side"]),
        tuning=fixture["tuning"],
        budget=float(fixture["budget"]),
        venue=ExecutionVenue.BACKTEST,
        rationale_prefix="backtest-plan",
    )


def _live_plan_from_fixture(fixture_name: str):
    fixture = load_fixture(fixture_name)
    generated_at = datetime.fromisoformat(fixture["generated_at"])
    ctx = RunContext(actor="test", channel="parity", invoked_at=generated_at, metadata={"fixture": fixture_name})
    return BuildOrderPlanCommand().execute(
        LivePlanningInput(
            market=fixture["market"],
            candidate=fixture["candidate"],
            tuning=fixture["tuning"],
            budget=float(fixture["budget"]),
            side=fixture["side"],
            generated_at=generated_at,
            rationale_prefix="backtest-plan",
            venue=ExecutionVenue.BACKTEST,
        ),
        ctx,
    )


def test_pm_open_plan_parity():
    live_plan, live_skip = _live_plan_from_fixture("parity_pm_open_fixture.json")
    bt_plan, bt_skip = _backtest_plan_from_fixture("parity_pm_open_fixture.json")
    _assert_same(live_skip, bt_skip)
    _assert_same(_canonical_plan_view(live_plan), _canonical_plan_view(bt_plan))


def test_intraday_plan_parity():
    live_plan, live_skip = _live_plan_from_fixture("parity_intraday_fixture.json")
    bt_plan, bt_skip = _backtest_plan_from_fixture("parity_intraday_fixture.json")
    _assert_same(live_skip, bt_skip)
    _assert_same(_canonical_plan_view(live_plan), _canonical_plan_view(bt_plan))


def test_risk_gate_skip_parity():
    live_plan, live_skip = _live_plan_from_fixture("parity_risk_skip_fixture.json")
    bt_plan, bt_skip = _backtest_plan_from_fixture("parity_risk_skip_fixture.json")
    assert live_plan is None and bt_plan is None
    _assert_same(live_skip, bt_skip)


def test_outcome_label_parity():
    ctx = RunContext(actor="test", channel="parity")
    query = OutcomeLabelParityQuery()
    assert query.execute(12.0, ctx) == OutcomeLabel.WIN
    assert query.execute(-3.0, ctx) == OutcomeLabel.LOSS
    assert query.execute(0.0, ctx) == OutcomeLabel.FLAT


def test_backtest_runner_uses_same_domain_language_for_fixture_case():
    req = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="parity-demo",
            market="US",
            start_date="2026-03-01",
            end_date="2026-03-24",
            symbols=["NVDA", "AAPL"],
        ),
        config=BacktestConfig(initial_capital=10000.0),
    )
    result = run_backtest(req, PM_BACKTEST_FIXTURE)
    assert result["summary"]["total_plans"] >= 1
    assert all("symbol" in p for p in result["plans"])
