from __future__ import annotations

from backtest_app.configs.models import BacktestConfig, BacktestScenario, RunnerRequest
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.runner.cli import run_backtest
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, Side
from tests.golden.golden_utils import load_fixture


FIXTURE_PATH = r"A:\vargg-workspace\30_trading\auto-trader-bot\tests\fixtures\backtest_historical_fixture.json"


def test_backtest_loader_supplies_features():
    data = JsonHistoricalDataLoader().load(FIXTURE_PATH)
    assert data.candidates
    first = data.candidates[0]
    assert "derived_bar_features" in first.provenance
    assert "external_vector" in first.provenance


def test_simulated_broker_is_deterministic_for_same_plan():
    data = JsonHistoricalDataLoader().load(FIXTURE_PATH)
    candidate = data.candidates[0]
    plan, skip = build_order_plan_from_candidate(
        candidate,
        generated_at=data.market_snapshot.as_of,
        market="US",
        side=Side.BUY,
        tuning={
            "MIN_TICK_GAP": 1,
            "ADAPTIVE_BASE_LEGS": 2,
            "ADAPTIVE_LEG_BOOST": 1.0,
            "MIN_TOTAL_SPREAD_PCT": 0.01,
            "ADAPTIVE_STRENGTH_SCALE": 0.1,
            "FIRST_LEG_BASE_PCT": 0.012,
            "FIRST_LEG_MIN_PCT": 0.006,
            "FIRST_LEG_MAX_PCT": 0.05,
            "FIRST_LEG_GAIN_WEIGHT": 0.6,
            "FIRST_LEG_ATR_WEIGHT": 0.5,
            "FIRST_LEG_REQ_FLOOR_PCT": 0.012,
            "MIN_FIRST_LEG_GAP_PCT": 0.03,
            "STRICT_MIN_FIRST_GAP": True,
            "ADAPTIVE_MAX_STEP_PCT": 0.06,
            "ADAPTIVE_FRAC_ALPHA": 1.25,
            "ADAPTIVE_GAIN_SCALE": 0.1,
            "MIN_LOT_QTY": 1,
        },
        budget=5000,
        venue=ExecutionVenue.BACKTEST,
    )
    assert skip is None
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=0.0, fee_bps=0.0, allow_partial_fills=True, partial_fill_ratio=0.5))
    fills1 = [f.to_dict() for f in broker.simulate_plan(plan, data.bars_by_symbol[plan.symbol])]
    fills2 = [f.to_dict() for f in broker.simulate_plan(plan, data.bars_by_symbol[plan.symbol])]
    assert fills1 == fills2


def test_backtest_runner_can_persist_results(tmp_path):
    req = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="demo-us-open",
            market="US",
            start_date="2026-03-01",
            end_date="2026-03-24",
            symbols=["NVDA", "AAPL"],
        ),
        config=BacktestConfig(initial_capital=10000.0),
    )
    result = run_backtest(req, FIXTURE_PATH, output_dir=str(tmp_path))
    assert result["summary"]["total_plans"] >= 1
    assert "result_path" in result
