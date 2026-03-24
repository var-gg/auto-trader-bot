from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from app.features.kis_test.models.kis_test_models import BootstrapRequest
from app.features.kis_test.services.bootstrap_service import BootstrapService
from app.features.premarket.services.headline_risk_service import HeadlineRiskService
from app.features.premarket.services.pm_open_session_service import allocate_symbol_budgets_pm
from app.features.premarket.services.pm_signal_service import PMSignalService
from app.features.premarket.services.pm_signal_service_v2 import PMSignalServiceV2
from app.features.premarket.utils.pm_ladder_generator import generate_pm_adaptive_ladder, qty_from_budget
from app.features.trading_hybrid.policy.tuning import Tuning
from app.features.trading_hybrid.repositories.order_repository import extract_reject_reason
from app.features.trading_hybrid.services import executor_service
from app.features.trading_hybrid.services.intraday_session_service import plan_intraday_actions
from app.features.trading_hybrid.engines import runbooks

from tests.golden.golden_utils import assert_matches_golden, load_fixture


def _canonical_bootstrap_response(resp):
    return {
        "overall_success": resp.overall_success,
        "step_order": [s.step_name for s in resp.steps],
        "successful_steps": resp.successful_steps,
        "failed_steps": resp.failed_steps,
        "skipped_steps": resp.skipped_steps,
        "step_summaries": {s.step_name: s.result_summary for s in resp.steps},
    }


def test_bootstrap_golden(monkeypatch):
    fixture = load_fixture("bootstrap_request.json")
    svc = BootstrapService(db=None)

    async def fake_refresh_tokens(self, threshold_hours):
        from app.features.kis_test.models.kis_test_models import BootstrapStepResult
        return BootstrapStepResult(step_name="token_refresh", step_description="KIS 토큰 갱신", success=True, duration_seconds=0.1, result_summary={"total_tokens": 2, "success_count": 2, "failure_count": 0}, error_message=None)

    async def fake_ingest_fred(self, lookback_days):
        from app.features.kis_test.models.kis_test_models import BootstrapStepResult
        return BootstrapStepResult(step_name="fred_ingest", step_description="FRED 매크로 데이터 수집", success=True, duration_seconds=0.1, result_summary={"status": "completed", "series_count": 3, "rows_upserted": 42}, error_message=None)

    async def fake_ingest_yahoo(self, period):
        from app.features.kis_test.models.kis_test_models import BootstrapStepResult
        return BootstrapStepResult(step_name="yahoo_ingest", step_description="Yahoo Finance 데이터 수집", success=True, duration_seconds=0.1, result_summary={"status": "completed", "total_symbols": 5, "successful_symbols": 5, "failed_symbols": 0}, error_message=None)

    async def fake_refresh_risk(self):
        from app.features.kis_test.models.kis_test_models import BootstrapStepResult
        return BootstrapStepResult(step_name="risk_refresh", step_description="프리마켓 리스크 스냅샷 갱신", success=True, duration_seconds=0.1, result_summary={"status": "completed", "scope": "GLOBAL", "snapshot_id": 7001}, error_message=None)

    async def fake_update_signals(self):
        from app.features.kis_test.models.kis_test_models import BootstrapStepResult
        return BootstrapStepResult(step_name="signal_update", step_description="프리마켓 시그널 갱신", success=True, duration_seconds=0.1, result_summary={"status": "completed", "total": 12, "success": 12, "failed": 0, "up_count": 8, "down_count": 4}, error_message=None)

    monkeypatch.setattr(BootstrapService, "_refresh_tokens", fake_refresh_tokens)
    monkeypatch.setattr(BootstrapService, "_ingest_fred_data", fake_ingest_fred)
    monkeypatch.setattr(BootstrapService, "_ingest_yahoo_data", fake_ingest_yahoo)
    monkeypatch.setattr(BootstrapService, "_refresh_premarket_risk", fake_refresh_risk)
    monkeypatch.setattr(BootstrapService, "_update_signals", fake_update_signals)

    import asyncio
    req = BootstrapRequest(**fixture)
    resp = asyncio.run(svc.run_bootstrap(req))
    assert_matches_golden("bootstrap.golden.json", _canonical_bootstrap_response(resp))


def test_pm_signal_v1_golden():
    fixture = load_fixture("pm_signal_fixture.json")
    svc = PMSignalService.__new__(PMSignalService)
    up = [{**r, "ctx_vec": np.array(r["ctx_vec"], dtype=float)} for r in fixture["up_ranked"]]
    dn = [{**r, "ctx_vec": np.array(r["ctx_vec"], dtype=float)} for r in fixture["dn_ranked"]]
    signal_1d, best_target, reason = svc._compute_signal(up, dn, np.array(fixture["q_ctx"], dtype=float), fixture["config"])
    actual = {
        "signal_1d": signal_1d,
        "best_target": best_target,
        "reason": reason,
        "up_count": len(up),
        "down_count": len(dn),
    }
    assert_matches_golden("pm_signal_v1.golden.json", actual)


def test_pm_signal_v2_golden():
    fixture = load_fixture("pm_signal_fixture.json")
    svc = PMSignalServiceV2.__new__(PMSignalServiceV2)
    up = [{**r, "ctx_vec": np.array(r["ctx_vec"], dtype=float)} for r in fixture["up_ranked"]]
    dn = [{**r, "ctx_vec": np.array(r["ctx_vec"], dtype=float)} for r in fixture["dn_ranked"]]
    signal_1d, best_target, reason, _diag, _prob = svc._compute_signal_v2(up, dn, np.array(fixture["q_ctx"], dtype=float), fixture["config"])
    actual = {
        "signal_1d": signal_1d,
        "best_target": best_target,
        "reason": reason,
        "up_count": len(up),
        "down_count": len(dn),
    }
    assert_matches_golden("pm_signal_v2.golden.json", actual)


def test_pm_risk_refresh_golden():
    fixture = load_fixture("pm_risk_normalize_fixture.json")
    svc = HeadlineRiskService.__new__(HeadlineRiskService)
    out = svc._normalize(fixture)
    actual = {
        "risk_score": out["risk_score"],
        "regime_score": out["regime_score"],
        "confidence": out["confidence"],
        "shock_type": out["shock_type"],
        "severity_band": out["severity_band"],
        "discount_multiplier": out["discount_multiplier"],
        "sell_markup_multiplier": out["sell_markup_multiplier"],
        "ttl_minutes": out["ttl_minutes"],
        "reason_short": out["reason_short"],
    }
    assert_matches_golden("pm_risk_refresh.golden.json", actual)


def test_pm_open_plan_golden():
    fixture = load_fixture("pm_open_candidates.json")
    import logging
    selected, budget_map, _skipped = allocate_symbol_budgets_pm(fixture["candidates"], fixture["caps"]["swing_cap_cash"], "US", logging.getLogger("test"))
    actual = {
        "selected_symbols": [c["symbol"] for c in selected],
        "budget_map": budget_map,
        "ladder_shapes": {},
    }
    for c in selected:
        qty = qty_from_budget(c["current_price"], budget_map[c["ticker_id"]])
        legs, _desc = generate_pm_adaptive_ladder("BUY", c["signal_1d"], 0.0, c["current_price"], qty, "US", fixture["ladder_params"]["buy"], c["tb_label"], c["iae_1_3"], c["has_long_recommendation"])
        actual["ladder_shapes"][c["symbol"]] = {
            "legs": len(legs),
            "quantities": [leg["quantity"] for leg in legs],
            "prices": [leg["limit_price"] for leg in legs],
        }
    assert_matches_golden("pm_open_plan.golden.json", actual)


def test_intraday_golden_preserves_current_error():
    fixture = load_fixture("intraday_preds.json")
    try:
        plan_intraday_actions(
            datetime(2026, 3, 24, 10, 5, tzinfo=timezone(timedelta(hours=9))),
            "US",
            "USD",
            fixture["preds"],
            fixture["account"],
            fixture["positions"],
            None,
            fixture["caps"],
            Tuning.default_for_market("US"),
        )
        actual = {"error_type": None, "message_contains": None}
    except Exception as e:  # preserve current odd behavior; do not fix in this stage
        actual = {"error_type": type(e).__name__, "message_contains": str(e)}
    assert_matches_golden("intraday_plan.golden.json", actual)


def test_fill_collection_sync_golden(monkeypatch):
    fixture = load_fixture("fill_collection_fixture.json")
    calls = []

    class FakePnl:
        def __init__(self, db):
            pass
        async def collect_and_save_realized_pnl(self, start_date_str, end_date_str):
            calls.append("pnl.collect_and_save_realized_pnl")
            return fixture["pnl_result"]

    class FakeAsset:
        def __init__(self, db):
            pass
        def collect_ovrs_account_snapshot(self, account_uid=None):
            calls.append("asset.collect_ovrs_account_snapshot")
            return fixture["snapshot_result"]

    class FakeOverseasFill:
        def __init__(self, db):
            pass
        async def collect_overseas_fills(self, days_back=7):
            calls.append("fills.collect_overseas_fills")
            return fixture["fill_result"]

    monkeypatch.setattr(runbooks, "TradeRealizedPnlService", FakePnl)
    monkeypatch.setattr(runbooks, "AssetSnapshotService", FakeAsset)
    monkeypatch.setattr(runbooks, "OverseasFillCollectionService", FakeOverseasFill)

    import asyncio
    asyncio.run(runbooks._sync_profit_and_account(db=None, market=fixture["market"]))
    actual = {
        "market": fixture["market"],
        "pnl_total_saved": fixture["pnl_result"]["total_saved"],
        "snapshot_id": fixture["snapshot_result"]["snapshot_id"],
        "fill_processed_count": fixture["fill_result"]["processed_count"],
        "fill_upserted_count": fixture["fill_result"]["upserted_count"],
        "call_order": calls,
    }
    assert_matches_golden("fill_collection_sync.golden.json", actual)


def test_write_intent_golden(monkeypatch):
    fixture = load_fixture("golden_write_intent_fixture.json")
    buy_plans = fixture["buy_plans"]
    sell_plans = fixture["sell_plans"]
    skipped = fixture["skipped"]
    calls = {"batches": [], "plans": []}
    submitted_leg_count = 0

    def fake_create_order_batch(db, asof_kst, mode, currency, meta):
        calls["batches"].append({"mode": mode, "currency": currency, "meta": meta})
        return 100 if mode == "BUY" else 200

    def fake_create_plan_with_legs(db, batch_id, plan, action, test_mode=False):
        nonlocal submitted_leg_count
        calls["plans"].append({"batch_id": batch_id, "ticker_id": plan["ticker_id"], "note": plan["note"], "legs": len(plan["legs"]), "action": action})
        submitted_leg_count += len(plan["legs"])
        return batch_id + plan["ticker_id"]

    monkeypatch.setattr(executor_service, "create_order_batch", fake_create_order_batch)
    monkeypatch.setattr(executor_service, "create_plan_with_legs", fake_create_plan_with_legs)

    now_kst = datetime.fromisoformat(fixture["now_kst"])
    response = executor_service.persist_batch_and_execute(None, now_kst, fixture["currency"], buy_plans, sell_plans, skipped, fixture["batch_meta"], test_mode=True)
    code, message = extract_reject_reason({"rt_cd": "1", "msg_cd": "OPSQ2001", "msg1": "주문가능수량이 부족합니다.", "output": {}})

    actual = {
        "batch_modes": [b["mode"] for b in calls["batches"]],
        "batch_meta_phase": calls["batches"][0]["meta"]["phase"],
        "buy_count": response["summary"]["buy_count"],
        "sell_count": response["summary"]["sell_count"],
        "skip_count": response["summary"]["skip_count"],
        "buy_plan_shape": calls["plans"][0],
        "sell_plan_shape": calls["plans"][1],
        "submitted_leg_count": submitted_leg_count,
        "reject_reason_example": {"code": code, "message": message},
    }
    assert_matches_golden("write_intent.golden.json", actual)
