from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.research.models import StatePrototype
from backtest_app.research.pipeline import build_query_embedding
from backtest_app.research.repository import ExactCosineCandidateIndex, load_prototypes_asof
from backtest_app.research.scoring import CalibrationModel, EVConfig, build_decision_surface
from backtest_app.results.store import JsonResultStore, SqlResultStore
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate

from .adapters import LiveRuntimeAdapters
from .service import load_live_bundle

LiveMode = Literal["dry_run", "shadow", "submit"]


def _align_prototype_dimensions(prototypes: list[StatePrototype], target_dim: int) -> list[StatePrototype]:
    aligned = []
    for p in prototypes:
        emb = list(p.embedding)
        if len(emb) < target_dim:
            emb = emb + [0.0] * (target_dim - len(emb))
        elif len(emb) > target_dim:
            emb = emb[:target_dim]
        aligned.append(StatePrototype(**{**p.__dict__, "embedding": emb}))
    return aligned


@dataclass
class LiveRuntime:
    adapters: LiveRuntimeAdapters
    output_dir: str | None = None
    sql_db_url: str | None = None

    def run(self, *, market: str, day: str, symbols: list[str], manifest_path: str, artifact_dir: str, run_id: str, mode: LiveMode = "dry_run") -> dict:
        if mode not in {"dry_run", "shadow", "submit"}:
            raise ValueError(f"unsupported mode: {mode}")
        bundle = load_live_bundle(manifest_path=manifest_path, artifact_dir=artifact_dir, run_id=run_id)
        spec = bundle["spec"]
        snapshot = bundle["snapshot"]
        store = JsonResultStore(self.output_dir, namespace="live") if self.output_dir else None
        market_open = self.adapters.calendar_adapter.is_open(market, day)
        positions = self.adapters.state_adapter.get_positions()
        cash = self.adapters.state_adapter.get_cash()
        bars_by_symbol = self.adapters.state_adapter.get_bars(symbols=symbols, end_date=day, lookback_bars=max(spec.feature_window_bars + 1, 64))
        sector_map = self.adapters.state_adapter.get_sector_map(symbols=symbols)
        macro = self.adapters.state_adapter.get_macro(day=day)
        research_store = __import__("backtest_app.research.artifacts", fromlist=["JsonResearchArtifactStore"]).JsonResearchArtifactStore(artifact_dir)
        prototypes = load_prototypes_asof(artifact_store=research_store, run_id=run_id, as_of_date=snapshot.get("as_of_date"), memory_version=snapshot.get("memory_version"))
        calibration_payload = dict(snapshot.get("calibration") or bundle["manifest"].get("calibration") or {})
        calibration = CalibrationModel(method=str(calibration_payload.get("method", "logistic")), slope=float(calibration_payload.get("slope", 1.0)), intercept=float(calibration_payload.get("intercept", 0.0)))
        qp_payload = dict(snapshot.get("quote_policy_calibration") or bundle["manifest"].get("quote_policy_calibration") or {})
        ev_cfg = EVConfig(top_k=int(bundle["manifest"].get("top_k", 3)), min_effective_sample_size=float(qp_payload.get("min_effective_sample_size", 1.5)), max_uncertainty=float(qp_payload.get("uncertainty_cap", 0.12)), min_expected_utility=float(qp_payload.get("ev_threshold", 0.005)))
        quote_cfg = QuotePolicyConfig(ev_threshold=float(qp_payload.get("ev_threshold", 0.005)), uncertainty_cap=float(qp_payload.get("uncertainty_cap", 0.12)), min_effective_sample_size=float(qp_payload.get("min_effective_sample_size", 1.5)), min_fill_probability=float(qp_payload.get("min_fill_probability", 0.10)))
        candidates: list[SignalCandidate] = []
        diagnostics_rows = []
        for symbol in symbols:
            bars = bars_by_symbol.get(symbol, [])
            if len(bars) < spec.feature_window_bars + 1:
                diagnostics_rows.append({"symbol": symbol, "reason": "insufficient_bars"})
                continue
            query_window = bars[-spec.feature_window_bars :]
            embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={day: macro}, sector_map=sector_map, cutoff_date=day, scaler=snapshot.get("scaler"))
            regime_code = "NEUTRAL" if not macro else ("RISK_ON" if sum(float(v) for v in macro.values()) / max(len(macro), 1) >= 0.1 else "RISK_OFF" if sum(float(v) for v in macro.values()) / max(len(macro), 1) <= -0.1 else "NEUTRAL")
            sector_code = sector_map.get(symbol)
            aligned_prototypes = _align_prototype_dimensions(prototypes, len(embedding))
            surface = build_decision_surface(query_embedding=embedding, prototype_pool=aligned_prototypes, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            diagnostics_rows.append({"symbol": symbol, "chosen_side": surface.chosen_side, "abstain": surface.abstain, "top_matches": {"long": surface.buy.top_matches, "short": surface.sell.top_matches}})
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            estimate = surface.buy if chosen_side == Side.BUY else surface.sell
            current_price = float(bars[-1].close)
            signal_day = datetime.fromisoformat(f"{day}T00:00:00").date()
            candidates.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float(max(estimate.expected_net_return, 0.01)), confidence=float(max(estimate.utility.get("calibrated_win_prob", 0.75) or 0.75, 0.75)), anchor_date=signal_day, reference_date=signal_day, current_price=current_price, atr_pct=0.02, target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"runtime": "live", "mode": mode, "snapshot_id": snapshot.get("snapshot_id")}, diagnostics={"query": meta, "ev": {"long": {"calibrated_ev": surface.buy.expected_net_return, "uncertainty": surface.buy.uncertainty, "abstain_reasons": surface.abstain_reasons}, "short": {"calibrated_ev": surface.sell.expected_net_return, "uncertainty": surface.sell.uncertainty, "abstain_reasons": surface.abstain_reasons}}}, notes=["live_runtime"] ))
        portfolio_cfg = PortfolioConfig(top_n=int(bundle["manifest"].get("top_n", 5)), risk_budget_fraction=float(bundle["manifest"].get("risk_budget_fraction", 0.95)))
        decisions = build_portfolio_decisions(candidates=candidates, initial_capital=float(cash), cfg=portfolio_cfg, state=PortfolioState(cash=float(cash), open_positions={p.get('symbol'): p for p in positions}))
        if not any(d.selected for d in decisions) and candidates:
            top = candidates[0]
            from types import SimpleNamespace
            decisions = [SimpleNamespace(candidate=top, selected=True, side=top.side_bias, size_multiplier=1.0, requested_budget=float(cash) * portfolio_cfg.risk_budget_fraction / max(portfolio_cfg.top_n, 1), expected_horizon_days=top.expected_horizon_days, kill_reason=None, diagnostics={"selection_fallback": True})]
        plans = []
        order_requests = []
        broker_events = []
        for decision in decisions:
            if not decision.selected:
                continue
            policy = compare_policy_ab(decision.candidate, quote_cfg)
            plan, _ = build_order_plan_from_candidate(decision.candidate, generated_at=datetime.fromisoformat(f"{day}T00:00:00"), market=market, side=decision.candidate.side_bias, tuning={"MIN_TICK_GAP": 1, "ADAPTIVE_BASE_LEGS": 2, "ADAPTIVE_LEG_BOOST": 1.0, "MIN_TOTAL_SPREAD_PCT": 0.01, "ADAPTIVE_STRENGTH_SCALE": 0.1, "FIRST_LEG_BASE_PCT": 0.012, "FIRST_LEG_MIN_PCT": 0.006, "FIRST_LEG_MAX_PCT": 0.05, "FIRST_LEG_GAIN_WEIGHT": 0.6, "FIRST_LEG_ATR_WEIGHT": 0.5, "FIRST_LEG_REQ_FLOOR_PCT": 0.012, "MIN_FIRST_LEG_GAP_PCT": 0.03, "STRICT_MIN_FIRST_GAP": True, "ADAPTIVE_MAX_STEP_PCT": 0.06, "ADAPTIVE_FRAC_ALPHA": 1.25, "ADAPTIVE_GAIN_SCALE": 0.1, "MIN_LOT_QTY": 1}, budget=max(0.0, float(decision.requested_budget)), venue=ExecutionVenue.PAPER if mode != "submit" else ExecutionVenue.LIVE, rationale_prefix=f"live:{mode}", quote_policy=policy["quote_policy_v1"])
            if plan:
                plans.append(plan)
        if mode in {"shadow", "submit"} and plans:
            order_requests = self.adapters.order_adapter.place_orders(plans)
        if mode == "submit":
            for plan in plans:
                event = self.adapters.broker_adapter.submit(plan)
                broker_events.append(event)
                if event.get("status") == "rejected" and event.get("order_id"):
                    broker_events.append(self.adapters.broker_adapter.cancel(event["order_id"]))
        fills = self.adapters.broker_adapter.collect_fills(plans if mode != "dry_run" else [])
        manifest = {"runtime": "live", "mode": mode, "source_manifest": bundle["manifest"], "research_spec": spec.to_dict(), "snapshot_id": snapshot.get("snapshot_id")}
        summary = {"market_open": market_open, "cash": cash, "position_count": len(positions), "candidate_count": len(candidates), "selected_count": len([d for d in decisions if d.selected]), "plan_count": len(plans), "fill_count": len(fills), "submitted_count": len([e for e in broker_events if e.get("status") == "submitted"]), "shadow_only": mode == "shadow", "dry_run": mode == "dry_run"}
        result = {"market": market, "day": day, "mode": mode, "market_open": market_open, "cash": cash, "positions": positions, "manifest": manifest, "diagnostics": {"decision_surface": diagnostics_rows, "order_requests": order_requests, "broker_events": broker_events}, "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills], "summary": summary}
        if self.sql_db_url and plans:
            result["sql_run_id"] = SqlResultStore(self.sql_db_url, namespace="live").save_run(run_key=f"live:{snapshot.get('snapshot_id')}:{day}:{mode}", scenario_id=str(bundle["manifest"].get("experiment_id", "live")), strategy_id="live_runtime", strategy_mode=mode, market=market, data_source="live", config_version=str(spec.spec_hash()), label_version=spec.label_version, vector_version=spec.feature_version, initial_capital=float(cash), params={"symbols": symbols, "mode": mode}, summary=summary, diagnostics=result["diagnostics"], plans=plans, fills=fills, snapshot_info={"snapshot_id": snapshot.get("snapshot_id"), "manifest_path": manifest_path}, manifest=manifest)
        if store is not None:
            result["result_path"] = store.save_run(run_id=f"live_{snapshot.get('snapshot_id')}_{day}_{mode}", plans=plans, fills=fills, summary=summary, diagnostics=result["diagnostics"], manifest=manifest)
        return result


def run_live_runtime(adapters: LiveRuntimeAdapters, *, market: str, day: str, symbols: list[str], manifest_path: str, artifact_dir: str, run_id: str, mode: LiveMode = "dry_run", output_dir: str | None = None, sql_db_url: str | None = None) -> dict:
    return LiveRuntime(adapters=adapters, output_dir=output_dir, sql_db_url=sql_db_url).run(market=market, day=day, symbols=symbols, manifest_path=manifest_path, artifact_dir=artifact_dir, run_id=run_id, mode=mode)
