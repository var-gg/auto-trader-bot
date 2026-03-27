from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from types import SimpleNamespace
from statistics import mean
from time import perf_counter
from typing import Dict, List, Tuple

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.features import build_multiscale_feature_vector, compute_bar_features, fit_feature_scaler
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate

from .artifacts import JsonResearchArtifactStore
from .labeling import EventLabelingConfig, build_event_outcome_record, label_event_window
from .models import EventOutcomeRecord, ResearchAnchor
from .prototype import PrototypeConfig, build_state_prototypes_from_event_memory
from .repository import ExactCosineCandidateIndex, load_prototypes_asof
from .scoring import CalibrationModel, CandidateScore, EVConfig, ScoringConfig, build_decision_surface, estimate_expected_value, score_candidates_exact

DECISION_CONVENTION = "EOD_T_SIGNAL__T1_OPEN_EXECUTION"


def _ev_config_from_metadata(metadata: dict | None = None, *, top_k: int = 3, abstain_margin: float | None = None) -> EVConfig:
    meta = metadata or {}
    resolved_abstain_margin = abstain_margin if abstain_margin is not None else meta.get("abstain_margin", 0.05)
    return EVConfig(
        top_k=int(top_k),
        min_effective_sample_size=float(meta.get("quote_min_effective_sample_size", meta.get("min_effective_sample_size", 1.5)) or 1.5),
        max_uncertainty=float(meta.get("quote_uncertainty_cap", meta.get("max_uncertainty", 0.08)) or 0.08),
        min_expected_utility=float(meta.get("quote_ev_threshold", meta.get("min_expected_utility", 0.005)) or 0.005),
        min_regime_alignment=float(meta.get("quote_min_regime_alignment", meta.get("min_regime_alignment", 0.5)) or 0.5),
        max_return_interval_width=float(meta.get("quote_max_return_interval_width", meta.get("max_return_interval_width", 0.08)) or 0.08),
        abstain_margin=float(resolved_abstain_margin or 0.0),
        diagnostic_disable_lower_bound_gate=str(meta.get("diagnostic_disable_lower_bound_gate", meta.get("disable_lower_bound_gate", "false"))).strip().lower() in {"1", "true", "yes", "on"},
    )


def _default_spec(feature_window_bars: int = 60, horizon_days: int = 5) -> ResearchExperimentSpec:
    return ResearchExperimentSpec(feature_window_bars=feature_window_bars, horizon_days=horizon_days, lookback_horizons=[horizon_days])


def _regime_from_macro(macro_payload: Dict[str, float]) -> str:
    if not macro_payload:
        return "NEUTRAL"
    avg = mean(float(v) for v in macro_payload.values())
    if avg >= 0.1:
        return "RISK_ON"
    if avg <= -0.1:
        return "RISK_OFF"
    return "NEUTRAL"


def _bars_until_date(bars: List[HistoricalBar], cutoff_date: str | None) -> List[HistoricalBar]:
    return [bar for bar in bars if not cutoff_date or str(bar.timestamp)[:10] <= cutoff_date]


def _market_proxy_bars(bars_by_symbol: Dict[str, List[HistoricalBar]], cutoff_date: str | None = None) -> List[HistoricalBar]:
    rows: List[HistoricalBar] = []
    series = [_bars_until_date(bars, cutoff_date) for bars in bars_by_symbol.values() if bars]
    series = [bars for bars in series if bars]
    if not series:
        return rows
    for idx in range(max(len(b) for b in series)):
        bucket = [bars[idx] for bars in series if idx < len(bars)]
        if not bucket:
            continue
        rows.append(HistoricalBar(symbol="MKT", timestamp=bucket[-1].timestamp, open=mean([b.open for b in bucket]), high=mean([b.high for b in bucket]), low=mean([b.low for b in bucket]), close=mean([b.close for b in bucket]), volume=mean([b.volume for b in bucket])))
    return rows


def _sector_proxy_bars(symbol: str, bars_by_symbol: Dict[str, List[HistoricalBar]], sector_map: Dict[str, str], cutoff_date: str | None = None) -> List[HistoricalBar]:
    sector = sector_map.get(symbol)
    peers = {s: bars for s, bars in bars_by_symbol.items() if s != symbol and sector and sector_map.get(s) == sector}
    return _market_proxy_bars(peers or {symbol: bars_by_symbol.get(symbol, [])}, cutoff_date=cutoff_date)


def build_query_embedding(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], cutoff_date: str | None, spec: ResearchExperimentSpec | None = None, scaler=None) -> tuple[list[float], dict]:
    sector_code = sector_map.get(symbol)
    shape_horizons = list((spec.lookback_horizons if spec and spec.lookback_horizons else [spec.horizon_days] if spec else []) or [])
    fv = build_multiscale_feature_vector(symbol=symbol, bars=bars, market_bars=_market_proxy_bars(bars_by_symbol, cutoff_date=cutoff_date), sector_bars=_sector_proxy_bars(symbol, bars_by_symbol, sector_map, cutoff_date=cutoff_date), macro_history=macro_history, sector_code=sector_code, scaler=scaler, shape_horizons=shape_horizons)
    return fv.embedding, {"shape_features": fv.shape_features, "residual_features": fv.residual_features, "context_features": fv.context_features, "shape_vector": fv.shape_vector, "ctx_vector": fv.ctx_vector, **fv.metadata}


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def _side_diag(ev, surface, side: str) -> dict:
    utility = dict(getattr(ev, "diagnostics", {}).get("ev_decomposition") or {})
    interval = dict(getattr(ev, "diagnostics", {}).get("interval") or {})
    top_matches = list(getattr(ev, "top_matches", []) or [])
    support_counts = [float(((m or {}).get("why") or {}).get("support", 0.0) or 0.0) for m in top_matches]
    summary = []
    for match in top_matches[:3]:
        why = dict((match or {}).get("why") or {})
        summary.append({
            "prototype_id": match.get("prototype_id"),
            "representative_symbol": match.get("representative_symbol"),
            "weight": match.get("weight"),
            "similarity": why.get("similarity"),
            "support": why.get("support"),
            "expected_return": match.get("expected_return"),
            "uncertainty": match.get("uncertainty"),
        })
    return {
        "side": side,
        "expected_net_return": getattr(ev, "expected_net_return", 0.0),
        "fallback_raw_ev": utility.get("fallback_raw_ev", getattr(ev, "expected_utility", 0.0)),
        "q10": interval.get("q10", 0.0),
        "q50": interval.get("q50", 0.0),
        "q90": interval.get("q90", 0.0),
        "uncertainty": getattr(ev, "uncertainty", 0.0),
        "lower_bound": interval.get("q10", 0.0) - float(getattr(ev, "uncertainty", 0.0) or 0.0),
        "support_count": float(sum(support_counts)),
        "n_eff": getattr(ev, "effective_sample_size", 0.0),
        "p_target": utility.get("p_target_first", getattr(ev, "p_up_first", 0.0)),
        "p_stop": utility.get("p_stop_first", getattr(ev, "p_down_first", 0.0)),
        "p_flat": utility.get("p_flat", 0.0),
        "p_ambiguous": utility.get("p_ambiguous", 0.0),
        "p_no_trade": utility.get("p_no_trade", 0.0),
        "top_matches_summary": summary,
        "side_stats_summary": {
            "match_count": len(top_matches),
            "prototype_ids": [m.get("prototype_id") for m in top_matches[:3]],
            "representative_symbols": [m.get("representative_symbol") for m in top_matches[:3]],
            "mean_support": (sum(support_counts) / len(support_counts)) if support_counts else 0.0,
            "max_support": max(support_counts) if support_counts else 0.0,
            "abstain_reasons": list(getattr(ev, "abstain_reasons", []) or []),
            "decision_summary": (surface.diagnostics.get("decision_rule") or {}).get("why_summary"),
        },
    }


def _label_cfg(spec: ResearchExperimentSpec) -> EventLabelingConfig:
    return EventLabelingConfig(target_return_pct=spec.target_return_pct, stop_return_pct=spec.stop_return_pct, horizon_days=spec.horizon_days, fee_bps=spec.fee_bps, slippage_bps=spec.slippage_bps, flat_return_band_pct=spec.flat_return_band_pct)


def build_event_memory_asof(*, decision_date: str, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, lookback_bars: int = 5) -> dict:
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    label_cfg = _label_cfg(spec)
    event_records: List[EventOutcomeRecord] = []
    anchor_library: List[ResearchAnchor] = []
    anchor_feature_rows: List[dict] = []
    excluded_reasons: list[dict] = []
    for lib_symbol, lib_bars in bars_by_symbol.items():
        if len(lib_bars) < min_required_bars + spec.horizon_days + 2:
            excluded_reasons.append({"symbol": lib_symbol, "reason": "insufficient_bars"})
            continue
        lib_sector = sector_map.get(lib_symbol)
        for j in range(min_required_bars - 1, len(lib_bars) - spec.horizon_days - 1):
            feature_end_date = str(lib_bars[j].timestamp)[:10]
            outcome_end_date = str(lib_bars[j + spec.horizon_days].timestamp)[:10]
            if feature_end_date > decision_date:
                break
            if outcome_end_date >= decision_date:
                break
            history_window = lib_bars[j - spec.feature_window_bars + 1 : j + 1]
            future_window = lib_bars[j + 1 : j + 1 + spec.horizon_days]
            macro_payload = dict(macro_history_by_date.get(feature_end_date, {}))
            regime_code = _regime_from_macro(macro_payload)
            event = build_event_outcome_record(future_window, label_cfg)
            raw_embedding, feature_meta = build_query_embedding(symbol=lib_symbol, bars=history_window, bars_by_symbol=bars_by_symbol, macro_history={feature_end_date: macro_payload}, sector_map=sector_map, cutoff_date=feature_end_date, spec=spec)
            anchor_feature_rows.append({**feature_meta.get("shape_features", {}), **feature_meta.get("context_features", {})})
            event_records.append(EventOutcomeRecord(symbol=lib_symbol, event_date=feature_end_date, outcome_end_date=outcome_end_date, schema_version=spec.label_version, path_summary={**event.path_summary, "path_label": event.path_label, "feature_end_date": feature_end_date, "embedding": raw_embedding}, side_outcomes=event.side_payload, diagnostics={**event.diagnostics, "decision_cutoff": decision_date, "feature_end_date": feature_end_date, "embedding": raw_embedding, "shape_vector": raw_embedding[:3], "ctx_vector": raw_embedding[3:], "regime_code": regime_code, "sector_code": lib_sector, "liquidity_score": max(0.0, min(1.0, compute_bar_features(history_window).get("volume_mean", 0.0) / 1_000_000.0)), "quality_score": float(event.quality_score)}))
    scaler = fit_feature_scaler(anchor_feature_rows)
    prototypes = build_state_prototypes_from_event_memory(event_records=event_records, as_of_date=decision_date, memory_version=spec.memory_version, spec_hash=spec.spec_hash(), config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version)) if event_records else []
    coverage = {"event_record_count": len(event_records), "anchor_count": len(anchor_library), "prototype_count": len(prototypes)}
    return {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "as_of_date": decision_date, "coverage": coverage, "excluded_reasons": excluded_reasons, "event_records": event_records, "anchor_library": anchor_library, "prototypes": prototypes, "scaler": scaler}


def _build_query_panel(*, decision_dates: list[str], spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], scaler=None):
    out = {}
    excluded_reasons = []
    allowed = set(decision_dates)
    for decision_date in decision_dates:
        per_date = {}
        for symbol, bars in bars_by_symbol.items():
            eligible = [i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == decision_date]
            if not eligible:
                continue
            idx = eligible[0]
            if idx < spec.feature_window_bars - 1 or idx + 1 >= len(bars):
                excluded_reasons.append({"symbol": symbol, "reason": "insufficient_query_history", "decision_date": decision_date})
                continue
            query_window = bars[idx - spec.feature_window_bars + 1 : idx + 1]
            embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history_by_date.items() if k <= decision_date}, sector_map=sector_map, cutoff_date=decision_date, spec=spec, scaler=scaler)
            per_date[symbol] = {"idx": idx, "query_window": query_window, "embedding": embedding, "meta": meta, "execution_bar": bars[idx + 1]}
        if decision_date in allowed:
            out[decision_date] = per_date
    return out, excluded_reasons


def fit_train_artifacts(*, run_id: str, artifact_store: JsonResearchArtifactStore, train_end: str, test_start: str, purge: int, embargo: int, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, calibration_artifact: dict | None = None, quote_policy_calibration: dict | None = None, metadata: dict | None = None) -> dict:
    memory = build_event_memory_asof(decision_date=train_end, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market)
    max_train_date = max((r.event_date for r in memory["event_records"]), default=None)
    max_outcome_end = max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)
    if max_outcome_end and max_outcome_end >= test_start:
        raise AssertionError("future event/outcome mixed into train artifact")
    snapshot_id = f"{run_id}:{train_end}:{spec.spec_hash()}"
    artifact_store.save_prototype_snapshot(run_id=run_id, as_of_date=train_end, memory_version=spec.memory_version, payload={"spec_hash": spec.spec_hash(), "snapshot_id": snapshot_id, "prototype_count": len(memory["prototypes"]), "prototypes": [p.__dict__ for p in memory["prototypes"]]})
    return {"run_id": run_id, "snapshot_id": snapshot_id, "spec_hash": spec.spec_hash(), "as_of_date": train_end, "train_end": train_end, "test_start": test_start, "purge": purge, "embargo": embargo, "memory_version": spec.memory_version, "prototype_snapshot_name": "prototype_snapshot", "max_train_date": max_train_date, "max_outcome_end_date": max_outcome_end, "prototypes": [p.__dict__ for p in memory["prototypes"]], "scaler": memory["scaler"], "calibration": dict(calibration_artifact or {"method": "logistic", "slope": 1.0, "intercept": 0.0, "ev_slope": 1.0, "ev_intercept": 0.0}), "quote_policy_calibration": dict(quote_policy_calibration or {"ev_threshold": 0.005, "uncertainty_cap": 0.12, "min_effective_sample_size": 1.5, "min_fill_probability": 0.1, "abstain_margin": 0.05}), "metadata": dict(metadata or {}), "snapshot_ids": {"prototype_snapshot_id": snapshot_id}}


def run_test_with_frozen_artifacts(*, train_artifact: dict, artifact_store: JsonResearchArtifactStore, decision_dates: list[str], spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, top_k: int | None = None) -> dict:
    if not train_artifact:
        raise AssertionError("train artifact required")
    min_test_decision_date = min(decision_dates) if decision_dates else None
    if train_artifact.get("max_train_date") and min_test_decision_date and train_artifact["max_train_date"] >= min_test_decision_date:
        raise AssertionError("max_train_date must be < min_test_decision_date")
    if train_artifact.get("max_outcome_end_date") and min_test_decision_date and train_artifact["max_outcome_end_date"] >= min_test_decision_date:
        raise AssertionError("future event/outcome mixed into test runtime memory")
    prototype_pool = load_prototypes_asof(artifact_store=artifact_store, run_id=train_artifact["run_id"], name=train_artifact.get("prototype_snapshot_name", "prototype_snapshot"), as_of_date=train_artifact["as_of_date"], memory_version=train_artifact["memory_version"])
    if not prototype_pool and train_artifact.get("prototypes"):
        from .models import StatePrototype
        prototype_pool = [StatePrototype(**p) for p in train_artifact.get("prototypes") or []]
    query_panel, excluded = _build_query_panel(decision_dates=decision_dates, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=train_artifact.get("scaler"))
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    qp = train_artifact.get("quote_policy_calibration") or {}
    metadata = train_artifact.get("metadata") or {}
    effective_top_k = int(top_k or metadata.get("portfolio_top_n", 3) or 3)
    ev_cfg = EVConfig(top_k=effective_top_k, min_effective_sample_size=float(qp.get("min_effective_sample_size", 1.5)), max_uncertainty=float(qp.get("uncertainty_cap", 0.12)), min_expected_utility=float(qp.get("ev_threshold", 0.005)), min_regime_alignment=float(qp.get("min_regime_alignment", metadata.get("quote_min_regime_alignment", 0.5)) or 0.5), max_return_interval_width=float(qp.get("max_return_interval_width", metadata.get("quote_max_return_interval_width", 0.08)) or 0.08), abstain_margin=float(qp.get("abstain_margin", metadata.get("abstain_margin", 0.05)) or 0.05))
    cal_payload = train_artifact.get("calibration") or {}
    calibration = CalibrationModel(method=str(cal_payload.get("method", "logistic")), slope=float(cal_payload.get("slope", 1.0)), intercept=float(cal_payload.get("intercept", 0.0)))
    ev_slope = float(cal_payload.get("ev_slope", 1.0))
    ev_intercept = float(cal_payload.get("ev_intercept", 0.0))
    panel_rows = []
    candidates = []
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=spec.slippage_bps, fee_bps=spec.fee_bps, allow_partial_fills=True))
    portfolio_cfg = PortfolioConfig(top_n=max(1, int(metadata.get("portfolio_top_n", effective_top_k) or effective_top_k)), risk_budget_fraction=float(metadata.get("portfolio_risk_budget_fraction", 0.95) or 0.95))
    tuning = {"MIN_TICK_GAP": 1, "ADAPTIVE_BASE_LEGS": 2, "ADAPTIVE_LEG_BOOST": 1.0, "MIN_TOTAL_SPREAD_PCT": 0.01, "ADAPTIVE_STRENGTH_SCALE": 0.1, "FIRST_LEG_BASE_PCT": 0.012, "FIRST_LEG_MIN_PCT": 0.006, "FIRST_LEG_MAX_PCT": 0.05, "FIRST_LEG_GAIN_WEIGHT": 0.6, "FIRST_LEG_ATR_WEIGHT": 0.5, "FIRST_LEG_REQ_FLOOR_PCT": 0.012, "MIN_FIRST_LEG_GAP_PCT": 0.03, "STRICT_MIN_FIRST_GAP": True, "ADAPTIVE_MAX_STEP_PCT": 0.06, "ADAPTIVE_FRAC_ALPHA": 1.25, "ADAPTIVE_GAIN_SCALE": 0.1, "MIN_LOT_QTY": 1}
    grouped_candidates = {}
    for decision_date, items in query_panel.items():
        batch = []
        for symbol, q in items.items():
            regime_code = _regime_from_macro(dict(macro_history_by_date.get(decision_date, {})))
            sector_code = sector_map.get(symbol)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            long_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            short_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.SELL.value)
            panel_rows.append({"decision_date": decision_date, "symbol": symbol, "prototype_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"], "prototype_count": len(prototype_pool), "chosen_side": surface.chosen_side, "top_matches": {"long": _topk(long_scores, effective_top_k), "short": _topk(short_scores, effective_top_k)}})
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            raw_ev = float(surface.buy.expected_net_return if chosen_side == Side.BUY else surface.sell.expected_net_return)
            score = ev_slope * raw_ev + ev_intercept
            confidence = calibration.calibrate_prob(score)
            candidate = SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=score, confidence=confidence, anchor_date=decision_date, reference_date=decision_date, current_price=float(q["execution_bar"].open), atr_pct=float(max(0.01, compute_bar_features(q["query_window"]).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": str(q["execution_bar"].timestamp)[:10], "spec_hash": spec.spec_hash(), "frozen_from_train_artifacts": True}, diagnostics={"decision_surface": {"chosen_side": surface.chosen_side}, "top_matches": panel_rows[-1]["top_matches"]}, notes=["frozen_validation_path=true"])
            batch.append(candidate)
            candidates.append(candidate)
        grouped_candidates[decision_date] = batch
    from backtest_app.research_runtime.engine import execute_daily_execution_loop
    execution = execute_daily_execution_loop(trading_dates=decision_dates, grouped_candidates=grouped_candidates, bars_by_symbol=bars_by_symbol, config=SimpleNamespace(initial_capital=10000.0, fee_bps=spec.fee_bps, slippage_bps=spec.slippage_bps, allow_partial_fills=True), market=market, strategy_mode="research_similarity_v2", portfolio_cfg=portfolio_cfg, quote_policy_cfg=QuotePolicyConfig(ev_threshold=float(qp.get("ev_threshold", 0.005)), uncertainty_cap=float(qp.get("uncertainty_cap", 0.12)), min_effective_sample_size=float(qp.get("min_effective_sample_size", 1.5)), min_fill_probability=float(qp.get("min_fill_probability", 0.1))), tuning=tuning, broker=broker)
    decisions = [{"symbol": d.candidate.symbol, "decision_date": str(d.candidate.reference_date)[:10] if getattr(d.candidate, "reference_date", None) else str(d.candidate.anchor_date)[:10], "selected": d.selected} for d in execution["portfolio_decisions_all"]]
    return {"decision_dates": decision_dates, "panel_rows": panel_rows, "candidates": [c.to_dict() for c in candidates], "portfolio_decisions": decisions, "plans": [p.to_dict() for p in execution["plans"]], "fills": [f.to_dict() for f in execution["fills"]], "excluded_reasons": excluded, "frozen_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"], "test_executed_from_frozen_train_artifacts": True}


def generate_similarity_candidates(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_payload: Dict[str, float], sector_map: Dict[str, str] | None = None, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    spec = spec or _default_spec()
    macro_history = {str(bar.timestamp)[:10]: dict(macro_payload) for bars in bars_by_symbol.values() for bar in bars}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, sector_map=sector_map, top_k=top_k, abstain_margin=abstain_margin, spec=spec)
    if not candidates:
        for symbol in bars_by_symbol.keys():
            diagnostics.setdefault(symbol, {"scores": {"abstained": True}, "strategy_mode": "research_similarity_v1"})
    return candidates, diagnostics


def generate_similarity_candidates_rolling(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str] | None = None, lookback_bars: int = 5, feature_window_bars: int = 60, horizon_days: int = 5, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None, metadata: dict | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    t0 = perf_counter()
    spec = spec or _default_spec(feature_window_bars=feature_window_bars, horizon_days=horizon_days)
    sector_map = sector_map or {}
    ev_cfg = _ev_config_from_metadata(metadata, top_k=top_k, abstain_margin=abstain_margin)
    diagnostics: Dict[str, dict] = {"pipeline": {"strategy_mode": "research_similarity_v2", "spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "lookback_bars": lookback_bars, "top_k": top_k, "abstain_margin": abstain_margin, "ev_config": {"min_effective_sample_size": ev_cfg.min_effective_sample_size, "max_uncertainty": ev_cfg.max_uncertainty, "max_return_interval_width": ev_cfg.max_return_interval_width, "min_regime_alignment": ev_cfg.min_regime_alignment, "min_expected_utility": ev_cfg.min_expected_utility, "diagnostic_disable_lower_bound_gate": ev_cfg.diagnostic_disable_lower_bound_gate}}}
    panel_rows: List[dict] = []
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    calibration = CalibrationModel(method="identity")
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    decision_dates = sorted({str(bars[i].timestamp)[:10] for bars in bars_by_symbol.values() if len(bars) >= min_required_bars + spec.horizon_days + 2 for i in range(min_required_bars - 1, len(bars) - spec.horizon_days - 1)})
    total_prototype_count = 0
    all_excluded_reasons: list[dict] = []
    event_record_batches: list[dict] = []
    for decision_date in decision_dates:
        memory = build_event_memory_asof(decision_date=decision_date, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, lookback_bars=lookback_bars)
        query_panel, query_excluded = _build_query_panel(decision_dates=[decision_date], spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=memory["scaler"])
        event_record_batches.append({"decision_date": decision_date, "records": [{"symbol": r.symbol, "event_date": r.event_date, "outcome_end_date": r.outcome_end_date, "side_outcomes": r.side_outcomes} for r in memory["event_records"]]})
        all_excluded_reasons.extend([{**r, "decision_date": decision_date} for r in memory["excluded_reasons"] + query_excluded])
        total_prototype_count += len(memory["prototypes"])
        prototype_pool = list(memory["prototypes"])
        for symbol, q in query_panel.get(decision_date, {}).items():
            query_macro = dict(macro_history_by_date.get(decision_date, {}))
            regime_code = _regime_from_macro(query_macro)
            sector_code = sector_map.get(symbol)
            execution_bar = q["execution_bar"]
            execution_date = str(execution_bar.timestamp)[:10]
            long_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            short_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.SELL.value)
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            row_diag = {"decision_date": decision_date, "symbol": symbol, "query": {"regime_code": regime_code, "sector_code": sector_code, "decision_date": decision_date, "execution_date": execution_date, "decision_convention": DECISION_CONVENTION, "price_reference_source": "next_open", "feature_window_bars": spec.feature_window_bars, "feature_coverage_bars": len(q["query_window"]), "query_panel_count": len(query_panel.get(decision_date, {})), "insufficient_history": False, "shape_horizons": q["meta"].get("shape_horizons", [])}, "library": {"event_record_count": len(memory["event_records"]), "max_outcome_end_before_decision": max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)}, "decision_surface": {"chosen_side": surface.chosen_side, "abstain": surface.abstain, "abstain_reasons": list(surface.abstain_reasons), "prototype_pool_size": surface.diagnostics.get("prototype_pool_size"), "chosen_lower_bound": (surface.diagnostics.get("decision_rule") or {}).get("chosen_lower_bound"), "chosen_interval_width": (surface.diagnostics.get("decision_rule") or {}).get("chosen_interval_width"), "chosen_effective_sample_size": (surface.diagnostics.get("decision_rule") or {}).get("chosen_effective_sample_size"), "chosen_uncertainty": (surface.diagnostics.get("decision_rule") or {}).get("chosen_uncertainty"), "gate_ablation": surface.diagnostics.get("gate_ablation"), "decision_rule": surface.diagnostics.get("decision_rule")}, "ev": {"buy": {"expected_utility": long_ev.expected_utility, "expected_net_return": long_ev.expected_net_return, "effective_sample_size": long_ev.effective_sample_size, "uncertainty": long_ev.uncertainty, "abstain_reasons": long_ev.abstain_reasons}, "sell": {"expected_utility": short_ev.expected_utility, "expected_net_return": short_ev.expected_net_return, "effective_sample_size": short_ev.effective_sample_size, "uncertainty": short_ev.uncertainty, "abstain_reasons": short_ev.abstain_reasons}}, "scorer_diagnostics": {"buy": _side_diag(long_ev, surface, "BUY"), "sell": _side_diag(short_ev, surface, "SELL")}, "top_matches": {"long": surface.buy.top_matches, "short": surface.sell.top_matches}}
            panel_rows.append(row_diag)
            diagnostics[f"{decision_date}:{symbol}"] = row_diag
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0] if short_scores else None
            out.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0), confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob), anchor_date=decision_date, reference_date=decision_date, current_price=float(execution_bar.open), atr_pct=float(max(0.01, compute_bar_features(q["query_window"]).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": execution_date, "spec_hash": spec.spec_hash()}, diagnostics=row_diag, notes=[f"prototype_id={(chosen.prototype_id if chosen else '')}"]))
    diagnostics["signal_panel"] = panel_rows
    diagnostics["signal_panel_jsonl"] = "\n".join(str(row) for row in panel_rows)
    diagnostics["cache_keys"] = {"library_cache_keys": [f"{d}:{spec.spec_hash()}" for d in decision_dates]}
    diagnostics["event_records"] = event_record_batches
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "prototype_count": total_prototype_count, "wall_clock_seconds": perf_counter() - t0}
    diagnostics["artifacts"] = {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "excluded_reasons": all_excluded_reasons, "excluded_reasons_histogram": dict(Counter(r.get("reason", "unknown") for r in all_excluded_reasons))}
    return out, diagnostics
