from __future__ import annotations

from dataclasses import asdict
from statistics import mean
from time import perf_counter
from typing import Dict, List, Tuple

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.features import build_multiscale_feature_vector, compute_bar_features, fit_feature_scaler
from backtest_app.historical_data.models import HistoricalBar
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate

from .artifacts import JsonResearchArtifactStore
from .labeling import EventLabelingConfig, build_event_outcome_record, label_event_window
from .models import EventOutcomeRecord, ResearchAnchor
from .prototype import PrototypeConfig, build_state_prototypes_from_event_memory
from .repository import ExactCosineCandidateIndex, load_prototypes_asof
from .scoring import CalibrationModel, CandidateScore, EVConfig, ScoringConfig, build_decision_surface, estimate_expected_value, score_candidates_exact

DECISION_CONVENTION = "EOD_T_SIGNAL__T1_OPEN_EXECUTION"


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


def build_query_embedding(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], cutoff_date: str | None, scaler=None) -> tuple[list[float], dict]:
    sector_code = sector_map.get(symbol)
    fv = build_multiscale_feature_vector(symbol=symbol, bars=bars, market_bars=_market_proxy_bars(bars_by_symbol, cutoff_date=cutoff_date), sector_bars=_sector_proxy_bars(symbol, bars_by_symbol, sector_map, cutoff_date=cutoff_date), macro_history=macro_history, sector_code=sector_code, scaler=scaler)
    return fv.embedding, {"shape_features": fv.shape_features, "residual_features": fv.residual_features, "context_features": fv.context_features, "shape_vector": fv.shape_vector, "ctx_vector": fv.ctx_vector, **fv.metadata}


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


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
            raw_embedding, feature_meta = build_query_embedding(symbol=lib_symbol, bars=history_window, bars_by_symbol=bars_by_symbol, macro_history={feature_end_date: macro_payload}, sector_map=sector_map, cutoff_date=feature_end_date)
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
            embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history_by_date.items() if k <= decision_date}, sector_map=sector_map, cutoff_date=decision_date, scaler=scaler)
            per_date[symbol] = {"idx": idx, "query_window": query_window, "embedding": embedding, "meta": meta, "execution_bar": bars[idx + 1]}
        if decision_date in allowed:
            out[decision_date] = per_date
    return out, excluded_reasons


def fit_train_artifacts(*, run_id: str, artifact_store: JsonResearchArtifactStore, train_end: str, test_start: str, purge: int, embargo: int, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str) -> dict:
    memory = build_event_memory_asof(decision_date=train_end, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market)
    max_train_date = max((r.event_date for r in memory["event_records"]), default=None)
    max_outcome_end = max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)
    if max_outcome_end and max_outcome_end >= test_start:
        raise AssertionError("future event/outcome mixed into train artifact")
    snapshot_id = f"{run_id}:{train_end}:{spec.spec_hash()}"
    artifact_store.save_prototype_snapshot(run_id=run_id, as_of_date=train_end, memory_version=spec.memory_version, payload={"spec_hash": spec.spec_hash(), "snapshot_id": snapshot_id, "prototype_count": len(memory["prototypes"]), "prototypes": [p.__dict__ for p in memory["prototypes"]]})
    return {"run_id": run_id, "snapshot_id": snapshot_id, "spec_hash": spec.spec_hash(), "as_of_date": train_end, "train_end": train_end, "test_start": test_start, "purge": purge, "embargo": embargo, "memory_version": spec.memory_version, "prototype_snapshot_name": "prototype_snapshot", "max_train_date": max_train_date, "max_outcome_end_date": max_outcome_end, "prototypes": [p.__dict__ for p in memory["prototypes"]], "scaler": memory["scaler"], "calibration": {"method": "logistic"}, "quote_policy_calibration": {"method": "train_only"}, "snapshot_ids": {"prototype_snapshot_id": snapshot_id}}


def run_test_with_frozen_artifacts(*, train_artifact: dict, artifact_store: JsonResearchArtifactStore, decision_dates: list[str], spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, top_k: int = 3) -> dict:
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
    ev_cfg = EVConfig(top_k=top_k)
    calibration = CalibrationModel(method="identity")
    panel_rows = []
    for decision_date, items in query_panel.items():
        for symbol, q in items.items():
            regime_code = _regime_from_macro(dict(macro_history_by_date.get(decision_date, {})))
            sector_code = sector_map.get(symbol)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            long_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            panel_rows.append({"decision_date": decision_date, "symbol": symbol, "prototype_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"], "prototype_count": len(prototype_pool), "chosen_side": surface.chosen_side, "top_matches": _topk(long_scores, top_k)})
    return {"decision_dates": decision_dates, "panel_rows": panel_rows, "excluded_reasons": excluded, "frozen_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"]}


def generate_similarity_candidates(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_payload: Dict[str, float], sector_map: Dict[str, str] | None = None, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    spec = spec or _default_spec()
    macro_history = {str(bar.timestamp)[:10]: dict(macro_payload) for bars in bars_by_symbol.values() for bar in bars}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, sector_map=sector_map, top_k=top_k, abstain_margin=abstain_margin, spec=spec)
    if not candidates:
        for symbol in bars_by_symbol.keys():
            diagnostics.setdefault(symbol, {"scores": {"abstained": True}, "strategy_mode": "research_similarity_v1"})
    return candidates, diagnostics


def generate_similarity_candidates_rolling(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str] | None = None, lookback_bars: int = 5, feature_window_bars: int = 60, horizon_days: int = 5, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    t0 = perf_counter()
    spec = spec or _default_spec(feature_window_bars=feature_window_bars, horizon_days=horizon_days)
    sector_map = sector_map or {}
    diagnostics: Dict[str, dict] = {"pipeline": {"strategy_mode": "research_similarity_v2", "spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "lookback_bars": lookback_bars, "top_k": top_k, "abstain_margin": abstain_margin}}
    panel_rows: List[dict] = []
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    ev_cfg = EVConfig(top_k=top_k)
    calibration = CalibrationModel(method="identity")
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    decision_dates = sorted({str(bars[i].timestamp)[:10] for bars in bars_by_symbol.values() if len(bars) >= min_required_bars + spec.horizon_days + 2 for i in range(min_required_bars - 1, len(bars) - spec.horizon_days - 1)})
    total_prototype_count = 0
    all_excluded_reasons: list[dict] = []
    for decision_date in decision_dates:
        memory = build_event_memory_asof(decision_date=decision_date, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, lookback_bars=lookback_bars)
        query_panel, query_excluded = _build_query_panel(decision_dates=[decision_date], spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=memory["scaler"])
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
            row_diag = {"decision_date": decision_date, "symbol": symbol, "query": {"execution_date": execution_date}, "decision_surface": {"chosen_side": surface.chosen_side}, "top_matches": {"long": surface.buy.top_matches, "short": surface.sell.top_matches}}
            panel_rows.append(row_diag)
            diagnostics[f"{decision_date}:{symbol}"] = row_diag
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0] if short_scores else None
            out.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0), confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob), anchor_date=decision_date, reference_date=decision_date, current_price=float(execution_bar.open), atr_pct=float(max(0.01, compute_bar_features(q["query_window"]).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": execution_date, "spec_hash": spec.spec_hash()}, diagnostics=row_diag, notes=[f"prototype_id={(chosen.prototype_id if chosen else '')}"]))
    diagnostics["signal_panel"] = panel_rows
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "prototype_count": total_prototype_count, "wall_clock_seconds": perf_counter() - t0}
    diagnostics["artifacts"] = {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "excluded_reasons": all_excluded_reasons}
    return out, diagnostics
