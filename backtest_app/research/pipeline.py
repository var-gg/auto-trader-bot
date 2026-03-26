from __future__ import annotations

from dataclasses import asdict
from statistics import mean
from time import perf_counter
from typing import Dict, List, Tuple

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.features import build_multiscale_feature_vector, compute_bar_features, fit_feature_scaler
from backtest_app.historical_data.models import HistoricalBar
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate

from .labeling import EventLabelingConfig, build_event_outcome_record, label_event_window
from .models import EventOutcomeRecord, ResearchAnchor
from .prototype import PrototypeConfig, build_state_prototypes_from_event_memory
from .repository import ExactCosineCandidateIndex
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
            for side_name in ("BUY", "SELL"):
                side_payload = event.side_payload.get(side_name, {})
                anchor_library.append(
                    ResearchAnchor(
                        symbol=lib_symbol,
                        anchor_code="SIMILARITY_V2",
                        reference_date=feature_end_date,
                        anchor_date=feature_end_date,
                        side=side_name,
                        embedding=raw_embedding,
                        shape_vector=raw_embedding[:3],
                        ctx_vector=raw_embedding[3:],
                        embedding_model="exact-cosine-manual",
                        vector_version=spec.memory_version,
                        vector_dim=len(raw_embedding),
                        anchor_quality=float(event.quality_score),
                        mae_pct=float(side_payload.get("mae_pct", 0.0) or 0.0),
                        mfe_pct=float(side_payload.get("mfe_pct", 0.0) or 0.0),
                        days_to_hit=side_payload.get("target_hit_day"),
                        after_cost_return_pct=float(side_payload.get("after_cost_return_pct", 0.0) or 0.0),
                        realized_return_pct=float(side_payload.get("horizon_return_pct", 0.0) or 0.0),
                        regime_code=regime_code,
                        sector_code=lib_sector,
                        liquidity_score=max(0.0, min(1.0, compute_bar_features(history_window).get("volume_mean", 0.0) / 1_000_000.0)),
                        metadata={"market": market, "feature_version": spec.feature_version, "label_version": spec.label_version, "memory_version": spec.memory_version, "decision_cutoff": decision_date, "event_payload": {"schema_version": spec.label_version, "raw_path_summary": event.path_summary, "side_outcomes": event.side_payload}},
                    )
                )

    scaler = fit_feature_scaler(anchor_feature_rows)
    normalized_library: List[ResearchAnchor] = []
    for anchor in anchor_library:
        history_window = next((bars_by_symbol[anchor.symbol][i - spec.feature_window_bars + 1 : i + 1] for i, b in enumerate(bars_by_symbol[anchor.symbol]) if str(b.timestamp)[:10] == anchor.reference_date), [])
        fv = build_multiscale_feature_vector(symbol=anchor.symbol, bars=history_window, market_bars=_market_proxy_bars(bars_by_symbol, cutoff_date=anchor.reference_date), sector_bars=_sector_proxy_bars(anchor.symbol, bars_by_symbol, sector_map, cutoff_date=anchor.reference_date), macro_history={anchor.reference_date: macro_history_by_date.get(anchor.reference_date, {})}, sector_code=anchor.sector_code, scaler=scaler)
        normalized_library.append(ResearchAnchor(symbol=anchor.symbol, anchor_code=anchor.anchor_code, reference_date=anchor.reference_date, anchor_date=anchor.anchor_date, side=anchor.side, embedding=fv.embedding, shape_vector=fv.shape_vector, ctx_vector=fv.ctx_vector, vector_version=spec.memory_version, embedding_model="manual-multiscale", vector_dim=len(fv.embedding), anchor_quality=anchor.anchor_quality, mae_pct=anchor.mae_pct, mfe_pct=anchor.mfe_pct, days_to_hit=anchor.days_to_hit, after_cost_return_pct=anchor.after_cost_return_pct, realized_return_pct=anchor.realized_return_pct, regime_code=anchor.regime_code, sector_code=anchor.sector_code, liquidity_score=anchor.liquidity_score, prototype_id=anchor.prototype_id, prototype_membership=dict(anchor.prototype_membership), metadata={**dict(anchor.metadata), "shape_dim": fv.metadata.get("shape_dim"), "ctx_dim": fv.metadata.get("ctx_dim")}))
    prototypes = build_state_prototypes_from_event_memory(event_records=event_records, as_of_date=decision_date, memory_version=spec.memory_version, spec_hash=spec.spec_hash(), config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version)) if event_records else []
    coverage = {"event_record_count": len(event_records), "anchor_count": len(normalized_library), "prototype_count": len(prototypes)}
    return {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "as_of_date": decision_date, "coverage": coverage, "excluded_reasons": excluded_reasons, "event_records": event_records, "anchor_library": normalized_library, "prototypes": prototypes, "scaler": scaler}


def _build_query_panel(*, decision_date: str, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], scaler=None):
    out = {}
    excluded_reasons = []
    for symbol, bars in bars_by_symbol.items():
        eligible = [i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == decision_date]
        if not eligible:
            excluded_reasons.append({"symbol": symbol, "reason": "missing_decision_date"})
            continue
        idx = eligible[0]
        if idx < spec.feature_window_bars - 1 or idx + 1 >= len(bars):
            excluded_reasons.append({"symbol": symbol, "reason": "insufficient_query_history"})
            continue
        query_window = bars[idx - spec.feature_window_bars + 1 : idx + 1]
        embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history_by_date.items() if k <= decision_date}, sector_map=sector_map, cutoff_date=decision_date, scaler=scaler)
        out[symbol] = {"idx": idx, "query_window": query_window, "embedding": embedding, "meta": meta, "execution_bar": bars[idx + 1]}
    return out, excluded_reasons


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
    library_cache = {}
    query_panel_cache = {}
    total_anchor_count = 0
    total_prototype_count = 0
    all_excluded_reasons: list[dict] = []
    label_cfg = _label_cfg(spec)

    for decision_date in decision_dates:
        cache_key = f"{decision_date}:{spec.spec_hash()}"
        memory = build_event_memory_asof(decision_date=decision_date, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, lookback_bars=lookback_bars)
        library_cache[cache_key] = memory
        query_panel, query_excluded = _build_query_panel(decision_date=decision_date, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=memory["scaler"])
        query_panel_cache[cache_key] = query_panel
        all_excluded_reasons.extend([{**r, "decision_date": decision_date} for r in memory["excluded_reasons"] + query_excluded])
        total_anchor_count += len(memory["anchor_library"])
        total_prototype_count += len(memory["prototypes"])
        prototype_pool = list(memory["prototypes"])
        for symbol, q in query_panel.items():
            query_macro = dict(macro_history_by_date.get(decision_date, {}))
            regime_code = _regime_from_macro(query_macro)
            sector_code = sector_map.get(symbol)
            query_embedding = q["embedding"]
            query_window = q["query_window"]
            execution_bar = q["execution_bar"]
            execution_date = str(execution_bar.timestamp)[:10]
            long_scores = score_candidates_exact(query_embedding=query_embedding, candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            short_scores = score_candidates_exact(query_embedding=query_embedding, candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.SELL.value)
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=query_embedding, candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=query_embedding, candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            surface = build_decision_surface(query_embedding=query_embedding, prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            long_best = long_scores[0].score if long_scores else 0.0
            short_best = short_scores[0].score if short_scores else 0.0
            margin = abs(long_best - short_best)
            bars = bars_by_symbol[symbol]
            idx = q["idx"]
            future_window = bars[idx + 1 : idx + 1 + spec.horizon_days]
            outcome = label_event_window(future_window, label_cfg)
            row_diag = {
                "decision_date": decision_date,
                "symbol": symbol,
                "strategy_mode": "research_similarity_v2",
                "query": {"regime_code": regime_code, "sector_code": sector_code, "decision_date": decision_date, "execution_date": execution_date, "decision_convention": DECISION_CONVENTION, "execution_price_source": "next_open", "price_reference_source": "next_open", "signal_timestamp": f"{decision_date}T15:30:00", "execution_start_timestamp": f"{execution_date}T09:00:00", "macro_payload": query_macro, "query_embedding_dim": len(query_embedding), "feature_coverage_bars": len(query_window), "feature_window_bars": spec.feature_window_bars, "insufficient_history": False},
                "library": {"anchor_count": len(memory["anchor_library"]), "prototype_count": len(memory["prototypes"]), "event_record_count": len(memory["event_records"]), "max_outcome_end_before_decision": max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None), "cache_key": cache_key},
                "scores": {"long_score": long_best, "short_score": short_best, "score_margin": margin, "baseline_abstained": margin < abstain_margin},
                "ev": {"long": {"calibrated_ev": long_ev.calibrated_ev, "abstained": long_ev.abstained}, "short": {"calibrated_ev": short_ev.calibrated_ev, "abstained": short_ev.abstained}},
                "decision_surface": {"chosen_side": surface.chosen_side, "abstain": surface.abstain, "abstain_reasons": surface.abstain_reasons, "buy": {"p_target_first": surface.buy.p_target_first, "p_stop_first": surface.buy.p_stop_first, "p_flat": surface.buy.p_flat, "expected_net_return": surface.buy.expected_net_return, "expected_mae": surface.buy.expected_mae, "expected_mfe": surface.buy.expected_mfe, "q10": surface.buy.q10_return, "q50": surface.buy.q50_return, "q90": surface.buy.q90_return, "effective_sample_size": surface.buy.effective_sample_size, "regime_alignment": surface.buy.regime_alignment, "uncertainty": surface.buy.uncertainty}, "sell": {"p_target_first": surface.sell.p_target_first, "p_stop_first": surface.sell.p_stop_first, "p_flat": surface.sell.p_flat, "expected_net_return": surface.sell.expected_net_return, "expected_mae": surface.sell.expected_mae, "expected_mfe": surface.sell.expected_mfe, "q10": surface.sell.q10_return, "q50": surface.sell.q50_return, "q90": surface.sell.q90_return, "effective_sample_size": surface.sell.effective_sample_size, "regime_alignment": surface.sell.regime_alignment, "uncertainty": surface.sell.uncertainty}, "diagnostics": surface.diagnostics},
                "top_matches": {"long": surface.buy.top_matches, "short": surface.sell.top_matches, "baseline_long": _topk(long_scores, top_k), "baseline_short": _topk(short_scores, top_k)},
                "observed_outcome": {"signal_generation_path": {"label": outcome.label, "after_cost_return_pct": outcome.after_cost_return_pct, "days_to_hit": outcome.days_to_hit, "path_start_date": decision_date}, "execution_realized_path": {"path_start_date": execution_date, "price_reference_source": "next_open"}},
            }
            panel_rows.append(row_diag)
            diagnostics[f"{decision_date}:{symbol}"] = row_diag
            if surface.abstain or ((long_ev.abstained and short_ev.abstained) or (not long_scores and not short_scores)):
                row_diag["scores"]["abstained"] = True
                continue
            row_diag["scores"]["abstained"] = False
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0] if short_scores else None
            out.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0), confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob), anchor_date=decision_date, reference_date=decision_date, current_price=float(execution_bar.open), atr_pct=float(max(0.01, compute_bar_features(query_window).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": execution_date, "signal_timestamp": f"{decision_date}T15:30:00", "execution_start_timestamp": f"{execution_date}T09:00:00", "price_reference_source": "next_open", "query_embedding_dim": len(query_embedding), "macro_payload": query_macro, "spec_hash": spec.spec_hash()}, diagnostics=row_diag, notes=["generated_by=research_similarity_v2", f"prototype_id={(chosen.prototype_id if chosen else '')}"]))

    diagnostics["signal_panel"] = panel_rows
    diagnostics["signal_panel_jsonl"] = "\n".join(str(row) for row in panel_rows)
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "anchor_count": total_anchor_count, "prototype_count": total_prototype_count, "wall_clock_seconds": perf_counter() - t0}
    diagnostics["cache_keys"] = {"library_cache_keys": sorted(library_cache.keys()), "query_panel_cache_keys": sorted(query_panel_cache.keys())}
    diagnostics["event_records"] = [{"decision_date": payload["as_of_date"], "spec_hash": payload["spec_hash"], "records": [{"symbol": r.symbol, "event_date": r.event_date, "outcome_end_date": r.outcome_end_date, "schema_version": r.schema_version, "path_summary": r.path_summary, "side_outcomes": r.side_outcomes, "diagnostics": r.diagnostics} for r in payload["event_records"]]} for payload in library_cache.values()]
    diagnostics["artifacts"] = {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "coverage": {"event_record_batches": len(diagnostics["event_records"]), "panel_rows": len(panel_rows)}, "excluded_reasons": all_excluded_reasons}
    return out, diagnostics
