from __future__ import annotations

from dataclasses import asdict
from statistics import mean
from time import perf_counter
from typing import Dict, Iterable, List, Tuple

DECISION_CONVENTION = "EOD_T_SIGNAL__T1_OPEN_EXECUTION"

from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate

from backtest_app.historical_data.features import build_multiscale_feature_vector, compute_bar_features, fit_feature_scaler
from backtest_app.historical_data.models import HistoricalBar

from .labeling import EventLabelingConfig, label_event_window
from .models import ResearchAnchor
from .prototype import PrototypeConfig, build_anchor_prototypes
from .repository import ExactCosineCandidateIndex
from .scoring import CandidateScore, CalibrationModel, EVConfig, ScoringConfig, estimate_expected_value, score_candidates_exact


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
    if not cutoff_date:
        return list(bars)
    return [bar for bar in bars if str(bar.timestamp)[:10] <= cutoff_date]


def _market_proxy_bars(bars_by_symbol: Dict[str, List[HistoricalBar]], cutoff_date: str | None = None) -> List[HistoricalBar]:
    rows: List[HistoricalBar] = []
    series = [_bars_until_date(bars, cutoff_date) for bars in bars_by_symbol.values() if bars]
    series = [bars for bars in series if bars]
    if not series:
        return rows
    max_len = max(len(b) for b in series)
    for idx in range(max_len):
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
    market_bars = _market_proxy_bars(bars_by_symbol, cutoff_date=cutoff_date)
    sector_bars = _sector_proxy_bars(symbol, bars_by_symbol, sector_map, cutoff_date=cutoff_date)
    fv = build_multiscale_feature_vector(symbol=symbol, bars=bars, market_bars=market_bars, sector_bars=sector_bars, macro_history=macro_history, sector_code=sector_code, scaler=scaler)
    return fv.embedding, {"shape_features": fv.shape_features, "residual_features": fv.residual_features, "context_features": fv.context_features, "shape_vector": fv.shape_vector, "ctx_vector": fv.ctx_vector, **fv.metadata}


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def _build_anchor_library_for_date(*, decision_date: str, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, feature_window_bars: int, lookback_bars: int, horizon_days: int):
    label_cfg = EventLabelingConfig(target_return_pct=0.04, stop_return_pct=0.03, horizon_days=horizon_days)
    min_required_bars = max(lookback_bars, feature_window_bars)
    anchor_library: List[ResearchAnchor] = []
    anchor_feature_rows: List[dict] = []
    raw_anchor_payloads: List[dict] = []
    for lib_symbol, lib_bars in bars_by_symbol.items():
        if len(lib_bars) < min_required_bars + horizon_days + 2:
            continue
        lib_sector = sector_map.get(lib_symbol)
        for j in range(min_required_bars - 1, len(lib_bars) - horizon_days - 1):
            anchor_date = str(lib_bars[j].timestamp)[:10]
            anchor_outcome_end = str(lib_bars[j + horizon_days].timestamp)[:10]
            if anchor_outcome_end >= decision_date:
                break
            history_window = lib_bars[j - feature_window_bars + 1 : j + 1]
            future_window = lib_bars[j + 1 : j + 1 + horizon_days]
            macro_payload = dict(macro_history_by_date.get(anchor_date, {}))
            lib_regime = _regime_from_macro(macro_payload)
            label = label_event_window(future_window, label_cfg)
            if label.no_trade or label.ambiguous:
                continue
            raw_embedding, feature_meta = build_query_embedding(symbol=lib_symbol, bars=history_window, bars_by_symbol=bars_by_symbol, macro_history={anchor_date: macro_payload}, sector_map=sector_map, cutoff_date=anchor_date)
            anchor_feature_rows.append({**feature_meta.get("shape_features", {}), **feature_meta.get("context_features", {})})
            raw_anchor_payloads.append({"history_window": list(history_window), "anchor_date": anchor_date, "lib_symbol": lib_symbol, "lib_sector": lib_sector, "cutoff_date": anchor_date})
            base = {
                "symbol": lib_symbol,
                "anchor_code": "SIMILARITY_V2",
                "reference_date": anchor_date,
                "anchor_date": anchor_date,
                "embedding": raw_embedding,
                "shape_vector": raw_embedding[:3],
                "ctx_vector": raw_embedding[3:],
                "embedding_model": "exact-cosine-manual",
                "vector_version": "research_similarity_v2",
                "vector_dim": len(raw_embedding),
                "anchor_quality": float(label.quality_score),
                "mae_pct": float(label.mae_pct),
                "mfe_pct": float(label.mfe_pct),
                "days_to_hit": label.days_to_hit,
                "after_cost_return_pct": float(label.after_cost_return_pct),
                "realized_return_pct": float(label.realized_return_pct),
                "regime_code": lib_regime,
                "sector_code": lib_sector,
                "liquidity_score": max(0.0, min(1.0, compute_bar_features(history_window).get("volume_mean", 0.0) / 1_000_000.0)),
                "metadata": {"market": market, "label": label.label, "path_label": label.path_label, "side_labels": label.side_labels, "decision_cutoff": decision_date},
            }
            if label.side_labels.get("BUY") in {"UP_FIRST", "HORIZON_UP"}:
                anchor_library.append(ResearchAnchor(side=Side.BUY.value, **base))
            if label.side_labels.get("SELL") in {"DOWN_FIRST", "HORIZON_DOWN"}:
                anchor_library.append(ResearchAnchor(side=Side.SELL.value, **base))
    scaler = fit_feature_scaler(anchor_feature_rows)
    normalized_library: List[ResearchAnchor] = []
    for anchor, payload in zip(anchor_library, raw_anchor_payloads):
        fv = build_multiscale_feature_vector(symbol=anchor.symbol, bars=payload["history_window"], market_bars=_market_proxy_bars(bars_by_symbol, cutoff_date=payload["cutoff_date"]), sector_bars=_sector_proxy_bars(anchor.symbol, bars_by_symbol, sector_map, cutoff_date=payload["cutoff_date"]), macro_history={anchor.reference_date: macro_history_by_date.get(anchor.reference_date, {})}, sector_code=anchor.sector_code, scaler=scaler)
        normalized_library.append(ResearchAnchor(symbol=anchor.symbol, anchor_code=anchor.anchor_code, reference_date=anchor.reference_date, anchor_date=anchor.anchor_date, side=anchor.side, embedding=fv.embedding, shape_vector=fv.shape_vector, ctx_vector=fv.ctx_vector, vector_version=str(fv.metadata.get("vector_version", anchor.vector_version)), embedding_model="manual-multiscale", vector_dim=len(fv.embedding), anchor_quality=anchor.anchor_quality, mae_pct=anchor.mae_pct, mfe_pct=anchor.mfe_pct, days_to_hit=anchor.days_to_hit, after_cost_return_pct=anchor.after_cost_return_pct, realized_return_pct=anchor.realized_return_pct, regime_code=anchor.regime_code, sector_code=anchor.sector_code, liquidity_score=anchor.liquidity_score, prototype_id=anchor.prototype_id, prototype_membership=dict(anchor.prototype_membership), metadata={**dict(anchor.metadata), "feature_version": fv.metadata.get("feature_version"), "shape_dim": fv.metadata.get("shape_dim"), "ctx_dim": fv.metadata.get("ctx_dim")}))
    prototypes = build_anchor_prototypes(normalized_library, PrototypeConfig(dedup_similarity_threshold=0.985)) if normalized_library else []
    return normalized_library, prototypes, scaler


def _build_query_panel(*, decision_date: str, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], feature_window_bars: int):
    out = {}
    for symbol, bars in bars_by_symbol.items():
        eligible = [i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == decision_date]
        if not eligible:
            continue
        idx = eligible[0]
        if idx < feature_window_bars - 1 or idx + 1 >= len(bars):
            continue
        query_window = bars[idx - feature_window_bars + 1 : idx + 1]
        embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history_by_date.items() if k <= decision_date}, sector_map=sector_map, cutoff_date=decision_date)
        out[symbol] = {"idx": idx, "query_window": query_window, "embedding": embedding, "meta": meta, "execution_bar": bars[idx + 1]}
    return out


def generate_similarity_candidates(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_payload: Dict[str, float], sector_map: Dict[str, str] | None = None, top_k: int = 3, abstain_margin: float = 0.05) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    macro_history = {}
    for bars in bars_by_symbol.values():
        for bar in bars:
            macro_history[str(bar.timestamp)[:10]] = dict(macro_payload)
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, sector_map=sector_map, top_k=top_k, abstain_margin=abstain_margin)
    if not candidates:
        for symbol in bars_by_symbol.keys():
            diagnostics.setdefault(symbol, {"scores": {"abstained": True}, "strategy_mode": "research_similarity_v1"})
    return candidates, diagnostics


def generate_similarity_candidates_rolling(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str] | None = None, lookback_bars: int = 5, feature_window_bars: int = 60, horizon_days: int = 5, top_k: int = 3, abstain_margin: float = 0.05) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    t0 = perf_counter()
    sector_map = sector_map or {}
    diagnostics: Dict[str, dict] = {"pipeline": {"strategy_mode": "research_similarity_v2", "lookback_bars": lookback_bars, "feature_window_bars": feature_window_bars, "horizon_days": horizon_days, "top_k": top_k, "abstain_margin": abstain_margin}}
    panel_rows: List[dict] = []
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    ev_cfg = EVConfig(top_k=top_k)
    calibration = CalibrationModel(method="identity")
    min_required_bars = max(lookback_bars, feature_window_bars)
    decision_dates = sorted({str(bars[i].timestamp)[:10] for bars in bars_by_symbol.values() if len(bars) >= min_required_bars + horizon_days + 2 for i in range(min_required_bars - 1, len(bars) - horizon_days - 1)})
    library_cache = {}
    query_panel_cache = {}
    total_anchor_count = 0
    total_prototype_count = 0
    label_cfg = EventLabelingConfig(target_return_pct=0.04, stop_return_pct=0.03, horizon_days=horizon_days)

    for decision_date in decision_dates:
        normalized_library, prototypes, scaler = _build_anchor_library_for_date(decision_date=decision_date, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, feature_window_bars=feature_window_bars, lookback_bars=lookback_bars, horizon_days=horizon_days)
        library_cache[decision_date] = {"anchor_library": normalized_library, "prototypes": prototypes, "scaler": scaler}
        query_panel_cache[decision_date] = _build_query_panel(decision_date=decision_date, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, feature_window_bars=feature_window_bars)
        total_anchor_count += len(normalized_library)
        total_prototype_count += len(prototypes)
        long_candidates = [p for p in prototypes if p.side == Side.BUY.value]
        short_candidates = [p for p in prototypes if p.side == Side.SELL.value]
        for symbol, q in query_panel_cache[decision_date].items():
            query_macro = dict(macro_history_by_date.get(decision_date, {}))
            regime_code = _regime_from_macro(query_macro)
            sector_code = sector_map.get(symbol)
            query_embedding = q["embedding"]
            query_window = q["query_window"]
            execution_bar = q["execution_bar"]
            feature_coverage = len(query_window)
            insufficient_history = feature_coverage < feature_window_bars
            long_scores = score_candidates_exact(query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
            short_scores = score_candidates_exact(query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            long_best = long_scores[0].score if long_scores else 0.0
            short_best = short_scores[0].score if short_scores else 0.0
            margin = abs(long_best - short_best)
            bars = bars_by_symbol[symbol]
            idx = q["idx"]
            future_window = bars[idx + 1 : idx + 1 + horizon_days]
            outcome = label_event_window(future_window, label_cfg)
            row_diag = {
                "decision_date": decision_date,
                "symbol": symbol,
                "strategy_mode": "research_similarity_v2",
                "query": {"regime_code": regime_code, "sector_code": sector_code, "decision_date": decision_date, "decision_convention": DECISION_CONVENTION, "execution_price_source": "next_open", "macro_payload": query_macro, "query_embedding_dim": len(query_embedding), "feature_coverage_bars": feature_coverage, "feature_window_bars": feature_window_bars, "insufficient_history": insufficient_history},
                "library": {"anchor_count": len(normalized_library), "prototype_count": len(prototypes), "max_outcome_end_before_decision": None},
                "scores": {"long_score": long_best, "short_score": short_best, "score_margin": margin, "baseline_abstained": margin < abstain_margin},
                "ev": {"long": {"calibrated_ev": long_ev.calibrated_ev, "abstained": long_ev.abstained}, "short": {"calibrated_ev": short_ev.calibrated_ev, "abstained": short_ev.abstained}},
                "top_matches": {"long": long_ev.top_matches, "short": short_ev.top_matches, "baseline_long": _topk(long_scores, top_k), "baseline_short": _topk(short_scores, top_k)},
                "observed_outcome": {"label": outcome.label, "after_cost_return_pct": outcome.after_cost_return_pct, "days_to_hit": outcome.days_to_hit},
            }
            panel_rows.append(row_diag)
            diagnostics[f"{decision_date}:{symbol}"] = row_diag
            if (long_ev.abstained and short_ev.abstained) or (not long_scores and not short_scores):
                row_diag["scores"]["abstained"] = True
                continue
            row_diag["scores"]["abstained"] = False
            chosen_side = Side.BUY if long_ev.calibrated_ev >= short_ev.calibrated_ev else Side.SELL
            chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0] if short_scores else None
            out.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0), confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob), anchor_date=decision_date, reference_date=decision_date, current_price=float(execution_bar.open), atr_pct=float(max(0.01, compute_bar_features(query_window).get("range_pct", 0.02) / 3.0)), target_return_pct=0.04, max_reverse_pct=0.03, expected_horizon_days=horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "query_embedding_dim": len(query_embedding), "macro_payload": query_macro}, diagnostics=row_diag, notes=["generated_by=research_similarity_v2", f"prototype_id={(chosen.prototype_id if chosen else '')}"]))

    wall_clock = perf_counter() - t0
    diagnostics["signal_panel"] = panel_rows
    diagnostics["signal_panel_jsonl"] = "\n".join(str(row) for row in panel_rows)
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "anchor_count": total_anchor_count, "prototype_count": total_prototype_count, "wall_clock_seconds": wall_clock}
    diagnostics["cache_keys"] = {"library_cache_keys": sorted(library_cache.keys()), "query_panel_cache_keys": sorted(query_panel_cache.keys())}
    return out, diagnostics
