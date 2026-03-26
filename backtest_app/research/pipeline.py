from __future__ import annotations

from dataclasses import asdict
from statistics import mean
from typing import Dict, Iterable, List, Tuple

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


def _market_proxy_bars(bars_by_symbol: Dict[str, List[HistoricalBar]], upto: int | None = None) -> List[HistoricalBar]:
    rows: List[HistoricalBar] = []
    series = [bars[:upto] if upto is not None else bars for bars in bars_by_symbol.values() if bars]
    if not series:
        return rows
    max_len = max(len(b) for b in series)
    for idx in range(max_len):
        bucket = [bars[idx] for bars in series if idx < len(bars)]
        if not bucket:
            continue
        rows.append(HistoricalBar(symbol="MKT", timestamp=bucket[-1].timestamp, open=mean([b.open for b in bucket]), high=mean([b.high for b in bucket]), low=mean([b.low for b in bucket]), close=mean([b.close for b in bucket]), volume=mean([b.volume for b in bucket])))
    return rows


def _sector_proxy_bars(symbol: str, bars_by_symbol: Dict[str, List[HistoricalBar]], sector_map: Dict[str, str], upto: int | None = None) -> List[HistoricalBar]:
    sector = sector_map.get(symbol)
    peers = {s: bars for s, bars in bars_by_symbol.items() if s != symbol and sector and sector_map.get(s) == sector}
    return _market_proxy_bars(peers or {symbol: bars_by_symbol.get(symbol, [])}, upto=upto)


def build_query_embedding(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], scaler=None) -> tuple[list[float], dict]:
    sector_code = sector_map.get(symbol)
    market_bars = _market_proxy_bars(bars_by_symbol)
    sector_bars = _sector_proxy_bars(symbol, bars_by_symbol, sector_map)
    fv = build_multiscale_feature_vector(symbol=symbol, bars=bars, market_bars=market_bars, sector_bars=sector_bars, macro_history=macro_history, sector_code=sector_code, scaler=scaler)
    return fv.embedding, {"shape_features": fv.shape_features, "residual_features": fv.residual_features, "context_features": fv.context_features, "shape_vector": fv.shape_vector, "ctx_vector": fv.ctx_vector, **fv.metadata}


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def build_historical_anchors(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    macro_payload: Dict[str, float],
    market: str,
    sector_map: Dict[str, str] | None = None,
    lookback_bars: int = 5,
    horizon_days: int = 5,
    target_return_pct: float = 0.04,
    stop_return_pct: float = 0.03,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> List[ResearchAnchor]:
    anchors: List[ResearchAnchor] = []
    sector_map = sector_map or {}
    label_cfg = EventLabelingConfig(
        target_return_pct=target_return_pct,
        stop_return_pct=stop_return_pct,
        horizon_days=horizon_days,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    for symbol, bars in bars_by_symbol.items():
        if len(bars) < lookback_bars + horizon_days + 1:
            continue
        sector_code = sector_map.get(symbol)
        regime_code = _regime_from_macro(macro_payload)
        anchor_feature_rows: List[dict] = []
        anchor_payloads: List[tuple[int, object]] = []
        for idx in range(lookback_bars, len(bars) - horizon_days):
            query_window = bars[idx - lookback_bars : idx]
            future_window = bars[idx : idx + horizon_days]
            label = label_event_window(future_window, label_cfg)
            if label.no_trade or label.ambiguous:
                continue
            macro_history = {str(query_window[-1].timestamp)[:10]: dict(macro_payload)}
            embedding, feature_meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history=macro_history, sector_map=sector_map)
            anchor_feature_rows.append({**feature_meta.get("shape_features", {}), **feature_meta.get("context_features", {})})
            anchor_payloads.append((idx, feature_meta))
            base = {
                "symbol": symbol,
                "anchor_code": "SIMILARITY_V1",
                "reference_date": str(query_window[-1].timestamp)[:10],
                "anchor_date": str(query_window[-1].timestamp)[:10],
                "embedding": embedding,
                "shape_vector": feature_meta.get("shape_vector", embedding[:0]),
                "ctx_vector": feature_meta.get("ctx_vector", embedding[:0]),
                "embedding_model": "manual-multiscale",
                "vector_version": str(feature_meta.get("vector_version", "research_similarity_v1")),
                "vector_dim": len(embedding),
                "anchor_quality": float(label.quality_score),
                "mae_pct": float(label.mae_pct),
                "mfe_pct": float(label.mfe_pct),
                "days_to_hit": label.days_to_hit,
                "after_cost_return_pct": float(label.after_cost_return_pct),
                "realized_return_pct": float(label.realized_return_pct),
                "regime_code": regime_code,
                "sector_code": sector_code,
                "liquidity_score": max(0.0, min(1.0, compute_bar_features(query_window).get("volume_mean", 0.0) / 1_000_000.0)),
                "metadata": {"market": market, "label": label.label, "path_label": label.path_label, "side_labels": label.side_labels, "diagnostics": label.diagnostics},
            }
            if label.side_labels.get("BUY") in {"UP_FIRST", "HORIZON_UP"}:
                anchors.append(ResearchAnchor(side=Side.BUY.value, **base))
            if label.side_labels.get("SELL") in {"DOWN_FIRST", "HORIZON_DOWN"}:
                anchors.append(ResearchAnchor(side=Side.SELL.value, **base))
    return anchors


def generate_similarity_candidates(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    market: str,
    macro_payload: Dict[str, float],
    sector_map: Dict[str, str] | None = None,
    top_k: int = 3,
    abstain_margin: float = 0.05,
) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    sector_map = sector_map or {}
    anchors = build_historical_anchors(bars_by_symbol=bars_by_symbol, macro_payload=macro_payload, market=market, sector_map=sector_map)
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.985))
    long_candidates = [p for p in prototypes if p.side == Side.BUY.value]
    short_candidates = [p for p in prototypes if p.side == Side.SELL.value]
    diagnostics: Dict[str, dict] = {
        "pipeline": {
            "anchor_count": len(anchors),
            "prototype_count": len(prototypes),
            "top_k": top_k,
            "abstain_margin": abstain_margin,
        }
    }
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    ev_cfg = EVConfig(top_k=top_k)
    calibration = CalibrationModel(method="identity")
    for symbol, bars in bars_by_symbol.items():
        if len(bars) < 5:
            continue
        query_window = bars[-5:]
        current_price = float(query_window[-1].close)
        regime_code = _regime_from_macro(macro_payload)
        sector_code = sector_map.get(symbol)
        query_embedding, feature_meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={str(query_window[-1].timestamp)[:10]: dict(macro_payload)}, sector_map=sector_map)
        bar_features = {**feature_meta.get("shape_features", {}), **feature_meta.get("residual_features", {})}
        long_scores = score_candidates_exact(query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
        short_scores = score_candidates_exact(query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
        long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
        short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
        long_best = long_scores[0].score if long_scores else 0.0
        short_best = short_scores[0].score if short_scores else 0.0
        margin = abs(long_best - short_best)
        symbol_diag = {
            "strategy_mode": "research_similarity_v1",
            "query": {"symbol": symbol, "regime_code": regime_code, "sector_code": sector_code, "query_embedding_dim": len(query_embedding), "bar_features": bar_features, "macro_payload": macro_payload},
            "scores": {"long_score": long_best, "short_score": short_best, "score_margin": margin, "baseline_abstained": margin < abstain_margin},
            "ev": {
                "long": {"calibrated_ev": long_ev.calibrated_ev, "expected_utility": long_ev.expected_utility, "expected_net_return": long_ev.expected_net_return, "p_up_first": long_ev.p_up_first, "p_down_first": long_ev.p_down_first, "expected_mae": long_ev.expected_mae, "expected_mfe": long_ev.expected_mfe, "uncertainty": long_ev.uncertainty, "dispersion": long_ev.dispersion, "effective_sample_size": long_ev.effective_sample_size, "abstained": long_ev.abstained, "abstain_reasons": long_ev.abstain_reasons},
                "short": {"calibrated_ev": short_ev.calibrated_ev, "expected_utility": short_ev.expected_utility, "expected_net_return": short_ev.expected_net_return, "p_up_first": short_ev.p_up_first, "p_down_first": short_ev.p_down_first, "expected_mae": short_ev.expected_mae, "expected_mfe": short_ev.expected_mfe, "uncertainty": short_ev.uncertainty, "dispersion": short_ev.dispersion, "effective_sample_size": short_ev.effective_sample_size, "abstained": short_ev.abstained, "abstain_reasons": short_ev.abstain_reasons},
            },
            "top_matches": {"long": long_ev.top_matches, "short": short_ev.top_matches, "baseline_long": _topk(long_scores, top_k), "baseline_short": _topk(short_scores, top_k)},
            "prototype_stats": {"anchor_count": len(anchors), "prototype_count": len(prototypes), "long_prototype_count": len(long_candidates), "short_prototype_count": len(short_candidates)},
        }
        diagnostics[symbol] = symbol_diag
        if long_ev.abstained and short_ev.abstained:
            symbol_diag["scores"]["abstained"] = True
            diagnostics[symbol] = symbol_diag
            continue
        symbol_diag["scores"]["abstained"] = False
        chosen_side = Side.BUY if long_ev.calibrated_ev >= short_ev.calibrated_ev else Side.SELL
        chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0]
        out.append(
            SignalCandidate(
                symbol=symbol,
                ticker_id=None,
                market=MarketCode(market),
                side_bias=chosen_side,
                signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0),
                confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob),
                anchor_date=None,
                reference_date=None,
                current_price=current_price,
                atr_pct=float(max(0.01, bar_features.get("range_pct", 0.02) / 3.0)),
                target_return_pct=0.04,
                max_reverse_pct=0.03,
                expected_horizon_days=5,
                outcome_label=OutcomeLabel.UNKNOWN,
                provenance={"strategy_mode": "research_similarity_v1", "derived_bar_features": bar_features, "macro_payload": macro_payload, "query_embedding_dim": len(query_embedding)},
                diagnostics=symbol_diag,
                notes=["generated_by=research_similarity_v1", f"prototype_id={(chosen.prototype_id if chosen else '')}"] if chosen else ["generated_by=research_similarity_v1"],
            )
        )
    return out, diagnostics


def generate_similarity_candidates_rolling(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    market: str,
    macro_history_by_date: Dict[str, Dict[str, float]],
    sector_map: Dict[str, str] | None = None,
    lookback_bars: int = 5,
    horizon_days: int = 5,
    top_k: int = 3,
    abstain_margin: float = 0.05,
) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    sector_map = sector_map or {}
    label_cfg = EventLabelingConfig(target_return_pct=0.04, stop_return_pct=0.03, horizon_days=horizon_days)
    panel_rows: List[dict] = []
    diagnostics: Dict[str, dict] = {"pipeline": {"strategy_mode": "research_similarity_v2", "lookback_bars": lookback_bars, "horizon_days": horizon_days, "top_k": top_k, "abstain_margin": abstain_margin}}
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    ev_cfg = EVConfig(top_k=top_k)
    calibration = CalibrationModel(method="identity")

    for symbol, bars in bars_by_symbol.items():
        if len(bars) < lookback_bars + horizon_days + 1:
            continue
        sector_code = sector_map.get(symbol)
        for idx in range(lookback_bars, len(bars) - horizon_days):
            decision_bar = bars[idx]
            decision_date = str(decision_bar.timestamp)[:10]
            query_window = bars[idx - lookback_bars : idx]
            query_macro = dict(macro_history_by_date.get(decision_date, {}))
            regime_code = _regime_from_macro(query_macro)

            anchor_library: List[ResearchAnchor] = []
            anchor_feature_rows: List[dict] = []
            raw_anchor_payloads: List[dict] = []
            for lib_symbol, lib_bars in bars_by_symbol.items():
                if len(lib_bars) < lookback_bars + horizon_days + 1:
                    continue
                lib_sector = sector_map.get(lib_symbol)
                for j in range(lookback_bars, len(lib_bars) - horizon_days):
                    if str(lib_bars[j].timestamp)[:10] >= decision_date:
                        break
                    anchor_date = str(lib_bars[j - 1].timestamp)[:10]
                    history_window = lib_bars[j - lookback_bars : j]
                    future_window = lib_bars[j : j + horizon_days]
                    macro_payload = dict(macro_history_by_date.get(anchor_date, {}))
                    lib_regime = _regime_from_macro(macro_payload)
                    label = label_event_window(future_window, label_cfg)
                    if label.no_trade or label.ambiguous:
                        continue
                    raw_embedding, feature_meta = build_query_embedding(symbol=lib_symbol, bars=history_window, bars_by_symbol=bars_by_symbol, macro_history={anchor_date: macro_payload}, sector_map=sector_map)
                    anchor_feature_rows.append({**feature_meta.get("shape_features", {}), **feature_meta.get("context_features", {})})
                    raw_anchor_payloads.append({"history_window": list(history_window), "anchor": None, "anchor_date": anchor_date, "lib_symbol": lib_symbol, "lib_sector": lib_sector})
                    embedding = raw_embedding
                    base = {
                        "symbol": lib_symbol,
                        "anchor_code": "SIMILARITY_V2",
                        "reference_date": anchor_date,
                        "anchor_date": anchor_date,
                        "embedding": embedding,
                        "shape_vector": embedding[:3],
                        "ctx_vector": embedding[3:],
                        "embedding_model": "exact-cosine-manual",
                        "vector_version": "research_similarity_v2",
                        "vector_dim": len(embedding),
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
            if anchor_library:
                normalized_library: List[ResearchAnchor] = []
                for anchor, payload in zip(anchor_library, raw_anchor_payloads):
                    fv = build_multiscale_feature_vector(symbol=anchor.symbol, bars=payload["history_window"], market_bars=_market_proxy_bars(bars_by_symbol, upto=len(payload["history_window"])), sector_bars=_sector_proxy_bars(anchor.symbol, bars_by_symbol, sector_map, upto=len(payload["history_window"])), macro_history={anchor.reference_date: macro_history_by_date.get(anchor.reference_date, {})}, sector_code=anchor.sector_code, scaler=scaler)
                    normalized_library.append(ResearchAnchor(symbol=anchor.symbol, anchor_code=anchor.anchor_code, reference_date=anchor.reference_date, anchor_date=anchor.anchor_date, side=anchor.side, embedding=fv.embedding, shape_vector=fv.shape_vector, ctx_vector=fv.ctx_vector, vector_version=str(fv.metadata.get("vector_version", anchor.vector_version)), embedding_model="manual-multiscale", vector_dim=len(fv.embedding), anchor_quality=anchor.anchor_quality, mae_pct=anchor.mae_pct, mfe_pct=anchor.mfe_pct, days_to_hit=anchor.days_to_hit, after_cost_return_pct=anchor.after_cost_return_pct, realized_return_pct=anchor.realized_return_pct, regime_code=anchor.regime_code, sector_code=anchor.sector_code, liquidity_score=anchor.liquidity_score, prototype_id=anchor.prototype_id, prototype_membership=dict(anchor.prototype_membership), metadata={**dict(anchor.metadata), "feature_version": fv.metadata.get("feature_version"), "shape_dim": fv.metadata.get("shape_dim"), "ctx_dim": fv.metadata.get("ctx_dim")}))
                anchor_library = normalized_library
            query_embedding, query_feature_meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history_by_date.items() if k <= decision_date}, sector_map=sector_map, scaler=scaler)
            prototypes = build_anchor_prototypes(anchor_library, PrototypeConfig(dedup_similarity_threshold=0.985))
            long_candidates = [p for p in prototypes if p.side == Side.BUY.value]
            short_candidates = [p for p in prototypes if p.side == Side.SELL.value]
            long_scores = score_candidates_exact(query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
            short_scores = score_candidates_exact(query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex())
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=query_embedding, candidates=long_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=query_embedding, candidates=short_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            long_best = long_scores[0].score if long_scores else 0.0
            short_best = short_scores[0].score if short_scores else 0.0
            margin = abs(long_best - short_best)
            future_window = bars[idx : idx + horizon_days]
            outcome = label_event_window(future_window, label_cfg)
            row_diag = {
                "decision_date": decision_date,
                "symbol": symbol,
                "strategy_mode": "research_similarity_v2",
                "query": {"regime_code": regime_code, "sector_code": sector_code, "macro_payload": query_macro, "query_embedding_dim": len(query_embedding)},
                "library": {"anchor_count": len(anchor_library), "prototype_count": len(prototypes)},
                "scores": {"long_score": long_best, "short_score": short_best, "score_margin": margin, "baseline_abstained": margin < abstain_margin},
                "ev": {
                    "long": {"calibrated_ev": long_ev.calibrated_ev, "expected_utility": long_ev.expected_utility, "expected_net_return": long_ev.expected_net_return, "p_up_first": long_ev.p_up_first, "p_down_first": long_ev.p_down_first, "expected_mae": long_ev.expected_mae, "expected_mfe": long_ev.expected_mfe, "uncertainty": long_ev.uncertainty, "dispersion": long_ev.dispersion, "effective_sample_size": long_ev.effective_sample_size, "abstained": long_ev.abstained, "abstain_reasons": long_ev.abstain_reasons, "decomposition": long_ev.diagnostics.get("ev_decomposition", {})},
                    "short": {"calibrated_ev": short_ev.calibrated_ev, "expected_utility": short_ev.expected_utility, "expected_net_return": short_ev.expected_net_return, "p_up_first": short_ev.p_up_first, "p_down_first": short_ev.p_down_first, "expected_mae": short_ev.expected_mae, "expected_mfe": short_ev.expected_mfe, "uncertainty": short_ev.uncertainty, "dispersion": short_ev.dispersion, "effective_sample_size": short_ev.effective_sample_size, "abstained": short_ev.abstained, "abstain_reasons": short_ev.abstain_reasons, "decomposition": short_ev.diagnostics.get("ev_decomposition", {})},
                },
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
            if chosen_side == Side.BUY and long_scores:
                chosen = long_scores[0]
            elif chosen_side == Side.SELL and short_scores:
                chosen = short_scores[0]
            elif long_scores:
                chosen = long_scores[0]
                chosen_side = Side.BUY
            else:
                chosen = short_scores[0]
                chosen_side = Side.SELL
            out.append(
                SignalCandidate(
                    symbol=symbol,
                    ticker_id=None,
                    market=MarketCode(market),
                    side_bias=chosen_side,
                    signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0),
                    confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob),
                    anchor_date=decision_date,
                    reference_date=decision_date,
                    current_price=float(decision_bar.close),
                    atr_pct=float(max(0.01, compute_bar_features(query_window).get("range_pct", 0.02) / 3.0)),
                    target_return_pct=0.04,
                    max_reverse_pct=0.03,
                    expected_horizon_days=horizon_days,
                    outcome_label=OutcomeLabel.UNKNOWN,
                    provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "query_embedding_dim": len(query_embedding), "macro_payload": query_macro},
                    diagnostics=row_diag,
                    notes=["generated_by=research_similarity_v2", f"prototype_id={(chosen.prototype_id if chosen else '')}"] if chosen else ["generated_by=research_similarity_v2"],
                )
            )
    diagnostics["signal_panel"] = panel_rows
    return out, diagnostics
