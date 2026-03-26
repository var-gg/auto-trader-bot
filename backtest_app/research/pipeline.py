from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from statistics import mean
from typing import Dict, Iterable, List, Tuple

from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate

from backtest_app.historical_data.features import compute_bar_features, compute_external_vector
from backtest_app.historical_data.models import HistoricalBar

from .labeling import EventLabelingConfig, label_event_window
from .models import PrototypeAnchor, ResearchAnchor
from .prototype import PrototypeConfig, build_anchor_prototypes
from .repository import ExactCosineCandidateIndex
from .scoring import CandidateScore, ScoringConfig, score_candidates_exact


def _regime_from_macro(macro_payload: Dict[str, float]) -> str:
    if not macro_payload:
        return "NEUTRAL"
    avg = mean(float(v) for v in macro_payload.values())
    if avg >= 0.1:
        return "RISK_ON"
    if avg <= -0.1:
        return "RISK_OFF"
    return "NEUTRAL"


def build_query_embedding(*, bars: List[HistoricalBar], macro_payload: Dict[str, float], sector_code: str | None, regime_code: str | None) -> list[float]:
    bar_features = compute_bar_features(bars)
    macro_vector = compute_external_vector(macro_payload)
    sector_hash = float((sum(ord(ch) for ch in (sector_code or "")) % 17) / 17.0)
    regime_map = {"RISK_OFF": -1.0, "NEUTRAL": 0.0, "RISK_ON": 1.0}
    query = [
        float(bar_features.get("return_1", 0.0)),
        float(bar_features.get("range_pct", 0.0)),
        float(bar_features.get("volume_mean", 0.0)),
        float(sector_hash),
        float(regime_map.get(regime_code or "NEUTRAL", 0.0)),
    ]
    return query + list(macro_vector)


def build_historical_anchors(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    macro_payload: Dict[str, float],
    market: str,
    lookback_bars: int = 5,
    horizon_days: int = 5,
    target_return_pct: float = 0.04,
    stop_return_pct: float = 0.03,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> List[ResearchAnchor]:
    anchors: List[ResearchAnchor] = []
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
        sector_code = symbol[:3]
        regime_code = _regime_from_macro(macro_payload)
        for idx in range(lookback_bars, len(bars) - horizon_days):
            query_window = bars[idx - lookback_bars : idx]
            future_window = bars[idx : idx + horizon_days]
            label = label_event_window(future_window, label_cfg)
            if label.no_trade or label.ambiguous:
                continue
            embedding = build_query_embedding(
                bars=query_window,
                macro_payload=macro_payload,
                sector_code=sector_code,
                regime_code=regime_code,
            )
            base = {
                "symbol": symbol,
                "anchor_code": "SIMILARITY_V1",
                "reference_date": str(query_window[-1].timestamp)[:10],
                "anchor_date": str(query_window[-1].timestamp)[:10],
                "embedding": embedding,
                "shape_vector": embedding[:3],
                "ctx_vector": embedding[3:],
                "embedding_model": "exact-cosine-manual",
                "vector_version": "research_similarity_v1",
                "vector_dim": len(embedding),
                "anchor_quality": float(label.quality_score),
                "mae_pct": float(label.mae_pct),
                "mfe_pct": float(label.mfe_pct),
                "days_to_hit": label.days_to_hit,
                "after_cost_return_pct": float(label.after_cost_return_pct),
                "regime_code": regime_code,
                "sector_code": sector_code,
                "liquidity_score": max(0.0, min(1.0, compute_bar_features(query_window).get("volume_mean", 0.0) / 1_000_000.0)),
                "metadata": {"market": market, "label": label.label, "diagnostics": label.diagnostics},
            }
            if label.label in {"UP_FIRST", "HORIZON_CLOSE"} and label.after_cost_return_pct >= 0:
                anchors.append(ResearchAnchor(side=Side.BUY.value, **base))
            if label.label in {"DOWN_FIRST", "HORIZON_CLOSE"} and label.mae_pct <= 0:
                anchors.append(ResearchAnchor(side=Side.SELL.value, **base))
    return anchors


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def generate_similarity_candidates(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    market: str,
    macro_payload: Dict[str, float],
    top_k: int = 3,
    abstain_margin: float = 0.05,
) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    anchors = build_historical_anchors(bars_by_symbol=bars_by_symbol, macro_payload=macro_payload, market=market)
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
    for symbol, bars in bars_by_symbol.items():
        if len(bars) < 5:
            continue
        query_window = bars[-5:]
        current_price = float(query_window[-1].close)
        bar_features = compute_bar_features(query_window)
        regime_code = _regime_from_macro(macro_payload)
        sector_code = symbol[:3]
        query_embedding = build_query_embedding(bars=query_window, macro_payload=macro_payload, sector_code=sector_code, regime_code=regime_code)
        long_scores = score_candidates_exact(
            query_embedding=query_embedding,
            candidates=long_candidates,
            regime_code=regime_code,
            sector_code=sector_code,
            config=scoring_cfg,
            candidate_index=ExactCosineCandidateIndex(),
        )
        short_scores = score_candidates_exact(
            query_embedding=query_embedding,
            candidates=short_candidates,
            regime_code=regime_code,
            sector_code=sector_code,
            config=scoring_cfg,
            candidate_index=ExactCosineCandidateIndex(),
        )
        long_best = long_scores[0].score if long_scores else 0.0
        short_best = short_scores[0].score if short_scores else 0.0
        margin = abs(long_best - short_best)
        symbol_diag = {
            "strategy_mode": "research_similarity_v1",
            "query": {
                "symbol": symbol,
                "regime_code": regime_code,
                "sector_code": sector_code,
                "query_embedding_dim": len(query_embedding),
                "bar_features": bar_features,
                "macro_payload": macro_payload,
            },
            "scores": {
                "long_score": long_best,
                "short_score": short_best,
                "score_margin": margin,
                "abstained": margin < abstain_margin,
            },
            "top_matches": {
                "long": _topk(long_scores, top_k),
                "short": _topk(short_scores, top_k),
            },
            "prototype_stats": {
                "anchor_count": len(anchors),
                "prototype_count": len(prototypes),
                "long_prototype_count": len(long_candidates),
                "short_prototype_count": len(short_candidates),
            },
        }
        diagnostics[symbol] = symbol_diag
        if margin < abstain_margin:
            continue
        chosen_side = Side.BUY if long_best >= short_best else Side.SELL
        chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0]
        out.append(
            SignalCandidate(
                symbol=symbol,
                ticker_id=None,
                market=MarketCode(market),
                side_bias=chosen_side,
                signal_strength=float(chosen.score if chosen else 0.0),
                confidence=float(min(1.0, margin + 0.5 * max(long_best, short_best))),
                anchor_date=None,
                reference_date=None,
                current_price=current_price,
                atr_pct=float(max(0.01, bar_features.get("range_pct", 0.02) / 3.0)),
                target_return_pct=0.04,
                max_reverse_pct=0.03,
                expected_horizon_days=5,
                outcome_label=OutcomeLabel.UNKNOWN,
                provenance={
                    "strategy_mode": "research_similarity_v1",
                    "derived_bar_features": bar_features,
                    "macro_payload": macro_payload,
                    "query_embedding_dim": len(query_embedding),
                },
                diagnostics=symbol_diag,
                notes=["generated_by=research_similarity_v1", f"prototype_id={(chosen.prototype_id if chosen else '')}"] if chosen else ["generated_by=research_similarity_v1"],
            )
        )
    return out, diagnostics
