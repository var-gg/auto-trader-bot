from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from statistics import mean, median, pstdev
from typing import Dict, Iterable, List

import numpy as np

from .models import PrototypeAnchor, ResearchAnchor


@dataclass(frozen=True)
class PrototypeConfig:
    dedup_similarity_threshold: float = 0.985
    min_anchor_quality: float = 0.0
    min_support_count: int = 1
    min_decayed_support: float = 0.0
    max_age_days: int = 365
    recency_halflife_days: float = 90.0


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _liq_bucket(value: float | None) -> str:
    x = float(value or 0.0)
    if x >= 0.8:
        return "HIGH"
    if x >= 0.4:
        return "MID"
    return "LOW"


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.fromisoformat(str(raw)[:10]).date()


def _freshness_days(members: List[ResearchAnchor]) -> float:
    dates = [d for d in (_parse_date(a.reference_date) for a in members) if d is not None]
    if not dates:
        return 9999.0
    latest = max(dates)
    today = latest
    return float((today - latest).days)


def _decayed_support(members: List[ResearchAnchor], halflife_days: float) -> float:
    dates = [d for d in (_parse_date(a.reference_date) for a in members) if d is not None]
    if not dates:
        return float(len(members))
    latest = max(dates)
    out = 0.0
    for d in dates:
        age = max(0, (latest - d).days)
        out += 0.5 ** (age / max(halflife_days, 1.0))
    return out


def _medoid(cluster: List[ResearchAnchor]) -> ResearchAnchor:
    if len(cluster) == 1:
        return cluster[0]
    arr = [np.asarray(c.embedding, dtype=float) for c in cluster]
    sims = []
    for i, a in enumerate(arr):
        total = 0.0
        for j, b in enumerate(arr):
            if i == j:
                continue
            total += _cos(a, b)
        sims.append((total, cluster[i]))
    sims.sort(key=lambda x: (x[0], x[1].anchor_quality, x[1].liquidity_score or 0.0), reverse=True)
    return sims[0][1]


def _cluster_key(anchor: ResearchAnchor) -> tuple:
    return (
        anchor.side,
        anchor.regime_code or "UNKNOWN",
        anchor.sector_code or "UNKNOWN",
        _liq_bucket(anchor.liquidity_score),
    )


def _cluster_stats(cluster: List[ResearchAnchor], cfg: PrototypeConfig) -> dict:
    returns = [float(a.after_cost_return_pct or 0.0) for a in cluster]
    maes = [float(a.mae_pct or 0.0) for a in cluster]
    mfes = [float(a.mfe_pct or 0.0) for a in cluster]
    support_count = len(cluster)
    decayed_support = _decayed_support(cluster, cfg.recency_halflife_days)
    dispersion = pstdev(returns) if len(returns) > 1 else 0.0
    uncertainty = dispersion / max(np.sqrt(support_count), 1.0)
    freshness = _freshness_days(cluster)
    return {
        "support_count": support_count,
        "decayed_support": decayed_support,
        "mean_return_pct": mean(returns) if returns else 0.0,
        "median_return_pct": median(returns) if returns else 0.0,
        "win_rate": sum(1 for r in returns if r > 0) / max(len(returns), 1),
        "mae_mean_pct": mean(maes) if maes else 0.0,
        "mfe_mean_pct": mean(mfes) if mfes else 0.0,
        "return_dispersion": dispersion,
        "uncertainty": uncertainty,
        "freshness_days": freshness,
    }


def build_anchor_prototypes(anchors: Iterable[ResearchAnchor], config: PrototypeConfig | None = None) -> List[PrototypeAnchor]:
    cfg = config or PrototypeConfig()
    groups: Dict[tuple, List[ResearchAnchor]] = {}
    for anchor in anchors:
        if anchor.anchor_quality < cfg.min_anchor_quality:
            continue
        groups.setdefault(_cluster_key(anchor), []).append(anchor)

    out: List[PrototypeAnchor] = []
    for (side, regime_bucket, sector_bucket, liquidity_bucket), members in groups.items():
        clusters: List[List[ResearchAnchor]] = []
        for anchor in members:
            vec = np.asarray(anchor.embedding, dtype=float)
            matched = False
            for cluster in clusters:
                rep = _medoid(cluster)
                if _cos(vec, np.asarray(rep.embedding, dtype=float)) >= cfg.dedup_similarity_threshold:
                    cluster.append(anchor)
                    matched = True
                    break
            if not matched:
                clusters.append([anchor])

        for idx, cluster in enumerate(clusters, start=1):
            stats = _cluster_stats(cluster, cfg)
            if stats["support_count"] < cfg.min_support_count:
                continue
            if stats["decayed_support"] < cfg.min_decayed_support:
                continue
            if stats["freshness_days"] > cfg.max_age_days:
                continue
            rep = _medoid(cluster)
            out.append(
                PrototypeAnchor(
                    prototype_id=f"{side}:{regime_bucket}:{sector_bucket}:{liquidity_bucket}:{idx}",
                    anchor_code=rep.anchor_code,
                    side=side,
                    embedding=list(rep.embedding),
                    member_count=len(cluster),
                    representative_symbol=rep.symbol,
                    representative_date=rep.reference_date,
                    shape_vector=list(rep.shape_vector),
                    ctx_vector=list(rep.ctx_vector),
                    vector_version=rep.vector_version,
                    feature_version=rep.metadata.get("feature_version"),
                    embedding_model=rep.embedding_model,
                    vector_dim=rep.vector_dim,
                    anchor_quality=float(mean([a.anchor_quality for a in cluster])),
                    regime_code=regime_bucket,
                    sector_code=sector_bucket,
                    liquidity_score=float(mean([float(a.liquidity_score or 0.0) for a in cluster])),
                    support_count=stats["support_count"],
                    decayed_support=stats["decayed_support"],
                    mean_return_pct=stats["mean_return_pct"],
                    median_return_pct=stats["median_return_pct"],
                    win_rate=stats["win_rate"],
                    mae_mean_pct=stats["mae_mean_pct"],
                    mfe_mean_pct=stats["mfe_mean_pct"],
                    return_dispersion=stats["return_dispersion"],
                    uncertainty=stats["uncertainty"],
                    freshness_days=stats["freshness_days"],
                    liquidity_bucket=liquidity_bucket,
                    regime_bucket=regime_bucket,
                    sector_bucket=sector_bucket,
                    prototype_membership={
                        "member_symbols": [c.symbol for c in cluster],
                        "member_dates": [c.reference_date for c in cluster],
                        "member_anchor_codes": [c.anchor_code for c in cluster],
                    },
                    metadata={
                        "cross_regime_merge": False,
                        "representative_kind": "medoid",
                    },
                )
            )
    return out
