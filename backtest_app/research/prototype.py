from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from statistics import mean, median, pstdev
from typing import Dict, Iterable, List

import numpy as np

from .models import EventOutcomeRecord, PrototypeAnchor, ResearchAnchor


@dataclass(frozen=True)
class PrototypeConfig:
    dedup_similarity_threshold: float = 0.985
    min_anchor_quality: float = 0.0
    min_support_count: int = 1
    min_decayed_support: float = 0.0
    max_age_days: int = 365
    recency_halflife_days: float = 90.0
    memory_version: str = "memory_asof_v1"


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


def _freshness_days(members: List[ResearchAnchor], as_of_date: str | None = None) -> float:
    dates = [d for d in (_parse_date(a.reference_date) for a in members) if d is not None]
    if not dates:
        return 9999.0
    latest = max(dates)
    ref = _parse_date(as_of_date) or latest
    return float((ref - latest).days)


def _decayed_support_from_dates(dates: List[date], halflife_days: float, as_of_date: str | None = None) -> float:
    if not dates:
        return 0.0
    ref = _parse_date(as_of_date) or max(dates)
    out = 0.0
    for d in dates:
        age = max(0, (ref - d).days)
        out += 0.5 ** (age / max(halflife_days, 1.0))
    return out


def _decayed_support(members: List[ResearchAnchor], halflife_days: float, as_of_date: str | None = None) -> float:
    dates = [d for d in (_parse_date(a.reference_date) for a in members) if d is not None]
    return _decayed_support_from_dates(dates, halflife_days, as_of_date=as_of_date)


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
        anchor.regime_code or "UNKNOWN",
        anchor.sector_code or "UNKNOWN",
        _liq_bucket(anchor.liquidity_score),
    )


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.quantile(np.asarray(values, dtype=float), q))


def _cluster_stats(cluster: List[ResearchAnchor], cfg: PrototypeConfig, *, as_of_date: str | None = None) -> dict:
    returns = [float(a.after_cost_return_pct or 0.0) for a in cluster]
    maes = [float(a.mae_pct or 0.0) for a in cluster]
    mfes = [float(a.mfe_pct or 0.0) for a in cluster]
    support_count = len(cluster)
    decayed_support = _decayed_support(cluster, cfg.recency_halflife_days, as_of_date=as_of_date)
    dispersion = pstdev(returns) if len(returns) > 1 else 0.0
    uncertainty = dispersion / max(np.sqrt(support_count), 1.0)
    freshness = _freshness_days(cluster, as_of_date=as_of_date)
    return {
        "support_count": support_count,
        "decayed_support": decayed_support,
        "mean_return_pct": mean(returns) if returns else 0.0,
        "median_return_pct": median(returns) if returns else 0.0,
        "win_rate": sum(1 for r in returns if r > 0) / max(len(returns), 1),
        "mae_mean_pct": mean(maes) if maes else 0.0,
        "mfe_mean_pct": mean(mfes) if mfes else 0.0,
        "return_q10_pct": _quantile(returns, 0.10),
        "return_q90_pct": _quantile(returns, 0.90),
        "return_dispersion": dispersion,
        "uncertainty": uncertainty,
        "freshness_days": freshness,
    }


def _representative_hash(anchor: ResearchAnchor) -> str:
    payload = json.dumps({"symbol": anchor.symbol, "reference_date": anchor.reference_date, "embedding": [round(float(x), 8) for x in anchor.embedding]}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _prototype_id(*, as_of_date: str | None, memory_version: str, cluster_key: tuple, representative: ResearchAnchor) -> str:
    rep_hash = _representative_hash(representative)
    return f"{as_of_date or representative.reference_date}:{memory_version}:{'|'.join(cluster_key)}:{rep_hash}"


def build_anchor_prototypes(anchors: Iterable[ResearchAnchor], config: PrototypeConfig | None = None, *, as_of_date: str | None = None) -> List[PrototypeAnchor]:
    cfg = config or PrototypeConfig()
    groups: Dict[tuple, List[ResearchAnchor]] = {}
    for anchor in anchors:
        if anchor.anchor_quality < cfg.min_anchor_quality:
            continue
        groups.setdefault((anchor.side, *_cluster_key(anchor)), []).append(anchor)

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

        for cluster in clusters:
            stats = _cluster_stats(cluster, cfg, as_of_date=as_of_date)
            if stats["support_count"] < cfg.min_support_count or stats["decayed_support"] < cfg.min_decayed_support or stats["freshness_days"] > cfg.max_age_days:
                continue
            rep = _medoid(cluster)
            prototype_id = _prototype_id(as_of_date=as_of_date, memory_version=cfg.memory_version or str(rep.vector_version or "memory"), cluster_key=(side, regime_bucket, sector_bucket, liquidity_bucket), representative=rep)
            out.append(
                PrototypeAnchor(
                    prototype_id=prototype_id,
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
                        "return_q10_pct": stats["return_q10_pct"],
                        "return_q90_pct": stats["return_q90_pct"],
                        "memory_version": cfg.memory_version,
                        "as_of_date": as_of_date,
                        "representative_hash": _representative_hash(rep),
                    },
                )
            )
    out.sort(key=lambda p: p.prototype_id)
    return out


def build_prototype_snapshot_from_event_memory(*, event_records: Iterable[EventOutcomeRecord], as_of_date: str, memory_version: str, config: PrototypeConfig | None = None) -> dict:
    cfg = config or PrototypeConfig(memory_version=memory_version)
    groups: Dict[tuple, list[dict]] = {}
    lineage: dict[str, list[dict]] = {}
    for event in event_records:
        if event.outcome_end_date and event.outcome_end_date >= as_of_date:
            continue
        path = dict(event.path_summary or {})
        regime = str(event.diagnostics.get("regime_code") or path.get("regime_code") or "UNKNOWN")
        sector = str(event.diagnostics.get("sector_code") or path.get("sector_code") or "UNKNOWN")
        liquidity_bucket = str(path.get("liquidity_bucket") or "UNKNOWN")
        for side in ("BUY", "SELL"):
            side_outcome = dict((event.side_outcomes or {}).get(side) or {})
            key = (regime, sector, liquidity_bucket)
            groups.setdefault(key, []).append({"event": event, "side": side, "outcome": side_outcome})

    prototypes = []
    for key, members in sorted(groups.items()):
        regime, sector, liquidity_bucket = key
        side_stats = {}
        member_refs = []
        representative_basis = []
        for side in ("BUY", "SELL"):
            side_members = [m for m in members if m["side"] == side]
            returns = [float(m["outcome"].get("after_cost_return_pct", 0.0) or 0.0) for m in side_members]
            maes = [float(m["outcome"].get("mae_pct", 0.0) or 0.0) for m in side_members]
            mfes = [float(m["outcome"].get("mfe_pct", 0.0) or 0.0) for m in side_members]
            dates = [_parse_date(m["event"].event_date) for m in side_members if _parse_date(m["event"].event_date) is not None]
            support_count = len(side_members)
            side_stats[side] = {
                "support_count": support_count,
                "decayed_support": _decayed_support_from_dates(dates, cfg.recency_halflife_days, as_of_date=as_of_date),
                "mean_return_pct": mean(returns) if returns else 0.0,
                "median_return_pct": median(returns) if returns else 0.0,
                "win_rate": sum(1 for r in returns if r > 0) / max(len(returns), 1),
                "mae_mean_pct": mean(maes) if maes else 0.0,
                "mfe_mean_pct": mean(mfes) if mfes else 0.0,
                "return_q10_pct": _quantile(returns, 0.10),
                "return_q90_pct": _quantile(returns, 0.90),
                "dispersion": pstdev(returns) if len(returns) > 1 else 0.0,
                "uncertainty": (pstdev(returns) if len(returns) > 1 else 0.0) / max(np.sqrt(support_count), 1.0),
                "freshness_days": float(((_parse_date(as_of_date) or date.today()) - max(dates)).days) if dates else 9999.0,
            }
            member_refs.extend([{"symbol": m["event"].symbol, "event_date": m["event"].event_date, "outcome_end_date": m["event"].outcome_end_date, "side": side} for m in side_members])
            representative_basis.extend([f"{m['event'].symbol}:{m['event'].event_date}:{side}" for m in side_members])
        rep_hash = hashlib.sha256("|".join(sorted(representative_basis)).encode("utf-8")).hexdigest()[:12]
        prototype_id = f"{as_of_date}:{memory_version}:{'|'.join(key)}:{rep_hash}"
        lineage[prototype_id] = member_refs
        prototypes.append({"prototype_id": prototype_id, "as_of_date": as_of_date, "memory_version": memory_version, "cluster_key": {"regime_code": regime, "sector_code": sector, "liquidity_bucket": liquidity_bucket}, "representative_hash": rep_hash, "stats": side_stats, "membership": member_refs})
    return {"as_of_date": as_of_date, "memory_version": memory_version, "prototype_count": len(prototypes), "prototypes": prototypes, "lineage": lineage}
