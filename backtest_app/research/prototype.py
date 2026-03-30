from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from statistics import mean, median, pstdev
from typing import Dict, Iterable, List

import numpy as np

from .models import EventOutcomeRecord, PrototypeAnchor, ResearchAnchor, StatePrototype


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


def _decayed_support_from_dates(dates: List[date], halflife_days: float, as_of_date: str | None = None) -> float:
    if not dates:
        return 0.0
    ref = _parse_date(as_of_date) or max(dates)
    out = 0.0
    for d in dates:
        age = max(0, (ref - d).days)
        out += 0.5 ** (age / max(halflife_days, 1.0))
    return out


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.quantile(np.asarray(values, dtype=float), q))


def _state_side_stats(members: list[dict], cfg: PrototypeConfig, *, as_of_date: str | None = None) -> dict:
    returns = [float(m.get("after_cost_return_pct", 0.0) or 0.0) for m in members]
    maes = [abs(float(m.get("mae_pct", 0.0) or 0.0)) for m in members]
    mfes = [float(m.get("mfe_pct", 0.0) or 0.0) for m in members]
    dates = [_parse_date(m.get("event_date")) for m in members if _parse_date(m.get("event_date")) is not None]
    support_count = len(members)
    dispersion = pstdev(returns) if len(returns) > 1 else 0.0

    def _count(member: dict, key: str, fallback: int) -> int:
        if key in member:
            return int(member.get(key, 0) or 0)
        return fallback

    target_first_count = sum(_count(m, "target_first_count", 1 if str(m.get("first_touch_label") or "").upper() == "UP_FIRST" else 0) for m in members)
    stop_first_count = sum(_count(m, "stop_first_count", 1 if str(m.get("first_touch_label") or "").upper() == "DOWN_FIRST" else 0) for m in members)
    flat_count = sum(_count(m, "flat_count", 1 if bool(m.get("flat")) or str(m.get("first_touch_label") or "").upper() == "FLAT" else 0) for m in members)
    ambiguous_count = sum(_count(m, "ambiguous_count", 1 if bool(m.get("ambiguous")) or str(m.get("first_touch_label") or "").upper() == "AMBIGUOUS" else 0) for m in members)
    no_trade_count = sum(_count(m, "no_trade_count", 1 if bool(m.get("no_trade")) or str(m.get("first_touch_label") or "").upper() == "NO_TRADE" else 0) for m in members)
    horizon_up_count = sum(_count(m, "horizon_up_count", 1 if str(m.get("first_touch_label") or "").upper() == "HORIZON_UP" else 0) for m in members)
    horizon_down_count = sum(_count(m, "horizon_down_count", 1 if str(m.get("first_touch_label") or "").upper() == "HORIZON_DOWN" else 0) for m in members)
    total_outcomes = max(target_first_count + stop_first_count + flat_count + ambiguous_count + no_trade_count, support_count, 1)
    return {
        "support_count": support_count,
        "decayed_support": _decayed_support_from_dates(dates, cfg.recency_halflife_days, as_of_date=as_of_date),
        "mean_return_pct": mean(returns) if returns else 0.0,
        "median_return_pct": median(returns) if returns else 0.0,
        "win_rate": sum(1 for r in returns if r > 0) / max(len(returns), 1),
        "mae_mean_pct": mean(maes) if maes else 0.0,
        "mfe_mean_pct": mean(mfes) if mfes else 0.0,
        "return_q10_pct": _quantile(returns, 0.10),
        "return_q50_pct": _quantile(returns, 0.50),
        "return_q90_pct": _quantile(returns, 0.90),
        "return_dispersion": dispersion,
        "uncertainty": dispersion / max(np.sqrt(support_count), 1.0),
        "freshness_days": float(((_parse_date(as_of_date) or date.today()) - max(dates)).days) if dates else 9999.0,
        "target_first_count": target_first_count,
        "stop_first_count": stop_first_count,
        "flat_count": flat_count,
        "ambiguous_count": ambiguous_count,
        "no_trade_count": no_trade_count,
        "horizon_up_count": horizon_up_count,
        "horizon_down_count": horizon_down_count,
        "p_target_first": target_first_count / total_outcomes,
        "p_stop_first": stop_first_count / total_outcomes,
        "p_flat": flat_count / total_outcomes,
        "p_ambiguous": ambiguous_count / total_outcomes,
        "p_no_trade": no_trade_count / total_outcomes,
    }


def _representative_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:12]


def build_state_prototypes_from_event_memory(*, event_records: Iterable[EventOutcomeRecord], as_of_date: str, memory_version: str, spec_hash: str, config: PrototypeConfig | None = None) -> list[StatePrototype]:
    cfg = config or PrototypeConfig(memory_version=memory_version)
    eligible = [e for e in event_records if not e.outcome_end_date or e.outcome_end_date < as_of_date]
    clusters: list[list[dict]] = []
    for event in eligible:
        transformed_features = dict(((event.diagnostics or {}).get("transformed_features") or (event.path_summary or {}).get("transformed_features") or {}))
        emb = list(((event.diagnostics or {}).get("embedding") or (event.path_summary or {}).get("embedding") or []))
        if transformed_features and not emb:
            feature_keys = sorted(transformed_features.keys())
            emb = [float(transformed_features[k]) for k in feature_keys]
        if not emb:
            emb = [0.0, 0.0]
        row = {
            "event": event,
            "embedding": emb,
            "shape_vector": list(((event.diagnostics or {}).get("shape_vector") or [])),
            "ctx_vector": list(((event.diagnostics or {}).get("ctx_vector") or [])),
            "raw_features": dict(((event.diagnostics or {}).get("raw_features") or (event.path_summary or {}).get("raw_features") or {})),
            "transformed_features": transformed_features,
            "transform_version": str((event.diagnostics or {}).get("transform_version") or (event.path_summary or {}).get("transform_version") or "unknown"),
            "regime_code": str(event.diagnostics.get("regime_code") or event.path_summary.get("regime_code") or "UNKNOWN"),
            "sector_code": str(event.diagnostics.get("sector_code") or event.path_summary.get("sector_code") or "UNKNOWN"),
            "liquidity_bucket": str(event.path_summary.get("liquidity_bucket") or "UNKNOWN"),
            "anchor_quality": float(event.diagnostics.get("quality_score", 1.0) or 1.0),
            "liquidity_score": float(event.diagnostics.get("liquidity_score", 0.0) or 0.0),
        }
        vec = np.asarray(row["embedding"], dtype=float)
        matched = False
        for cluster in clusters:
            rep = np.asarray(cluster[0]["embedding"], dtype=float)
            if _cos(vec, rep) >= cfg.dedup_similarity_threshold:
                cluster.append(row)
                matched = True
                break
        if not matched:
            clusters.append([row])
    prototypes: list[StatePrototype] = []
    for cluster in clusters:
        events = [r["event"] for r in cluster]
        member_dates = [_parse_date(e.event_date) for e in events if _parse_date(e.event_date) is not None]
        decayed_support = _decayed_support_from_dates(member_dates, cfg.recency_halflife_days, as_of_date=as_of_date)
        if len(cluster) < cfg.min_support_count or decayed_support < cfg.min_decayed_support:
            continue
        rep = max(cluster, key=lambda r: (r["anchor_quality"], r["liquidity_score"]))
        rep_payload = {"symbol": rep["event"].symbol, "event_date": rep["event"].event_date, "embedding": [round(float(x), 8) for x in rep["embedding"]]}
        representative_hash = _representative_hash(rep_payload)
        prior_buckets = {"regime": sorted({r["regime_code"] for r in cluster}), "sector": sorted({r["sector_code"] for r in cluster}), "liquidity": sorted({r["liquidity_bucket"] for r in cluster})}
        member_refs = [{"symbol": e.symbol, "event_date": e.event_date, "outcome_end_date": e.outcome_end_date} for e in events]
        lineage = [{
            "ref": f"{e.symbol}:{e.event_date}",
            "side_outcomes": dict(e.side_outcomes or {}),
            "raw_features": dict((e.diagnostics or {}).get("raw_features") or (e.path_summary or {}).get("raw_features") or {}),
            "transformed_features": dict((e.diagnostics or {}).get("transformed_features") or (e.path_summary or {}).get("transformed_features") or {}),
            "transform_version": (e.diagnostics or {}).get("transform_version") or (e.path_summary or {}).get("transform_version"),
        } for e in events]
        side_stats = {side: _state_side_stats([{**dict((e.side_outcomes or {}).get(side) or {}), "event_date": e.event_date} for e in events], cfg, as_of_date=as_of_date) for side in ("BUY", "SELL")}
        prototype_id = f"{as_of_date}:{memory_version}:{representative_hash}"
        prototypes.append(StatePrototype(prototype_id=prototype_id, anchor_code="STATE_MEMORY_V1", embedding=list(rep["embedding"]), member_count=len(cluster), representative_symbol=rep["event"].symbol, representative_date=rep["event"].event_date, representative_hash=representative_hash, shape_vector=list(rep["shape_vector"]), ctx_vector=list(rep["ctx_vector"]), vector_version=memory_version, feature_version=spec_hash, embedding_model="event-memory-state", vector_dim=len(rep["embedding"]), anchor_quality=float(mean([r["anchor_quality"] for r in cluster])), regime_code=rep["regime_code"], sector_code=rep["sector_code"], liquidity_score=float(mean([r["liquidity_score"] for r in cluster])), support_count=len(cluster), decayed_support=decayed_support, freshness_days=float(((_parse_date(as_of_date) or date.today()) - max(member_dates)).days) if member_dates else 9999.0, exchange_code=rep["event"].exchange_code, country_code=rep["event"].country_code, exchange_tz=rep["event"].exchange_tz, session_date_local=rep["event"].session_date_local, session_close_ts_utc=rep["event"].session_close_ts_utc, feature_anchor_ts_utc=rep["event"].feature_anchor_ts_utc, prototype_membership={"member_refs": member_refs, "lineage": lineage}, side_stats=side_stats, metadata={"as_of_date": as_of_date, "memory_version": memory_version, "spec_hash": spec_hash, "representative_hash": representative_hash, "prior_buckets": prior_buckets, "raw_features": rep["raw_features"], "transformed_features": rep["transformed_features"], "transform_version": rep["transform_version"], "exchange_code": rep["event"].exchange_code, "country_code": rep["event"].country_code, "exchange_tz": rep["event"].exchange_tz, "session_date_local": rep["event"].session_date_local, "session_close_ts_utc": rep["event"].session_close_ts_utc, "feature_anchor_ts_utc": rep["event"].feature_anchor_ts_utc}))
    prototypes.sort(key=lambda p: p.prototype_id)
    return prototypes


def build_anchor_prototypes(anchors: Iterable[ResearchAnchor], config: PrototypeConfig | None = None, *, as_of_date: str | None = None) -> List[PrototypeAnchor]:
    cfg = config or PrototypeConfig()
    out: List[PrototypeAnchor] = []
    for anchor in anchors:
        rep_hash = _representative_hash({"symbol": anchor.symbol, "reference_date": anchor.reference_date, "embedding": [round(float(x), 8) for x in anchor.embedding]})
        out.append(PrototypeAnchor(prototype_id=f"{as_of_date or anchor.reference_date}:{cfg.memory_version}:{anchor.side}:{rep_hash}", anchor_code=anchor.anchor_code, side=anchor.side, embedding=list(anchor.embedding), member_count=1, representative_symbol=anchor.symbol, representative_date=anchor.reference_date, shape_vector=list(anchor.shape_vector), ctx_vector=list(anchor.ctx_vector), vector_version=anchor.vector_version, feature_version=anchor.metadata.get("feature_version"), embedding_model=anchor.embedding_model, vector_dim=anchor.vector_dim, anchor_quality=anchor.anchor_quality, regime_code=anchor.regime_code, sector_code=anchor.sector_code, liquidity_score=anchor.liquidity_score, support_count=1, decayed_support=1.0, mean_return_pct=float(anchor.after_cost_return_pct or 0.0), median_return_pct=float(anchor.after_cost_return_pct or 0.0), win_rate=1.0 if float(anchor.after_cost_return_pct or 0.0) > 0 else 0.0, mae_mean_pct=abs(float(anchor.mae_pct or 0.0)), mfe_mean_pct=float(anchor.mfe_pct or 0.0), return_dispersion=0.0, uncertainty=0.0, freshness_days=0.0, liquidity_bucket=_liq_bucket(anchor.liquidity_score), regime_bucket=anchor.regime_code, sector_bucket=anchor.sector_code, exchange_code=anchor.exchange_code, country_code=anchor.country_code, exchange_tz=anchor.exchange_tz, session_date_local=anchor.session_date_local, session_close_ts_utc=anchor.session_close_ts_utc, feature_anchor_ts_utc=anchor.feature_anchor_ts_utc, prototype_membership=anchor.prototype_membership, metadata={"representative_hash": rep_hash, "legacy_wrapper": True}))
    return out


def build_prototype_snapshot_from_event_memory(*, event_records: Iterable[EventOutcomeRecord], as_of_date: str, memory_version: str, config: PrototypeConfig | None = None, spec_hash: str = "unknown") -> dict:
    prototypes = build_state_prototypes_from_event_memory(event_records=event_records, as_of_date=as_of_date, memory_version=memory_version, spec_hash=spec_hash, config=config)
    return {"as_of_date": as_of_date, "memory_version": memory_version, "spec_hash": spec_hash, "prototype_count": len(prototypes), "prototypes": [p.__dict__ for p in prototypes], "lineage": {p.prototype_id: p.prototype_membership.get("member_refs", []) for p in prototypes}}
