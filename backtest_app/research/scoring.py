from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional

import numpy as np

from .models import DecisionSurface, DistributionEstimate, StatePrototype
from .repository import CandidateIndex


@dataclass(frozen=True)
class ScoringConfig:
    similarity_weight: float = 0.35
    anchor_quality_weight: float = 0.15
    regime_match_weight: float = 0.10
    sector_match_weight: float = 0.10
    liquidity_weight: float = 0.05
    support_weight: float = 0.10
    return_weight: float = 0.10
    win_rate_weight: float = 0.03
    uncertainty_penalty_weight: float = 0.02
    min_liquidity_score: float = 0.0
    require_sector_match: bool = False
    min_support_count: int = 1


@dataclass(frozen=True)
class EVConfig:
    top_k: int = 5
    kernel_temperature: float = 12.0
    prototype_retrieval_k: int = 24
    member_retrieval_k: int = 96
    min_effective_sample_size: float = 1.5
    max_uncertainty: float = 0.08
    min_expected_utility: float = 0.005
    min_regime_alignment: float = 0.5
    use_kernel_weighting: bool = True
    max_return_interval_width: float = 0.08
    abstain_margin: float = 0.05
    diagnostic_disable_lower_bound_gate: bool = False
    diagnostic_disable_ess_gate: bool = False
    diagnostic_lower_bound_formula: str = "lb_v1"
    diagnostic_feasible_side_chooser: bool = False


@dataclass(frozen=True)
class CandidateScore:
    prototype_id: str
    anchor_code: str
    score: float
    similarity: float
    anchor_quality: float
    regime_match: float
    sector_match: float
    liquidity_score: float
    diagnostics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class EVEstimate:
    side: str
    expected_utility: float
    expected_net_return: float
    p_up_first: float
    p_down_first: float
    expected_mae: float
    expected_mfe: float
    uncertainty: float
    dispersion: float
    effective_sample_size: float
    regime_alignment: float
    calibrated_ev: float
    calibrated_win_prob: float
    abstained: bool
    abstain_reasons: list[str] = field(default_factory=list)
    top_matches: list[dict] = field(default_factory=list)
    diagnostics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CalibrationModel:
    method: str = "logistic"
    slope: float = 1.0
    intercept: float = 0.0

    def calibrate_prob(self, x: float) -> float:
        if self.method == "identity":
            return max(0.0, min(1.0, x))
        z = self.slope * float(x) + self.intercept
        return float(1.0 / (1.0 + np.exp(-z)))

    def calibrate_ev(self, ev: float) -> float:
        if self.method == "identity":
            return float(ev)
        return float(ev * max(0.0, min(1.5, self.slope)) + self.intercept * 0.01)


@dataclass(frozen=True)
class CalibrationFoldArtifact:
    fold_id: str
    train_indices: list[int]
    test_indices: list[int]
    model: CalibrationModel
    train_size: int
    test_size: int
    raw_mean: float
    calibrated_mean: float


def fit_calibration(*, scores: list[float], targets: list[int], method: str = "logistic") -> CalibrationModel:
    if not scores or not targets or len(scores) != len(targets):
        return CalibrationModel(method="identity")
    x = np.asarray(scores, dtype=float)
    y = np.asarray(targets, dtype=float)
    x_mean = float(np.mean(x)) if len(x) else 0.0
    y_mean = float(np.mean(y)) if len(y) else 0.5
    cov = float(np.mean((x - x_mean) * (y - y_mean))) if len(x) else 0.0
    var = float(np.mean((x - x_mean) ** 2)) if len(x) else 0.0
    slope = 1.0 if var <= 1e-12 else max(0.1, min(10.0, cov / var * 10.0))
    intercept = float(np.log(max(1e-6, y_mean) / max(1e-6, 1.0 - y_mean))) - slope * x_mean
    return CalibrationModel(method=method, slope=slope, intercept=intercept)


def fit_calibration_on_fold(*, fold_id: str, raw_scores: list[float], targets: list[int], train_indices: list[int], test_indices: list[int], method: str = "logistic") -> CalibrationFoldArtifact:
    train_scores = [raw_scores[i] for i in train_indices if i < len(raw_scores)]
    train_targets = [targets[i] for i in train_indices if i < len(targets)]
    model = fit_calibration(scores=train_scores, targets=train_targets, method=method)
    calibrated_test = [model.calibrate_ev(raw_scores[i]) for i in test_indices if i < len(raw_scores)]
    raw_test = [raw_scores[i] for i in test_indices if i < len(raw_scores)]
    return CalibrationFoldArtifact(fold_id=fold_id, train_indices=list(train_indices), test_indices=list(test_indices), model=model, train_size=len(train_scores), test_size=len(raw_test), raw_mean=float(np.mean(raw_test)) if raw_test else 0.0, calibrated_mean=float(np.mean(calibrated_test)) if calibrated_test else 0.0)


def apply_calibration_to_test(*, raw_scores: list[float], raw_probs: list[float], fold: CalibrationFoldArtifact) -> dict:
    calibrated_scores = []
    calibrated_probs = []
    for i in fold.test_indices:
        if i >= len(raw_scores) or i >= len(raw_probs):
            continue
        calibrated_scores.append({"index": i, "raw_ev": raw_scores[i], "calibrated_ev": fold.model.calibrate_ev(raw_scores[i])})
        calibrated_probs.append({"index": i, "raw_prob": raw_probs[i], "calibrated_prob": fold.model.calibrate_prob(raw_probs[i])})
    return {"fold_id": fold.fold_id, "calibrated_scores": calibrated_scores, "calibrated_probs": calibrated_probs, "artifact": {"method": fold.model.method, "slope": fold.model.slope, "intercept": fold.model.intercept, "train_size": fold.train_size, "test_size": fold.test_size, "raw_mean": fold.raw_mean, "calibrated_mean": fold.calibrated_mean}}


def _cos(a: list[float], b: list[float]) -> float:
    av = np.asarray(a, dtype=float)
    bv = np.asarray(b, dtype=float)
    an = np.linalg.norm(av)
    bn = np.linalg.norm(bv)
    if an <= 0.0 or bn <= 0.0:
        return 0.0
    return float(np.dot(av, bv) / (an * bn))


def _ranked_candidates(*, query_embedding: list[float], candidates: Iterable[StatePrototype], candidate_index: CandidateIndex | None) -> list[StatePrototype]:
    ranked_candidates = list(candidates)
    if candidate_index is not None:
        ranked_candidates = candidate_index.rank(query_embedding=query_embedding, candidates=ranked_candidates)
    return ranked_candidates


def _side_row(candidate: StatePrototype, side: str) -> dict:
    return dict((candidate.side_stats or {}).get(side) or {})


def score_candidates_exact(*, query_embedding: list[float], candidates: Iterable[StatePrototype], regime_code: Optional[str], sector_code: Optional[str], min_liquidity_score: float = 0.0, config: ScoringConfig | None = None, candidate_index: CandidateIndex | None = None, side: str = "BUY") -> List[CandidateScore]:
    cfg = config or ScoringConfig(min_liquidity_score=min_liquidity_score)
    ranked_candidates = _ranked_candidates(query_embedding=query_embedding, candidates=candidates, candidate_index=candidate_index)
    out: List[CandidateScore] = []
    for candidate in ranked_candidates:
        side_stats = _side_row(candidate, side)
        liquidity = float(candidate.liquidity_score or 0.0)
        support_count = int(side_stats.get("support_count", candidate.support_count or candidate.member_count or 0))
        if liquidity < cfg.min_liquidity_score or support_count < cfg.min_support_count:
            continue
        sector_match = 1.0 if sector_code and candidate.sector_code == sector_code else 0.0
        if cfg.require_sector_match and sector_code and sector_match <= 0.0:
            continue
        regime_match = 1.0 if regime_code and candidate.regime_code == regime_code else 0.0
        similarity = _cos(query_embedding, candidate.embedding)
        support_score = min(1.0, float(side_stats.get("decayed_support", candidate.decayed_support or 0.0)) / 5.0)
        return_score = float(side_stats.get("mean_return_pct", 0.0))
        win_rate = float(side_stats.get("win_rate", 0.0))
        uncertainty = float(side_stats.get("uncertainty", 0.0))
        score = cfg.similarity_weight * similarity + cfg.anchor_quality_weight * float(candidate.anchor_quality) + cfg.regime_match_weight * regime_match + cfg.sector_match_weight * sector_match + cfg.liquidity_weight * liquidity + cfg.support_weight * support_score + cfg.return_weight * return_score + cfg.win_rate_weight * win_rate - cfg.uncertainty_penalty_weight * uncertainty
        out.append(CandidateScore(prototype_id=candidate.prototype_id, anchor_code=candidate.anchor_code, score=float(score), similarity=float(similarity), anchor_quality=float(candidate.anchor_quality), regime_match=regime_match, sector_match=sector_match, liquidity_score=liquidity, diagnostics={"member_count": candidate.member_count, "support_count": support_count, "decayed_support": side_stats.get("decayed_support"), "mean_return_pct": return_score, "win_rate": win_rate, "uncertainty": uncertainty, "representative_symbol": candidate.representative_symbol, "regime_bucket": candidate.metadata.get("prior_buckets", {}).get("regime"), "sector_bucket": candidate.metadata.get("prior_buckets", {}).get("sector"), "liquidity_bucket": candidate.metadata.get("prior_buckets", {}).get("liquidity")}))
    out.sort(key=lambda x: x.score, reverse=True)
    return out


def _weighted_quantile(values: list[float], weights: np.ndarray, q: float) -> float:
    if not values:
        return 0.0
    order = np.argsort(np.asarray(values, dtype=float))
    vals = np.asarray(values, dtype=float)[order]
    w = weights[order]
    cdf = np.cumsum(w) / max(float(np.sum(w)), 1e-12)
    return float(vals[np.searchsorted(cdf, q, side="left")])


def _weighted_mean(values: list[float], weights: np.ndarray) -> float:
    if not values:
        return 0.0
    vals = np.asarray(values, dtype=float)
    return float(np.sum(vals * weights))


def _weighted_std(values: list[float], weights: np.ndarray) -> float:
    if not values:
        return 0.0
    vals = np.asarray(values, dtype=float)
    mean_value = float(np.sum(vals * weights))
    var = float(np.sum(weights * ((vals - mean_value) ** 2)))
    return float(np.sqrt(max(var, 0.0)))


def _resolve_lower_bound(*, formula: str, q10: float, q25: float, q50: float, q90: float, expected_net_return: float, uncertainty: float) -> float:
    interval_width = float(q90 - q10)
    key = str(formula or "lb_v1").strip().lower()
    if key == "lb_v3":
        return float(q25 - 0.5 * uncertainty)
    if key == "lb_v4":
        return float(expected_net_return - 0.5 * interval_width)
    return float(q10 - uncertainty)


def _consensus_signature(rows: list[dict]) -> str:
    signatures: list[str] = []
    for row in rows[:3]:
        candidate = row.get("candidate")
        representative_hash = getattr(candidate, "representative_hash", None)
        prototype_id = getattr(candidate, "prototype_id", None)
        token = str(representative_hash or prototype_id or "").strip()
        if token:
            signatures.append(token)
    return "|".join(signatures)


def _member_signature(rows: list[dict]) -> str:
    tokens: list[str] = []
    for row in rows[:3]:
        token = str(row.get("member_key") or "").strip()
        if token:
            tokens.append(token)
    return "|".join(tokens)


def _parse_member_ref(ref: str | None) -> tuple[str | None, str | None]:
    text = str(ref or "").strip()
    if not text:
        return None, None
    if ":" not in text:
        return text, None
    symbol, event_date = text.split(":", 1)
    return symbol or None, event_date or None


def _member_embedding(transformed_features: dict | None, expected_dim: int) -> list[float] | None:
    features = dict(transformed_features or {})
    if not features:
        return None
    values = [float(features[key]) for key in sorted(features.keys())]
    if expected_dim and len(values) != expected_dim:
        return None
    return values


def _member_side_stats(side_payload: dict) -> dict:
    label = str(side_payload.get("first_touch_label") or "").upper()
    return {
        "after_cost_return_pct": float(side_payload.get("after_cost_return_pct", 0.0) or 0.0),
        "mae_pct": abs(float(side_payload.get("mae_pct", 0.0) or 0.0)),
        "mfe_pct": float(side_payload.get("mfe_pct", 0.0) or 0.0),
        "close_return_d2_pct": float(side_payload.get("close_return_d2_pct", 0.0) or 0.0),
        "close_return_d3_pct": float(side_payload.get("close_return_d3_pct", 0.0) or 0.0),
        "resolved_by_d2": bool(side_payload.get("resolved_by_d2", False)),
        "resolved_by_d3": bool(side_payload.get("resolved_by_d3", False)),
        "p_target_first": 1.0 if label == "UP_FIRST" else 0.0,
        "p_stop_first": 1.0 if label == "DOWN_FIRST" else 0.0,
        "p_flat": 1.0 if bool(side_payload.get("flat")) or label == "FLAT" else 0.0,
        "p_ambiguous": 1.0 if bool(side_payload.get("ambiguous")) or label == "AMBIGUOUS" else 0.0,
        "p_no_trade": 1.0 if bool(side_payload.get("no_trade")) or label == "NO_TRADE" else 0.0,
        "first_touch_label": label,
    }


def _prototype_candidate_rows(
    *,
    side: str,
    query_embedding: list[float],
    candidates: Iterable[StatePrototype],
    regime_code: Optional[str],
    sector_code: Optional[str],
    cfg: EVConfig,
    candidate_index: CandidateIndex | None,
) -> tuple[list[StatePrototype], list[dict]]:
    ranked = _ranked_candidates(query_embedding=query_embedding, candidates=candidates, candidate_index=candidate_index)
    rows: list[dict] = []
    for candidate in ranked:
        side_stats = _side_row(candidate, side)
        if not side_stats:
            continue
        similarity = max(0.0, _cos(query_embedding, candidate.embedding))
        regime_alignment = 1.0 if regime_code and candidate.regime_code == regime_code else 0.0
        sector_alignment = 1.0 if sector_code and candidate.sector_code == sector_code else 0.0
        freshness_score = 1.0 / (1.0 + max(0.0, float(side_stats.get("freshness_days", candidate.freshness_days or 0.0))) / 30.0)
        support_score = min(1.0, float(side_stats.get("decayed_support", candidate.decayed_support or 0.0)) / 5.0)
        kernel = np.exp(cfg.kernel_temperature * (similarity - 1.0)) if cfg.use_kernel_weighting else similarity
        weight = float(kernel * (0.45 + 0.30 * support_score + 0.25 * freshness_score) * (0.40 + 0.60 * max(regime_alignment, sector_alignment)))
        rows.append(
            {
                "candidate": candidate,
                "similarity": similarity,
                "prototype_weight": weight,
                "regime_alignment": regime_alignment,
                "sector_alignment": sector_alignment,
                "support_score": support_score,
                "freshness_score": freshness_score,
                "side_stats": side_stats,
            }
        )
    rows.sort(key=lambda row: row["prototype_weight"], reverse=True)
    return ranked, rows


def exact_block_prototype_topk(
    *,
    query_embeddings: np.ndarray,
    prototype_embeddings: np.ndarray,
    query_regime_codes: Sequence[str | None],
    query_sector_codes: Sequence[str | None],
    prototype_regime_codes: Sequence[str | None],
    prototype_sector_codes: Sequence[str | None],
    prototype_side_stats: Sequence[dict[str, dict] | None],
    prototype_decayed_support: Sequence[float | None],
    prototype_freshness_days: Sequence[float | None],
    cfg: EVConfig | None = None,
) -> list[dict[str, Any]]:
    resolved_cfg = cfg or EVConfig()
    q = np.asarray(query_embeddings, dtype=float)
    if q.ndim == 1:
        q = q.reshape(1, -1)
    p = np.asarray(prototype_embeddings, dtype=float)
    if p.size == 0:
        return [{"BUY": {"top_indices": [], "prototype_pool_size": 0, "pre_truncation_candidate_count": 0, "positive_weight_candidate_count": 0}, "SELL": {"top_indices": [], "prototype_pool_size": 0, "pre_truncation_candidate_count": 0, "positive_weight_candidate_count": 0}, "similarities": np.zeros((0,), dtype=float)} for _ in range(q.shape[0])]
    q_norms = np.linalg.norm(q, axis=1, keepdims=True)
    q_norms = np.where(q_norms <= 1e-12, 1.0, q_norms)
    query_normed = q / q_norms
    similarities = np.clip(query_normed @ p.T, 0.0, None)
    freshness_scores = np.asarray(
        [1.0 / (1.0 + max(0.0, float(item or 0.0)) / 30.0) for item in prototype_freshness_days],
        dtype=float,
    )
    side_support_scores: dict[str, np.ndarray] = {}
    side_available: dict[str, np.ndarray] = {}
    for side in ("BUY", "SELL"):
        side_stats = [dict((payload or {}).get(side) or {}) for payload in prototype_side_stats]
        side_available[side] = np.asarray([bool(item) for item in side_stats], dtype=bool)
        side_support_scores[side] = np.asarray(
            [
                min(
                    1.0,
                    float((item.get("decayed_support") if item else None) or (prototype_decayed_support[idx] or 0.0)) / 5.0,
                )
                for idx, item in enumerate(side_stats)
            ],
            dtype=float,
        )
    out: list[dict[str, Any]] = []
    prototype_pool_size = int(p.shape[0])
    prototype_regime_codes = [str(item or "") for item in prototype_regime_codes]
    prototype_sector_codes = [str(item or "") for item in prototype_sector_codes]
    original_indices = np.arange(prototype_pool_size, dtype=int)
    for row_index in range(q.shape[0]):
        regime_code = str(query_regime_codes[row_index] or "")
        sector_code = str(query_sector_codes[row_index] or "")
        side_results: dict[str, Any] = {}
        similarity_row = np.asarray(similarities[row_index], dtype=float)
        for side in ("BUY", "SELL"):
            available_mask = side_available[side]
            available_indices = np.flatnonzero(available_mask)
            if available_indices.size == 0:
                side_results[side] = {
                    "top_indices": [],
                    "prototype_pool_size": prototype_pool_size,
                    "pre_truncation_candidate_count": 0,
                    "positive_weight_candidate_count": 0,
                }
                continue
            regime_alignment = np.asarray(
                [1.0 if regime_code and prototype_regime_codes[idx] == regime_code else 0.0 for idx in range(prototype_pool_size)],
                dtype=float,
            )
            sector_alignment = np.asarray(
                [1.0 if sector_code and prototype_sector_codes[idx] == sector_code else 0.0 for idx in range(prototype_pool_size)],
                dtype=float,
            )
            context_alignment = 0.40 + 0.60 * np.maximum(regime_alignment, sector_alignment)
            support_scores = side_support_scores[side]
            if resolved_cfg.use_kernel_weighting:
                kernel = np.exp(resolved_cfg.kernel_temperature * (similarity_row - 1.0))
            else:
                kernel = similarity_row
            weights = kernel * (0.45 + 0.30 * support_scores + 0.25 * freshness_scores) * context_alignment
            positive_weight_count = int(np.count_nonzero(weights[available_indices] > 0.0))
            top_k = min(int(resolved_cfg.prototype_retrieval_k), int(available_indices.size))
            if top_k <= 0:
                top_indices: list[int] = []
            elif available_indices.size <= top_k:
                ordered = np.lexsort(
                    (
                        original_indices[available_indices],
                        -similarity_row[available_indices],
                        -weights[available_indices],
                    )
                )
                top_indices = [int(value) for value in available_indices[ordered][:top_k]]
            else:
                candidate_weights = weights[available_indices]
                partition = np.argpartition(candidate_weights, -top_k)[-top_k:]
                threshold = float(np.min(candidate_weights[partition]))
                shortlist = available_indices[candidate_weights >= threshold]
                ordered = np.lexsort(
                    (
                        original_indices[shortlist],
                        -similarity_row[shortlist],
                        -weights[shortlist],
                    )
                )
                top_indices = [int(value) for value in shortlist[ordered][:top_k]]
            side_results[side] = {
                "top_indices": top_indices,
                "prototype_pool_size": prototype_pool_size,
                "pre_truncation_candidate_count": int(available_indices.size),
                "positive_weight_candidate_count": positive_weight_count,
            }
        side_results["similarities"] = similarity_row
        out.append(side_results)
    return out


def _expand_member_rows(
    *,
    side: str,
    query_embedding: list[float],
    query_date: str | None,
    prototype_rows: list[dict],
    regime_code: Optional[str],
    sector_code: Optional[str],
    cfg: EVConfig,
) -> tuple[list[dict], int]:
    raw_member_count = 0
    deduped: dict[tuple[str, str, str], dict] = {}
    query_dt = datetime.fromisoformat(query_date).date() if query_date else None
    expected_dim = len(query_embedding)
    for prototype_row in prototype_rows:
        candidate = prototype_row["candidate"]
        lineage = list((candidate.prototype_membership or {}).get("lineage") or [])
        for member in lineage:
            side_payload = dict((member.get("side_outcomes") or {}).get(side) or {})
            embedding = _member_embedding(dict(member.get("transformed_features") or {}), expected_dim)
            if not side_payload or embedding is None:
                continue
            raw_member_count += 1
            symbol, event_date = _parse_member_ref(member.get("ref"))
            if event_date is None:
                event_date = getattr(candidate, "representative_date", None)
            if symbol is None:
                symbol = getattr(candidate, "representative_symbol", None)
            dedup_key = (str(symbol or "UNKNOWN"), str(event_date or "UNKNOWN"), side)
            if dedup_key in deduped:
                continue
            similarity = max(0.0, _cos(query_embedding, embedding))
            regime_alignment = 1.0 if regime_code and candidate.regime_code == regime_code else 0.0
            sector_alignment = 1.0 if sector_code and candidate.sector_code == sector_code else 0.0
            support_prior = 0.55 + 0.45 * min(1.0, float((prototype_row.get("side_stats") or {}).get("decayed_support", candidate.decayed_support or 0.0)) / 5.0)
            age_days = float(candidate.freshness_days or 0.0)
            if query_dt is not None and event_date:
                try:
                    age_days = max(0.0, float((query_dt - datetime.fromisoformat(str(event_date)[:10]).date()).days))
                except ValueError:
                    age_days = float(candidate.freshness_days or 0.0)
            freshness_prior = 1.0 / (1.0 + max(age_days, 0.0) / 30.0)
            context_alignment = 0.40 + 0.60 * max(regime_alignment, sector_alignment)
            member_kernel = np.exp(cfg.kernel_temperature * (similarity - 1.0)) if cfg.use_kernel_weighting else similarity
            member_weight = float(member_kernel * support_prior * freshness_prior * context_alignment)
            deduped[dedup_key] = {
                "candidate": candidate,
                "member_key": f"{dedup_key[0]}:{dedup_key[1]}:{side}",
                "symbol": dedup_key[0],
                "event_date": dedup_key[1],
                "similarity": similarity,
                "member_weight": member_weight,
                "regime_alignment": regime_alignment,
                "sector_alignment": sector_alignment,
                "support_prior": support_prior,
                "freshness_prior": freshness_prior,
                "context_alignment": context_alignment,
                "member_stats": _member_side_stats(side_payload),
            }
    rows = list(deduped.values())
    rows.sort(key=lambda row: row["member_weight"], reverse=True)
    return rows[: cfg.member_retrieval_k], raw_member_count


def estimate_distribution(
    *,
    side: str,
    query_embedding: list[float],
    candidates: Iterable[StatePrototype],
    regime_code: Optional[str],
    sector_code: Optional[str],
    ev_config: EVConfig | None = None,
    candidate_index: CandidateIndex | None = None,
    calibration: CalibrationModel | None = None,
    query_date: str | None = None,
) -> DistributionEstimate:
    cfg = ev_config or EVConfig()
    calibration = calibration or CalibrationModel(method="identity")
    ranked, prototype_rows_all = _prototype_candidate_rows(
        side=side,
        query_embedding=query_embedding,
        candidates=candidates,
        regime_code=regime_code,
        sector_code=sector_code,
        cfg=cfg,
        candidate_index=candidate_index,
    )
    prototype_pool_size = len(ranked)
    prototype_pre_truncation_count = len(prototype_rows_all)
    prototype_positive_weight_count = sum(1 for row in prototype_rows_all if float(row.get("prototype_weight", 0.0) or 0.0) > 0.0)
    prototype_rows = prototype_rows_all[: cfg.prototype_retrieval_k]
    prototype_total_w = sum(float(row.get("prototype_weight", 0.0) or 0.0) for row in prototype_rows)
    if prototype_total_w > 1e-12:
        prototype_weights = np.asarray([float(row["prototype_weight"]) / float(prototype_total_w) for row in prototype_rows], dtype=float)
        prototype_top1_weight_share = float(prototype_weights[0]) if len(prototype_weights) else 0.0
        prototype_cumulative_top3 = float(np.sum(prototype_weights[:3])) if len(prototype_weights) else 0.0
        prototype_mixture_ess = float(1.0 / np.sum(prototype_weights ** 2))
    else:
        prototype_top1_weight_share = 0.0
        prototype_cumulative_top3 = 0.0
        prototype_mixture_ess = 0.0
    prototype_support_sum = float(
        sum(float((row.get("side_stats") or {}).get("support_count", 0.0) or 0.0) for row in prototype_rows)
    )
    prototype_consensus_signature = _consensus_signature(prototype_rows)
    top_matches = [
        {
            "prototype_id": row["candidate"].prototype_id,
            "representative_hash": row["candidate"].representative_hash,
            "weight": row["prototype_weight"],
            "weight_share": float(prototype_weights[idx]) if prototype_total_w > 1e-12 and idx < len(prototype_weights) else 0.0,
            "why": {
                "similarity": row["similarity"],
                "support": float((row["side_stats"] or {}).get("support_count", 0.0)),
                "freshness_days": float((row["side_stats"] or {}).get("freshness_days", 0.0)),
                "target_first_count": (row["side_stats"] or {}).get("target_first_count", 0),
                "stop_first_count": (row["side_stats"] or {}).get("stop_first_count", 0),
                "flat_count": (row["side_stats"] or {}).get("flat_count", 0),
                "ambiguous_count": (row["side_stats"] or {}).get("ambiguous_count", 0),
                "no_trade_count": (row["side_stats"] or {}).get("no_trade_count", 0),
            },
            "representative_symbol": row["candidate"].representative_symbol,
            "expected_return": (row["side_stats"] or {}).get("mean_return_pct"),
            "uncertainty": (row["side_stats"] or {}).get("uncertainty"),
        }
        for idx, row in enumerate(prototype_rows)
    ]
    member_rows, raw_member_count = _expand_member_rows(
        side=side,
        query_embedding=query_embedding,
        query_date=query_date,
        prototype_rows=prototype_rows,
        regime_code=regime_code,
        sector_code=sector_code,
        cfg=cfg,
    )
    member_pre_truncation_count = len(member_rows)
    positive_weight_member_count = sum(1 for row in member_rows if float(row.get("member_weight", 0.0) or 0.0) > 0.0)
    total_member_w = sum(float(row.get("member_weight", 0.0) or 0.0) for row in member_rows)
    if total_member_w <= 1e-12:
        return DistributionEstimate(
            side=side,
            uncertainty=1.0,
            effective_sample_size=0.0,
            prototype_pool_size=prototype_pool_size,
            ranked_candidate_count=len(prototype_rows),
            positive_weight_candidate_count=prototype_positive_weight_count,
            pre_truncation_candidate_count=prototype_pre_truncation_count,
            top1_weight_share=prototype_top1_weight_share,
            cumulative_weight_top3=prototype_cumulative_top3,
            mixture_ess=prototype_mixture_ess,
            member_support_sum=prototype_support_sum,
            consensus_signature=prototype_consensus_signature,
            member_candidate_count=raw_member_count,
            member_pre_truncation_count=member_pre_truncation_count,
            positive_weight_member_count=positive_weight_member_count,
            utility={"fallback_raw_ev": 0.0},
            top_matches=top_matches,
        )
    weights = np.asarray([float(row["member_weight"]) / float(total_member_w) for row in member_rows], dtype=float)
    p_target = _weighted_mean([float(row["member_stats"]["p_target_first"]) for row in member_rows], weights)
    p_stop = _weighted_mean([float(row["member_stats"]["p_stop_first"]) for row in member_rows], weights)
    p_flat = _weighted_mean([float(row["member_stats"]["p_flat"]) for row in member_rows], weights)
    p_ambiguous = _weighted_mean([float(row["member_stats"]["p_ambiguous"]) for row in member_rows], weights)
    p_no_trade = _weighted_mean([float(row["member_stats"]["p_no_trade"]) for row in member_rows], weights)
    values = [float(row["member_stats"]["after_cost_return_pct"]) for row in member_rows]
    exp_ret = _weighted_mean(values, weights)
    exp_mae = _weighted_mean([float(row["member_stats"]["mae_pct"]) for row in member_rows], weights)
    exp_mfe = _weighted_mean([float(row["member_stats"]["mfe_pct"]) for row in member_rows], weights)
    q50_d2 = _weighted_quantile([float(row["member_stats"]["close_return_d2_pct"]) for row in member_rows], weights, 0.50)
    q50_d3 = _weighted_quantile([float(row["member_stats"]["close_return_d3_pct"]) for row in member_rows], weights, 0.50)
    p_resolved_by_d2 = _weighted_mean([1.0 if bool(row["member_stats"]["resolved_by_d2"]) else 0.0 for row in member_rows], weights)
    p_resolved_by_d3 = _weighted_mean([1.0 if bool(row["member_stats"]["resolved_by_d3"]) else 0.0 for row in member_rows], weights)
    regime_alignment = _weighted_mean([float(row["regime_alignment"]) for row in member_rows], weights)
    n_eff = float(1.0 / np.sum(weights ** 2))
    dispersion = _weighted_std(values, weights)
    uncertainty = float(dispersion / max(np.sqrt(n_eff), 1.0))
    q10 = _weighted_quantile(values, weights, 0.10)
    q25 = _weighted_quantile(values, weights, 0.25)
    q50 = _weighted_quantile(values, weights, 0.50)
    q90 = _weighted_quantile(values, weights, 0.90)
    lower_bound = _resolve_lower_bound(formula=cfg.diagnostic_lower_bound_formula, q10=q10, q25=q25, q50=q50, q90=q90, expected_net_return=exp_ret, uncertainty=uncertainty)
    upper_bound = q90 + uncertainty
    member_top1_weight_share = float(weights[0]) if len(weights) else 0.0
    member_cumulative_weight_top3 = float(np.sum(weights[:3])) if len(weights) else 0.0
    member_mixture_ess = n_eff
    member_consensus_signature = _member_signature(member_rows)
    member_top_matches = [
        {
            "member_key": row["member_key"],
            "symbol": row["symbol"],
            "event_date": row["event_date"],
            "prototype_id": row["candidate"].prototype_id,
            "representative_hash": row["candidate"].representative_hash,
            "weight": row["member_weight"],
            "weight_share": float(weights[idx]) if idx < len(weights) else 0.0,
            "similarity": row["similarity"],
            "after_cost_return_pct": row["member_stats"]["after_cost_return_pct"],
            "close_return_d2_pct": row["member_stats"]["close_return_d2_pct"],
            "close_return_d3_pct": row["member_stats"]["close_return_d3_pct"],
            "resolved_by_d2": row["member_stats"]["resolved_by_d2"],
            "resolved_by_d3": row["member_stats"]["resolved_by_d3"],
            "first_touch_label": row["member_stats"]["first_touch_label"],
        }
        for idx, row in enumerate(member_rows[: min(len(member_rows), 8)])
    ]
    utility = {
        "expected_net_return": exp_ret,
        "p_target_first": p_target,
        "p_stop_first": p_stop,
        "p_flat": p_flat,
        "p_ambiguous": p_ambiguous,
        "p_no_trade": p_no_trade,
        "mae_penalty": 0.5 * exp_mae,
        "mfe_credit": 0.25 * exp_mfe,
        "ambiguous_penalty": 0.5 * p_ambiguous,
        "no_trade_penalty": 0.75 * p_no_trade,
        "uncertainty_penalty": uncertainty,
        "fallback_raw_ev": exp_ret - 0.5 * exp_mae + 0.25 * exp_mfe - 0.5 * p_ambiguous - 0.75 * p_no_trade - uncertainty,
        "q25_return": q25,
        "interval_width": float(q90 - q10),
        "lower_bound_formula": cfg.diagnostic_lower_bound_formula,
        "q50_d2_return": q50_d2,
        "q50_d3_return": q50_d3,
        "p_resolved_by_d2": p_resolved_by_d2,
        "p_resolved_by_d3": p_resolved_by_d3,
        "prototype_pool_size": prototype_pool_size,
        "ranked_candidate_count": len(prototype_rows),
        "positive_weight_candidate_count": prototype_positive_weight_count,
        "pre_truncation_candidate_count": prototype_pre_truncation_count,
        "top1_weight_share": prototype_top1_weight_share,
        "cumulative_weight_top3": prototype_cumulative_top3,
        "mixture_ess": prototype_mixture_ess,
        "member_support_sum": prototype_support_sum,
        "consensus_signature": prototype_consensus_signature,
        "member_candidate_count": raw_member_count,
        "member_pre_truncation_count": member_pre_truncation_count,
        "positive_weight_member_count": positive_weight_member_count,
        "member_top1_weight_share": member_top1_weight_share,
        "member_cumulative_weight_top3": member_cumulative_weight_top3,
        "member_mixture_ess": member_mixture_ess,
        "member_consensus_signature": member_consensus_signature,
        "member_top_matches": member_top_matches,
    }
    return DistributionEstimate(
        side=side,
        p_target_first=calibration.calibrate_prob(p_target),
        p_stop_first=calibration.calibrate_prob(p_stop),
        p_flat=max(0.0, min(1.0, p_flat)),
        expected_net_return=calibration.calibrate_ev(exp_ret),
        expected_mae=exp_mae,
        expected_mfe=exp_mfe,
        q10_return=q10,
        q50_return=q50,
        q90_return=q90,
        effective_sample_size=n_eff,
        regime_alignment=regime_alignment,
        uncertainty=uncertainty,
        lower_bound_return=lower_bound,
        upper_bound_return=upper_bound,
        q50_d2_return=q50_d2,
        q50_d3_return=q50_d3,
        p_resolved_by_d2=p_resolved_by_d2,
        p_resolved_by_d3=p_resolved_by_d3,
        prototype_pool_size=prototype_pool_size,
        ranked_candidate_count=len(prototype_rows),
        positive_weight_candidate_count=prototype_positive_weight_count,
        pre_truncation_candidate_count=prototype_pre_truncation_count,
        top1_weight_share=prototype_top1_weight_share,
        cumulative_weight_top3=prototype_cumulative_top3,
        mixture_ess=prototype_mixture_ess,
        member_support_sum=prototype_support_sum,
        consensus_signature=prototype_consensus_signature,
        member_candidate_count=raw_member_count,
        member_pre_truncation_count=member_pre_truncation_count,
        positive_weight_member_count=positive_weight_member_count,
        member_top1_weight_share=member_top1_weight_share,
        member_cumulative_weight_top3=member_cumulative_weight_top3,
        member_mixture_ess=member_mixture_ess,
        member_consensus_signature=member_consensus_signature,
        utility=utility,
        top_matches=top_matches,
    )


def build_decision_surface(*, query_embedding: list[float], prototype_pool: Iterable[StatePrototype], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None, query_date: str | None = None) -> DecisionSurface:
    cfg = ev_config or EVConfig()
    prototype_pool_rows = list(prototype_pool) if not isinstance(prototype_pool, list) else prototype_pool
    buy = estimate_distribution(side="BUY", query_embedding=query_embedding, candidates=prototype_pool_rows, regime_code=regime_code, sector_code=sector_code, ev_config=cfg, candidate_index=candidate_index, calibration=calibration, query_date=query_date)
    sell = estimate_distribution(side="SELL", query_embedding=query_embedding, candidates=prototype_pool_rows, regime_code=regime_code, sector_code=sector_code, ev_config=cfg, candidate_index=candidate_index, calibration=calibration, query_date=query_date)

    def _side_reasons(dist: DistributionEstimate) -> tuple[list[str], float]:
        side_reasons = []
        side_interval_width = dist.q90_return - dist.q10_return
        if dist.effective_sample_size < cfg.min_effective_sample_size and not cfg.diagnostic_disable_ess_gate:
            side_reasons.append("low_ess")
        if dist.uncertainty > cfg.max_uncertainty:
            side_reasons.append("high_uncertainty")
        if side_interval_width > cfg.max_return_interval_width:
            side_reasons.append("wide_interval")
        if dist.regime_alignment < cfg.min_regime_alignment:
            side_reasons.append("regime_mismatch")
        if dist.lower_bound_return <= 0.0 and not cfg.diagnostic_disable_lower_bound_gate:
            side_reasons.append("lower_bound_non_positive")
        if float(dist.utility.get("fallback_raw_ev", 0.0)) < cfg.min_expected_utility:
            side_reasons.append("low_ev")
        if float(dist.utility.get("p_ambiguous", 0.0)) >= 0.30:
            side_reasons.append("high_ambiguous_share")
        if float(dist.utility.get("p_no_trade", 0.0)) >= 0.30:
            side_reasons.append("high_no_trade_share")
        return side_reasons, side_interval_width

    buy_reasons, buy_interval_width = _side_reasons(buy)
    sell_reasons, sell_interval_width = _side_reasons(sell)
    buy_pass = not buy_reasons
    sell_pass = not sell_reasons
    buy_edge = float(buy.expected_net_return - sell.expected_net_return)
    sell_edge = float(sell.expected_net_return - buy.expected_net_return)

    if cfg.diagnostic_feasible_side_chooser:
        if buy_pass and sell_pass:
            better = "BUY" if buy.expected_net_return >= sell.expected_net_return else "SELL"
        elif buy_pass:
            better = "BUY"
        elif sell_pass:
            better = "SELL"
        else:
            better = "ABSTAIN"
    else:
        better = "BUY" if buy.expected_net_return >= sell.expected_net_return else "SELL"

    chosen = buy if better == "BUY" else sell
    interval_width = chosen.q90_return - chosen.q10_return if better in {"BUY", "SELL"} else max(buy_interval_width, sell_interval_width)
    reasons = []
    if cfg.diagnostic_feasible_side_chooser:
        if better == "ABSTAIN":
            reasons.append("no_feasible_side")
            reasons.extend(sorted(set(buy_reasons + sell_reasons)))
    else:
        reasons.extend(buy_reasons if better == "BUY" else sell_reasons)
        if max(buy_edge, sell_edge) < cfg.abstain_margin:
            reasons.append("decision_margin_too_small")

    abstain = bool(reasons)
    why = better if not abstain else "ABSTAIN"
    return DecisionSurface(
        buy=buy,
        sell=sell,
        chosen_side="ABSTAIN" if abstain else better,
        abstain=abstain,
        abstain_reasons=reasons,
        diagnostics={
            "prototype_pool_size": len(prototype_pool_rows),
            "shared_neighbor_pool": True,
            "buy_summary": buy.utility,
            "sell_summary": sell.utility,
            "gate_ablation": {
                "diagnostic_disable_lower_bound_gate": cfg.diagnostic_disable_lower_bound_gate,
                "diagnostic_disable_ess_gate": cfg.diagnostic_disable_ess_gate,
                "diagnostic_lower_bound_formula": cfg.diagnostic_lower_bound_formula,
                "diagnostic_feasible_side_chooser": cfg.diagnostic_feasible_side_chooser,
            },
            "side_gate_eval": {"buy_pass": buy_pass, "sell_pass": sell_pass, "buy_reasons": buy_reasons, "sell_reasons": sell_reasons},
            "decision_rule": {
                "winner": better,
                "abstain_margin": cfg.abstain_margin,
                "buy_sell_ev_gap": buy_edge,
                "sell_buy_ev_gap": sell_edge,
                "chosen_lower_bound": chosen.lower_bound_return if better in {"BUY", "SELL"} else None,
                "chosen_interval_width": interval_width,
                "chosen_effective_sample_size": chosen.effective_sample_size if better in {"BUY", "SELL"} else None,
                "chosen_uncertainty": chosen.uncertainty if better in {"BUY", "SELL"} else None,
                "diagnostic_disable_lower_bound_gate": cfg.diagnostic_disable_lower_bound_gate,
                "diagnostic_disable_ess_gate": cfg.diagnostic_disable_ess_gate,
                "diagnostic_lower_bound_formula": cfg.diagnostic_lower_bound_formula,
                "diagnostic_feasible_side_chooser": cfg.diagnostic_feasible_side_chooser,
                "why": why,
                "why_summary": f"{why}: p_target={chosen.utility.get('p_target_first', 0.0):.2f}, p_stop={chosen.utility.get('p_stop_first', 0.0):.2f}, p_flat={chosen.utility.get('p_flat', 0.0):.2f}, p_ambiguous={chosen.utility.get('p_ambiguous', 0.0):.2f}, p_no_trade={chosen.utility.get('p_no_trade', 0.0):.2f}",
            },
        },
    )


def estimate_expected_value(*, side: str, query_embedding: list[float], candidates: Iterable[StatePrototype], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None, query_date: str | None = None) -> EVEstimate:
    dist = estimate_distribution(side=side, query_embedding=query_embedding, candidates=candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_config, candidate_index=candidate_index, calibration=calibration, query_date=query_date)
    cfg = ev_config or EVConfig()
    reasons = []
    if dist.utility.get("fallback_raw_ev", 0.0) < cfg.min_expected_utility:
        reasons.append("low_ev")
    if dist.uncertainty > cfg.max_uncertainty:
        reasons.append("high_uncertainty")
    if dist.effective_sample_size < cfg.min_effective_sample_size and not cfg.diagnostic_disable_ess_gate:
        reasons.append("low_neff")
    if dist.regime_alignment < cfg.min_regime_alignment:
        reasons.append("regime_mismatch")
    if dist.lower_bound_return <= 0.0:
        reasons.append("lower_bound_non_positive")
    return EVEstimate(
        side=side,
        expected_utility=float(dist.utility.get("fallback_raw_ev", 0.0)),
        expected_net_return=dist.expected_net_return,
        p_up_first=dist.p_target_first,
        p_down_first=dist.p_stop_first,
        expected_mae=dist.expected_mae,
        expected_mfe=dist.expected_mfe,
        uncertainty=dist.uncertainty,
        dispersion=float(max(dist.q90_return - dist.q10_return, 0.0)),
        effective_sample_size=dist.effective_sample_size,
        regime_alignment=dist.regime_alignment,
        calibrated_ev=dist.expected_net_return,
        calibrated_win_prob=dist.p_target_first,
        abstained=bool(reasons),
        abstain_reasons=reasons,
        top_matches=dist.top_matches,
        diagnostics={
            "ev_decomposition": dist.utility,
            "raw_ev": dist.utility.get("fallback_raw_ev", 0.0),
            "decision_surface_compatible": True,
            "interval": {"q10": dist.q10_return, "q50": dist.q50_return, "q90": dist.q90_return},
            "telemetry": {
                "prototype_pool_size": dist.prototype_pool_size,
                "ranked_candidate_count": dist.ranked_candidate_count,
                "positive_weight_candidate_count": dist.positive_weight_candidate_count,
                "pre_truncation_candidate_count": dist.pre_truncation_candidate_count,
                "top1_weight_share": dist.top1_weight_share,
                "cumulative_weight_top3": dist.cumulative_weight_top3,
                "mixture_ess": dist.mixture_ess,
                "member_support_sum": dist.member_support_sum,
                "consensus_signature": dist.consensus_signature,
                "member_candidate_count": dist.member_candidate_count,
                "member_pre_truncation_count": dist.member_pre_truncation_count,
                "positive_weight_member_count": dist.positive_weight_member_count,
                "member_top1_weight_share": dist.member_top1_weight_share,
                "member_cumulative_weight_top3": dist.member_cumulative_weight_top3,
                "member_mixture_ess": dist.member_mixture_ess,
                "member_consensus_signature": dist.member_consensus_signature,
            },
            "member_top_matches": dist.utility.get("member_top_matches", []),
        },
    )
