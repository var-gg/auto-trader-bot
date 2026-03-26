from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import numpy as np

from .models import DecisionSurface, DistributionEstimate, PrototypeAnchor
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
    min_effective_sample_size: float = 1.5
    max_uncertainty: float = 0.08
    min_expected_utility: float = 0.005
    min_regime_alignment: float = 0.5
    use_kernel_weighting: bool = True
    max_return_interval_width: float = 0.08


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


def _ranked_candidates(*, query_embedding: list[float], candidates: Iterable[PrototypeAnchor], candidate_index: CandidateIndex | None) -> list[PrototypeAnchor]:
    ranked_candidates = list(candidates)
    if candidate_index is not None:
        ranked_candidates = candidate_index.rank(query_embedding=query_embedding, candidates=ranked_candidates)
    return ranked_candidates


def score_candidates_exact(*, query_embedding: list[float], candidates: Iterable[PrototypeAnchor], regime_code: Optional[str], sector_code: Optional[str], min_liquidity_score: float = 0.0, config: ScoringConfig | None = None, candidate_index: CandidateIndex | None = None) -> List[CandidateScore]:
    cfg = config or ScoringConfig(min_liquidity_score=min_liquidity_score)
    ranked_candidates = _ranked_candidates(query_embedding=query_embedding, candidates=candidates, candidate_index=candidate_index)
    out: List[CandidateScore] = []
    for candidate in ranked_candidates:
        liquidity = float(candidate.liquidity_score or 0.0)
        if liquidity < cfg.min_liquidity_score:
            continue
        if int(candidate.support_count or candidate.member_count or 0) < cfg.min_support_count:
            continue
        sector_match = 1.0 if sector_code and candidate.sector_code == sector_code else 0.0
        if cfg.require_sector_match and sector_code and sector_match <= 0.0:
            continue
        regime_match = 1.0 if regime_code and candidate.regime_code == regime_code else 0.0
        similarity = _cos(query_embedding, candidate.embedding)
        support_score = min(1.0, float(candidate.decayed_support or candidate.support_count or 0.0) / 5.0)
        return_score = float(candidate.mean_return_pct or 0.0)
        win_rate = float(candidate.win_rate or 0.0)
        uncertainty = float(candidate.uncertainty or 0.0)
        score = cfg.similarity_weight * similarity + cfg.anchor_quality_weight * float(candidate.anchor_quality) + cfg.regime_match_weight * regime_match + cfg.sector_match_weight * sector_match + cfg.liquidity_weight * liquidity + cfg.support_weight * support_score + cfg.return_weight * return_score + cfg.win_rate_weight * win_rate - cfg.uncertainty_penalty_weight * uncertainty
        out.append(CandidateScore(prototype_id=candidate.prototype_id, anchor_code=candidate.anchor_code, score=float(score), similarity=float(similarity), anchor_quality=float(candidate.anchor_quality), regime_match=regime_match, sector_match=sector_match, liquidity_score=liquidity, diagnostics={"member_count": candidate.member_count, "support_count": candidate.support_count, "decayed_support": candidate.decayed_support, "mean_return_pct": candidate.mean_return_pct, "win_rate": candidate.win_rate, "uncertainty": candidate.uncertainty, "representative_symbol": candidate.representative_symbol, "regime_bucket": candidate.regime_bucket, "sector_bucket": candidate.sector_bucket, "liquidity_bucket": candidate.liquidity_bucket}))
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


def _row_side_metrics(side: str, candidate: PrototypeAnchor) -> tuple[float, float, float, float]:
    if side == "BUY":
        p_target = float(candidate.win_rate or 0.0)
        p_stop = max(0.0, 1.0 - p_target)
        p_flat = 0.0
        exp_ret = float(candidate.mean_return_pct or 0.0)
    else:
        p_target = float(candidate.win_rate or 0.0)
        p_stop = max(0.0, 1.0 - p_target)
        p_flat = 0.0
        exp_ret = float(candidate.mean_return_pct or 0.0)
    return p_target, p_stop, p_flat, exp_ret


def estimate_distribution(*, side: str, query_embedding: list[float], candidates: Iterable[PrototypeAnchor], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None) -> DistributionEstimate:
    cfg = ev_config or EVConfig()
    calibration = calibration or CalibrationModel(method="identity")
    ranked = _ranked_candidates(query_embedding=query_embedding, candidates=candidates, candidate_index=candidate_index)
    rows = []
    for c in ranked:
        similarity = max(0.0, _cos(query_embedding, c.embedding))
        regime_alignment = 1.0 if regime_code and c.regime_code == regime_code else 0.0
        sector_alignment = 1.0 if sector_code and c.sector_code == sector_code else 0.0
        freshness_score = 1.0 / (1.0 + max(0.0, float(c.freshness_days or 0.0)) / 30.0)
        support_score = min(1.0, float(c.decayed_support or c.support_count or 0.0) / 5.0)
        kernel = np.exp(cfg.kernel_temperature * (similarity - 1.0)) if cfg.use_kernel_weighting else similarity
        weight = float(kernel * (0.45 + 0.30 * support_score + 0.25 * freshness_score) * (0.40 + 0.60 * max(regime_alignment, sector_alignment)))
        p_target, p_stop, p_flat, exp_ret = _row_side_metrics(side, c)
        rows.append({"candidate": c, "similarity": similarity, "weight": weight, "p_target": p_target, "p_stop": p_stop, "p_flat": p_flat, "ret": exp_ret, "mae": abs(float(c.mae_mean_pct or 0.0)), "mfe": float(c.mfe_mean_pct or 0.0), "uncertainty": float(c.uncertainty or 0.0), "dispersion": float(c.return_dispersion or 0.0), "regime_alignment": regime_alignment, "freshness_score": freshness_score, "support_score": support_score})
    rows.sort(key=lambda x: x["weight"], reverse=True)
    rows = rows[: cfg.top_k]
    total_w = sum(r["weight"] for r in rows)
    if total_w <= 1e-12:
        return DistributionEstimate(side=side, uncertainty=1.0, utility={"fallback_raw_ev": 0.0}, top_matches=[])
    weights = np.asarray([r["weight"] / total_w for r in rows], dtype=float)
    p_target = float(sum(w * r["p_target"] for w, r in zip(weights, rows)))
    p_stop = float(sum(w * r["p_stop"] for w, r in zip(weights, rows)))
    p_flat = float(sum(w * r["p_flat"] for w, r in zip(weights, rows)))
    exp_ret = float(sum(w * r["ret"] for w, r in zip(weights, rows)))
    exp_mae = float(sum(w * r["mae"] for w, r in zip(weights, rows)))
    exp_mfe = float(sum(w * r["mfe"] for w, r in zip(weights, rows)))
    uncertainty = float(sum(w * r["uncertainty"] for w, r in zip(weights, rows)))
    regime_alignment = float(sum(w * r["regime_alignment"] for w, r in zip(weights, rows)))
    n_eff = float(1.0 / np.sum(weights ** 2))
    values = [float(r["ret"]) for r in rows]
    q10 = _weighted_quantile(values, weights, 0.10)
    q50 = _weighted_quantile(values, weights, 0.50)
    q90 = _weighted_quantile(values, weights, 0.90)
    lower_bound = q10 - uncertainty
    upper_bound = q90 + uncertainty
    utility = {
        "expected_net_return": exp_ret,
        "mae_penalty": 0.5 * exp_mae,
        "mfe_credit": 0.25 * exp_mfe,
        "uncertainty_penalty": uncertainty,
        "fallback_raw_ev": exp_ret - 0.5 * exp_mae + 0.25 * exp_mfe - uncertainty,
    }
    top_matches = [{"prototype_id": r["candidate"].prototype_id, "weight": r["weight"], "why": {"similarity": r["similarity"], "support": float(r["candidate"].support_count or 0.0), "freshness_days": float(r["candidate"].freshness_days or 0.0)}, "representative_symbol": r["candidate"].representative_symbol, "expected_return": r["candidate"].mean_return_pct, "uncertainty": r["candidate"].uncertainty} for r in rows]
    return DistributionEstimate(side=side, p_target_first=calibration.calibrate_prob(p_target), p_stop_first=calibration.calibrate_prob(p_stop), p_flat=max(0.0, min(1.0, p_flat)), expected_net_return=calibration.calibrate_ev(exp_ret), expected_mae=exp_mae, expected_mfe=exp_mfe, q10_return=q10, q50_return=q50, q90_return=q90, effective_sample_size=n_eff, regime_alignment=regime_alignment, uncertainty=uncertainty, lower_bound_return=lower_bound, upper_bound_return=upper_bound, utility=utility, top_matches=top_matches)


def build_decision_surface(*, query_embedding: list[float], buy_candidates: Iterable[PrototypeAnchor], sell_candidates: Iterable[PrototypeAnchor], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None) -> DecisionSurface:
    cfg = ev_config or EVConfig()
    buy = estimate_distribution(side="BUY", query_embedding=query_embedding, candidates=buy_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=cfg, candidate_index=candidate_index, calibration=calibration)
    sell = estimate_distribution(side="SELL", query_embedding=query_embedding, candidates=sell_candidates, regime_code=regime_code, sector_code=sector_code, ev_config=cfg, candidate_index=candidate_index, calibration=calibration)
    reasons = []
    better = "BUY" if buy.expected_net_return >= sell.expected_net_return else "SELL"
    chosen = buy if better == "BUY" else sell
    interval_width = chosen.q90_return - chosen.q10_return
    if chosen.effective_sample_size < cfg.min_effective_sample_size:
        reasons.append("low_ess")
    if chosen.uncertainty > cfg.max_uncertainty:
        reasons.append("high_uncertainty")
    if interval_width > cfg.max_return_interval_width:
        reasons.append("wide_interval")
    if chosen.regime_alignment < cfg.min_regime_alignment:
        reasons.append("regime_mismatch")
    if chosen.lower_bound_return <= 0.0:
        reasons.append("lower_bound_non_positive")
    abstain = bool(reasons)
    return DecisionSurface(buy=buy, sell=sell, chosen_side="ABSTAIN" if abstain else better, abstain=abstain, abstain_reasons=reasons, diagnostics={"buy_summary": buy.utility, "sell_summary": sell.utility, "decision_rule": {"winner": better, "chosen_lower_bound": chosen.lower_bound_return, "chosen_interval_width": interval_width}})


def estimate_expected_value(*, side: str, query_embedding: list[float], candidates: Iterable[PrototypeAnchor], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None) -> EVEstimate:
    dist = estimate_distribution(side=side, query_embedding=query_embedding, candidates=candidates, regime_code=regime_code, sector_code=sector_code, ev_config=ev_config, candidate_index=candidate_index, calibration=calibration)
    cfg = ev_config or EVConfig()
    reasons = []
    if dist.utility.get("fallback_raw_ev", 0.0) < cfg.min_expected_utility:
        reasons.append("low_ev")
    if dist.uncertainty > cfg.max_uncertainty:
        reasons.append("high_uncertainty")
    if dist.effective_sample_size < cfg.min_effective_sample_size:
        reasons.append("low_neff")
    if dist.regime_alignment < cfg.min_regime_alignment:
        reasons.append("regime_mismatch")
    if dist.lower_bound_return <= 0.0:
        reasons.append("lower_bound_non_positive")
    return EVEstimate(side=side, expected_utility=float(dist.utility.get("fallback_raw_ev", 0.0)), expected_net_return=dist.expected_net_return, p_up_first=dist.p_target_first, p_down_first=dist.p_stop_first, expected_mae=dist.expected_mae, expected_mfe=dist.expected_mfe, uncertainty=dist.uncertainty, dispersion=float(max(dist.q90_return - dist.q10_return, 0.0)), effective_sample_size=dist.effective_sample_size, regime_alignment=dist.regime_alignment, calibrated_ev=dist.expected_net_return, calibrated_win_prob=dist.p_target_first, abstained=bool(reasons), abstain_reasons=reasons, top_matches=dist.top_matches, diagnostics={"ev_decomposition": dist.utility, "raw_ev": dist.utility.get("fallback_raw_ev", 0.0), "decision_surface_compatible": True, "interval": {"q10": dist.q10_return, "q50": dist.q50_return, "q90": dist.q90_return}})
