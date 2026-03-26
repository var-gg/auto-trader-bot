from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import numpy as np

from .models import PrototypeAnchor
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
    return CalibrationFoldArtifact(
        fold_id=fold_id,
        train_indices=list(train_indices),
        test_indices=list(test_indices),
        model=model,
        train_size=len(train_scores),
        test_size=len(raw_test),
        raw_mean=float(np.mean(raw_test)) if raw_test else 0.0,
        calibrated_mean=float(np.mean(calibrated_test)) if calibrated_test else 0.0,
    )


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
        score = (
            cfg.similarity_weight * similarity
            + cfg.anchor_quality_weight * float(candidate.anchor_quality)
            + cfg.regime_match_weight * regime_match
            + cfg.sector_match_weight * sector_match
            + cfg.liquidity_weight * liquidity
            + cfg.support_weight * support_score
            + cfg.return_weight * return_score
            + cfg.win_rate_weight * win_rate
            - cfg.uncertainty_penalty_weight * uncertainty
        )
        out.append(CandidateScore(prototype_id=candidate.prototype_id, anchor_code=candidate.anchor_code, score=float(score), similarity=float(similarity), anchor_quality=float(candidate.anchor_quality), regime_match=regime_match, sector_match=sector_match, liquidity_score=liquidity, diagnostics={"member_count": candidate.member_count, "support_count": candidate.support_count, "decayed_support": candidate.decayed_support, "mean_return_pct": candidate.mean_return_pct, "win_rate": candidate.win_rate, "uncertainty": candidate.uncertainty}))
    out.sort(key=lambda x: x.score, reverse=True)
    return out


def estimate_expected_value(*, side: str, query_embedding: list[float], candidates: Iterable[PrototypeAnchor], regime_code: Optional[str], sector_code: Optional[str], ev_config: EVConfig | None = None, candidate_index: CandidateIndex | None = None, calibration: CalibrationModel | None = None) -> EVEstimate:
    cfg = ev_config or EVConfig()
    calibration = calibration or CalibrationModel(method="identity")
    ranked = _ranked_candidates(query_embedding=query_embedding, candidates=candidates, candidate_index=candidate_index)
    rows = []
    for c in ranked:
        similarity = max(0.0, _cos(query_embedding, c.embedding))
        regime_alignment = 1.0 if regime_code and c.regime_code == regime_code else 0.0
        sector_alignment = 1.0 if sector_code and c.sector_code == sector_code else 0.0
        freshness_penalty = 1.0 / (1.0 + max(0.0, float(c.freshness_days or 0.0)) / 30.0)
        support_boost = min(1.0, float(c.decayed_support or c.support_count or 0.0) / 5.0)
        kernel = np.exp(cfg.kernel_temperature * (similarity - 1.0)) if cfg.use_kernel_weighting else similarity
        weight = float(kernel * (0.5 + 0.3 * support_boost + 0.2 * freshness_penalty) * (0.5 + 0.5 * max(regime_alignment, sector_alignment)))
        p_up = float(c.win_rate or 0.0) if side == "BUY" else float(1.0 - float(c.win_rate or 0.0))
        p_down = 1.0 - p_up
        rows.append({"candidate": c, "similarity": similarity, "weight": weight, "p_up": p_up, "p_down": p_down, "ret": float(c.mean_return_pct or 0.0), "mae": abs(float(c.mae_mean_pct or 0.0)), "mfe": float(c.mfe_mean_pct or 0.0), "uncertainty": float(c.uncertainty or 0.0), "dispersion": float(c.return_dispersion or 0.0), "regime_alignment": regime_alignment})
    rows.sort(key=lambda x: x["weight"], reverse=True)
    rows = rows[: cfg.top_k]
    total_w = sum(r["weight"] for r in rows)
    if total_w <= 1e-12:
        return EVEstimate(side=side, expected_utility=0.0, expected_net_return=0.0, p_up_first=0.0, p_down_first=0.0, expected_mae=0.0, expected_mfe=0.0, uncertainty=1.0, dispersion=1.0, effective_sample_size=0.0, regime_alignment=0.0, calibrated_ev=0.0, calibrated_win_prob=0.0, abstained=True, abstain_reasons=["no_neighbors"], top_matches=[], diagnostics={"ev_decomposition": {}, "calibration": {"method": calibration.method, "slope": calibration.slope, "intercept": calibration.intercept}, "raw_ev": 0.0})
    weights = np.asarray([r["weight"] / total_w for r in rows], dtype=float)
    n_eff = float(1.0 / np.sum(weights ** 2))
    p_up = float(sum(w * r["p_up"] for w, r in zip(weights, rows)))
    p_down = float(sum(w * r["p_down"] for w, r in zip(weights, rows)))
    exp_ret = float(sum(w * r["ret"] for w, r in zip(weights, rows)))
    exp_mae = float(sum(w * r["mae"] for w, r in zip(weights, rows)))
    exp_mfe = float(sum(w * r["mfe"] for w, r in zip(weights, rows)))
    dispersion = float(sum(w * r["dispersion"] for w, r in zip(weights, rows)))
    uncertainty = float(sum(w * r["uncertainty"] for w, r in zip(weights, rows)))
    regime_alignment = float(sum(w * r["regime_alignment"] for w, r in zip(weights, rows)))
    raw_ev = exp_ret - 0.5 * exp_mae + 0.25 * exp_mfe - uncertainty
    calibrated_prob = calibration.calibrate_prob(p_up if side == "BUY" else p_down)
    calibrated_ev = calibration.calibrate_ev(raw_ev)
    reasons = []
    if calibrated_ev < cfg.min_expected_utility:
        reasons.append("low_ev")
    if uncertainty > cfg.max_uncertainty:
        reasons.append("high_uncertainty")
    if n_eff < cfg.min_effective_sample_size:
        reasons.append("low_neff")
    if regime_alignment < cfg.min_regime_alignment:
        reasons.append("regime_mismatch")
    top_matches = [{"prototype_id": r["candidate"].prototype_id, "weight": r["weight"], "similarity": r["similarity"], "support_count": r["candidate"].support_count, "mean_return_pct": r["candidate"].mean_return_pct, "uncertainty": r["candidate"].uncertainty} for r in rows]
    return EVEstimate(side=side, expected_utility=raw_ev, expected_net_return=exp_ret, p_up_first=p_up if side == "BUY" else p_down, p_down_first=p_down if side == "BUY" else p_up, expected_mae=exp_mae, expected_mfe=exp_mfe, uncertainty=uncertainty, dispersion=dispersion, effective_sample_size=n_eff, regime_alignment=regime_alignment, calibrated_ev=calibrated_ev, calibrated_win_prob=calibrated_prob, abstained=bool(reasons), abstain_reasons=reasons, top_matches=top_matches, diagnostics={"ev_decomposition": {"expected_net_return": exp_ret, "mae_penalty": 0.5 * exp_mae, "mfe_credit": 0.25 * exp_mfe, "uncertainty_penalty": uncertainty}, "calibration": {"method": calibration.method, "slope": calibration.slope, "intercept": calibration.intercept}, "raw_ev": raw_ev, "ev_lift": calibrated_ev - raw_ev})
