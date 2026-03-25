from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import numpy as np

from .models import PrototypeAnchor
from .repository import CandidateIndex


@dataclass(frozen=True)
class ScoringConfig:
    similarity_weight: float = 0.45
    anchor_quality_weight: float = 0.25
    regime_match_weight: float = 0.15
    sector_match_weight: float = 0.10
    liquidity_weight: float = 0.05
    min_liquidity_score: float = 0.0
    require_sector_match: bool = False


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


def _cos(a: list[float], b: list[float]) -> float:
    av = np.asarray(a, dtype=float)
    bv = np.asarray(b, dtype=float)
    an = np.linalg.norm(av)
    bn = np.linalg.norm(bv)
    if an <= 0.0 or bn <= 0.0:
        return 0.0
    return float(np.dot(av, bv) / (an * bn))


def score_candidates_exact(
    *,
    query_embedding: list[float],
    candidates: Iterable[PrototypeAnchor],
    regime_code: Optional[str],
    sector_code: Optional[str],
    min_liquidity_score: float = 0.0,
    config: ScoringConfig | None = None,
    candidate_index: CandidateIndex | None = None,
) -> List[CandidateScore]:
    cfg = config or ScoringConfig(min_liquidity_score=min_liquidity_score)
    ranked_candidates = list(candidates)
    if candidate_index is not None:
        ranked_candidates = candidate_index.rank(query_embedding=query_embedding, candidates=ranked_candidates)

    out: List[CandidateScore] = []
    for candidate in ranked_candidates:
        liquidity = float(candidate.liquidity_score or 0.0)
        if liquidity < cfg.min_liquidity_score:
            continue
        sector_match = 1.0 if sector_code and candidate.sector_code == sector_code else 0.0
        if cfg.require_sector_match and sector_code and sector_match <= 0.0:
            continue
        regime_match = 1.0 if regime_code and candidate.regime_code == regime_code else 0.0
        similarity = _cos(query_embedding, candidate.embedding)
        score = (
            cfg.similarity_weight * similarity
            + cfg.anchor_quality_weight * float(candidate.anchor_quality)
            + cfg.regime_match_weight * regime_match
            + cfg.sector_match_weight * sector_match
            + cfg.liquidity_weight * liquidity
        )
        out.append(
            CandidateScore(
                prototype_id=candidate.prototype_id,
                anchor_code=candidate.anchor_code,
                score=float(score),
                similarity=float(similarity),
                anchor_quality=float(candidate.anchor_quality),
                regime_match=regime_match,
                sector_match=sector_match,
                liquidity_score=liquidity,
                diagnostics={"member_count": candidate.member_count, "representative_symbol": candidate.representative_symbol},
            )
        )
    out.sort(key=lambda x: x.score, reverse=True)
    return out
