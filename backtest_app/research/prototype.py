from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

import numpy as np

from .models import PrototypeAnchor, ResearchAnchor


@dataclass(frozen=True)
class PrototypeConfig:
    dedup_similarity_threshold: float = 0.985
    min_anchor_quality: float = 0.0


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def build_anchor_prototypes(anchors: Iterable[ResearchAnchor], config: PrototypeConfig | None = None) -> List[PrototypeAnchor]:
    cfg = config or PrototypeConfig()
    groups: Dict[tuple, List[ResearchAnchor]] = {}
    for anchor in anchors:
        if anchor.anchor_quality < cfg.min_anchor_quality:
            continue
        groups.setdefault((anchor.anchor_code, anchor.side), []).append(anchor)

    out: List[PrototypeAnchor] = []
    for (anchor_code, side), members in groups.items():
        clusters: List[List[ResearchAnchor]] = []
        for anchor in members:
            vec = np.asarray(anchor.embedding, dtype=float)
            matched = False
            for cluster in clusters:
                center = np.mean(np.asarray([m.embedding for m in cluster], dtype=float), axis=0)
                if _cos(vec, center) >= cfg.dedup_similarity_threshold:
                    cluster.append(anchor)
                    matched = True
                    break
            if not matched:
                clusters.append([anchor])

        for idx, cluster in enumerate(clusters, start=1):
            arr = np.asarray([c.embedding for c in cluster], dtype=float)
            center = np.mean(arr, axis=0).tolist() if len(arr) else []
            rep = max(cluster, key=lambda x: (x.anchor_quality, x.liquidity_score or 0.0))
            out.append(
                PrototypeAnchor(
                    prototype_id=f"{anchor_code}:{side}:{idx}",
                    anchor_code=anchor_code,
                    side=side,
                    embedding=center,
                    member_count=len(cluster),
                    representative_symbol=rep.symbol,
                    representative_date=rep.reference_date,
                    anchor_quality=sum(x.anchor_quality for x in cluster) / max(len(cluster), 1),
                    regime_code=rep.regime_code,
                    sector_code=rep.sector_code,
                    liquidity_score=rep.liquidity_score,
                    metadata={"member_symbols": [c.symbol for c in cluster]},
                )
            )
    return out
