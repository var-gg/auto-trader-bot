from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .models import PrototypeAnchor, ResearchAnchor


class AnchorSearchRepository(Protocol):
    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]: ...


@dataclass
class InMemoryAnchorRepository:
    anchors: list[ResearchAnchor]

    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]:
        return [a for a in self.anchors if a.metadata.get("market") in {None, market} and a.side == side]


class CandidateIndex(Protocol):
    def rank(self, *, query_embedding: list[float], candidates: Iterable[PrototypeAnchor]) -> list[PrototypeAnchor]: ...


@dataclass
class ExactCosineCandidateIndex:
    def rank(self, *, query_embedding: list[float], candidates: Iterable[PrototypeAnchor]) -> list[PrototypeAnchor]:
        import numpy as np

        q = np.asarray(query_embedding, dtype=float)
        qn = np.linalg.norm(q)
        if qn <= 0.0:
            return list(candidates)
        scored = []
        for c in candidates:
            v = np.asarray(c.embedding, dtype=float)
            vn = np.linalg.norm(v)
            sim = 0.0 if vn <= 0.0 else float(np.dot(q, v) / (qn * vn))
            scored.append((sim, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored]
