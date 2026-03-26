from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .artifacts import JsonResearchArtifactStore
from .models import ResearchAnchor, StatePrototype


class AnchorSearchRepository(Protocol):
    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]: ...


@dataclass
class InMemoryAnchorRepository:
    anchors: list[ResearchAnchor]

    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]:
        return [a for a in self.anchors if a.metadata.get("market") in {None, market} and a.side == side]


class CandidateIndex(Protocol):
    def rank(self, *, query_embedding: list[float], candidates: Iterable[StatePrototype]) -> list[StatePrototype]: ...


@dataclass
class ExactCosineCandidateIndex:
    def rank(self, *, query_embedding: list[float], candidates: Iterable[StatePrototype]) -> list[StatePrototype]:
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


def load_prototypes_asof(*, artifact_store: JsonResearchArtifactStore, run_id: str, name: str = "prototype_snapshot", as_of_date: str | None = None, memory_version: str | None = None, side: str | None = None) -> list[StatePrototype]:
    payload = artifact_store.load_prototype_snapshot(run_id=run_id, name=name)
    if not payload:
        return []
    if as_of_date and payload.get("as_of_date") != as_of_date:
        return []
    if memory_version and payload.get("memory_version") != memory_version:
        return []
    prototypes = [StatePrototype(**p) for p in list(payload.get("prototypes") or [])]
    if side is None:
        return prototypes
    return [p for p in prototypes if side in (p.side_stats or {})]
