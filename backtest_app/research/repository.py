from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol, Sequence

import numpy as np

from .artifacts import JsonResearchArtifactStore
from .models import ResearchAnchor, StatePrototype


class AnchorSearchRepository(Protocol):
    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]: ...


@dataclass
class InMemoryAnchorRepository:
    anchors: list[ResearchAnchor]

    def get_candidate_anchors(self, *, market: str, side: str) -> Iterable[ResearchAnchor]:
        return [a for a in self.anchors if a.metadata.get("market") in {None, market} and a.side == side]


@dataclass(frozen=True)
class PreparedCandidateMatrix:
    payloads: tuple[Any, ...]
    normalized_embeddings: np.ndarray
    regime_codes: tuple[str, ...]
    sector_codes: tuple[str, ...]
    side_stats: tuple[dict[str, dict[str, Any]], ...]
    decayed_support: np.ndarray
    freshness_days: np.ndarray

    @property
    def size(self) -> int:
        return len(self.payloads)


class CandidateIndex(Protocol):
    def rank(self, *, query_embedding: list[float], candidates: Iterable[StatePrototype]) -> list[StatePrototype]: ...

    def prepare_matrix(
        self,
        *,
        candidates: Sequence[StatePrototype | Mapping[str, Any]],
        embeddings: np.ndarray | Sequence[Sequence[float]] | None = None,
    ) -> PreparedCandidateMatrix: ...

    def topk_scored(
        self,
        *,
        query_embedding: Sequence[float],
        k: int,
        candidates: Sequence[StatePrototype | Mapping[str, Any]] | None = None,
        prepared: PreparedCandidateMatrix | None = None,
        side: str | None = None,
        regime_code: str | None = None,
        sector_code: str | None = None,
        use_kernel_weighting: bool = True,
        kernel_temperature: float = 12.0,
    ) -> list[tuple[Any, float]]: ...


def _payload_get(payload: StatePrototype | Mapping[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _payload_embedding(payload: StatePrototype | Mapping[str, Any]) -> list[float]:
    embedding = _payload_get(payload, "embedding", [])
    return list(embedding or [])


def _payload_side_stats(payload: StatePrototype | Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = _payload_get(payload, "side_stats", {}) or {}
    return {str(key): dict(value or {}) for key, value in dict(raw).items()}


def _payload_regime_code(payload: StatePrototype | Mapping[str, Any]) -> str:
    return str(_payload_get(payload, "regime_code", "") or "")


def _payload_sector_code(payload: StatePrototype | Mapping[str, Any]) -> str:
    return str(_payload_get(payload, "sector_code", "") or "")


def _payload_decayed_support(payload: StatePrototype | Mapping[str, Any]) -> float:
    return float(_payload_get(payload, "decayed_support", 0.0) or 0.0)


def _payload_freshness_days(payload: StatePrototype | Mapping[str, Any]) -> float:
    return float(_payload_get(payload, "freshness_days", 0.0) or 0.0)


def _normalize_matrix(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.zeros((0, 0), dtype=np.float64)
    resolved = np.asarray(matrix, dtype=np.float64)
    if resolved.ndim == 1:
        resolved = resolved.reshape(1, -1)
    norms = np.linalg.norm(resolved, axis=1, keepdims=True)
    norms = np.where(norms <= 1e-12, 1.0, norms)
    return resolved / norms


def _topk_order(values: np.ndarray, *, k: int) -> np.ndarray:
    resolved = np.asarray(values, dtype=np.float64)
    if resolved.size == 0 or k <= 0:
        return np.asarray([], dtype=np.int64)
    top_k = min(int(k), int(resolved.size))
    indices = np.arange(resolved.size, dtype=np.int64)
    if top_k >= resolved.size:
        return np.lexsort((indices, -resolved)).astype(np.int64)
    partition = np.argpartition(resolved, -top_k)[-top_k:]
    shortlist = partition[np.lexsort((indices[partition], -resolved[partition]))]
    return shortlist.astype(np.int64)


@dataclass
class ExactCosineCandidateIndex:
    def prepare_matrix(
        self,
        *,
        candidates: Sequence[StatePrototype | Mapping[str, Any]],
        embeddings: np.ndarray | Sequence[Sequence[float]] | None = None,
    ) -> PreparedCandidateMatrix:
        payloads = tuple(candidates)
        if embeddings is None:
            raw_embeddings = np.asarray([_payload_embedding(payload) for payload in payloads], dtype=np.float64)
        else:
            raw_embeddings = np.asarray(embeddings, dtype=np.float64)
        if raw_embeddings.size == 0:
            raw_embeddings = np.zeros((len(payloads), 0), dtype=np.float64)
        if raw_embeddings.ndim == 1:
            raw_embeddings = raw_embeddings.reshape(1, -1)
        if raw_embeddings.shape[0] != len(payloads):
            raise ValueError("candidate payload count does not match embedding matrix row count")
        return PreparedCandidateMatrix(
            payloads=payloads,
            normalized_embeddings=_normalize_matrix(raw_embeddings),
            regime_codes=tuple(_payload_regime_code(payload) for payload in payloads),
            sector_codes=tuple(_payload_sector_code(payload) for payload in payloads),
            side_stats=tuple(_payload_side_stats(payload) for payload in payloads),
            decayed_support=np.asarray([_payload_decayed_support(payload) for payload in payloads], dtype=np.float64),
            freshness_days=np.asarray([_payload_freshness_days(payload) for payload in payloads], dtype=np.float64),
        )

    def topk_scored(
        self,
        *,
        query_embedding: Sequence[float],
        k: int,
        candidates: Sequence[StatePrototype | Mapping[str, Any]] | None = None,
        prepared: PreparedCandidateMatrix | None = None,
        side: str | None = None,
        regime_code: str | None = None,
        sector_code: str | None = None,
        use_kernel_weighting: bool = True,
        kernel_temperature: float = 12.0,
    ) -> list[tuple[Any, float]]:
        resolved = prepared or self.prepare_matrix(candidates=list(candidates or []))
        if resolved.size <= 0:
            return []
        block = self.topk_scored_block(
            query_embeddings=np.asarray([query_embedding], dtype=np.float64),
            prepared=resolved,
            query_regime_codes=[str(regime_code or "")],
            query_sector_codes=[str(sector_code or "")],
            prototype_retrieval_k=k,
            use_kernel_weighting=use_kernel_weighting,
            kernel_temperature=kernel_temperature,
        )[0]
        if side in {"BUY", "SELL"}:
            indices = list((block.get(side) or {}).get("top_indices") or [])
            similarity_payload = block.get("similarities")
            similarities = np.asarray(
                similarity_payload if similarity_payload is not None else np.zeros((0,), dtype=np.float64),
                dtype=np.float64,
            )
            return [(resolved.payloads[idx], float(similarities[idx])) for idx in indices if 0 <= int(idx) < resolved.size]
        similarity_payload = block.get("similarities")
        similarities = np.asarray(
            similarity_payload if similarity_payload is not None else np.zeros((0,), dtype=np.float64),
            dtype=np.float64,
        )
        ordered = _topk_order(similarities, k=k if int(k or 0) > 0 else resolved.size)
        return [(resolved.payloads[int(idx)], float(similarities[int(idx)])) for idx in ordered]

    def topk_scored_block(
        self,
        *,
        query_embeddings: np.ndarray,
        prepared: PreparedCandidateMatrix,
        query_regime_codes: Sequence[str | None],
        query_sector_codes: Sequence[str | None],
        prototype_retrieval_k: int,
        use_kernel_weighting: bool = True,
        kernel_temperature: float = 12.0,
    ) -> list[dict[str, Any]]:
        q = np.asarray(query_embeddings, dtype=np.float64)
        if q.ndim == 1:
            q = q.reshape(1, -1)
        if prepared.size <= 0 or prepared.normalized_embeddings.size == 0:
            empty = {
                "BUY": {"top_indices": [], "prototype_pool_size": 0, "pre_truncation_candidate_count": 0, "positive_weight_candidate_count": 0},
                "SELL": {"top_indices": [], "prototype_pool_size": 0, "pre_truncation_candidate_count": 0, "positive_weight_candidate_count": 0},
                "similarities": np.zeros((0,), dtype=np.float64),
            }
            return [dict(empty) for _ in range(q.shape[0])]
        query_normed = _normalize_matrix(q)
        similarities = np.clip(query_normed @ prepared.normalized_embeddings.T, 0.0, None)
        freshness_scores = np.asarray(
            [1.0 / (1.0 + max(0.0, float(value)) / 30.0) for value in prepared.freshness_days],
            dtype=np.float64,
        )
        side_available: dict[str, np.ndarray] = {}
        side_support_scores: dict[str, np.ndarray] = {}
        for side in ("BUY", "SELL"):
            side_rows = [dict((payload or {}).get(side) or {}) for payload in prepared.side_stats]
            side_available[side] = np.asarray([bool(payload) for payload in side_rows], dtype=bool)
            side_support_scores[side] = np.asarray(
                [
                    min(1.0, float((payload.get("decayed_support") if payload else None) or prepared.decayed_support[idx]) / 5.0)
                    for idx, payload in enumerate(side_rows)
                ],
                dtype=np.float64,
            )
        original_indices = np.arange(prepared.size, dtype=np.int64)
        out: list[dict[str, Any]] = []
        for row_index in range(q.shape[0]):
            regime_code = str(query_regime_codes[row_index] or "")
            sector_code = str(query_sector_codes[row_index] or "")
            similarity_row = np.asarray(similarities[row_index], dtype=np.float64)
            regime_alignment = np.asarray(
                [1.0 if regime_code and code == regime_code else 0.0 for code in prepared.regime_codes],
                dtype=np.float64,
            )
            sector_alignment = np.asarray(
                [1.0 if sector_code and code == sector_code else 0.0 for code in prepared.sector_codes],
                dtype=np.float64,
            )
            context_alignment = 0.40 + 0.60 * np.maximum(regime_alignment, sector_alignment)
            row_payload: dict[str, Any] = {}
            for side in ("BUY", "SELL"):
                available_indices = np.flatnonzero(side_available[side])
                if available_indices.size == 0:
                    row_payload[side] = {
                        "top_indices": [],
                        "prototype_pool_size": prepared.size,
                        "pre_truncation_candidate_count": 0,
                        "positive_weight_candidate_count": 0,
                    }
                    continue
                if use_kernel_weighting:
                    kernel = np.exp(kernel_temperature * (similarity_row - 1.0))
                else:
                    kernel = similarity_row
                weights = kernel * (0.45 + 0.30 * side_support_scores[side] + 0.25 * freshness_scores) * context_alignment
                positive_weight_count = int(np.count_nonzero(weights[available_indices] > 0.0))
                top_k = min(int(prototype_retrieval_k), int(available_indices.size))
                if top_k <= 0:
                    top_indices: list[int] = []
                else:
                    candidate_weights = weights[available_indices]
                    ordered = _topk_order(candidate_weights, k=top_k)
                    shortlisted = available_indices[ordered]
                    ordered_shortlisted = np.lexsort(
                        (
                            original_indices[shortlisted],
                            -similarity_row[shortlisted],
                            -weights[shortlisted],
                        )
                    )
                    top_indices = [int(value) for value in shortlisted[ordered_shortlisted][:top_k]]
                row_payload[side] = {
                    "top_indices": top_indices,
                    "prototype_pool_size": prepared.size,
                    "pre_truncation_candidate_count": int(available_indices.size),
                    "positive_weight_candidate_count": positive_weight_count,
                }
            row_payload["similarities"] = similarity_row
            out.append(row_payload)
        return out

    def rank(self, *, query_embedding: list[float], candidates: Iterable[StatePrototype]) -> list[StatePrototype]:
        resolved = self.prepare_matrix(candidates=list(candidates))
        if resolved.size <= 0:
            return []
        ranked = self.topk_scored(
            query_embedding=query_embedding,
            k=resolved.size,
            prepared=resolved,
            use_kernel_weighting=False,
        )
        return [candidate for candidate, _ in ranked]


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
