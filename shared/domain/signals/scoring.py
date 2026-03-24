from __future__ import annotations

from math import exp, log
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate


def _l2_norm(vec: Sequence[float]) -> float:
    return sum(float(x) * float(x) for x in vec) ** 0.5


def _dot(a: Sequence[float], b: Sequence[float]) -> float:
    n = min(len(a), len(b))
    return sum(float(a[i]) * float(b[i]) for i in range(n))


def _normalize(vec: Sequence[float]) -> List[float]:
    n = _l2_norm(vec)
    if n <= 0.0:
        return [float(x) for x in vec]
    return [float(x) / n for x in vec]


def _logsumexp_tau(scores: Sequence[float], tau: float) -> float:
    tau = max(float(tau), 1e-6)
    if not scores:
        return float("-inf")
    scaled = [float(s) / tau for s in scores]
    m = max(scaled)
    return m + log(sum(exp(s - m) for s in scaled) + 1e-12)


def normalize_ranked_candidates(
    candidates: Iterable[Dict[str, Any]],
    *,
    market: MarketCode,
    side: Side,
) -> List[SignalCandidate]:
    out: List[SignalCandidate] = []
    for row in candidates:
        tb_label = str(row.get("tb_label") or "UNKNOWN").upper()
        label = OutcomeLabel(tb_label) if tb_label in {e.value for e in OutcomeLabel} else OutcomeLabel.UNKNOWN
        out.append(
            SignalCandidate(
                symbol=str(row.get("symbol")),
                ticker_id=row.get("ticker_id"),
                market=market,
                side_bias=side,
                signal_strength=float(row.get("cos") or row.get("signal_strength") or 0.0),
                outcome_label=label,
                reverse_breach_day=row.get("reverse_breach_day"),
                provenance={
                    "anchor_date": row.get("anchor_date"),
                    "ctx_vec": list(row.get("ctx_vec") or []),
                    "cos": float(row.get("cos") or 0.0),
                },
                diagnostics={"iae_1_3": row.get("iae_1_3")},
            )
        )
    return out


def compute_pm_signal(
    up_candidates: Sequence[SignalCandidate],
    down_candidates: Sequence[SignalCandidate],
    query_context_vector: Sequence[float],
    config: Dict[str, float],
) -> Tuple[float, Dict[str, Any], str]:
    alpha = float(config["alpha"])
    beta = float(config["beta"])
    tau = float(config["tau_softmax"])
    threshold = float(config["threshold"])

    sum_w = alpha + beta
    if sum_w <= 0.0:
        sum_w = 1.0
    alpha_norm = alpha / sum_w
    beta_norm = beta / sum_w
    q_ctx = _normalize(query_context_vector)

    def rerank(items: Sequence[SignalCandidate]) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        for c in items:
            ctx_vec = _normalize(c.provenance.get("ctx_vec") or [])
            cos_ctx = _dot(q_ctx, ctx_vec) if ctx_vec else 0.0
            cos_shape = float(c.provenance.get("cos") or c.signal_strength or 0.0)
            score = alpha_norm * cos_shape + beta_norm * cos_ctx
            ranked.append(
                {
                    "symbol": c.symbol,
                    "anchor_date": c.provenance.get("anchor_date"),
                    "direction": c.side_bias.value,
                    "score": score,
                }
            )
        ranked.sort(key=lambda r: r["score"], reverse=True)
        return ranked

    up_ranked = rerank(up_candidates)
    dn_ranked = rerank(down_candidates)

    s_u = [r["score"] for r in up_ranked]
    s_d = [r["score"] for r in dn_ranked]
    log_u = _logsumexp_tau(s_u, tau)
    log_d = _logsumexp_tau(s_d, tau)

    if log_u == float("-inf") and log_d == float("-inf"):
        raise ValueError("No valid candidates")

    margin = max(-50.0, min(50.0, log_u - log_d))
    p_up = 1.0 / (1.0 + exp(-margin))
    p_down = 1.0 - p_up
    p_raw = max(p_up, p_down)
    sign = 1.0 if p_up > p_down else -1.0
    best_dir = "UP" if p_up > p_down else "DOWN"
    signal_1d = sign * p_raw

    best_ranked = up_ranked if best_dir == "UP" else dn_ranked
    best = best_ranked[0] if best_ranked else {"symbol": None, "anchor_date": None, "score": 0.0}
    best_info = {
        "symbol": best["symbol"],
        "anchor_date": best["anchor_date"],
        "direction": best_dir,
        "score": best["score"],
    }

    total_count = len(up_ranked) + len(dn_ranked)
    if p_raw >= threshold:
        reason = "OK"
    elif total_count < 10:
        reason = "TOO_FEW"
    else:
        reason = f"LOW_CONF(p={p_raw:.3f})"

    return float(signal_1d), best_info, reason
