"""Direct seed row generator — bypasses train-snapshots and calibration-bundle.

Uses event_raw_cache_v2 (memmap + prefix stats) and query_feature_rows (DB)
to produce seed rows directly via vectorized cosine similarity, skipping
prototype clustering and multi-stage artifact generation.

Expected wall time: ~2-5 minutes for 418k queries against 407k events.
Expected peak memory: ~3GB.
"""
from __future__ import annotations

import json
import math
import os
import time
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from backtest_app.db.local_session import create_backtest_session_factory
from backtest_app.db.local_write_session import create_backtest_write_session_factory
from backtest_app.research.pipeline import EventRawCacheHandle


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _json_loads(v: Any, default: Any = None):
    if v is None or v == "":
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(str(v))
    except Exception:
        return default


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    sv = values[order]
    sw = weights[order]
    cdf = np.cumsum(sw)
    cdf /= cdf[-1]
    idx = np.searchsorted(cdf, q)
    idx = min(idx, len(sv) - 1)
    return float(sv[idx])


# ---------------------------------------------------------------------------
# Event data loading (one-time)
# ---------------------------------------------------------------------------

_NEEDED_PARQUET_COLS = [
    "event_ordinal", "symbol", "feature_end_date", "outcome_end_date",
    "lib_sector", "regime_code", "event_side_payload_json",
]


def _load_event_metadata(events_path: str) -> dict[str, Any]:
    """Stream events.parquet and return lightweight metadata + outcome strings."""
    ordinals: list[int] = []
    outcome_dates: list[str] = []
    regime_codes: list[str] = []
    sector_codes: list[str] = []
    side_payload_jsons: list[str] = []

    pf = pq.ParquetFile(events_path)
    for batch in pf.iter_batches(batch_size=5000, columns=_NEEDED_PARQUET_COLS):
        d = batch.to_pydict()
        n = len(d["event_ordinal"])
        for i in range(n):
            ordinals.append(int(d["event_ordinal"][i] or 0))
            outcome_dates.append(str(d["outcome_end_date"][i] or ""))
            regime_codes.append(str(d["regime_code"][i] or "UNKNOWN"))
            sector_codes.append(str(d["lib_sector"][i] or "UNKNOWN"))
            side_payload_jsons.append(str(d["event_side_payload_json"][i] or "{}"))
        del d

    return {
        "ordinals": np.array(ordinals, dtype=np.int64),
        "outcome_dates": outcome_dates,
        "regime_codes": regime_codes,
        "sector_codes": sector_codes,
        "side_payload_jsons": side_payload_jsons,
    }


# ---------------------------------------------------------------------------
# Scaler reconstruction from prefix stats
# ---------------------------------------------------------------------------

def _reconstruct_scaler_arrays(
    *,
    handle: EventRawCacheHandle,
    n_eligible: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (means, stds) arrays of shape (n_features,) using prefix stats."""
    if n_eligible <= 0:
        d = len(handle.feature_keys)
        return np.zeros(d, dtype=np.float64), np.ones(d, dtype=np.float64)

    prefix_sum = handle.load_prefix_sum(mmap_mode="r")
    prefix_sumsq = handle.load_prefix_sumsq(mmap_mode="r")

    sums = np.asarray(prefix_sum[n_eligible - 1], dtype=np.float64)
    sumsqs = np.asarray(prefix_sumsq[n_eligible - 1], dtype=np.float64)
    n = float(n_eligible)

    means = sums / n
    variance = sumsqs / n - means * means
    variance = np.maximum(variance, 0.0)
    stds = np.sqrt(variance)
    stds = np.where(stds < 1e-8, 1.0, stds)
    return means, stds


# ---------------------------------------------------------------------------
# Core scoring: cosine similarity + weighted distribution
# ---------------------------------------------------------------------------

def _score_batch(
    *,
    query_raw: np.ndarray,           # (B, d) raw features
    event_raw: np.ndarray,           # (N, d) raw features (memmap slice)
    means: np.ndarray,               # (d,)
    stds: np.ndarray,                # (d,)
    event_regime_codes: list[str],   # length N
    event_sector_codes: list[str],   # length N
    query_regime_codes: list[str],   # length B
    query_sector_codes: list[str],   # length B
    event_outcome_dates: list[str],  # length N (for freshness)
    query_dates: list[str],          # length B
    side_payload_jsons: list[str],   # length N
    top_k: int = 96,
    kernel_temperature: float = 12.0,
) -> list[dict[str, Any]]:
    """Score a batch of queries against eligible events. Returns seed-row-ready dicts."""
    B = query_raw.shape[0]
    N = event_raw.shape[0]

    if N == 0 or B == 0:
        return [_empty_side_result() for _ in range(B)]

    # Transform + normalize
    q_transformed = (query_raw - means) / stds
    q_norms = np.linalg.norm(q_transformed, axis=1, keepdims=True)
    q_norms = np.where(q_norms < 1e-12, 1.0, q_norms)
    q_normed = q_transformed / q_norms

    # For events, transform in chunks to avoid huge memory spike
    chunk_size = 50000
    similarities = np.empty((B, N), dtype=np.float32)
    for start in range(0, N, chunk_size):
        end = min(start + chunk_size, N)
        e_chunk = np.asarray(event_raw[start:end], dtype=np.float64)
        e_transformed = (e_chunk - means) / stds
        e_norms = np.linalg.norm(e_transformed, axis=1, keepdims=True)
        e_norms = np.where(e_norms < 1e-12, 1.0, e_norms)
        e_normed = e_transformed / e_norms
        similarities[:, start:end] = (q_normed @ e_normed.T).astype(np.float32)
        del e_chunk, e_transformed, e_normed

    similarities = np.clip(similarities, 0.0, None)

    # Pre-compute regime/sector arrays for context alignment
    e_regime_arr = np.array(event_regime_codes)
    e_sector_arr = np.array(event_sector_codes)

    results: list[dict[str, Any]] = []
    actual_k = min(top_k, N)

    for i in range(B):
        sim_row = similarities[i]
        top_idx = np.argpartition(sim_row, -actual_k)[-actual_k:]
        top_sims = sim_row[top_idx]

        # Context alignment: 0.4 + 0.6 * max(regime_match, sector_match)
        q_regime = query_regime_codes[i]
        q_sector = query_sector_codes[i]
        regime_match = (e_regime_arr[top_idx] == q_regime).astype(np.float32)
        sector_match = (e_sector_arr[top_idx] == q_sector).astype(np.float32)
        context_align = 0.4 + 0.6 * np.maximum(regime_match, sector_match)

        # Freshness: 1/(1 + days/30)
        q_date = query_dates[i]
        freshness = np.ones(actual_k, dtype=np.float32)
        for j, idx in enumerate(top_idx):
            e_date = event_outcome_dates[idx]
            if q_date and e_date:
                try:
                    days = max(0, (pd.Timestamp(q_date) - pd.Timestamp(e_date)).days)
                    freshness[j] = 1.0 / (1.0 + days / 30.0)
                except Exception:
                    pass

        # Kernel weighting
        kernel = np.exp(kernel_temperature * (top_sims - 1.0))
        weights = kernel * context_align * freshness
        total_w = float(np.sum(weights))

        if total_w < 1e-12:
            results.append(_empty_side_result())
            continue

        weights_norm = weights / total_w

        # Parse side payloads for top-k events only
        side_result = _compute_distribution(
            top_idx=top_idx,
            weights=weights_norm,
            side_payload_jsons=side_payload_jsons,
            regime_alignment=float(np.mean(regime_match)),
        )
        results.append(side_result)

    return results


def _empty_side_result() -> dict[str, Any]:
    empty = {
        "q10_return": 0.0, "q50_return": 0.0, "q90_return": 0.0,
        "lower_bound": 0.0, "interval_width": 1.0, "uncertainty": 1.0,
        "expected_net_return": 0.0, "member_mixture_ess": 0.0,
        "member_top1_weight_share": 0.0, "member_pre_truncation_count": 0,
        "member_consensus_signature": "no_consensus",
        "member_support_sum": 0.0, "member_candidate_count": 0,
        "positive_weight_member_count": 0,
        "q50_d2_return": 0.0, "q50_d3_return": 0.0,
        "p_resolved_by_d2": 0.0, "p_resolved_by_d3": 0.0,
        "regime_alignment": 0.0,
    }
    return {"BUY": dict(empty), "SELL": dict(empty)}


def _compute_distribution(
    *,
    top_idx: np.ndarray,
    weights: np.ndarray,
    side_payload_jsons: list[str],
    regime_alignment: float,
) -> dict[str, Any]:
    """Compute BUY and SELL distributions from top-k events."""
    # Parse side outcomes for top events
    outcomes: list[dict[str, Any]] = []
    for idx in top_idx:
        payload = _json_loads(side_payload_jsons[int(idx)], {})
        outcomes.append(payload)

    result: dict[str, dict[str, Any]] = {}
    for side in ("BUY", "SELL"):
        returns = []
        mae_vals = []
        mfe_vals = []
        d2_returns = []
        d3_returns = []
        resolved_d2 = []
        resolved_d3 = []
        labels = []
        valid_weights = []

        for j, outcome in enumerate(outcomes):
            side_data = outcome.get(side) or outcome.get(side.lower()) or {}
            if not side_data:
                continue
            ret = _to_float(side_data.get("after_cost_return_pct"))
            returns.append(ret)
            mae_vals.append(_to_float(side_data.get("mae_pct")))
            mfe_vals.append(_to_float(side_data.get("mfe_pct")))
            d2_returns.append(_to_float(side_data.get("close_return_d2_pct")))
            d3_returns.append(_to_float(side_data.get("close_return_d3_pct")))
            resolved_d2.append(1.0 if side_data.get("resolved_by_d2") else 0.0)
            resolved_d3.append(1.0 if side_data.get("resolved_by_d3") else 0.0)
            labels.append(str(side_data.get("first_touch_label") or ""))
            valid_weights.append(float(weights[j]))

        if not returns:
            result[side] = dict(_empty_side_result()["BUY"])
            continue

        w = np.array(valid_weights, dtype=np.float64)
        w_sum = w.sum()
        if w_sum < 1e-12:
            result[side] = dict(_empty_side_result()["BUY"])
            continue
        w = w / w_sum

        r = np.array(returns, dtype=np.float64)
        exp_ret = float(np.dot(w, r))
        q10 = _weighted_quantile(r, w, 0.10)
        q25 = _weighted_quantile(r, w, 0.25)
        q50 = _weighted_quantile(r, w, 0.50)
        q75 = _weighted_quantile(r, w, 0.75)
        q90 = _weighted_quantile(r, w, 0.90)
        interval_width = max(q90 - q10, 0.0)
        n_eff = float(1.0 / np.sum(w ** 2))
        dispersion = float(np.sqrt(np.dot(w, (r - exp_ret) ** 2)))
        uncertainty = dispersion / max(math.sqrt(n_eff), 1.0)
        lower_bound = q10 - uncertainty

        result[side] = {
            "q10_return": q10,
            "q25_return": q25,
            "q50_return": q50,
            "q75_return": q75,
            "q90_return": q90,
            "expected_net_return": exp_ret,
            "lower_bound": lower_bound,
            "interval_width": interval_width,
            "uncertainty": uncertainty,
            "member_mixture_ess": n_eff,
            "member_top1_weight_share": float(w[0]) if len(w) else 0.0,
            "member_pre_truncation_count": len(returns),
            "member_consensus_signature": "direct_scoring",
            "member_support_sum": float(len(returns)),
            "member_candidate_count": len(returns),
            "positive_weight_member_count": int(np.sum(w > 0)),
            "q50_d2_return": float(np.dot(w, np.array(d2_returns))) if d2_returns else 0.0,
            "q50_d3_return": float(np.dot(w, np.array(d3_returns))) if d3_returns else 0.0,
            "p_resolved_by_d2": float(np.dot(w, np.array(resolved_d2))) if resolved_d2 else 0.0,
            "p_resolved_by_d3": float(np.dot(w, np.array(resolved_d3))) if resolved_d3 else 0.0,
            "regime_alignment": regime_alignment,
        }

    return result


# ---------------------------------------------------------------------------
# Bar path loading from source DB
# ---------------------------------------------------------------------------

def _load_replay_bar_paths(
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load replay bars from backtest DB and build bar_path dicts.

    Uses pandas bulk read for speed (4.1M rows in ~10s vs minutes with row-by-row).
    Returns: {(decision_date, symbol): bar_path_dict}
    """
    import psycopg2
    from dotenv import dotenv_values

    env = dotenv_values(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"))
    db_url = env.get("BACKTEST_DB_URL", "")
    conn = psycopg2.connect(db_url)
    try:
        df = pd.read_sql(
            """
            SELECT decision_date, symbol, bar_n, session_date, open, high, low, close
            FROM bt_result.calibration_replay_bar
            WHERE bundle_run_id = %s AND side = 'BUY'
            ORDER BY decision_date, symbol, bar_n
            """,
            conn,
            params=[bundle_run_id],
        )
    finally:
        conn.close()

    # Vectorized grouping — avoid iterrows for 4M+ rows
    df["decision_date"] = df["decision_date"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["session_date"] = df["session_date"].astype(str)
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for (dd, sym), grp in df.groupby(["decision_date", "symbol"], sort=False):
        grp_sorted = grp.sort_values("bar_n")
        bars = [
            {"bar_n": int(r.bar_n), "session_date": r.session_date,
             "open": float(r.open), "high": float(r.high),
             "low": float(r.low), "close": float(r.close)}
            for r in grp_sorted.itertuples(index=False)
        ]
        grouped[(str(dd), str(sym))] = bars
    del df

    result: dict[tuple[str, str], dict[str, Any]] = {}
    empty = {
        "execution_date": None, "t1_open": None,
        "d1_open": None, "d1_high": None, "d1_low": None, "d1_close": None,
        "bar_path_d1_to_d5": "[]", "path_length": 0, "last_path_close": None,
    }

    for key, bars in grouped.items():
        bars.sort(key=lambda b: b["bar_n"])
        path = [
            {"session_date": b["session_date"], "open": b["open"],
             "high": b["high"], "low": b["low"], "close": b["close"]}
            for b in bars
        ]
        if not path:
            result[key] = dict(empty)
            continue
        first = path[0]
        result[key] = {
            "execution_date": first["session_date"],
            "t1_open": first["open"],
            "d1_open": first["open"],
            "d1_high": first["high"],
            "d1_low": first["low"],
            "d1_close": first["close"],
            "bar_path_d1_to_d5": json.dumps(path, ensure_ascii=False),
            "path_length": len(path),
            "last_path_close": path[-1]["close"],
        }

    return result


# ---------------------------------------------------------------------------
# Seed row assembly
# ---------------------------------------------------------------------------

def _assemble_seed_row(
    *,
    query_row: dict[str, Any],
    scoring: dict[str, dict[str, Any]],
    bar_path: dict[str, Any],
    policy_scope: str,
    run_label: str,
) -> list[dict[str, Any]]:
    """Assemble BUY and SELL seed rows from a single query's scoring result."""
    rows = []
    decision_date = str(query_row.get("decision_date") or "")
    symbol = str(query_row.get("symbol") or "")
    regime_code = str(query_row.get("regime_code") or "UNKNOWN")
    sector_code = str(query_row.get("sector_code") or "UNKNOWN")
    market = "US"

    for side in ("BUY", "SELL"):
        metrics = scoring.get(side) or {}
        q10 = _to_float(metrics.get("q10_return"))
        q50 = _to_float(metrics.get("q50_return"))
        q90 = _to_float(metrics.get("q90_return"))
        interval_width = _to_float(metrics.get("interval_width"), max(q90 - q10, 0.0))
        uncertainty = _to_float(metrics.get("uncertainty"))
        ess = _to_float(metrics.get("member_mixture_ess"))

        # Eligibility: relaxed for distribution-based pricing (EXP-001)
        optuna_eligible = (
            interval_width < 0.12
            and _to_float(metrics.get("lower_bound")) > -0.03
            and ess >= 1.5
            and uncertainty < 0.10
        )

        rows.append({
            "decision_date": decision_date,
            "execution_date": bar_path.get("execution_date") or "",
            "symbol": symbol,
            "side": side,
            "run_label": run_label,
            "policy_scope": policy_scope,
            "pattern_key": f"{side}|direct|{regime_code}|{sector_code}",
            "policy_family": "directional_wide",
            "optuna_eligible": optuna_eligible,
            "forecast_selected": False,
            "single_prototype_collapse": ess <= 1.05,
            "q10_return": q10,
            "q25_return": _to_float(metrics.get("q25_return")),
            "q50_return": q50,
            "q75_return": _to_float(metrics.get("q75_return")),
            "q90_return": q90,
            "lower_bound": _to_float(metrics.get("lower_bound")),
            "interval_width": interval_width,
            "uncertainty": uncertainty,
            "member_mixture_ess": ess,
            "member_top1_weight_share": _to_float(metrics.get("member_top1_weight_share")),
            "member_pre_truncation_count": int(metrics.get("member_pre_truncation_count") or 0),
            "member_support_sum": _to_float(metrics.get("member_support_sum")),
            "member_consensus_signature": str(metrics.get("member_consensus_signature") or "direct_scoring"),
            "q50_d2_return": _to_float(metrics.get("q50_d2_return")),
            "q50_d3_return": _to_float(metrics.get("q50_d3_return")),
            "p_resolved_by_d2": _to_float(metrics.get("p_resolved_by_d2")),
            "p_resolved_by_d3": _to_float(metrics.get("p_resolved_by_d3")),
            "regime_code": regime_code,
            "sector_code": sector_code,
            "market": market,
            "t1_open": _to_float(bar_path.get("t1_open") or query_row.get("t1_open")),
            "d1_open": _to_float(bar_path.get("d1_open")),
            "d1_high": _to_float(bar_path.get("d1_high")),
            "d1_low": _to_float(bar_path.get("d1_low")),
            "d1_close": _to_float(bar_path.get("d1_close")),
            "bar_path_d1_to_d5": bar_path.get("bar_path_d1_to_d5", "[]"),
            "path_length": int(bar_path.get("path_length") or 0),
            "last_path_close": _to_float(bar_path.get("last_path_close")),
        })

    return rows


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_seed_rows_direct(
    *,
    event_cache_handle: EventRawCacheHandle,
    bundle_run_id: int,
    bundle_key: str,
    output_dir: str,
    policy_scope: str = "directional_wide_only",
    batch_size: int = 256,
    top_k: int = 96,
    kernel_temperature: float = 12.0,
) -> dict[str, Any]:
    """Generate seed rows directly from event_raw_cache + query_feature_rows.

    Bypasses train-snapshots and calibration-bundle entirely.
    """
    started = time.time()
    run_label = f"direct-{bundle_key}"

    print(f"[direct-seed] Loading event metadata from {event_cache_handle.events_path}...")
    t0 = time.time()
    event_meta = _load_event_metadata(event_cache_handle.events_path)
    ordinals = event_meta["ordinals"]
    outcome_dates = event_meta["outcome_dates"]
    print(f"[direct-seed] Loaded {len(ordinals)} events in {time.time()-t0:.1f}s")

    # Load raw feature matrix as memmap
    raw_matrix = event_cache_handle.load_raw_features(mmap_mode="r")
    prefix_event_ids = event_cache_handle.load_prefix_event_ids(mmap_mode="r")
    feature_keys = list(event_cache_handle.feature_keys)
    feature_index = event_cache_handle.feature_index
    n_features = len(feature_keys)

    # Sort events by outcome_end_date for binary search
    # The prefix arrays are already in outcome_end_date order
    sorted_outcome_dates = outcome_dates  # already sorted from parquet build

    # Load query rows from DB
    print("[direct-seed] Loading query rows from DB...")
    t0 = time.time()
    backtest_factory = create_backtest_session_factory()
    with backtest_factory() as session:
        query_result = session.execute(
            text("""
                SELECT decision_date, symbol, t1_open, regime_code, sector_code,
                       raw_features_json
                FROM bt_result.calibration_query_feature_row
                WHERE bundle_run_id = :bundle_run_id
                ORDER BY decision_date, symbol
            """),
            {"bundle_run_id": bundle_run_id},
        )
        query_rows = [dict(row._mapping) for row in query_result]
    print(f"[direct-seed] Loaded {len(query_rows)} query rows in {time.time()-t0:.1f}s")

    # Load replay bar paths from backtest DB (already stored during query-cache stage)
    print("[direct-seed] Loading replay bar paths from backtest DB...")
    t0 = time.time()
    bar_paths = _load_replay_bar_paths(backtest_factory, bundle_run_id)
    print(f"[direct-seed] Loaded {len(bar_paths)} bar paths in {time.time()-t0:.1f}s")

    # Group queries by decision_date
    queries_by_date: dict[str, list[dict]] = defaultdict(list)
    for row in query_rows:
        queries_by_date[str(row["decision_date"])].append(row)
    del query_rows

    decision_dates = sorted(queries_by_date.keys())
    print(f"[direct-seed] Processing {len(decision_dates)} decision dates...")

    all_seed_rows: list[dict[str, Any]] = []
    last_n_eligible = -1
    last_means: np.ndarray | None = None
    last_stds: np.ndarray | None = None
    processed_queries = 0
    t_scoring = time.time()

    for date_idx, decision_date in enumerate(decision_dates):
        date_queries = queries_by_date[decision_date]

        # Determine eligible events: outcome_end_date < decision_date
        n_eligible = bisect_right(sorted_outcome_dates, decision_date)
        if n_eligible <= 0:
            continue

        # Reconstruct scaler (only if n_eligible changed)
        if n_eligible != last_n_eligible:
            means, stds = _reconstruct_scaler_arrays(
                handle=event_cache_handle, n_eligible=n_eligible,
            )
            last_n_eligible = n_eligible
            last_means = means
            last_stds = stds

        # Get eligible event ordinals for raw feature access
        eligible_ordinals = np.asarray(prefix_event_ids[:n_eligible], dtype=np.int64)

        # Process queries in batches
        for batch_start in range(0, len(date_queries), batch_size):
            batch = date_queries[batch_start:batch_start + batch_size]
            B = len(batch)

            # Parse query raw features
            query_raw = np.zeros((B, n_features), dtype=np.float64)
            q_regimes = []
            q_sectors = []
            q_dates = []
            for j, qrow in enumerate(batch):
                raw_features = _json_loads(qrow.get("raw_features_json"), {})
                for key, value in raw_features.items():
                    idx = feature_index.get(str(key))
                    if idx is not None:
                        query_raw[j, idx] = _to_float(value)
                q_regimes.append(str(qrow.get("regime_code") or "UNKNOWN"))
                q_sectors.append(str(qrow.get("sector_code") or "UNKNOWN"))
                q_dates.append(str(qrow.get("decision_date") or ""))

            # Get eligible event raw features via fancy indexing
            event_raw = raw_matrix[eligible_ordinals]  # (N, d) from memmap

            # Score
            scorings = _score_batch(
                query_raw=query_raw,
                event_raw=event_raw,
                means=last_means,
                stds=last_stds,
                event_regime_codes=event_meta["regime_codes"][:n_eligible],
                event_sector_codes=event_meta["sector_codes"][:n_eligible],
                query_regime_codes=q_regimes,
                query_sector_codes=q_sectors,
                event_outcome_dates=sorted_outcome_dates[:n_eligible],
                query_dates=q_dates,
                side_payload_jsons=event_meta["side_payload_jsons"][:n_eligible],
                top_k=top_k,
                kernel_temperature=kernel_temperature,
            )

            # Assemble seed rows
            for j, qrow in enumerate(batch):
                symbol = str(qrow["symbol"])
                dd = str(qrow["decision_date"])
                bar_path = bar_paths.get((dd, symbol), {
                    "execution_date": None, "t1_open": None,
                    "d1_open": None, "d1_high": None, "d1_low": None, "d1_close": None,
                    "bar_path_d1_to_d5": "[]", "path_length": 0, "last_path_close": None,
                })
                seed_rows = _assemble_seed_row(
                    query_row=qrow,
                    scoring=scorings[j],
                    bar_path=bar_path,
                    policy_scope=policy_scope,
                    run_label=run_label,
                )
                all_seed_rows.extend(seed_rows)

            processed_queries += B

        if (date_idx + 1) % 50 == 0 or date_idx == len(decision_dates) - 1:
            elapsed = time.time() - t_scoring
            rate = processed_queries / max(elapsed, 0.01)
            print(f"[direct-seed] {date_idx+1}/{len(decision_dates)} dates, {processed_queries} queries, {len(all_seed_rows)} seeds, {rate:.0f} q/s")

    total_elapsed = time.time() - started
    print(f"[direct-seed] Done: {len(all_seed_rows)} seed rows in {total_elapsed:.1f}s")

    # Write study cache
    print("[direct-seed] Writing study cache...")
    from backtest_app.research_runtime.frozen_seed import (
        STUDY_CACHE_COLUMNS,
        write_study_cache_from_rows,
    )

    study_cache_dir = os.path.join(output_dir, "study_cache")
    os.makedirs(study_cache_dir, exist_ok=True)

    cache_result = write_study_cache_from_rows(
        seed_rows=all_seed_rows,
        output_dir=study_cache_dir,
        policy_scope=policy_scope,
        seed_profile="calibration_universe_v1",
    )

    print(f"[direct-seed] Study cache written: {cache_result.get('total_rows', 0)} rows, "
          f"{cache_result.get('fold_count', 0)} folds")

    return {
        "status": "ok",
        "total_seed_rows": len(all_seed_rows),
        "total_queries": processed_queries,
        "elapsed_seconds": total_elapsed,
        "study_cache_dir": study_cache_dir,
        "study_cache_result": cache_result,
    }
