from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from backtest_app.historical_data.models import HistoricalBar
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution.ladder import round_to_tick
from shared.domain.models import ExecutionVenue, FillStatus, LadderLeg, OrderPlan, OrderType, Side

PROOF_SUBSET_SEED_PROFILE = "proof_subset_v1"
CALIBRATION_UNIVERSE_SEED_PROFILE = "calibration_universe_v1"
STUDY_CACHE_DIRNAME = "study_cache"
STUDY_CACHE_MANIFEST_NAME = "manifest.json"
STUDY_CACHE_COLUMNS = [
    "decision_date",
    "execution_date",
    "symbol",
    "side",
    "pattern_key",
    "policy_family",
    "lower_bound",
    "q10_return",
    "q50_return",
    "q25_return",
    "q90_return",
    "q75_return",
    "interval_width",
    "uncertainty",
    "member_mixture_ess",
    "member_top1_weight_share",
    "member_pre_truncation_count",
    "member_consensus_signature",
    "t1_open",
    "d1_open",
    "d1_high",
    "d1_low",
    "d1_close",
    "market",
    "regime_code",
    "sector_code",
    "forecast_selected",
    "optuna_eligible",
    "single_prototype_collapse",
]


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _clip(value: float, lower: float, upper: float) -> float:
    return float(max(lower, min(value, upper)))


def _shape_bucket(q50: float, interval_width: float) -> str:
    width = max(float(interval_width), 0.0)
    abs_q50 = abs(float(q50))
    if width <= abs_q50:
        return "tight"
    if width <= 2.0 * abs_q50:
        return "mid"
    return "wide"


def _market_for_row(row: Mapping[str, Any]) -> str:
    country = str(row.get("country_code") or "").upper()
    exchange = str(row.get("exchange_code") or "").upper()
    if country == "KR" or exchange in {"KRX", "KOSPI", "KOSDAQ"}:
        return "KR"
    return "US"


def _extract_bar_path(*, bars_by_symbol: Mapping[str, Sequence[HistoricalBar]], symbol: str, decision_date: str, max_days: int = 5) -> dict[str, Any]:
    bars = list((bars_by_symbol or {}).get(symbol) or [])
    if not bars:
        return {
            "execution_date": None,
            "t1_open": None,
            "d1_open": None,
            "d1_high": None,
            "d1_low": None,
            "d1_close": None,
            "bar_path_d1_to_d5": json.dumps([], ensure_ascii=False),
            "path_length": 0,
            "last_path_close": None,
        }
    idx = next((i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == decision_date), None)
    if idx is None or idx + 1 >= len(bars):
        return {
            "execution_date": None,
            "t1_open": None,
            "d1_open": None,
            "d1_high": None,
            "d1_low": None,
            "d1_close": None,
            "bar_path_d1_to_d5": json.dumps([], ensure_ascii=False),
            "path_length": 0,
            "last_path_close": None,
        }
    path = []
    for bar in bars[idx + 1 : idx + 1 + max_days]:
        path.append(
            {
                "session_date": str(bar.timestamp)[:10],
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            }
        )
    first = path[0] if path else {}
    return {
        "execution_date": first.get("session_date"),
        "t1_open": first.get("open"),
        "d1_open": first.get("open"),
        "d1_high": first.get("high"),
        "d1_low": first.get("low"),
        "d1_close": first.get("close"),
        "bar_path_d1_to_d5": json.dumps(path, ensure_ascii=False),
        "path_length": len(path),
        "last_path_close": path[-1]["close"] if path else None,
    }


def _precomputed_bar_path(row: Mapping[str, Any]) -> dict[str, Any] | None:
    path_text = str(row.get("bar_path_d1_to_d5") or "").strip()
    has_t1_open = row.get("t1_open") not in (None, "")
    if not path_text and not has_t1_open:
        return None
    if path_text:
        try:
            path = json.loads(path_text)
        except json.JSONDecodeError:
            path = []
    else:
        path = []
    first = path[0] if path else {}
    return {
        "execution_date": row.get("execution_date") or first.get("session_date"),
        "t1_open": _to_float(row.get("t1_open"), _to_float(first.get("open"))),
        "d1_open": _to_float(row.get("d1_open"), _to_float(first.get("open"))),
        "d1_high": _to_float(row.get("d1_high"), _to_float(first.get("high"))),
        "d1_low": _to_float(row.get("d1_low"), _to_float(first.get("low"))),
        "d1_close": _to_float(row.get("d1_close"), _to_float(first.get("close"))),
        "bar_path_d1_to_d5": path_text or json.dumps(path, ensure_ascii=False),
        "path_length": _to_int(row.get("path_length"), len(path)),
        "last_path_close": _to_float(row.get("last_path_close"), _to_float(path[-1].get("close")) if path else 0.0),
    }


def _side_seed_metrics(row: Mapping[str, Any], side: str) -> dict[str, Any]:
    prefix = "buy" if side == "BUY" else "sell"
    q10 = _to_float(row.get(f"{prefix}_q10"))
    q50 = _to_float(row.get(f"{prefix}_q50"))
    q90 = _to_float(row.get(f"{prefix}_q90"))
    interval_width = _to_float(row.get(f"{prefix}_interval_width"), max(q90 - q10, 0.0))
    uncertainty = _to_float(row.get(f"{prefix}_uncertainty"))
    member_mixture_ess = _to_float(row.get(f"{prefix}_member_mixture_ess"), _to_float(row.get(f"{prefix}_mixture_ess")))
    member_top1_weight_share = _to_float(row.get(f"{prefix}_member_top1_weight_share"), _to_float(row.get(f"{prefix}_top1_weight_share")))
    member_pre_truncation_count = _to_int(row.get(f"{prefix}_member_pre_truncation_count"), _to_int(row.get(f"{prefix}_pre_truncation_candidate_count")))
    positive_weight_member_count = _to_int(row.get(f"{prefix}_positive_weight_member_count"), _to_int(row.get(f"{prefix}_positive_weight_candidate_count")))
    member_consensus_signature = str(row.get(f"{prefix}_member_consensus_signature") or row.get(f"{prefix}_consensus_signature") or "no_consensus")
    return {
        "expected_net_return": _to_float(row.get(f"{prefix}_expected_net_return")),
        "q10_return": q10,
        "q50_return": q50,
        "q90_return": q90,
        "q50_d2_return": _to_float(row.get(f"{prefix}_q50_d2_return")),
        "q50_d3_return": _to_float(row.get(f"{prefix}_q50_d3_return")),
        "p_resolved_by_d2": _to_float(row.get(f"{prefix}_p_resolved_by_d2")),
        "p_resolved_by_d3": _to_float(row.get(f"{prefix}_p_resolved_by_d3")),
        "interval_width": max(interval_width, max(q90 - q10, 0.0)),
        "uncertainty": uncertainty,
        "member_mixture_ess": member_mixture_ess,
        "member_top1_weight_share": member_top1_weight_share,
        "member_pre_truncation_count": member_pre_truncation_count,
        "member_support_sum": _to_float(row.get(f"{prefix}_member_support_sum"), _to_float(row.get(f"{prefix}_top_match_support_sum"))),
        "member_consensus_signature": member_consensus_signature,
        "member_candidate_count": _to_int(row.get(f"{prefix}_member_candidate_count"), member_pre_truncation_count),
        "positive_weight_member_count": positive_weight_member_count,
        "lower_bound": q10,
    }


def _policy_family(row: Mapping[str, Any], *, recurring_family: bool) -> str:
    if not recurring_family:
        return "echo_or_collapse"
    q10 = _to_float(row.get("q10_return"))
    q50 = _to_float(row.get("q50_return"))
    ess = _to_float(row.get("member_mixture_ess"))
    top1 = _to_float(row.get("member_top1_weight_share"))
    if q10 > 0.0 and ess >= 2.0 and top1 <= 0.75:
        return "tight_consensus"
    if q50 > 0.0 and ess >= 1.5 and top1 <= 0.85:
        return "directional_wide"
    return "echo_or_collapse"


def _build_side_seed_rows(
    *,
    forecast_rows: Sequence[Mapping[str, Any]],
    bars_by_symbol: Mapping[str, Sequence[HistoricalBar]],
    run_label: str,
    policy_scope: str,
) -> dict[str, Any]:
    side_rows: list[dict[str, Any]] = []
    for raw in forecast_rows:
        row = dict(raw)
        decision_date = str(row.get("decision_date") or "")
        symbol = str(row.get("symbol") or "")
        if not decision_date or not symbol:
            continue
        chosen_side = str(row.get("chosen_side_before_deploy") or "").upper()
        dominant_side = str(row.get("dominant_side") or chosen_side).upper()
        for side in ("BUY", "SELL"):
            metrics = _side_seed_metrics(row, side)
            consensus = str(metrics["member_consensus_signature"] or "no_consensus")
            regime_code = str(row.get("query_regime_code") or row.get("regime_code") or "UNKNOWN")
            sector_code = str(row.get("query_sector_code") or row.get("sector_code") or "UNKNOWN")
            shape_bucket = _shape_bucket(metrics["q50_return"], metrics["interval_width"])
            single_prototype_collapse = bool(
                metrics["member_candidate_count"] <= 1
                or metrics["positive_weight_member_count"] <= 1
                or metrics["member_top1_weight_share"] >= 0.95
                or metrics["member_mixture_ess"] <= 1.05
            )
            use_precomputed = side == dominant_side and any(key in row for key in ("pattern_key", "policy_family", "optuna_eligible"))
            precomputed_pattern_key = str(row.get("pattern_key") or "").strip()
            bar_path = _precomputed_bar_path(row) or _extract_bar_path(bars_by_symbol=bars_by_symbol, symbol=symbol, decision_date=decision_date)
            side_rows.append(
                {
                    "decision_date": decision_date,
                    "execution_date": bar_path["execution_date"],
                    "symbol": symbol,
                    "side": side,
                    "run_label": run_label,
                    "policy_scope": policy_scope,
                    "pattern_key": precomputed_pattern_key or "|".join([side, consensus, regime_code, sector_code, shape_bucket]),
                    "policy_family": "echo_or_collapse",
                    "optuna_eligible": False,
                    "forecast_selected": _to_bool(row.get("forecast_selected")) and chosen_side == side,
                    "chosen_side_before_deploy": row.get("chosen_side_before_deploy"),
                    "abstain": bool(row.get("abstain", False)),
                    "single_prototype_collapse": single_prototype_collapse,
                    "policy_edge_score": None,
                    "q10_return": metrics["q10_return"],
                    "q50_return": metrics["q50_return"],
                    "q90_return": metrics["q90_return"],
                    "lower_bound": metrics["lower_bound"],
                    "interval_width": metrics["interval_width"],
                    "uncertainty": metrics["uncertainty"],
                    "member_mixture_ess": metrics["member_mixture_ess"],
                    "member_top1_weight_share": metrics["member_top1_weight_share"],
                    "member_pre_truncation_count": metrics["member_pre_truncation_count"],
                    "member_support_sum": metrics["member_support_sum"],
                    "member_consensus_signature": consensus,
                    "q50_d2_return": metrics["q50_d2_return"],
                    "q50_d3_return": metrics["q50_d3_return"],
                    "p_resolved_by_d2": metrics["p_resolved_by_d2"],
                    "p_resolved_by_d3": metrics["p_resolved_by_d3"],
                    "regime_code": regime_code,
                    "sector_code": sector_code,
                    "country_code": row.get("country_code"),
                    "exchange_code": row.get("exchange_code"),
                    "exchange_tz": row.get("exchange_tz"),
                    "shape_bucket": shape_bucket,
                    "market": _market_for_row(row),
                    "t1_open": bar_path["t1_open"],
                    "d1_open": bar_path["d1_open"],
                    "d1_high": bar_path["d1_high"],
                    "d1_low": bar_path["d1_low"],
                    "d1_close": bar_path["d1_close"],
                    "bar_path_d1_to_d5": bar_path["bar_path_d1_to_d5"],
                    "path_length": bar_path["path_length"],
                    "last_path_close": bar_path["last_path_close"],
                    "_precomputed_pre_optuna": use_precomputed,
                    "_precomputed_recurring_family": _to_bool(row.get("recurring_family")) if use_precomputed else False,
                    "_precomputed_policy_family": str(row.get("policy_family") or "echo_or_collapse") if use_precomputed else "",
                    "_precomputed_optuna_eligible": _to_bool(row.get("optuna_eligible")) if use_precomputed else False,
                }
            )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in side_rows:
        grouped[str(row.get("pattern_key") or "missing")].append(row)
    recurring_keys: set[str] = set()
    for pattern_key, group in grouped.items():
        dates = {str(item.get("decision_date") or "") for item in group if item.get("decision_date")}
        if len(dates) >= 3 and len(group) >= 5:
            recurring_keys.add(pattern_key)
    for row in side_rows:
        recurring_family = str(row.get("pattern_key") or "") in recurring_keys
        if bool(row.pop("_precomputed_pre_optuna", False)):
            row["recurring_family"] = _to_bool(row.pop("_precomputed_recurring_family", False), recurring_family)
            row["policy_family"] = str(row.pop("_precomputed_policy_family", "echo_or_collapse") or "echo_or_collapse")
            row["optuna_eligible"] = _to_bool(row.pop("_precomputed_optuna_eligible", False))
        else:
            row.pop("_precomputed_recurring_family", None)
            row.pop("_precomputed_policy_family", None)
            row.pop("_precomputed_optuna_eligible", None)
            row["recurring_family"] = recurring_family
            row["policy_family"] = _policy_family(row, recurring_family=recurring_family)
            row["optuna_eligible"] = bool(
                recurring_family
                and not bool(row.get("single_prototype_collapse"))
                and bool(row.get("forecast_selected") or row.get("side") == "SELL")
                and (
                    (policy_scope == "directional_wide_only" and row["policy_family"] == "directional_wide")
                    or (policy_scope == "tight_consensus_only" and row["policy_family"] == "tight_consensus")
                    or (policy_scope == "mixed_families" and row["policy_family"] in {"tight_consensus", "directional_wide"})
                    or (policy_scope not in {"directional_wide_only", "tight_consensus_only", "mixed_families"} and row["policy_family"] != "echo_or_collapse")
                )
            )
    side_rows.sort(key=lambda item: (str(item.get("decision_date") or ""), str(item.get("symbol") or ""), str(item.get("side") or "")))
    top_families = []
    for pattern_key, group in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        top_families.append(
            {
                "pattern_key": pattern_key,
                "row_count": len(group),
                "decision_date_count": len({str(item.get("decision_date") or "") for item in group if item.get("decision_date")}),
                "side": group[0].get("side"),
                "policy_family": group[0].get("policy_family"),
            }
        )
        if len(top_families) >= 5:
            break
    summary = {
        "row_count": len(side_rows),
        "buy_row_count": sum(1 for row in side_rows if row["side"] == "BUY"),
        "sell_row_count": sum(1 for row in side_rows if row["side"] == "SELL"),
        "optuna_eligible_row_count": sum(1 for row in side_rows if bool(row.get("optuna_eligible"))),
        "buy_selected_row_count": sum(1 for row in side_rows if row["side"] == "BUY" and bool(row.get("forecast_selected"))),
        "sell_signal_row_count": sum(1 for row in side_rows if row["side"] == "SELL"),
        "recurring_pattern_count": len(recurring_keys),
        "policy_scope": policy_scope,
        "top_pattern_families": top_families,
    }
    return {"seed_rows": side_rows, "summary": summary}


def build_optuna_replay_seed(
    *,
    forecast_rows: Sequence[Mapping[str, Any]],
    bars_by_symbol: Mapping[str, Sequence[HistoricalBar]],
    run_label: str,
    policy_scope: str = "directional_wide_only",
) -> dict[str, Any]:
    analysis = _build_side_seed_rows(
        forecast_rows=forecast_rows,
        bars_by_symbol=bars_by_symbol,
        run_label=run_label,
        policy_scope=policy_scope,
    )
    return {
        "seed_rows": analysis["seed_rows"],
        "summary": {
            **analysis["summary"],
            "source_run_label": run_label,
        },
    }


def write_optuna_replay_seed_artifacts(*, run_dir: Path, replay_seed: Mapping[str, Any]) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = list(replay_seed.get("seed_rows") or [])
    summary = dict(replay_seed.get("summary") or {})
    seed_path = run_dir / "optuna_replay_seed.parquet"
    summary_path = run_dir / "optuna_replay_seed_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    seed_format = "parquet"
    try:
        import pandas as pd
    except Exception:
        seed_path.write_text(json.dumps({"format": "json_fallback", "rows": rows}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        seed_format = "json_fallback"
    else:
        pd.DataFrame(rows).to_parquet(seed_path, index=False)
    return {
        "optuna_replay_seed_path": str(seed_path),
        "optuna_replay_seed_format": seed_format,
        "optuna_replay_seed_summary_path": str(summary_path),
        "optuna_replay_seed_row_count": len(rows),
        "optuna_replay_seed_summary": summary,
    }


def write_calibration_bundle_artifacts(
    *,
    output_dir: Path,
    seed_rows: Sequence[Mapping[str, Any]],
    source_chunks: Sequence[Mapping[str, Any]],
    policy_scope: str,
    proof_reference_run: str = "",
    universe_symbol_count: int = 0,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    failed_chunk_count = sum(1 for chunk in source_chunks if str(chunk.get("status") or "") == "failed")
    summary = summarize_seed_rows(
        seed_rows=seed_rows,
        policy_scope=policy_scope,
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
        proof_reference_run=proof_reference_run,
        source_chunk_count=len(list(source_chunks)),
        failed_chunk_count=failed_chunk_count,
        universe_symbol_count=universe_symbol_count,
    )
    replay_seed_artifacts = write_optuna_replay_seed_artifacts(
        run_dir=output_dir,
        replay_seed={
            "seed_rows": list(seed_rows),
            "summary": {
                **summary,
                "source_run_label": "calibration_bundle",
            },
        },
    )
    source_chunks_path = output_dir / "source_chunks.json"
    coverage_summary_path = output_dir / "coverage_summary.json"
    source_chunks_path.write_text(json.dumps(list(source_chunks), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    coverage_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {
        **replay_seed_artifacts,
        "source_chunks_path": str(source_chunks_path),
        "coverage_summary_path": str(coverage_summary_path),
        "coverage_summary": summary,
    }


def write_study_cache_from_rows(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    output_dir: str,
    policy_scope: str = "directional_wide_only",
    seed_profile: str = CALIBRATION_UNIVERSE_SEED_PROFILE,
    seed_filter: str = "",
    source_seed_root: str = "",
    source_seed_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    filtered_rows = filter_optuna_seed_rows(
        seed_rows=[dict(row) for row in seed_rows],
        policy_scope=policy_scope,
        seed_profile=seed_profile,
        seed_filter=seed_filter,
    )
    cache_root = Path(output_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("pandas is required to build study cache") from exc
    folds = _date_groups(filtered_rows)
    filtered_rows_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in filtered_rows:
        filtered_rows_by_date[str(row.get("decision_date") or "")].append(dict(row))
    fold_entries: list[dict[str, Any]] = []
    for idx, fold in enumerate(folds, start=1):
        allowed_dates = set(str(date or "") for date in (fold.get("dates") or []))
        fold_rows = [
            {column: row.get(column) for column in STUDY_CACHE_COLUMNS}
            for decision_date in sorted(allowed_dates)
            for row in filtered_rows_by_date.get(decision_date, [])
        ]
        fold_path = cache_root / f"fold_{idx:03d}.parquet"
        pd.DataFrame(fold_rows, columns=STUDY_CACHE_COLUMNS).to_parquet(fold_path, index=False)
        fold_entries.append(
            {
                "fold_index": idx,
                "path": str(fold_path),
                "start_date": fold.get("start_date"),
                "end_date": fold.get("end_date"),
                "decision_date_count": len(allowed_dates),
                "row_count": len(fold_rows),
                "buy_row_count": sum(1 for row in fold_rows if str(row.get("side") or "") == "BUY"),
                "sell_row_count": sum(1 for row in fold_rows if str(row.get("side") or "") == "SELL"),
            }
        )
    source_summary = dict(source_seed_summary or {})
    filtered_summary = summarize_seed_rows(
        seed_rows=filtered_rows,
        policy_scope=policy_scope,
        seed_profile=seed_profile,
        proof_reference_run=str(source_summary.get("proof_reference_run") or ""),
        source_chunk_count=int(source_summary.get("source_chunk_count") or 0),
        failed_chunk_count=int(source_summary.get("failed_chunk_count") or 0),
        universe_symbol_count=int(source_summary.get("universe_symbol_count") or 0),
    )
    manifest = {
        "source_seed_root": source_seed_root,
        "source_seed_summary": source_summary,
        "policy_scope": policy_scope,
        "seed_profile": seed_profile,
        "seed_filter": seed_filter,
        "columns": list(STUDY_CACHE_COLUMNS),
        "row_count": len(filtered_rows),
        "buy_row_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "BUY"),
        "sell_row_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "SELL"),
        "decision_date_count": len({str(row.get("decision_date") or "") for row in filtered_rows if row.get("decision_date")}),
        "folds": fold_entries,
        "filtered_summary": filtered_summary,
    }
    manifest_path = cache_root / STUDY_CACHE_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {
        "study_cache_root": str(cache_root),
        "study_cache_manifest_path": str(manifest_path),
        "study_cache_manifest": manifest,
    }


def build_study_cache(
    *,
    seed_artifact_root: str,
    output_dir: str = "",
    policy_scope: str = "directional_wide_only",
    seed_profile: str = CALIBRATION_UNIVERSE_SEED_PROFILE,
    seed_filter: str = "",
) -> dict[str, Any]:
    seed_bundle = load_optuna_replay_seed(seed_artifact_root)
    cache_root = Path(output_dir) if output_dir else Path(seed_bundle["root"]) / STUDY_CACHE_DIRNAME
    return write_study_cache_from_rows(
        seed_rows=[dict(row) for row in (seed_bundle.get("rows") or [])],
        output_dir=str(cache_root),
        policy_scope=policy_scope,
        seed_profile=seed_profile,
        seed_filter=seed_filter,
        source_seed_root=str(seed_bundle.get("root") or ""),
        source_seed_summary=seed_bundle.get("summary") or {},
    )


def _resolve_seed_root(seed_artifact_root: str) -> Path:
    root = Path(seed_artifact_root)
    if (root / "optuna_replay_seed_summary.json").exists():
        return root
    matches = list(root.glob("research/*/optuna_replay_seed_summary.json"))
    if len(matches) == 1:
        return matches[0].parent
    raise FileNotFoundError(f"optuna_replay_seed_summary.json not found under {seed_artifact_root}")


def load_optuna_replay_seed(seed_artifact_root: str) -> dict[str, Any]:
    root = _resolve_seed_root(seed_artifact_root)
    summary_path = root / "optuna_replay_seed_summary.json"
    seed_path = root / "optuna_replay_seed.parquet"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    rows: list[dict[str, Any]]
    if not seed_path.exists():
        rows = []
    else:
        try:
            import pandas as pd

            rows = pd.read_parquet(seed_path).to_dict(orient="records")
        except Exception:
            payload = json.loads(seed_path.read_text(encoding="utf-8"))
            rows = list(payload.get("rows") or [])
    return {
        "root": str(root),
        "seed_path": str(seed_path),
        "summary_path": str(summary_path),
        "rows": rows,
        "summary": summary,
    }


def _resolve_study_cache_root(seed_artifact_root: str) -> Path:
    raw_root = Path(seed_artifact_root)
    direct_manifest = raw_root / STUDY_CACHE_MANIFEST_NAME
    nested_manifest = raw_root / STUDY_CACHE_DIRNAME / STUDY_CACHE_MANIFEST_NAME
    if direct_manifest.exists():
        return raw_root
    if nested_manifest.exists():
        return raw_root / STUDY_CACHE_DIRNAME
    root = _resolve_seed_root(seed_artifact_root)
    direct_manifest = root / STUDY_CACHE_MANIFEST_NAME
    nested_manifest = root / STUDY_CACHE_DIRNAME / STUDY_CACHE_MANIFEST_NAME
    if direct_manifest.exists():
        return root
    if nested_manifest.exists():
        return root / STUDY_CACHE_DIRNAME
    raise FileNotFoundError(f"{STUDY_CACHE_MANIFEST_NAME} not found under {seed_artifact_root}")


def load_study_cache_manifest(seed_artifact_root: str) -> dict[str, Any] | None:
    try:
        cache_root = _resolve_study_cache_root(seed_artifact_root)
    except FileNotFoundError:
        return None
    manifest_path = cache_root / STUDY_CACHE_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "cache_root": str(cache_root),
        "manifest_path": str(manifest_path),
        "manifest": manifest,
    }


def resolve_seed_profile(*, seed_profile: str = "", seed_filter: str = "") -> str:
    normalized_profile = str(seed_profile or "").strip()
    if normalized_profile in {PROOF_SUBSET_SEED_PROFILE, CALIBRATION_UNIVERSE_SEED_PROFILE}:
        return normalized_profile
    normalized_filter = str(seed_filter or "").strip()
    if normalized_filter in {"pre_optuna_family", "forecast_selected", "eligible_only"}:
        return PROOF_SUBSET_SEED_PROFILE
    return CALIBRATION_UNIVERSE_SEED_PROFILE


def _buy_family_matches_scope(row: Mapping[str, Any], policy_scope: str) -> bool:
    family = str(row.get("policy_family") or "")
    if not policy_scope or policy_scope == "mixed_families":
        return family in {"tight_consensus", "directional_wide"}
    if policy_scope == "directional_wide_only":
        return family == "directional_wide"
    if policy_scope == "tight_consensus_only":
        return family == "tight_consensus"
    return family == policy_scope


def _has_replay_path(row: Mapping[str, Any]) -> bool:
    return (
        bool(str(row.get("execution_date") or "").strip())
        and _to_float(row.get("t1_open")) > 0.0
        and _to_int(row.get("path_length")) > 0
    )


def _proof_subset_rows(*, seed_rows: Sequence[Mapping[str, Any]], policy_scope: str, seed_filter: str) -> list[dict[str, Any]]:
    normalized_rows = [dict(raw) for raw in seed_rows]
    if seed_filter in {"", "pre_optuna_family"}:
        eligible_buys = [
            row
            for row in normalized_rows
            if str(row.get("side") or "") == "BUY"
            and _buy_family_matches_scope(row, policy_scope)
            and bool(row.get("optuna_eligible"))
        ]
        if not eligible_buys:
            return []
        first_buy_date_by_symbol: dict[str, str] = {}
        for row in eligible_buys:
            symbol = str(row.get("symbol") or "")
            decision_date = str(row.get("decision_date") or "")
            if symbol and decision_date:
                first_buy_date_by_symbol[symbol] = min(decision_date, first_buy_date_by_symbol.get(symbol, decision_date))
        filtered = list(eligible_buys)
        for row in normalized_rows:
            if str(row.get("side") or "") != "SELL":
                continue
            symbol = str(row.get("symbol") or "")
            decision_date = str(row.get("decision_date") or "")
            first_buy_date = first_buy_date_by_symbol.get(symbol)
            if first_buy_date and decision_date and decision_date >= first_buy_date:
                filtered.append(row)
        filtered.sort(key=lambda item: (str(item.get("decision_date") or ""), str(item.get("symbol") or ""), str(item.get("side") or "")))
        return filtered

    filtered: list[dict[str, Any]] = []
    for row in normalized_rows:
        side = str(row.get("side") or "")
        if side == "BUY":
            if not _buy_family_matches_scope(row, policy_scope):
                continue
            if seed_filter == "forecast_selected" and not bool(row.get("forecast_selected")):
                continue
            if seed_filter == "eligible_only" and not bool(row.get("optuna_eligible")):
                continue
        elif side == "SELL":
            if seed_filter == "eligible_only" and not _buy_family_matches_scope(row, policy_scope):
                continue
        filtered.append(row)
    filtered.sort(key=lambda item: (str(item.get("decision_date") or ""), str(item.get("symbol") or ""), str(item.get("side") or "")))
    return filtered


def _calibration_universe_rows(*, seed_rows: Sequence[Mapping[str, Any]], policy_scope: str) -> list[dict[str, Any]]:
    normalized_rows = [dict(raw) for raw in seed_rows]
    calibration_buys = [
        row
        for row in normalized_rows
        if str(row.get("side") or "") == "BUY"
        and _buy_family_matches_scope(row, policy_scope)
        and not bool(row.get("single_prototype_collapse"))
        and _has_replay_path(row)
    ]
    if not calibration_buys:
        return []
    first_buy_date_by_symbol: dict[str, str] = {}
    for row in calibration_buys:
        symbol = str(row.get("symbol") or "")
        decision_date = str(row.get("decision_date") or "")
        if symbol and decision_date:
            first_buy_date_by_symbol[symbol] = min(decision_date, first_buy_date_by_symbol.get(symbol, decision_date))
    filtered = list(calibration_buys)
    for row in normalized_rows:
        if str(row.get("side") or "") != "SELL":
            continue
        if not _has_replay_path(row):
            continue
        symbol = str(row.get("symbol") or "")
        decision_date = str(row.get("decision_date") or "")
        first_buy_date = first_buy_date_by_symbol.get(symbol)
        if first_buy_date and decision_date and decision_date >= first_buy_date:
            filtered.append(row)
    filtered.sort(key=lambda item: (str(item.get("decision_date") or ""), str(item.get("symbol") or ""), str(item.get("side") or "")))
    return filtered


def filter_optuna_seed_rows(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    policy_scope: str = "directional_wide_only",
    seed_filter: str = "",
    seed_profile: str = "",
) -> list[dict[str, Any]]:
    resolved_profile = resolve_seed_profile(seed_profile=seed_profile, seed_filter=seed_filter)
    if resolved_profile == PROOF_SUBSET_SEED_PROFILE:
        return _proof_subset_rows(seed_rows=seed_rows, policy_scope=policy_scope, seed_filter=seed_filter)
    return _calibration_universe_rows(seed_rows=seed_rows, policy_scope=policy_scope)


def summarize_seed_rows(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    policy_scope: str,
    seed_profile: str,
    proof_reference_run: str = "",
    source_chunk_count: int = 0,
    failed_chunk_count: int = 0,
    universe_symbol_count: int = 0,
) -> dict[str, Any]:
    rows = [dict(raw) for raw in seed_rows]
    filtered_rows = filter_optuna_seed_rows(
        seed_rows=rows,
        policy_scope=policy_scope,
        seed_profile=seed_profile,
    )
    decision_dates = sorted({str(row.get("decision_date") or "") for row in rows if row.get("decision_date")})
    filtered_dates = sorted({str(row.get("decision_date") or "") for row in filtered_rows if row.get("decision_date")})
    return {
        "row_count": len(rows),
        "buy_row_count": sum(1 for row in rows if str(row.get("side") or "") == "BUY"),
        "sell_row_count": sum(1 for row in rows if str(row.get("side") or "") == "SELL"),
        "optuna_eligible_row_count": sum(1 for row in rows if bool(row.get("optuna_eligible"))),
        "calibration_universe_row_count": len(filtered_rows) if seed_profile == CALIBRATION_UNIVERSE_SEED_PROFILE else 0,
        "calibration_universe_buy_row_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "BUY"),
        "calibration_universe_sell_row_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "SELL"),
        "universe_symbol_count": universe_symbol_count or len({str(row.get("symbol") or "") for row in rows if row.get("symbol")}),
        "universe_date_count": len(decision_dates),
        "buy_candidate_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "BUY"),
        "sell_replay_row_count": sum(1 for row in filtered_rows if str(row.get("side") or "") == "SELL"),
        "seed_profile": seed_profile,
        "policy_scope": policy_scope,
        "source_chunk_count": source_chunk_count,
        "failed_chunk_count": failed_chunk_count,
        "proof_reference_run": proof_reference_run,
        "filtered_decision_date_count": len(filtered_dates),
    }


def default_frozen_seed_search_space() -> dict[str, dict[str, Any]]:
    return {
        "execution_mode": {"type": "categorical", "choices": ["single_leg", "ladder_v1"]},
        # --- scoring weights (unchanged) ---
        "w_lb": {"type": "float", "low": 0.0, "high": 3.0, "step": 0.25},
        "w_q50": {"type": "float", "low": 0.0, "high": 3.0, "step": 0.25},
        "w_width": {"type": "float", "low": 0.0, "high": 3.0, "step": 0.25},
        "w_uncertainty": {"type": "float", "low": 0.0, "high": 3.0, "step": 0.25},
        "w_ess": {"type": "float", "low": 0.0, "high": 2.0, "step": 0.25},
        "min_buy_score": {"type": "float", "low": -0.01, "high": 0.05, "step": 0.005},
        "min_lower_bound": {"type": "float", "low": -0.03, "high": 0.03, "step": 0.005},
        "min_member_ess": {"type": "float", "low": 1.0, "high": 8.0, "step": 0.5},
        # --- portfolio sizing (unchanged) ---
        "max_new_buys": {"type": "int", "low": 1, "high": 6, "step": 1},
        "buy_budget_fraction": {"type": "float", "low": 0.20, "high": 1.00, "step": 0.05},
        "per_name_cap_fraction": {"type": "float", "low": 0.05, "high": 0.35, "step": 0.05},
        # --- distribution-based buy pricing ---
        "buy_dist_blend": {"type": "float", "low": 0.0, "high": 1.0, "step": 0.1},
        "use_skew_adjust": {"type": "categorical", "choices": [True, False]},
        "skew_dampener": {"type": "float", "low": 0.0, "high": 0.5, "step": 0.05},
        "use_ess_tightening": {"type": "categorical", "choices": [True, False]},
        "ess_cap": {"type": "float", "low": 5.0, "high": 50.0, "step": 5.0},
        "tighten_ratio": {"type": "float", "low": 0.0, "high": 0.5, "step": 0.05},
        # --- distribution-based sell pricing ---
        "sell_dist_blend": {"type": "float", "low": 0.0, "high": 1.0, "step": 0.1},
        "fallback_min_sell_markup": {"type": "float", "low": 0.001, "high": 0.01, "step": 0.001},
        "use_sell_skew_adjust": {"type": "categorical", "choices": [True, False]},
        "sell_skew_floor": {"type": "float", "low": 0.3, "high": 0.8, "step": 0.1},
        "sell_skew_ceil": {"type": "float", "low": 1.2, "high": 2.0, "step": 0.1},
        "use_uncertainty_discount": {"type": "categorical", "choices": [True, False]},
        "sell_unc_weight": {"type": "float", "low": 0.0, "high": 2.0, "step": 0.1},
        # --- ladder params ---
        "buy_leg_count": {"type": "int", "low": 1, "high": 3, "step": 1},
        "buy_leg_spread_ratio": {"type": "float", "low": 1.0, "high": 3.0, "step": 0.25},
        "buy_leg_weight_alpha": {"type": "float", "low": 0.5, "high": 2.0, "step": 0.1},
        "sell_leg_count": {"type": "int", "low": 1, "high": 3, "step": 1},
        "sell_leg_spread_ratio": {"type": "float", "low": 1.0, "high": 3.0, "step": 0.25},
        "sell_leg_weight_alpha": {"type": "float", "low": 0.5, "high": 2.0, "step": 0.1},
    }


def default_frozen_seed_warm_start_trials() -> list[dict[str, Any]]:
    shared = {
        "w_lb": 1.0,
        "w_q50": 1.0,
        "w_width": 0.25,
        "w_uncertainty": 0.25,
        "w_ess": 0.5,
        "min_buy_score": -0.005,
        "min_lower_bound": -0.01,
        "min_member_ess": 1.0,
        "max_new_buys": 3,
        "buy_budget_fraction": 0.95,
        "per_name_cap_fraction": 0.20,
        "buy_dist_blend": 0.5,
        "sell_dist_blend": 0.5,
        "fallback_min_sell_markup": 0.003,
        "use_skew_adjust": False,
        "skew_dampener": 0.2,
        "use_ess_tightening": False,
        "ess_cap": 20.0,
        "tighten_ratio": 0.3,
        "use_sell_skew_adjust": False,
        "sell_skew_floor": 0.5,
        "sell_skew_ceil": 1.5,
        "use_uncertainty_discount": False,
        "sell_unc_weight": 0.5,
        "buy_leg_count": 1,
        "buy_leg_spread_ratio": 1.5,
        "buy_leg_weight_alpha": 1.0,
        "sell_leg_count": 1,
        "sell_leg_spread_ratio": 1.5,
        "sell_leg_weight_alpha": 1.0,
    }
    return [
        {
            **shared,
            "execution_mode": "single_leg",
        },
        {
            **shared,
            "execution_mode": "single_leg",
            "use_skew_adjust": True,
            "use_ess_tightening": True,
        },
        {
            **shared,
            "execution_mode": "single_leg",
            "buy_dist_blend": 0.3,
            "sell_dist_blend": 0.7,
            "use_sell_skew_adjust": True,
            "use_uncertainty_discount": True,
            "max_new_buys": 2,
            "buy_budget_fraction": 0.75,
        },
        {
            **shared,
            "execution_mode": "single_leg",
            "buy_dist_blend": 0.8,
            "sell_dist_blend": 0.3,
        },
        {
            **shared,
            "execution_mode": "single_leg",
            "use_skew_adjust": True,
            "use_ess_tightening": True,
            "use_sell_skew_adjust": True,
            "use_uncertainty_discount": True,
        },
        {
            **shared,
            "execution_mode": "ladder_v1",
            "buy_leg_count": 2,
            "sell_leg_count": 2,
            "use_skew_adjust": True,
            "use_ess_tightening": True,
        },
    ]


def _date_groups(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    all_dates = sorted({str(row.get("decision_date") or "") for row in rows if row.get("decision_date")})
    if not all_dates:
        return []
    pivot_dates = sorted(
        {
            str(row.get("decision_date") or "")
            for row in rows
            if row.get("decision_date") and str(row.get("side") or "") == "BUY"
        }
    ) or all_dates
    fold_count = min(3, len(pivot_dates))
    pivot_groups: list[list[str]] = [[] for _ in range(fold_count)]
    for idx, decision_date in enumerate(pivot_dates):
        pivot_groups[min(idx * fold_count // len(pivot_dates), fold_count - 1)].append(decision_date)
    pivot_groups = [group for group in pivot_groups if group]
    windows: list[dict[str, Any]] = []
    for idx, group in enumerate(pivot_groups):
        start_date = group[0]
        if idx + 1 < len(pivot_groups):
            next_start = pivot_groups[idx + 1][0]
            fold_dates = [decision_date for decision_date in all_dates if start_date <= decision_date < next_start]
        else:
            fold_dates = [decision_date for decision_date in all_dates if decision_date >= start_date]
        if not fold_dates:
            continue
        windows.append(
            {
                "dates": fold_dates,
                "start_date": fold_dates[0],
                "end_date": fold_dates[-1],
            }
        )
    return windows


def _row_bar(row: Mapping[str, Any]) -> HistoricalBar | None:
    session_date = str(row.get("execution_date") or "")
    if not session_date or row.get("d1_open") in (None, ""):
        return None
    return HistoricalBar(
        symbol=str(row.get("symbol") or ""),
        timestamp=session_date,
        open=_to_float(row.get("d1_open")),
        high=_to_float(row.get("d1_high")),
        low=_to_float(row.get("d1_low")),
        close=_to_float(row.get("d1_close")),
        volume=1.0,
    )


def _leg_quantities(total_qty: int, n_legs: int, alpha: float) -> list[int]:
    if total_qty <= 0:
        return []
    n_legs = max(1, min(int(n_legs), total_qty))
    raw = [math.exp(-float(alpha) * idx / max(1, n_legs - 1)) for idx in range(n_legs)]
    total = sum(raw) or 1.0
    base = [int(total_qty * (weight / total)) for weight in raw]
    assigned = sum(base)
    for idx in range(total_qty - assigned):
        base[idx % len(base)] += 1
    base = [qty for qty in base if qty > 0]
    return base or [total_qty]


def _interp_steps(first: float, last: float, count: int) -> list[float]:
    if count <= 1:
        return [first]
    return [first + (last - first) * (idx / max(1, count - 1)) for idx in range(count)]


def _distribution_buy_offset(row: Mapping[str, Any], params: Mapping[str, Any]) -> float:
    """Compute buy entry offset from distribution stats (returns positive value to subtract from open)."""
    q10 = _to_float(row.get("q10_return"))
    lb = _to_float(row.get("lower_bound"))
    q50 = _to_float(row.get("q50_return"))
    interval_width = _to_float(row.get("interval_width"))
    uncertainty = _to_float(row.get("uncertainty"))
    ess = _to_float(row.get("member_mixture_ess"))

    alpha = _to_float(params.get("buy_dist_blend"), 0.5)
    raw = alpha * q10 + (1.0 - alpha) * lb

    if params.get("use_skew_adjust") in (True, "true", "True"):
        lower_spread = q50 - q10
        dampener = _to_float(params.get("skew_dampener"), 0.2)
        width = max(interval_width, 1e-6)
        raw *= (1.0 + dampener * (lower_spread / width - 0.5))

    if params.get("use_ess_tightening") in (True, "true", "True"):
        ess_cap = _to_float(params.get("ess_cap"), 20.0)
        tighten = _to_float(params.get("tighten_ratio"), 0.3)
        confidence = min(math.log1p(max(ess, 0)) / max(math.log1p(ess_cap), 1e-6), 1.0)
        raw *= (1.0 - confidence * tighten)

    return _clip(-raw, 0.001, 0.15)


def _distribution_sell_markup(row: Mapping[str, Any], params: Mapping[str, Any]) -> float:
    """Compute sell markup from distribution stats."""
    q50 = _to_float(row.get("q50_return"))
    q90 = _to_float(row.get("q90_return"))
    q10 = _to_float(row.get("q10_return"))
    uncertainty = _to_float(row.get("uncertainty"))
    fallback = _to_float(params.get("fallback_min_sell_markup"), 0.003)

    beta = _to_float(params.get("sell_dist_blend"), 0.5)
    raw = beta * max(q90, 0.0) + (1.0 - beta) * max(q50, 0.0)

    if raw <= 0:
        return fallback

    if params.get("use_sell_skew_adjust") in (True, "true", "True"):
        lower_spread = q50 - q10
        upper_spread = q90 - q50
        skew_ratio = upper_spread / max(lower_spread, 1e-6)
        floor = _to_float(params.get("sell_skew_floor"), 0.5)
        ceil = _to_float(params.get("sell_skew_ceil"), 1.5)
        raw *= _clip(skew_ratio, floor, ceil)

    if params.get("use_uncertainty_discount") in (True, "true", "True"):
        unc_w = _to_float(params.get("sell_unc_weight"), 0.5)
        raw -= unc_w * uncertainty

    return max(raw, fallback)


def _build_limit_legs(
    *,
    side: str,
    symbol: str,
    market: str,
    reference_price: float,
    total_quantity: int,
    params: Mapping[str, Any],
) -> list[LadderLeg]:
    mode = str(params.get("execution_mode") or "single_leg")
    if total_quantity <= 0 or reference_price <= 0:
        return []
    if mode == "ladder_v1":
        if side == "BUY":
            leg_count = _to_int(params.get("buy_leg_count"), 2)
            first_offset = _to_float(params.get("_dist_buy_offset"), 0.01)
            spread_ratio = _to_float(params.get("buy_leg_spread_ratio"), 1.5)
            last_offset = max(first_offset * spread_ratio, first_offset)
            alpha = _to_float(params.get("buy_leg_weight_alpha"), 1.25)
            offsets = _interp_steps(first_offset, last_offset, leg_count)
            quantities = _leg_quantities(total_quantity, leg_count, alpha)
            prices = [round_to_tick(reference_price * (1.0 - offset), market, side="BUY") for offset in offsets[: len(quantities)]]
        else:
            leg_count = _to_int(params.get("sell_leg_count"), 2)
            first_markup = _to_float(params.get("_dist_sell_markup"), 0.02)
            spread_ratio = _to_float(params.get("sell_leg_spread_ratio"), 1.5)
            last_markup = max(first_markup * spread_ratio, first_markup)
            alpha = _to_float(params.get("sell_leg_weight_alpha"), 1.25)
            offsets = _interp_steps(first_markup, last_markup, leg_count)
            quantities = _leg_quantities(total_quantity, leg_count, alpha)
            prices = [round_to_tick(reference_price * (1.0 + offset), market, side="SELL") for offset in offsets[: len(quantities)]]
    else:
        quantities = [total_quantity]
        if side == "BUY":
            offset = _to_float(params.get("_dist_buy_offset"), 0.01)
            prices = [round_to_tick(reference_price * (1.0 - offset), market, side="BUY")]
        else:
            markup = _to_float(params.get("_dist_sell_markup"), 0.02)
            prices = [round_to_tick(reference_price * (1.0 + markup), market, side="SELL")]
    return [
        LadderLeg(
            leg_id=f"{symbol.lower()}-{side.lower()}-{idx + 1}",
            side=Side[side],
            order_type=OrderType.LIMIT,
            quantity=int(qty),
            limit_price=float(prices[idx]),
            metadata={"execution_mode": mode},
        )
        for idx, qty in enumerate(quantities)
        if idx < len(prices) and qty > 0
    ]


def _policy_edge_score(row: Mapping[str, Any], params: Mapping[str, Any]) -> float:
    return (
        _to_float(params.get("w_lb"), 1.0) * _to_float(row.get("lower_bound"))
        + _to_float(params.get("w_q50"), 1.0) * _to_float(row.get("q50_return"))
        - _to_float(params.get("w_width"), 1.0) * _to_float(row.get("interval_width"))
        - _to_float(params.get("w_uncertainty"), 1.0) * _to_float(row.get("uncertainty"))
        + _to_float(params.get("w_ess"), 1.0) * math.log1p(max(_to_float(row.get("member_mixture_ess")), 0.0))
    )


def _buy_rows_for_date(rows: Sequence[Mapping[str, Any]], params: Mapping[str, Any], held_symbols: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if str(row.get("side") or "") != "BUY":
            continue
        if row.get("symbol") in held_symbols:
            continue
        score = _policy_edge_score(row, params)
        row["policy_edge_score"] = score
        if score < _to_float(params.get("min_buy_score"), 0.0):
            continue
        if _to_float(row.get("lower_bound")) < _to_float(params.get("min_lower_bound"), 0.0):
            continue
        if _to_float(row.get("member_mixture_ess")) < _to_float(params.get("min_member_ess"), 1.0):
            continue
        out.append(row)
    out.sort(key=lambda item: (-_to_float(item.get("policy_edge_score")), -_to_float(item.get("q50_return")), str(item.get("symbol") or "")))
    return out


def _buy_plan_from_row(row: Mapping[str, Any], *, name_budget: float, params: Mapping[str, Any]) -> OrderPlan | None:
    market = str(row.get("market") or "US")
    reference_price = _to_float(row.get("t1_open"))
    if reference_price <= 0 or name_budget <= 0:
        return None
    dist_offset = _distribution_buy_offset(row, params)
    max_price = reference_price * (1.0 - dist_offset)
    max_price = max(0.01, max_price)
    total_qty = max(0, int(name_budget // max_price))
    if total_qty <= 0:
        return None
    effective_params = dict(params)
    effective_params["_dist_buy_offset"] = dist_offset
    legs = _build_limit_legs(side="BUY", symbol=str(row.get("symbol") or ""), market=market, reference_price=reference_price, total_quantity=total_qty, params=effective_params)
    if not legs:
        return None
    return OrderPlan(
        plan_id=f"{row.get('decision_date')}::{row.get('symbol')}::BUY::{params.get('execution_mode', 'single_leg')}",
        symbol=str(row.get("symbol") or ""),
        ticker_id=None,
        side=Side.BUY,
        generated_at=datetime.now(timezone.utc),
        status="READY",
        rationale="frozen_seed_buy",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=float(name_budget),
        requested_quantity=total_qty,
        legs=legs,
        metadata={"decision_date": row.get("decision_date"), "execution_mode": params.get("execution_mode", "single_leg"), "policy_edge_score": row.get("policy_edge_score")},
    )


def _sell_markup(row: Mapping[str, Any], params: Mapping[str, Any]) -> float:
    return _distribution_sell_markup(row, params)


def _sell_plan_from_row(row: Mapping[str, Any], *, quantity: int, params: Mapping[str, Any]) -> OrderPlan | None:
    reference_price = _to_float(row.get("t1_open"))
    if quantity <= 0 or reference_price <= 0:
        return None
    mode = str(params.get("execution_mode") or "single_leg")
    effective_params = dict(params)
    effective_params["_dist_sell_markup"] = _sell_markup(row, params)
    legs = _build_limit_legs(side="SELL", symbol=str(row.get("symbol") or ""), market=str(row.get("market") or "US"), reference_price=reference_price, total_quantity=quantity, params=effective_params)
    if not legs:
        return None
    return OrderPlan(
        plan_id=f"{row.get('decision_date')}::{row.get('symbol')}::SELL::{mode}",
        symbol=str(row.get("symbol") or ""),
        ticker_id=None,
        side=Side.SELL,
        generated_at=datetime.now(timezone.utc),
        status="READY",
        rationale="frozen_seed_sell",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=None,
        requested_quantity=quantity,
        legs=legs,
        metadata={"decision_date": row.get("decision_date"), "execution_mode": mode},
    )


def _simulate_plan(plan: OrderPlan, row: Mapping[str, Any], broker: SimulatedBroker):
    bar = _row_bar(row)
    if bar is None:
        return []
    return broker.simulate_plan(plan, [bar])


def _weighted_average_fill(fills: Sequence[Any]) -> float:
    qty = sum(float(getattr(fill, "filled_quantity", 0.0) or 0.0) for fill in fills if getattr(fill, "fill_status", None) in {FillStatus.FULL, FillStatus.PARTIAL})
    if qty <= 0:
        return 0.0
    return sum(float(getattr(fill, "average_fill_price", 0.0) or 0.0) * float(getattr(fill, "filled_quantity", 0.0) or 0.0) for fill in fills if getattr(fill, "fill_status", None) in {FillStatus.FULL, FillStatus.PARTIAL}) / qty


def _mark_price_for_date(rows: Sequence[Mapping[str, Any]], symbol: str, decision_date: str, positions: Mapping[str, Any]) -> float:
    candidates = [row for row in rows if str(row.get("decision_date") or "") == decision_date and str(row.get("symbol") or "") == symbol]
    for row in candidates:
        close = _to_float(row.get("d1_close"))
        if close > 0:
            return close
    position = positions.get(symbol) or {}
    return _to_float(position.get("last_mark"), _to_float(position.get("avg_price")))


def simulate_frozen_seed_trial(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    params: Mapping[str, Any],
    initial_capital: float,
    objective_cfg,
) -> dict[str, Any]:
    rows = [dict(row) for row in seed_rows if isinstance(row, Mapping)]
    decision_dates = sorted({str(row.get("decision_date") or "") for row in rows if row.get("decision_date")})
    rows_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_date[str(row.get("decision_date") or "")].append(row)
    broker = SimulatedBroker(rules=SimulationRules())
    cash = float(initial_capital)
    positions: dict[str, dict[str, Any]] = {}
    equity_path: list[float] = []
    idle_cash_ratios: list[float] = []
    max_concentration = 0.0
    trade_count = 0
    sell_fill_count = 0
    plan_count = 0
    fill_count = 0
    day_summaries: list[dict[str, Any]] = []
    for decision_date in decision_dates:
        today_rows = rows_by_date.get(decision_date, [])
        for symbol in sorted(list(positions.keys())):
            position = positions.get(symbol) or {}
            sell_row = next((row for row in today_rows if row.get("symbol") == symbol and row.get("side") == "SELL"), None)
            if not sell_row:
                continue
            plan = _sell_plan_from_row(sell_row, quantity=_to_int(position.get("quantity")), params=params)
            if not plan:
                continue
            plan_count += 1
            fills = _simulate_plan(plan, sell_row, broker)
            fill_rows = [fill for fill in fills if getattr(fill, "fill_status", None) in {FillStatus.FULL, FillStatus.PARTIAL} and float(getattr(fill, "filled_quantity", 0.0) or 0.0) > 0.0]
            if fill_rows:
                trade_count += 1
                sell_fill_count += 1
                fill_count += len(fill_rows)
                sold_qty = sum(float(fill.filled_quantity or 0.0) for fill in fill_rows)
                cash += sum(float(fill.average_fill_price or 0.0) * float(fill.filled_quantity or 0.0) for fill in fill_rows)
                remaining = max(0.0, float(position.get("quantity") or 0.0) - sold_qty)
                if remaining <= 0:
                    positions.pop(symbol, None)
                else:
                    position["quantity"] = remaining
                    position["last_mark"] = _to_float(sell_row.get("d1_close"), _to_float(position.get("last_mark")))
        held_symbols = set(positions)
        ranked_buys = _buy_rows_for_date(today_rows, params, held_symbols)
        daily_budget_remaining = cash * _to_float(params.get("buy_budget_fraction"), 1.0)
        total_equity_for_cap = cash + sum(float(pos.get("quantity") or 0.0) * float(pos.get("last_mark") or pos.get("avg_price") or 0.0) for pos in positions.values())
        buys_done = 0
        for row in ranked_buys:
            if buys_done >= _to_int(params.get("max_new_buys"), 3):
                break
            per_name_cap = max(0.0, total_equity_for_cap * _to_float(params.get("per_name_cap_fraction"), 0.20))
            name_budget = min(daily_budget_remaining, per_name_cap)
            if name_budget <= 0:
                break
            plan = _buy_plan_from_row(row, name_budget=name_budget, params=params)
            if not plan:
                continue
            buys_done += 1
            daily_budget_remaining -= name_budget
            plan_count += 1
            fills = _simulate_plan(plan, row, broker)
            fill_rows = [fill for fill in fills if getattr(fill, "fill_status", None) in {FillStatus.FULL, FillStatus.PARTIAL} and float(getattr(fill, "filled_quantity", 0.0) or 0.0) > 0.0]
            if fill_rows:
                trade_count += 1
                fill_count += len(fill_rows)
                bought_qty = sum(float(fill.filled_quantity or 0.0) for fill in fill_rows)
                avg_fill = _weighted_average_fill(fill_rows)
                spend = sum(float(fill.average_fill_price or 0.0) * float(fill.filled_quantity or 0.0) for fill in fill_rows)
                cash -= spend
                positions[row["symbol"]] = {
                    "quantity": float(positions.get(row["symbol"], {}).get("quantity") or 0.0) + bought_qty,
                    "avg_price": avg_fill,
                    "last_mark": _to_float(row.get("d1_close"), avg_fill),
                }
        equity = cash
        concentration_candidates = []
        for symbol, position in positions.items():
            mark = _mark_price_for_date(rows, symbol, decision_date, positions)
            position["last_mark"] = mark
            value = float(position.get("quantity") or 0.0) * mark
            concentration_candidates.append(value)
            equity += value
        if equity > 0:
            max_concentration = max(max_concentration, (max(concentration_candidates) / equity) if concentration_candidates else 0.0)
            idle_cash_ratios.append(cash / equity)
        equity_path.append(equity)
        day_summaries.append({"decision_date": decision_date, "equity": equity, "cash": cash, "open_positions": len(positions)})
    final_equity = equity_path[-1] if equity_path else float(initial_capital)
    peak = 0.0
    max_drawdown = 0.0
    for equity in equity_path:
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    mean_idle_cash_ratio = sum(idle_cash_ratios) / max(len(idle_cash_ratios), 1)
    feasible = trade_count >= int(getattr(objective_cfg, "min_trade_count", 0)) and sell_fill_count >= int(getattr(objective_cfg, "min_sell_fill_count", 0))
    score = (final_equity / max(float(initial_capital), 1e-9))
    score -= float(getattr(objective_cfg, "lambda_drawdown", 0.0)) * max(0.0, max_drawdown - float(getattr(objective_cfg, "allowed_drawdown", 1.0)))
    score -= float(getattr(objective_cfg, "lambda_idle_cash", 0.0)) * mean_idle_cash_ratio
    score -= float(getattr(objective_cfg, "lambda_concentration", 0.0)) * max(0.0, max_concentration - float(getattr(objective_cfg, "concentration_cap", 1.0)))
    if not feasible:
        score = -1e9
    return {
        "objective": score,
        "final_equity": final_equity,
        "final_equity_ratio": final_equity / max(float(initial_capital), 1e-9),
        "max_drawdown": max_drawdown,
        "mean_idle_cash_ratio": mean_idle_cash_ratio,
        "position_concentration": max_concentration,
        "trade_count": trade_count,
        "sell_fill_count": sell_fill_count,
        "plan_count": plan_count,
        "fill_count": fill_count,
        "feasible": feasible,
        "day_summaries": day_summaries,
    }


def evaluate_frozen_seed_params(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    params: Mapping[str, Any],
    initial_capital: float,
    objective_cfg,
) -> dict[str, Any]:
    groups = _date_groups(seed_rows)
    folds = []
    for idx, group in enumerate(groups, start=1):
        allowed_dates = set(group.get("dates") or [])
        fold_rows = [row for row in seed_rows if str(row.get("decision_date") or "") in allowed_dates]
        fold_result = simulate_frozen_seed_trial(seed_rows=fold_rows, params=params, initial_capital=initial_capital, objective_cfg=objective_cfg)
        fold_result["fold_index"] = idx
        fold_result["start_date"] = group.get("start_date")
        fold_result["end_date"] = group.get("end_date")
        folds.append(fold_result)
    aggregate = {
        "final_equity_ratio_mean": sum(_to_float(fold.get("final_equity_ratio")) for fold in folds) / max(len(folds), 1),
        "max_drawdown": max((_to_float(fold.get("max_drawdown")) for fold in folds), default=0.0),
        "mean_idle_cash_ratio": sum(_to_float(fold.get("mean_idle_cash_ratio")) for fold in folds) / max(len(folds), 1),
        "position_concentration": max((_to_float(fold.get("position_concentration")) for fold in folds), default=0.0),
        "trade_count": sum(_to_int(fold.get("trade_count")) for fold in folds),
        "sell_fill_count": sum(_to_int(fold.get("sell_fill_count")) for fold in folds),
        "feasible": all(bool(fold.get("feasible")) for fold in folds) if folds else False,
    }
    objective = aggregate["final_equity_ratio_mean"]
    objective -= float(getattr(objective_cfg, "lambda_drawdown", 0.0)) * max(0.0, aggregate["max_drawdown"] - float(getattr(objective_cfg, "allowed_drawdown", 1.0)))
    objective -= float(getattr(objective_cfg, "lambda_idle_cash", 0.0)) * aggregate["mean_idle_cash_ratio"]
    objective -= float(getattr(objective_cfg, "lambda_concentration", 0.0)) * max(0.0, aggregate["position_concentration"] - float(getattr(objective_cfg, "concentration_cap", 1.0)))
    if not aggregate["feasible"]:
        objective = -1e9
    aggregate["objective"] = objective
    return {"folds": folds, "aggregate": aggregate}


def evaluate_frozen_seed_params_from_cache(
    *,
    study_cache_root: str,
    params: Mapping[str, Any],
    initial_capital: float,
    objective_cfg,
) -> dict[str, Any]:
    loaded = load_study_cache_manifest(study_cache_root)
    if not loaded:
        raise FileNotFoundError(f"study cache not found under {study_cache_root}")
    manifest = dict(loaded.get("manifest") or {})
    folds = []
    for fold in manifest.get("folds") or []:
        fold_path = Path(str(fold.get("path") or ""))
        if not fold_path.exists():
            raise FileNotFoundError(f"study cache fold parquet missing: {fold_path}")
        try:
            import pandas as pd
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("pandas is required to load study cache") from exc
        fold_rows = pd.read_parquet(fold_path).to_dict(orient="records")
        fold_result = simulate_frozen_seed_trial(
            seed_rows=fold_rows,
            params=params,
            initial_capital=initial_capital,
            objective_cfg=objective_cfg,
        )
        fold_result["fold_index"] = int(fold.get("fold_index") or len(folds) + 1)
        fold_result["start_date"] = fold.get("start_date")
        fold_result["end_date"] = fold.get("end_date")
        folds.append(fold_result)
    aggregate = {
        "final_equity_ratio_mean": sum(_to_float(fold.get("final_equity_ratio")) for fold in folds) / max(len(folds), 1),
        "max_drawdown": max((_to_float(fold.get("max_drawdown")) for fold in folds), default=0.0),
        "mean_idle_cash_ratio": sum(_to_float(fold.get("mean_idle_cash_ratio")) for fold in folds) / max(len(folds), 1),
        "position_concentration": max((_to_float(fold.get("position_concentration")) for fold in folds), default=0.0),
        "trade_count": sum(_to_int(fold.get("trade_count")) for fold in folds),
        "sell_fill_count": sum(_to_int(fold.get("sell_fill_count")) for fold in folds),
        "feasible": all(bool(fold.get("feasible")) for fold in folds) if folds else False,
    }
    objective = aggregate["final_equity_ratio_mean"]
    objective -= float(getattr(objective_cfg, "lambda_drawdown", 0.0)) * max(0.0, aggregate["max_drawdown"] - float(getattr(objective_cfg, "allowed_drawdown", 1.0)))
    objective -= float(getattr(objective_cfg, "lambda_idle_cash", 0.0)) * aggregate["mean_idle_cash_ratio"]
    objective -= float(getattr(objective_cfg, "lambda_concentration", 0.0)) * max(0.0, aggregate["position_concentration"] - float(getattr(objective_cfg, "concentration_cap", 1.0)))
    if not aggregate["feasible"]:
        objective = -1e9
    aggregate["objective"] = objective
    return {"folds": folds, "aggregate": aggregate, "manifest": manifest}


def summarize_execution_mode_comparison(trials: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    feasible_trials = [dict(trial) for trial in trials if bool(trial.get("feasible"))]
    best_by_mode: dict[str, dict[str, Any]] = {}
    for trial in feasible_trials:
        mode = str((trial.get("params") or {}).get("execution_mode") or "unknown")
        current = best_by_mode.get(mode)
        if current is None or _to_float(trial.get("objective")) > _to_float(current.get("objective")):
            best_by_mode[mode] = trial
    single = best_by_mode.get("single_leg")
    ladder = best_by_mode.get("ladder_v1")
    promotion = {
        "recommended_mode": "single_leg",
        "ladder_v1_promoted": False,
        "reason": "single_leg baseline retained",
    }
    if single and ladder:
        single_folds = list(single.get("fold_metrics") or [])
        ladder_folds = list(ladder.get("fold_metrics") or [])
        ladder_wins = sum(
            1
            for single_fold, ladder_fold in zip(single_folds, ladder_folds)
            if _to_float(ladder_fold.get("final_equity_ratio")) > _to_float(single_fold.get("final_equity_ratio"))
        )
        uplift = 0.0
        if single_folds and ladder_folds:
            single_sorted = sorted(_to_float(fold.get("final_equity_ratio")) for fold in single_folds)
            ladder_sorted = sorted(_to_float(fold.get("final_equity_ratio")) for fold in ladder_folds)
            uplift = ladder_sorted[len(ladder_sorted) // 2] - single_sorted[len(single_sorted) // 2]
        drawdown_diff = _to_float((ladder.get("aggregate") or {}).get("max_drawdown")) - _to_float((single.get("aggregate") or {}).get("max_drawdown"))
        if ladder_wins >= 2 and uplift >= 0.01 and drawdown_diff <= 0.02:
            promotion = {
                "recommended_mode": "ladder_v1",
                "ladder_v1_promoted": True,
                "reason": "ladder_v1 beat single_leg in at least two folds with >=1% median equity uplift and <=2%p drawdown degradation.",
                "ladder_fold_wins": ladder_wins,
                "ladder_median_uplift": uplift,
                "ladder_drawdown_diff": drawdown_diff,
            }
        else:
            promotion = {
                "recommended_mode": "single_leg",
                "ladder_v1_promoted": False,
                "reason": "ladder_v1 did not clear the fold/uplift/drawdown promotion thresholds.",
                "ladder_fold_wins": ladder_wins,
                "ladder_median_uplift": uplift,
                "ladder_drawdown_diff": drawdown_diff,
            }
    return {
        "best_by_mode": {
            mode: {
                "trial_number": trial.get("trial_number"),
                "objective": trial.get("objective"),
                "aggregate": trial.get("aggregate"),
                "params": trial.get("params"),
            }
            for mode, trial in best_by_mode.items()
        },
        "promotion": promotion,
    }


def build_preopen_signal_snapshot(
    *,
    seed_rows: Sequence[Mapping[str, Any]],
    as_of_date: str,
    policy_params: Mapping[str, Any],
    available_cash: float,
    holdings: Sequence[Mapping[str, Any]] | None = None,
    policy_scope: str = "directional_wide_only",
    seed_profile: str = CALIBRATION_UNIVERSE_SEED_PROFILE,
    seed_filter: str = "",
) -> dict[str, Any]:
    raw_rows = [dict(row) for row in seed_rows]
    filtered_rows = filter_optuna_seed_rows(
        seed_rows=seed_rows,
        policy_scope=policy_scope,
        seed_profile=seed_profile,
        seed_filter=seed_filter,
    )
    rows = [dict(row) for row in filtered_rows if str(row.get("decision_date") or "") == as_of_date]
    raw_rows_for_date = [dict(row) for row in raw_rows if str(row.get("decision_date") or "") == as_of_date]
    holdings_map = {
        str(item.get("symbol") or ""): {
            "quantity": _to_float(item.get("quantity")),
            "avg_price": _to_float(item.get("avg_price")),
        }
        for item in (holdings or [])
        if item.get("symbol")
    }
    held_symbols = set(holdings_map)
    buy_rows = _buy_rows_for_date(rows, policy_params, held_symbols)
    buy_rows = buy_rows[: max(0, _to_int(policy_params.get("max_new_buys"), len(buy_rows)))]
    daily_budget_remaining = float(available_cash) * _to_float(policy_params.get("buy_budget_fraction"), 1.0)
    per_name_cap = float(available_cash) * _to_float(policy_params.get("per_name_cap_fraction"), 0.20)
    snapshot_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(buy_rows, start=1):
        name_budget = min(daily_budget_remaining, per_name_cap)
        if name_budget <= 0:
            break
        plan = _buy_plan_from_row(row, name_budget=name_budget, params=policy_params)
        if not plan:
            continue
        daily_budget_remaining -= name_budget
        snapshot_rows.append(
            {
                "as_of_date": as_of_date,
                "symbol": row.get("symbol"),
                "side": "BUY",
                "policy_family": row.get("policy_family"),
                "pattern_key": row.get("pattern_key"),
                "execution_mode": policy_params.get("execution_mode", "single_leg"),
                "q10_return": row.get("q10_return"),
                "q50_return": row.get("q50_return"),
                "q90_return": row.get("q90_return"),
                "ess": row.get("member_mixture_ess"),
                "interval_width": row.get("interval_width"),
                "uncertainty": row.get("uncertainty"),
                "policy_edge_score": row.get("policy_edge_score"),
                "buy_rank": rank,
                "buy_limit_prices": json.dumps([leg.limit_price for leg in plan.legs], ensure_ascii=False),
                "buy_leg_quantities": json.dumps([leg.quantity for leg in plan.legs], ensure_ascii=False),
                "sell_limit_prices": json.dumps([], ensure_ascii=False),
                "sell_leg_quantities": json.dumps([], ensure_ascii=False),
                "capital_inserted": name_budget,
                "member_consensus_signature": row.get("member_consensus_signature"),
                "regime_code": row.get("regime_code"),
                "sector_code": row.get("sector_code"),
                "policy_params_version": "frozen_seed_v1",
            }
        )
    for symbol, holding in holdings_map.items():
        sell_row = next((row for row in raw_rows_for_date if row.get("symbol") == symbol and row.get("side") == "SELL"), None)
        if not sell_row:
            continue
        plan = _sell_plan_from_row(sell_row, quantity=max(1, _to_int(holding.get("quantity"), 0)), params=policy_params)
        if not plan:
            continue
        snapshot_rows.append(
            {
                "as_of_date": as_of_date,
                "symbol": symbol,
                "side": "SELL",
                "policy_family": sell_row.get("policy_family"),
                "pattern_key": sell_row.get("pattern_key"),
                "execution_mode": policy_params.get("execution_mode", "single_leg"),
                "q10_return": sell_row.get("q10_return"),
                "q50_return": sell_row.get("q50_return"),
                "q90_return": sell_row.get("q90_return"),
                "ess": sell_row.get("member_mixture_ess"),
                "interval_width": sell_row.get("interval_width"),
                "uncertainty": sell_row.get("uncertainty"),
                "policy_edge_score": None,
                "buy_rank": None,
                "buy_limit_prices": json.dumps([], ensure_ascii=False),
                "buy_leg_quantities": json.dumps([], ensure_ascii=False),
                "sell_limit_prices": json.dumps([leg.limit_price for leg in plan.legs], ensure_ascii=False),
                "sell_leg_quantities": json.dumps([leg.quantity for leg in plan.legs], ensure_ascii=False),
                "capital_inserted": 0.0,
                "member_consensus_signature": sell_row.get("member_consensus_signature"),
                "regime_code": sell_row.get("regime_code"),
                "sector_code": sell_row.get("sector_code"),
                "policy_params_version": "frozen_seed_v1",
            }
        )
    snapshot_rows.sort(key=lambda row: (0 if row["side"] == "BUY" else 1, row.get("buy_rank") or 999, str(row.get("symbol") or "")))
    return {
        "as_of_date": as_of_date,
        "row_count": len(snapshot_rows),
        "snapshot_rows": snapshot_rows,
        "buy_count": sum(1 for row in snapshot_rows if row["side"] == "BUY"),
        "sell_count": sum(1 for row in snapshot_rows if row["side"] == "SELL"),
        "execution_mode": policy_params.get("execution_mode", "single_leg"),
        "seed_profile": seed_profile,
    }


def write_preopen_signal_snapshot_artifacts(*, output_dir: str, snapshot_payload: Mapping[str, Any]) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = list(snapshot_payload.get("snapshot_rows") or [])
    json_path = out_dir / "preopen_signal_snapshot.json"
    parquet_path = out_dir / "preopen_signal_snapshot.parquet"
    json_path.write_text(json.dumps(dict(snapshot_payload), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    parquet_format = "parquet"
    try:
        import pandas as pd
    except Exception:
        parquet_path.write_text(json.dumps({"format": "json_fallback", "rows": rows}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        parquet_format = "json_fallback"
    else:
        pd.DataFrame(rows).to_parquet(parquet_path, index=False)
    return {
        "preopen_signal_snapshot_path": str(parquet_path),
        "preopen_signal_snapshot_format": parquet_format,
        "preopen_signal_snapshot_json_path": str(json_path),
        "row_count": len(rows),
    }
