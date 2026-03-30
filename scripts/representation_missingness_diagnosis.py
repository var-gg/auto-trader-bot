from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.historical_data.features import SIMILARITY_CTX_SERIES
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import BREADTH_POLICY_DIAGNOSTICS_ONLY_V1, _build_query_panel, build_event_memory_asof

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]
DEFAULT_OUTPUT_ROOT = Path("runs") / "representation_missingness_diagnosis"
TOP_N = 5
MACRO_COVERAGE_SCOPE = "similarity_enabled_ctx_series"
VALID_MISSINGNESS_FAMILIES = {"none", "structural_missing", "stale_but_usable", "data_quality_missing"}


def _parse_symbols(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_SYMBOLS)
    return [token.strip().upper() for token in raw.split(",") if token.strip()]


def _latest_common_dates(session_factory, schema: str, symbols: list[str]) -> list[str]:
    sql = text(
        f"""
        SELECT trade_date, COUNT(DISTINCT symbol) AS n
          FROM {schema}.bt_mirror_ohlcv_daily
         WHERE symbol = ANY(:symbols)
         GROUP BY trade_date
         HAVING COUNT(DISTINCT symbol) = :symbol_count
         ORDER BY trade_date
        """
    )
    with session_factory() as session:
        return [str(row._mapping["trade_date"]) for row in session.execute(sql, {"symbols": symbols, "symbol_count": len(symbols)})]


def _resolve_default_window(session_factory, schema: str, symbols: list[str]) -> tuple[str, str]:
    dates = _latest_common_dates(session_factory, schema, symbols)
    if not dates:
        raise RuntimeError("No common OHLCV coverage found for requested symbols")
    first_date = dates[0]
    latest_date = dates[-1]
    latest_dt = datetime.fromisoformat(latest_date).date()
    preferred_start = (latest_dt - timedelta(days=365)).isoformat()
    preferred_disc_end = (latest_dt - timedelta(days=92)).isoformat()
    fallback_start = (latest_dt - timedelta(days=244)).isoformat()
    fallback_disc_end = (latest_dt - timedelta(days=61)).isoformat()
    if preferred_start >= first_date:
        return preferred_start, preferred_disc_end
    return fallback_start, fallback_disc_end


def _trade_dates_in_window(bars_by_symbol: dict[str, list[HistoricalBar]], *, start_date: str, end_date: str) -> list[str]:
    dates = {
        str(bar.timestamp)[:10]
        for bars in bars_by_symbol.values()
        for bar in bars
        if start_date <= str(bar.timestamp)[:10] <= end_date
    }
    return sorted(dates)


def _top_n(values: list[str], *, n: int = TOP_N) -> list[str]:
    out = [str(value) for value in values if str(value)]
    return out[:n]


def compute_feature_coverage_score(
    *,
    raw_zero_default_keys_count: int,
    raw_feature_count: int,
    transform_missing_keys_filled_zero_count: int,
    transform_feature_count: int,
    macro_series_present_count: int,
    macro_series_scope_count: int = len(SIMILARITY_CTX_SERIES),
    sector_proxy_fallback_to_self: bool,
) -> float:
    raw_zero_default_ratio = raw_zero_default_keys_count / max(1, raw_feature_count)
    transform_zero_fill_ratio = transform_missing_keys_filled_zero_count / max(1, transform_feature_count)
    macro_missing_ratio = 1 - (macro_series_present_count / max(1, macro_series_scope_count))
    proxy_fallback_penalty = 0.25 if sector_proxy_fallback_to_self else 0.0
    score = max(0.0, 1 - (0.35 * raw_zero_default_ratio + 0.35 * transform_zero_fill_ratio + 0.20 * macro_missing_ratio + proxy_fallback_penalty))
    return float(score)


def _macro_window(macro_history_by_date: dict[str, dict[str, float]], decision_date: str) -> dict[str, dict[str, float]]:
    return {k: dict(v or {}) for k, v in sorted(macro_history_by_date.items()) if k <= decision_date}


def _latest_macro_payload(macro_window: dict[str, dict[str, float]]) -> dict[str, float]:
    if not macro_window:
        return {}
    latest_date = max(macro_window.keys())
    return dict(macro_window.get(latest_date, {}) or {})


def _timestamp_order_violation(source_ts_utc: str | None, anchor_ts_utc: str | None) -> bool:
    if not source_ts_utc or not anchor_ts_utc:
        return False
    return datetime.fromisoformat(str(source_ts_utc)) > datetime.fromisoformat(str(anchor_ts_utc))


def build_row_record(
    *,
    symbol: str,
    decision_date: str,
    query_item: dict[str, Any] | None,
    query_reasons: list[str],
    library_reasons: list[str],
    macro_window: dict[str, dict[str, float]],
) -> dict[str, Any]:
    latest_macro = _latest_macro_payload(macro_window)
    query_meta = dict((query_item or {}).get("meta") or {})
    proxy_diagnostics = dict(query_meta.get("proxy_diagnostics") or {})
    market_proxy = dict(proxy_diagnostics.get("market") or {})
    sector_proxy = dict(proxy_diagnostics.get("sector") or {})
    market_peer_counts = [int(value) for value in dict(market_proxy.get("peer_count_by_date") or {}).values()]
    raw_zero_default_keys = [str(value) for value in list(query_meta.get("raw_zero_default_keys") or [])]
    transform_missing_keys = [str(value) for value in list(query_meta.get("transform_missing_keys_filled_zero") or [])]
    transformed_zero_keys = [str(value) for value in list(query_meta.get("transformed_zero_feature_keys") or [])]
    raw_feature_count = len(dict(query_meta.get("raw_features") or {}))
    transform_feature_count = len(list(query_meta.get("feature_keys") or []))
    sector_proxy_fallback_to_self = bool(sector_proxy.get("fallback_to_self", False))
    macro_freshness_summary = dict(query_meta.get("macro_freshness_summary") or {})
    relevant_macro_freshness_summary = {
        series_name: dict(macro_freshness_summary.get(series_name) or {})
        for series_name in SIMILARITY_CTX_SERIES
    }
    stale_macro = any(bool((item or {}).get("is_stale_flag", False)) for item in relevant_macro_freshness_summary.values())
    breadth_policy = str(query_meta.get("breadth_policy") or BREADTH_POLICY_DIAGNOSTICS_ONLY_V1)
    breadth_present = bool(query_meta.get("breadth_present", False))
    breadth_missing_reason = query_meta.get("breadth_missing_reason")
    macro_series_scope_count = len(SIMILARITY_CTX_SERIES)
    macro_series_present_count = int(query_meta.get("macro_series_present_count") or sum(1 for key in SIMILARITY_CTX_SERIES if latest_macro.get(key) is not None))
    macro_asof_ts_utc = query_meta.get("macro_asof_ts_utc")
    feature_anchor_ts_utc = query_meta.get("feature_anchor_ts_utc")
    macro_asof_ordering_violation = _timestamp_order_violation(macro_asof_ts_utc, feature_anchor_ts_utc)
    derived_macro_publish_time_present = any(bool((item or {}).get("source_ts_is_derived", False)) for item in relevant_macro_freshness_summary.values())
    feature_coverage_score = compute_feature_coverage_score(
        raw_zero_default_keys_count=len(raw_zero_default_keys),
        raw_feature_count=raw_feature_count,
        transform_missing_keys_filled_zero_count=len(transform_missing_keys),
        transform_feature_count=transform_feature_count,
        macro_series_present_count=macro_series_present_count,
        macro_series_scope_count=macro_series_scope_count,
        sector_proxy_fallback_to_self=sector_proxy_fallback_to_self,
    )
    exclude_reasons = sorted(set(query_reasons + library_reasons))
    structural_missing = bool("insufficient_query_history" in exclude_reasons or "insufficient_bars" in exclude_reasons or "missing_decision_bar" in exclude_reasons)
    data_quality_missing = bool("unknown_exchange_session" in exclude_reasons or query_meta.get("anchor_missing_reason"))
    zero_imputation_ratio = len(transform_missing_keys) / max(1, transform_feature_count)
    return {
        "decision_date": decision_date,
        "symbol": symbol,
        "query_available": bool(query_item),
        "exclude_reasons": exclude_reasons,
        "insufficient_query_history": "insufficient_query_history" in query_reasons,
        "insufficient_bars": "insufficient_bars" in library_reasons,
        "missing_decision_bar": "missing_decision_bar" in query_reasons,
        "sector_proxy_fallback_to_self": sector_proxy_fallback_to_self,
        "market_proxy_peer_count_min": min(market_peer_counts) if market_peer_counts else 0,
        "market_proxy_peer_count_max": max(market_peer_counts) if market_peer_counts else 0,
        "macro_history_length": len(macro_window),
        "macro_series_present_count": macro_series_present_count,
        "macro_series_scope_count": macro_series_scope_count,
        "macro_coverage_scope": MACRO_COVERAGE_SCOPE,
        "raw_feature_count": raw_feature_count,
        "transform_feature_count": transform_feature_count,
        "raw_zero_default_keys_count": len(raw_zero_default_keys),
        "raw_zero_default_keys_top_n": _top_n(raw_zero_default_keys),
        "transform_missing_keys_filled_zero_count": len(transform_missing_keys),
        "transform_missing_keys_filled_zero_top_n": _top_n(transform_missing_keys),
        "zero_imputed_feature_keys": _top_n(transform_missing_keys),
        "zero_imputed_feature_count": len(transform_missing_keys),
        "zero_imputation_ratio": zero_imputation_ratio,
        "transformed_zero_dominant_keys_top_n": _top_n(transformed_zero_keys),
        "exchange_code": query_meta.get("exchange_code"),
        "exchange_tz": query_meta.get("exchange_tz"),
        "session_date_local": query_meta.get("session_date_local"),
        "feature_anchor_ts_utc": feature_anchor_ts_utc,
        "macro_asof_ts_utc": macro_asof_ts_utc,
        "macro_asof_ordering_violation": macro_asof_ordering_violation,
        "proxy_mode": market_proxy.get("proxy_mode"),
        "same_exchange_peer_count": int(market_proxy.get("same_exchange_peer_count") or 0),
        "cross_exchange_proxy_used": bool(market_proxy.get("cross_exchange_proxy_used", False)),
        "stale_macro": stale_macro,
        "breadth_policy": breadth_policy,
        "breadth_present": breadth_present,
        "breadth_missing_reason": breadth_missing_reason,
        "derived_macro_publish_time_present": derived_macro_publish_time_present,
        "structural_missing": structural_missing,
        "data_quality_missing": data_quality_missing,
        "missingness_family": "data_quality_missing" if data_quality_missing else "structural_missing" if structural_missing else "stale_but_usable" if stale_macro else "none",
        "feature_coverage_score": feature_coverage_score,
        "too_sparse": feature_coverage_score < 0.6,
    }


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    exclude_hist = Counter()
    raw_zero_hist = Counter()
    transform_zero_hist = Counter()
    fallback_hist = Counter()
    transformed_zero_hist = Counter()
    sparse_rows = 0
    macro_coverage_sum = 0.0
    sector_peer_rows = 0
    query_available_rows = 0
    structural_missing_rows = 0
    stale_macro_rows = 0
    data_quality_missing_rows = 0
    breadth_missing_rows = 0
    missingness_family_hist = Counter()
    zero_imputation_sum = 0.0
    macro_asof_ordering_violation_rows = 0
    derived_macro_publish_time_rows = 0
    for row in rows:
        if row.get("query_available"):
            query_available_rows += 1
        if row.get("too_sparse"):
            sparse_rows += 1
        if row.get("structural_missing"):
            structural_missing_rows += 1
        if row.get("stale_macro"):
            stale_macro_rows += 1
        if row.get("data_quality_missing"):
            data_quality_missing_rows += 1
        if row.get("breadth_missing_reason") == "canonical_source_missing":
            breadth_missing_rows += 1
        missingness_family_hist[str(row.get("missingness_family") or "unknown")] += 1
        if row.get("macro_asof_ordering_violation"):
            macro_asof_ordering_violation_rows += 1
        if row.get("derived_macro_publish_time_present"):
            derived_macro_publish_time_rows += 1
        macro_coverage_sum += float(row.get("macro_series_present_count", 0)) / max(1.0, float(row.get("macro_series_scope_count", len(SIMILARITY_CTX_SERIES)) or len(SIMILARITY_CTX_SERIES)))
        zero_imputation_sum += float(row.get("zero_imputation_ratio", 0.0) or 0.0)
        if not row.get("sector_proxy_fallback_to_self"):
            sector_peer_rows += 1
        fallback_hist[f"sector_proxy_fallback_to_self={bool(row.get('sector_proxy_fallback_to_self'))}"] += 1
        for reason in row.get("exclude_reasons") or []:
            exclude_hist[str(reason)] += 1
        for key in row.get("raw_zero_default_keys_top_n") or []:
            raw_zero_hist[str(key)] += 1
        for key in row.get("transform_missing_keys_filled_zero_top_n") or []:
            transform_zero_hist[str(key)] += 1
        for key in row.get("transformed_zero_dominant_keys_top_n") or []:
            transformed_zero_hist[str(key)] += 1
    total_rows = len(rows)
    sector_peer_coverage_ratio = sector_peer_rows / max(1, total_rows)
    macro_coverage_ratio = macro_coverage_sum / max(1, total_rows)
    row_level_feature_sparsity_ratio = sparse_rows / max(1, total_rows)
    query_available_ratio = query_available_rows / max(1, total_rows)
    structural_missing_ratio = structural_missing_rows / max(1, total_rows)
    stale_macro_ratio = stale_macro_rows / max(1, total_rows)
    data_quality_missing_ratio = data_quality_missing_rows / max(1, total_rows)
    zero_imputation_ratio = zero_imputation_sum / max(1, total_rows)
    sector_self_fallback_ratio = fallback_hist.get("sector_proxy_fallback_to_self=True", 0) / max(1, total_rows)
    breadth_canonical_missing_ratio = breadth_missing_rows / max(1, total_rows)
    medium_verdict_hold_reasons: list[str] = []
    if sector_self_fallback_ratio > 0.10:
        medium_verdict_hold_reasons.append("sector_self_fallback_ratio_gt_0.10")
    if row_level_feature_sparsity_ratio > 0.10:
        medium_verdict_hold_reasons.append("row_level_feature_sparsity_ratio_gt_0.10")
    if macro_coverage_ratio < 0.95:
        medium_verdict_hold_reasons.append("macro_coverage_ratio_lt_0.95")
    if data_quality_missing_ratio > 0.0:
        medium_verdict_hold_reasons.append("data_quality_missing_ratio_gt_0.00")
    if stale_macro_ratio > 0.10:
        medium_verdict_hold_reasons.append("stale_macro_ratio_gt_0.10")
    medium_verdict_hold_recommended = bool(medium_verdict_hold_reasons)
    return {
        "row_count": total_rows,
        "breadth_policy": BREADTH_POLICY_DIAGNOSTICS_ONLY_V1,
        "query_available_ratio": query_available_ratio,
        "exclude_reason_histogram": dict(exclude_hist),
        "missingness_family_histogram": dict(missingness_family_hist),
        "zero_filled_feature_histogram": {
            "raw_zero_defaults": dict(raw_zero_hist),
            "transform_missing_keys_filled_zero": dict(transform_zero_hist),
            "transformed_zero_dominant_keys": dict(transformed_zero_hist),
        },
        "fallback_histogram": dict(fallback_hist),
        "sector_peer_coverage_ratio": sector_peer_coverage_ratio,
        "macro_coverage_ratio": macro_coverage_ratio,
        "macro_coverage_scope": MACRO_COVERAGE_SCOPE,
        "macro_series_scope_count": len(SIMILARITY_CTX_SERIES),
        "row_level_feature_sparsity_ratio": row_level_feature_sparsity_ratio,
        "structural_missing_ratio": structural_missing_ratio,
        "stale_macro_ratio": stale_macro_ratio,
        "data_quality_missing_ratio": data_quality_missing_ratio,
        "zero_imputation_ratio": zero_imputation_ratio,
        "sector_self_fallback_ratio": sector_self_fallback_ratio,
        "breadth_canonical_missing_ratio": breadth_canonical_missing_ratio,
        "breadth_blocking": False,
        "query_macro_asof_ordering_violation_count": macro_asof_ordering_violation_rows,
        "derived_macro_publish_time_ratio": derived_macro_publish_time_rows / max(1, total_rows),
        "too_sparse_row_count": sparse_rows,
        "medium_verdict_hold_recommended": medium_verdict_hold_recommended,
        "medium_verdict_hold_reasons": medium_verdict_hold_reasons,
    }


def render_report(summary: dict[str, Any]) -> str:
    exclude_hist = summary.get("exclude_reason_histogram") or {}
    fallback_hist = summary.get("fallback_histogram") or {}
    zero_hist = (summary.get("zero_filled_feature_histogram") or {}).get("raw_zero_defaults") or {}
    hold_reasons = ", ".join(summary.get("medium_verdict_hold_reasons") or []) or "none"
    top_excludes = ", ".join(f"{k}={v}" for k, v in sorted(exclude_hist.items(), key=lambda item: (-item[1], item[0]))[:5]) or "none"
    top_zero_keys = ", ".join(f"{k}={v}" for k, v in sorted(zero_hist.items(), key=lambda item: (-item[1], item[0]))[:5]) or "none"
    lines = [
        "# Representation Missingness Diagnosis",
        "",
        f"- breadth_policy: {summary.get('breadth_policy')}",
        f"- macro_coverage_scope: {summary.get('macro_coverage_scope')}",
        f"- rows: {summary.get('row_count', 0)}",
        f"- query_available_ratio: {summary.get('query_available_ratio', 0.0):.3f}",
        f"- sector_peer_coverage_ratio: {summary.get('sector_peer_coverage_ratio', 0.0):.3f}",
        f"- macro_coverage_ratio: {summary.get('macro_coverage_ratio', 0.0):.3f}",
        f"- row_level_feature_sparsity_ratio: {summary.get('row_level_feature_sparsity_ratio', 0.0):.3f}",
        f"- structural_missing_ratio: {summary.get('structural_missing_ratio', 0.0):.3f}",
        f"- stale_macro_ratio: {summary.get('stale_macro_ratio', 0.0):.3f}",
        f"- data_quality_missing_ratio: {summary.get('data_quality_missing_ratio', 0.0):.3f}",
        f"- zero_imputation_ratio: {summary.get('zero_imputation_ratio', 0.0):.3f}",
        f"- sector_self_fallback_ratio: {summary.get('sector_self_fallback_ratio', 0.0):.3f}",
        f"- breadth_canonical_missing_ratio: {summary.get('breadth_canonical_missing_ratio', 0.0):.3f}",
        f"- breadth_blocking: {bool(summary.get('breadth_blocking', False))}",
        f"- medium_verdict_hold_recommended: {bool(summary.get('medium_verdict_hold_recommended', False))}",
        f"- medium_verdict_hold_reasons: {hold_reasons}",
        f"- representation_landed: {bool(summary.get('representation_landed', False))}",
        "",
        f"- top exclude reasons: {top_excludes}",
        f"- fallback histogram: {json.dumps(fallback_hist, ensure_ascii=False, sort_keys=True)}",
        f"- top raw zero-default keys: {top_zero_keys}",
        "",
        "Breadth is policy-disabled for v1 and remains diagnostics-only; non-breadth residual risks stay visible in this report.",
    ]
    if summary.get("checkpoint_sha"):
        lines.insert(2, f"- checkpoint_sha: {summary.get('checkpoint_sha')}")
    if summary.get("landing_sha"):
        lines.insert(3 if summary.get("checkpoint_sha") else 2, f"- landing_sha: {summary.get('landing_sha')}")
    return "\n".join(lines) + "\n"


def render_landing_review(summary: dict[str, Any]) -> str:
    contract_checks = dict(summary.get("contract_checks") or {})
    residual_risks = list(summary.get("residual_risks") or [])
    lines = [
        "# Representation Landing Review",
        "",
        f"- checkpoint_sha: {summary.get('checkpoint_sha') or 'unknown'}",
        f"- landing_sha: {summary.get('landing_sha') or 'unknown'}",
        f"- breadth_policy: {summary.get('breadth_policy')}",
        f"- representation_landed: {bool(summary.get('representation_landed', False))}",
        f"- breadth_blocking: {bool(summary.get('breadth_blocking', False))}",
        "",
        "## Contract Checks",
        "",
        f"- session_anchor_semantics_consistent: {bool(contract_checks.get('session_anchor_semantics_consistent', False))}",
        f"- macro_attach_respects_anchor_rule: {bool(contract_checks.get('macro_attach_respects_anchor_rule', False))}",
        f"- missingness_taxonomy_partitioned: {bool(contract_checks.get('missingness_taxonomy_partitioned', False))}",
        f"- zero_imputation_explicit: {bool(contract_checks.get('zero_imputation_explicit', False))}",
        f"- breadth_excluded_from_similarity_and_gating: {bool(contract_checks.get('breadth_excluded_from_similarity_and_gating', False))}",
        f"- summary_no_breadth_blocker: {bool(contract_checks.get('summary_no_breadth_blocker', False))}",
        "",
        "## Residual Risks",
        "",
    ]
    if residual_risks:
        lines.extend([f"- {risk}" for risk in residual_risks])
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _finalize_landing_summary(summary: dict[str, Any], contract: dict[str, int]) -> dict[str, Any]:
    missingness_family_keys = set((summary.get("missingness_family_histogram") or {}).keys())
    contract_checks = {
        "session_anchor_semantics_consistent": (
            contract.get("query_rows_total", 0) > 0
            and contract.get("event_records_total", 0) > 0
            and contract.get("prototypes_total", 0) > 0
            and contract.get("query_rows_total", 0) == contract.get("query_rows_with_anchor", 0)
            and contract.get("event_records_total", 0) == contract.get("event_records_with_anchor", 0)
            and contract.get("prototypes_total", 0) == contract.get("prototypes_with_anchor", 0)
        ),
        "macro_attach_respects_anchor_rule": contract.get("query_macro_asof_violations", 0) == 0 and contract.get("event_macro_asof_violations", 0) == 0,
        "missingness_taxonomy_partitioned": missingness_family_keys.issubset(VALID_MISSINGNESS_FAMILIES),
        "zero_imputation_explicit": contract.get("query_rows_total", 0) > 0 and contract.get("query_rows_total", 0) == contract.get("query_rows_with_zero_imputation_fields", 0),
        "breadth_excluded_from_similarity_and_gating": "breadth" not in SIMILARITY_CTX_SERIES and contract.get("query_rows_total", 0) > 0 and contract.get("query_rows_total", 0) == contract.get("query_rows_with_expected_breadth_policy", 0),
        "summary_no_breadth_blocker": not bool(summary.get("breadth_blocking", False)) and not any("breadth" in str(reason) for reason in list(summary.get("medium_verdict_hold_reasons") or [])),
    }
    residual_risks: list[str] = []
    if float(summary.get("sector_self_fallback_ratio", 0.0) or 0.0) > 0.0:
        residual_risks.append("sector self-fallback remains non-zero and can still distort similarity for peer-poor names.")
    if contract.get("rows_with_derived_macro_publish_time", 0) > 0:
        residual_risks.append("derived macro publish timestamps remain in use; as-of ordering is preserved but timestamp provenance stays approximate.")
    summary.update(
        {
            "contract_checks": contract_checks,
            "contract_check_counters": contract,
            "residual_risks": residual_risks,
            "representation_landed": all(contract_checks.values()),
        }
    )
    return summary


def write_outputs(*, rows: list[dict[str, Any]], summary: dict[str, Any], output_root: Path) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    rows_path = output_root / "rows.csv"
    summary_path = output_root / "summary.json"
    report_path = output_root / "report.md"
    landing_review_path = output_root / "landing_review.md"
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with rows_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False)
                    if isinstance(value, (list, dict))
                    else value
                    for key, value in row.items()
                }
            )
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(render_report(summary), encoding="utf-8")
    landing_review_path.write_text(render_landing_review(summary), encoding="utf-8")
    return {
        "rows_path": str(rows_path),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "landing_review_path": str(landing_review_path),
    }


def diagnose_missingness(
    *,
    loader: LocalPostgresLoader,
    symbols: list[str],
    start_date: str,
    end_date: str,
    max_dates: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    spec = ResearchExperimentSpec(
        feature_window_bars=60,
        lookback_horizons=[5],
        horizon_days=5,
        target_return_pct=0.04,
        stop_return_pct=0.03,
        flat_return_band_pct=0.005,
    )
    bars_by_symbol = loader._load_bars(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        warmup_days=max(120, spec.feature_window_bars * 2),
    )
    session_metadata_by_symbol, missing_session_metadata_symbols = loader._load_session_metadata(symbols)
    macro_history_by_date = loader._load_macro_history(
        start_date=start_date,
        end_date=end_date,
        prewarm_days=max(120, spec.feature_window_bars * 2),
    )
    macro_series_history = loader._load_macro_series_history(
        start_date=start_date,
        end_date=end_date,
        prewarm_days=max(120, spec.feature_window_bars * 2),
    )
    sector_map = loader._load_sector_map(symbols)
    decision_dates = _trade_dates_in_window(bars_by_symbol, start_date=start_date, end_date=end_date)
    if max_dates > 0:
        decision_dates = decision_dates[-max_dates:]
    rows: list[dict[str, Any]] = []
    contract = {
        "query_rows_total": 0,
        "query_rows_with_anchor": 0,
        "query_rows_with_zero_imputation_fields": 0,
        "query_rows_with_expected_breadth_policy": 0,
        "query_macro_asof_violations": 0,
        "event_records_total": 0,
        "event_records_with_anchor": 0,
        "event_records_with_expected_breadth_policy": 0,
        "event_macro_asof_violations": 0,
        "prototypes_total": 0,
        "prototypes_with_anchor": 0,
        "rows_with_derived_macro_publish_time": 0,
    }
    for decision_date in decision_dates:
        memory = build_event_memory_asof(
            decision_date=decision_date,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            market="US",
            metadata=None,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
        )
        for record in memory["event_records"]:
            contract["event_records_total"] += 1
            if getattr(record, "feature_anchor_ts_utc", None):
                contract["event_records_with_anchor"] += 1
            if ((getattr(record, "diagnostics", {}) or {}).get("breadth_policy")) == BREADTH_POLICY_DIAGNOSTICS_ONLY_V1:
                contract["event_records_with_expected_breadth_policy"] += 1
            if _timestamp_order_violation(getattr(record, "macro_asof_ts_utc", None), getattr(record, "feature_anchor_ts_utc", None)):
                contract["event_macro_asof_violations"] += 1
        for prototype in memory["prototypes"]:
            contract["prototypes_total"] += 1
            if getattr(prototype, "feature_anchor_ts_utc", None):
                contract["prototypes_with_anchor"] += 1
        query_panel, query_excluded = _build_query_panel(
            decision_dates=[decision_date],
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            scaler=memory["scaler"],
            transform=memory["transform"],
            metadata=None,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
        )
        query_rows = query_panel.get(decision_date, {})
        for query_item in query_rows.values():
            query_meta = dict((query_item or {}).get("meta") or {})
            contract["query_rows_total"] += 1
            if query_meta.get("feature_anchor_ts_utc"):
                contract["query_rows_with_anchor"] += 1
            if "transform_missing_keys_filled_zero" in query_meta:
                contract["query_rows_with_zero_imputation_fields"] += 1
            if str(query_meta.get("breadth_policy") or "") == BREADTH_POLICY_DIAGNOSTICS_ONLY_V1:
                contract["query_rows_with_expected_breadth_policy"] += 1
            if _timestamp_order_violation(query_meta.get("macro_asof_ts_utc"), query_meta.get("feature_anchor_ts_utc")):
                contract["query_macro_asof_violations"] += 1
            freshness_summary = dict(query_meta.get("macro_freshness_summary") or {})
            if any(bool((freshness_summary.get(series_name) or {}).get("source_ts_is_derived", False)) for series_name in SIMILARITY_CTX_SERIES):
                contract["rows_with_derived_macro_publish_time"] += 1
        query_reasons_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in symbols}
        library_reasons_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in symbols}
        for item in query_excluded:
            query_reasons_by_symbol.setdefault(str(item.get("symbol")), []).append(str(item.get("reason") or "unknown"))
        for item in memory["excluded_reasons"]:
            library_reasons_by_symbol.setdefault(str(item.get("symbol")), []).append(str(item.get("reason") or "unknown"))
        for symbol in symbols:
            symbol_dates = {str(bar.timestamp)[:10] for bar in bars_by_symbol.get(symbol, [])}
            if decision_date not in symbol_dates:
                query_reasons_by_symbol.setdefault(symbol, []).append("missing_decision_bar")
            macro_window = _macro_window(macro_history_by_date, decision_date)
            rows.append(
                build_row_record(
                    symbol=symbol,
                    decision_date=decision_date,
                    query_item=query_rows.get(symbol),
                    query_reasons=query_reasons_by_symbol.get(symbol, []),
                    library_reasons=library_reasons_by_symbol.get(symbol, []),
                    macro_window=macro_window,
                )
            )
    summary = summarize_rows(rows)
    summary = _finalize_landing_summary(summary, contract)
    summary.update(
        {
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
            "max_dates": max_dates,
            "discovery_only": True,
            "missing_session_metadata_symbols": missing_session_metadata_symbols,
        }
    )
    return rows, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose representation missingness / feature coverage")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--max-dates", type=int, default=45)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--checkpoint-sha", default="")
    parser.add_argument("--landing-sha", default="")
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)
    loader = LocalPostgresLoader(session_factory=session_factory, schema=cfg.schema)
    start_date = args.start_date
    end_date = args.end_date
    if not start_date or not end_date:
        start_date, end_date = _resolve_default_window(session_factory, cfg.schema, symbols)
    rows, summary = diagnose_missingness(
        loader=loader,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        max_dates=args.max_dates,
    )
    if args.checkpoint_sha:
        summary["checkpoint_sha"] = args.checkpoint_sha
    if args.landing_sha:
        summary["landing_sha"] = args.landing_sha
    outputs = write_outputs(rows=rows, summary=summary, output_root=Path(args.output_root))
    print(json.dumps(outputs, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
