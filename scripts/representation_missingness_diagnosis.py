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
from backtest_app.historical_data.features import CTX_SERIES
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import _build_query_panel, build_event_memory_asof

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]
DEFAULT_OUTPUT_ROOT = Path("runs") / "representation_missingness_diagnosis"
TOP_N = 5


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
    sector_proxy_fallback_to_self: bool,
) -> float:
    raw_zero_default_ratio = raw_zero_default_keys_count / max(1, raw_feature_count)
    transform_zero_fill_ratio = transform_missing_keys_filled_zero_count / max(1, transform_feature_count)
    macro_missing_ratio = 1 - (macro_series_present_count / 5.0)
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
    macro_series_present_count = int(query_meta.get("macro_series_present_count") or sum(1 for key in CTX_SERIES if latest_macro.get(key) is not None))
    feature_coverage_score = compute_feature_coverage_score(
        raw_zero_default_keys_count=len(raw_zero_default_keys),
        raw_feature_count=raw_feature_count,
        transform_missing_keys_filled_zero_count=len(transform_missing_keys),
        transform_feature_count=transform_feature_count,
        macro_series_present_count=macro_series_present_count,
        sector_proxy_fallback_to_self=sector_proxy_fallback_to_self,
    )
    exclude_reasons = sorted(set(query_reasons + library_reasons))
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
        "raw_feature_count": raw_feature_count,
        "transform_feature_count": transform_feature_count,
        "raw_zero_default_keys_count": len(raw_zero_default_keys),
        "raw_zero_default_keys_top_n": _top_n(raw_zero_default_keys),
        "transform_missing_keys_filled_zero_count": len(transform_missing_keys),
        "transform_missing_keys_filled_zero_top_n": _top_n(transform_missing_keys),
        "transformed_zero_dominant_keys_top_n": _top_n(transformed_zero_keys),
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
    for row in rows:
        if row.get("query_available"):
            query_available_rows += 1
        if row.get("too_sparse"):
            sparse_rows += 1
        macro_coverage_sum += float(row.get("macro_series_present_count", 0)) / 5.0
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
    medium_verdict_hold_recommended = (
        fallback_hist.get("sector_proxy_fallback_to_self=True", 0) / max(1, total_rows) > 0.10
        or row_level_feature_sparsity_ratio > 0.10
        or macro_coverage_ratio < 0.95
    )
    return {
        "row_count": total_rows,
        "query_available_ratio": query_available_ratio,
        "exclude_reason_histogram": dict(exclude_hist),
        "zero_filled_feature_histogram": {
            "raw_zero_defaults": dict(raw_zero_hist),
            "transform_missing_keys_filled_zero": dict(transform_zero_hist),
            "transformed_zero_dominant_keys": dict(transformed_zero_hist),
        },
        "fallback_histogram": dict(fallback_hist),
        "sector_peer_coverage_ratio": sector_peer_coverage_ratio,
        "macro_coverage_ratio": macro_coverage_ratio,
        "row_level_feature_sparsity_ratio": row_level_feature_sparsity_ratio,
        "too_sparse_row_count": sparse_rows,
        "medium_verdict_hold_recommended": medium_verdict_hold_recommended,
    }


def render_report(summary: dict[str, Any]) -> str:
    exclude_hist = summary.get("exclude_reason_histogram") or {}
    fallback_hist = summary.get("fallback_histogram") or {}
    zero_hist = (summary.get("zero_filled_feature_histogram") or {}).get("raw_zero_defaults") or {}
    top_excludes = ", ".join(f"{k}={v}" for k, v in sorted(exclude_hist.items(), key=lambda item: (-item[1], item[0]))[:5]) or "none"
    top_zero_keys = ", ".join(f"{k}={v}" for k, v in sorted(zero_hist.items(), key=lambda item: (-item[1], item[0]))[:5]) or "none"
    lines = [
        "# Representation Missingness Diagnosis",
        "",
        f"- rows: {summary.get('row_count', 0)}",
        f"- query_available_ratio: {summary.get('query_available_ratio', 0.0):.3f}",
        f"- sector_peer_coverage_ratio: {summary.get('sector_peer_coverage_ratio', 0.0):.3f}",
        f"- macro_coverage_ratio: {summary.get('macro_coverage_ratio', 0.0):.3f}",
        f"- row_level_feature_sparsity_ratio: {summary.get('row_level_feature_sparsity_ratio', 0.0):.3f}",
        f"- medium_verdict_hold_recommended: {bool(summary.get('medium_verdict_hold_recommended', False))}",
        "",
        f"- top exclude reasons: {top_excludes}",
        f"- fallback histogram: {json.dumps(fallback_hist, ensure_ascii=False, sort_keys=True)}",
        f"- top raw zero-default keys: {top_zero_keys}",
        "",
        "Current representation should return to medium viability only after these coverage/fallback ratios look healthy.",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(*, rows: list[dict[str, Any]], summary: dict[str, Any], output_root: Path) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    rows_path = output_root / "rows.csv"
    summary_path = output_root / "summary.json"
    report_path = output_root / "report.md"
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
    return {
        "rows_path": str(rows_path),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
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
    macro_history_by_date = loader._load_macro_history(
        start_date=start_date,
        end_date=end_date,
        prewarm_days=max(120, spec.feature_window_bars * 2),
    )
    sector_map = loader._load_sector_map(symbols)
    decision_dates = _trade_dates_in_window(bars_by_symbol, start_date=start_date, end_date=end_date)
    if max_dates > 0:
        decision_dates = decision_dates[-max_dates:]
    rows: list[dict[str, Any]] = []
    for decision_date in decision_dates:
        memory = build_event_memory_asof(
            decision_date=decision_date,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            market="US",
            metadata=None,
        )
        query_panel, query_excluded = _build_query_panel(
            decision_dates=[decision_date],
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            scaler=memory["scaler"],
            transform=memory["transform"],
            metadata=None,
        )
        query_rows = query_panel.get(decision_date, {})
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
    summary.update(
        {
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
            "max_dates": max_dates,
            "discovery_only": True,
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
    outputs = write_outputs(rows=rows, summary=summary, output_root=Path(args.output_root))
    print(json.dumps(outputs, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
