from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest_app.historical_data.features import CTX_SERIES, SIMILARITY_CTX_SERIES, build_multiscale_feature_vector
from backtest_app.historical_data.models import HistoricalBar

DOC_PATH = REPO_ROOT / "docs" / "research_representation_truth_audit.md"
JSON_PATH = REPO_ROOT / "runs" / "representation_audit" / "current_contract_audit.json"


def _sample_bars(symbol: str, *, days: int = 90, start: date = date(2025, 1, 1)) -> list[HistoricalBar]:
    rows: list[HistoricalBar] = []
    price = 100.0 + (hash(symbol) % 9)
    for offset in range(days):
        ts = (start + timedelta(days=offset)).isoformat()
        open_ = price
        close = price * (1.001 + ((offset % 5) * 0.0004))
        high = close * 1.01
        low = open_ * 0.99
        volume = 1_000_000 + (offset * 5_000)
        rows.append(HistoricalBar(symbol=symbol, timestamp=ts, open=open_, high=high, low=low, close=close, volume=volume))
        price = close
    return rows


def _sample_macro_history(*, days: int = 45, start: date = date(2025, 2, 1)) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for offset in range(days):
        ts = (start + timedelta(days=offset)).isoformat()
        out[ts] = {
            "vix": 17.0 + offset * 0.15,
            "rate": 3.2 + offset * 0.02,
            "dollar": 99.0 + offset * 0.08,
            "oil": 70.0 + offset * 0.12,
            "breadth": -0.25 + offset * 0.03,
        }
    return out


def _find_line_ref(path: Path, needle: str) -> str:
    text = path.read_text(encoding="utf-8").splitlines()
    for idx, line in enumerate(text, start=1):
        if needle in line:
            rel = path.relative_to(REPO_ROOT).as_posix()
            return f"{rel}:{idx}"
    raise ValueError(f"needle not found: {needle} in {path}")


def build_audit_payload() -> dict[str, Any]:
    features_path = REPO_ROOT / "backtest_app" / "historical_data" / "features.py"
    pipeline_path = REPO_ROOT / "backtest_app" / "research" / "pipeline.py"
    loader_path = REPO_ROOT / "backtest_app" / "historical_data" / "local_postgres_loader.py"
    session_path = REPO_ROOT / "backtest_app" / "historical_data" / "session_alignment.py"

    sample = build_multiscale_feature_vector(
        symbol="AAPL",
        bars=_sample_bars("AAPL"),
        market_bars=_sample_bars("MKT"),
        sector_bars=_sample_bars("TECH"),
        macro_history=_sample_macro_history(),
        sector_code="TECH",
        macro_freshness_features={"vix_days_since_update": 1.0, "vix_bars_since_update": 1.0, "vix_is_stale": 0.0, "vix_age_bucket": 0.0},
        additional_metadata={
            "anchor_fields": {
                "exchange_code": "NMS",
                "exchange_tz": "America/New_York",
                "session_date_local": "2025-03-01",
                "session_close_ts_utc": "2025-03-01T21:00:00+00:00",
                "feature_anchor_ts_utc": "2025-03-01T21:00:00+00:00",
            },
            "macro_asof_ts_utc": "2025-03-01T21:00:00+00:00",
            "breadth_present": False,
            "breadth_missing_reason": "canonical_source_missing",
        },
    )
    similarity_keys = sorted(sample.raw_features.keys())
    regime_only_keys = sorted(sample.raw_regime_context_features.keys())

    payload = {
        "files": {
            "features": {"path": "backtest_app/historical_data/features.py"},
            "pipeline": {"path": "backtest_app/research/pipeline.py"},
            "loader": {"path": "backtest_app/historical_data/local_postgres_loader.py"},
            "session_alignment": {"path": "backtest_app/historical_data/session_alignment.py"},
        },
        "similarity_feature_keys": similarity_keys,
        "regime_only_keys": regime_only_keys,
        "defaults": {
            "use_macro_level_in_similarity": bool(sample.metadata.get("use_macro_level_in_similarity", False)),
            "use_dollar_volume_absolute": bool(sample.metadata.get("use_dollar_volume_absolute", False)),
            "absolute_macro_level_in_similarity_by_default": False,
            "raw_dollar_volume_in_similarity_by_default": False,
        },
        "loader_canonical_macro_series": ["vix", "rate", "dollar", "oil"],
        "disabled_similarity_series": ["breadth"],
        "session_alignment_contract": {
            "session_metadata_object": ["exchange_code", "country_code", "exchange_tz", "session_close_local_time"],
            "anchor_fields": ["session_date_local", "session_close_ts_local", "session_close_ts_utc", "feature_anchor_ts_utc"],
            "exchange_mapping_line_ref": _find_line_ref(session_path, '\"KOE\": ExchangeSessionConfig('),
            "anchor_derivation_line_ref": _find_line_ref(session_path, "def derive_session_anchor_from_date"),
        },
        "missingness_handling": [
            {
                "category": "exclude row",
                "mechanisms": [
                    {
                        "name": "query insufficient history",
                        "risk": "harmless",
                        "line_ref": _find_line_ref(pipeline_path, '"reason": "insufficient_query_history"'),
                    },
                    {
                        "name": "event library insufficient bars",
                        "risk": "harmless",
                        "line_ref": _find_line_ref(pipeline_path, '"reason": "insufficient_bars"'),
                    },
                    {
                        "name": "unknown exchange/session metadata is classified as data_quality_missing",
                        "risk": "likely distorts similarity",
                        "line_ref": _find_line_ref(pipeline_path, '"reason": "unknown_exchange_session"'),
                    },
                ],
            },
            {
                "category": "zero fill in raw feature function",
                "mechanisms": [
                    {
                        "name": "context/liquidity/helper defaults collapse to zero when history is short or absent",
                        "risk": "acceptable but monitor",
                        "line_ref": _find_line_ref(features_path, "def _context_series_features"),
                    },
                ],
            },
            {
                "category": "zero fill in transform stage",
                "mechanisms": [
                    {
                        "name": "missing feature keys are inserted as 0.0 before scaling",
                        "risk": "acceptable but monitor",
                        "line_ref": _find_line_ref(features_path, "ordered_raw = {key: float(raw_features.get(key, 0.0) or 0.0) for key in self.feature_keys}"),
                    },
                ],
            },
            {
                "category": "forward fill in macro loader",
                "mechanisms": [
                    {
                        "name": "calendar-day macro history forward fill via last_seen",
                        "risk": "acceptable but monitor",
                        "line_ref": _find_line_ref(loader_path, "def _macro_history_by_obs_date"),
                    },
                ],
            },
            {
                "category": "fallback to market proxy",
                "mechanisms": [
                    {
                        "name": "market proxy is session-aware same-exchange by default",
                        "risk": "acceptable but monitor",
                        "line_ref": _find_line_ref(pipeline_path, 'proxy_mode="session_aware_same_exchange"'),
                    },
                ],
            },
            {
                "category": "fallback to self sector proxy",
                "mechanisms": [
                    {
                        "name": "sector proxy falls back to the symbol itself when no same-exchange peer exists",
                        "risk": "likely distorts similarity",
                        "line_ref": _find_line_ref(pipeline_path, "fallback_to_self = not bool(peers)"),
                    },
                ],
            },
        ],
        "history_scope": {
            "feature_window_bars": {"value": 60, "line_ref": _find_line_ref(pipeline_path, "feature_window_bars: int = 60")},
            "loaded_warmup_bars": {"value": "max(120, feature_window_bars * 2)", "line_ref": _find_line_ref(loader_path, "max(WARMUP_DAYS, spec.feature_window_bars * 2)")},
            "event_memory_actual_usage": {
                "value": "feature_end_date <= decision_date and outcome_end_date < decision_date inside decision-window build",
                "line_ref": _find_line_ref(pipeline_path, "if outcome_end_date >= decision_date:"),
            },
            "query_window_scope": {
                "value": "spec.feature_window_bars trailing bars ending at decision_date",
                "line_ref": _find_line_ref(pipeline_path, "query_window = bars[idx - spec.feature_window_bars + 1 : idx + 1]"),
            },
            "macro_join_scope": {
                "value": "latest series observation whose source_ts_utc <= feature_anchor_ts_utc",
                "line_ref": _find_line_ref(pipeline_path, "def _macro_history_until_anchor"),
            },
        },
        "risk_matrix": [
            {
                "topic": "absolute macro level in default similarity",
                "status": "disabled by default",
                "risk": "harmless",
                "line_ref": _find_line_ref(features_path, "if use_macro_level_in_similarity:"),
            },
            {
                "topic": "raw dollar volume in default similarity",
                "status": "disabled by default",
                "risk": "harmless",
                "line_ref": _find_line_ref(features_path, "if use_dollar_volume_absolute:"),
            },
            {
                "topic": "session alignment",
                "status": "exchange-local session metadata derives feature_anchor_ts_utc",
                "risk": "acceptable but monitor",
                "line_ref": _find_line_ref(session_path, "def derive_session_anchor_from_date"),
            },
            {
                "topic": "market/sector proxy alignment",
                "status": "same-exchange session-aware proxy",
                "risk": "acceptable but monitor",
                "line_ref": _find_line_ref(pipeline_path, "focus_symbol=symbol"),
            },
            {
                "topic": "sector proxy self fallback",
                "status": "still enabled for missing peers",
                "risk": "likely distorts similarity",
                "line_ref": _find_line_ref(pipeline_path, "fallback_to_self = not bool(peers)"),
            },
            {
                "topic": "regime gate source",
                "status": "normalized regime context is primary path",
                "risk": "acceptable but monitor",
                "line_ref": _find_line_ref(pipeline_path, '"regime_source": REGIME_SOURCE_NORMALIZED'),
            },
            {
                "topic": "macro as-of join",
                "status": "anchor-time as-of join with derived source_ts_utc",
                "risk": "acceptable but monitor",
                "line_ref": _find_line_ref(loader_path, "def _load_macro_series_history"),
            },
            {
                "topic": "breadth similarity path",
                "status": "disabled from similarity, explicit missingness only",
                "risk": "acceptable but monitor",
                "line_ref": _find_line_ref(pipeline_path, 'BREADTH_MISSING_REASON_CANONICAL_SOURCE_MISSING'),
            },
            {
                "topic": "calendar-day macro snapshot artifact",
                "status": "legacy snapshot still persisted for compatibility, as-of join uses macro_series_history",
                "risk": "likely distorts regime gate",
                "line_ref": _find_line_ref(loader_path, "def _macro_history_by_obs_date"),
            },
        ],
        "regime_only_keys_count": len(regime_only_keys),
        "similarity_feature_key_count": len(similarity_keys),
        "ctx_series": list(CTX_SERIES),
        "similarity_ctx_series": list(SIMILARITY_CTX_SERIES),
    }
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    similarity_keys = ", ".join(payload["similarity_feature_keys"])
    regime_only_keys = ", ".join(payload["regime_only_keys"])
    defaults = payload["defaults"]
    lines = [
        "# Research Representation Truth Audit",
        "",
        "Current public-branch representation contract audit for `features.py`, `pipeline.py`, and `local_postgres_loader.py`.",
        "",
        f"Canonical loader macro aliases: {', '.join(payload['loader_canonical_macro_series'])}. Similarity-disabled series: {', '.join(payload['disabled_similarity_series'])}.",
        "",
        "## Contract Summary",
        "",
        "| Item | Value | Line Reference | Risk |",
        "| --- | --- | --- | --- |",
        f"| Similarity feature keys | {similarity_keys} | {payload['files']['features']['path']} | acceptable but monitor |",
        f"| Regime-only keys | {regime_only_keys} | {payload['files']['features']['path']} | acceptable but monitor |",
        f"| Absolute macro level in similarity by default | {defaults['absolute_macro_level_in_similarity_by_default']} | {next(item['line_ref'] for item in payload['risk_matrix'] if item['topic'] == 'absolute macro level in default similarity')} | harmless |",
        f"| Raw dollar volume in similarity by default | {defaults['raw_dollar_volume_in_similarity_by_default']} | {next(item['line_ref'] for item in payload['risk_matrix'] if item['topic'] == 'raw dollar volume in default similarity')} | harmless |",
        f"| Session anchor fields | {', '.join(payload['session_alignment_contract']['anchor_fields'])} | {payload['session_alignment_contract']['anchor_derivation_line_ref']} | acceptable but monitor |",
        f"| Session metadata object | {', '.join(payload['session_alignment_contract']['session_metadata_object'])} | {payload['session_alignment_contract']['exchange_mapping_line_ref']} | acceptable but monitor |",
        "",
        "## Missingness Handling",
        "",
        "| Category | Mechanism | Line Reference | Risk |",
        "| --- | --- | --- | --- |",
    ]
    for category in payload["missingness_handling"]:
        for mechanism in category["mechanisms"]:
            lines.append(f"| {category['category']} | {mechanism['name']} | {mechanism['line_ref']} | {mechanism['risk']} |")
    lines.extend(
        [
            "",
            "## History Scope",
            "",
            "| Scope | Value | Line Reference |",
            "| --- | --- | --- |",
        ]
    )
    for key, row in payload["history_scope"].items():
        lines.append(f"| {key} | {row['value']} | {row['line_ref']} |")
    lines.extend(
        [
            "",
            "## Risk Matrix",
            "",
            "| Topic | Status | Line Reference | Risk |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in payload["risk_matrix"]:
        lines.append(f"| {row['topic']} | {row['status']} | {row['line_ref']} | {row['risk']} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    payload = build_audit_payload()
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOC_PATH.write_text(render_markdown(payload), encoding="utf-8")
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"doc_path": str(DOC_PATH), "json_path": str(JSON_PATH)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
