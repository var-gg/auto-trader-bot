from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text

from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only

MANIFEST_TABLE = "meta.bt_scenario_snapshot_manifest"
ALLOWED_PHASES = {"discovery", "holdout"}
ALLOWED_SOURCE_KINDS = {"import-json", "import-jsonl", "import-csv", "copy"}


def stable_hash(payload: Any, length: int = 16) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()[:length]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str) and value.strip():
        return json.loads(value)
    return fallback


def _normalize_row(row: dict[str, Any], *, scenario_id: str, market: str) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "market": str(row.get("market") or market),
        "symbol": str(row["symbol"]),
        "ticker_id": row.get("ticker_id"),
        "event_time": str(row.get("event_time") or f"{row['reference_date']}T00:00:00+00:00"),
        "anchor_date": row.get("anchor_date"),
        "reference_date": str(row["reference_date"]),
        "side_bias": str(row.get("side_bias") or "BUY"),
        "signal_strength": float(row.get("signal_strength") or 0.0),
        "confidence": (float(row["confidence"]) if row.get("confidence") not in (None, "") else None),
        "current_price": (float(row["current_price"]) if row.get("current_price") not in (None, "") else None),
        "atr_pct": (float(row["atr_pct"]) if row.get("atr_pct") not in (None, "") else None),
        "target_return_pct": (float(row["target_return_pct"]) if row.get("target_return_pct") not in (None, "") else None),
        "max_reverse_pct": (float(row["max_reverse_pct"]) if row.get("max_reverse_pct") not in (None, "") else None),
        "expected_horizon_days": (int(row["expected_horizon_days"]) if row.get("expected_horizon_days") not in (None, "") else None),
        "reverse_breach_day": (int(row["reverse_breach_day"]) if row.get("reverse_breach_day") not in (None, "") else None),
        "outcome_label": row.get("outcome_label"),
        "provenance": _coerce_json(row.get("provenance"), {}),
        "diagnostics": _coerce_json(row.get("diagnostics"), {}),
        "notes": _coerce_json(row.get("notes"), []),
    }


def load_rows_from_file(path: Path, *, scenario_id: str, market: str) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    rows: list[dict[str, Any]] = []
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            raw_rows = payload.get("rows") or payload.get("items") or payload.get("bt_event_window") or []
        elif isinstance(payload, list):
            raw_rows = payload
        else:
            raise ValueError("JSON import must be a list or object with rows/items/bt_event_window")
        rows = [_normalize_row(dict(item), scenario_id=scenario_id, market=market) for item in raw_rows]
    elif suffix == ".jsonl":
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(_normalize_row(json.loads(line), scenario_id=scenario_id, market=market))
    elif suffix == ".csv":
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [_normalize_row(dict(item), scenario_id=scenario_id, market=market) for item in reader]
    else:
        raise ValueError(f"Unsupported import file type: {path.suffix}")
    if not rows:
        raise ValueError("No bt_event_window rows loaded from source file")
    return rows


def compute_manifest(*, scenario_id: str, phase: str, source_kind: str, market: str, rows: Iterable[dict[str, Any]], notes: str = "", source_path: str | None = None, copied_from_scenario_id: str | None = None) -> dict[str, Any]:
    rows = list(rows)
    reference_dates = sorted(str(r["reference_date"])[:10] for r in rows)
    symbols = sorted({str(r["symbol"]) for r in rows})
    universe_hash = stable_hash(symbols)
    spec_payload = [{k: v for k, v in r.items() if k not in {"scenario_id", "market", "symbol", "ticker_id", "event_time", "anchor_date", "reference_date", "provenance", "diagnostics", "notes"}} for r in rows]
    spec_hash = stable_hash(spec_payload)
    manifest_key = {
        "scenario_id": scenario_id,
        "phase": phase,
        "source_kind": source_kind,
        "market": market,
        "window_start": reference_dates[0],
        "window_end": reference_dates[-1],
        "universe_hash": universe_hash,
        "spec_hash": spec_hash,
        "row_count": len(rows),
    }
    snapshot_id = stable_hash(manifest_key)
    return {
        "snapshot_id": snapshot_id,
        "scenario_id": scenario_id,
        "phase": phase,
        "source_kind": source_kind,
        "market": market,
        "window_start": reference_dates[0],
        "window_end": reference_dates[-1],
        "universe_hash": universe_hash,
        "spec_hash": spec_hash,
        "row_count": len(rows),
        "created_at": utc_now_iso(),
        "notes": notes or None,
        "source_path": source_path,
        "copied_from_scenario_id": copied_from_scenario_id,
        "symbols": symbols,
    }


def ensure_manifest_table(session, schema: str) -> None:
    session.execute(text("CREATE SCHEMA IF NOT EXISTS meta"))
    session.execute(text(
        f"""
        CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
            snapshot_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL UNIQUE,
            phase TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            market TEXT NOT NULL,
            window_start DATE NOT NULL,
            window_end DATE NOT NULL,
            universe_hash TEXT NOT NULL,
            spec_hash TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            notes TEXT,
            source_path TEXT,
            copied_from_scenario_id TEXT
        )
        """
    ))
    session.execute(text(f"CREATE INDEX IF NOT EXISTS ix_bt_scenario_snapshot_manifest_phase_market ON {MANIFEST_TABLE}(phase, market, window_start, window_end)"))
    session.execute(text(f"CREATE INDEX IF NOT EXISTS ix_bt_event_window_scenario_market_refdate ON {schema}.bt_event_window(scenario_id, market, reference_date)"))


def replace_scenario_rows(session, *, schema: str, scenario_id: str, rows: list[dict[str, Any]]) -> None:
    session.execute(text(f"DELETE FROM {schema}.bt_event_window WHERE scenario_id = :scenario_id"), {"scenario_id": scenario_id})
    insert_sql = text(
        f"""
        INSERT INTO {schema}.bt_event_window (
            scenario_id, market, symbol, ticker_id, event_time, anchor_date, reference_date,
            side_bias, signal_strength, confidence, current_price, atr_pct, target_return_pct,
            max_reverse_pct, expected_horizon_days, reverse_breach_day, outcome_label,
            provenance, diagnostics, notes
        ) VALUES (
            :scenario_id, :market, :symbol, :ticker_id, CAST(:event_time AS timestamptz), CAST(:anchor_date AS date), CAST(:reference_date AS date),
            :side_bias, :signal_strength, :confidence, :current_price, :atr_pct, :target_return_pct,
            :max_reverse_pct, :expected_horizon_days, :reverse_breach_day, :outcome_label,
            CAST(:provenance AS jsonb), CAST(:diagnostics AS jsonb), CAST(:notes AS jsonb)
        )
        """
    )
    for row in rows:
        payload = dict(row)
        payload["provenance"] = json.dumps(payload.get("provenance") or {}, ensure_ascii=False)
        payload["diagnostics"] = json.dumps(payload.get("diagnostics") or {}, ensure_ascii=False)
        payload["notes"] = json.dumps(payload.get("notes") or [], ensure_ascii=False)
        session.execute(insert_sql, payload)


def upsert_manifest(session, manifest: dict[str, Any]) -> None:
    session.execute(
        text(
            f"""
            INSERT INTO {MANIFEST_TABLE} (
                snapshot_id, scenario_id, phase, source_kind, market, window_start, window_end,
                universe_hash, spec_hash, row_count, created_at, notes, source_path, copied_from_scenario_id
            ) VALUES (
                :snapshot_id, :scenario_id, :phase, :source_kind, :market, CAST(:window_start AS date), CAST(:window_end AS date),
                :universe_hash, :spec_hash, :row_count, CAST(:created_at AS timestamptz), :notes, :source_path, :copied_from_scenario_id
            )
            ON CONFLICT (snapshot_id) DO UPDATE SET
                scenario_id = EXCLUDED.scenario_id,
                phase = EXCLUDED.phase,
                source_kind = EXCLUDED.source_kind,
                market = EXCLUDED.market,
                window_start = EXCLUDED.window_start,
                window_end = EXCLUDED.window_end,
                universe_hash = EXCLUDED.universe_hash,
                spec_hash = EXCLUDED.spec_hash,
                row_count = EXCLUDED.row_count,
                created_at = EXCLUDED.created_at,
                notes = EXCLUDED.notes,
                source_path = EXCLUDED.source_path,
                copied_from_scenario_id = EXCLUDED.copied_from_scenario_id
            """
        ),
        manifest,
    )


def load_existing_rows(session, *, schema: str, source_scenario_id: str) -> list[dict[str, Any]]:
    sql = text(
        f"""
        SELECT scenario_id, market, symbol, ticker_id, event_time, anchor_date, reference_date,
               side_bias, signal_strength, confidence, current_price, atr_pct, target_return_pct,
               max_reverse_pct, expected_horizon_days, reverse_breach_day, outcome_label,
               provenance, diagnostics, notes
          FROM {schema}.bt_event_window
         WHERE scenario_id = :scenario_id
         ORDER BY reference_date, symbol, event_time
        """
    )
    rows = [dict(r._mapping) for r in session.execute(sql, {"scenario_id": source_scenario_id})]
    if not rows:
        raise ValueError(f"No bt_event_window rows found for source scenario_id={source_scenario_id}")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize reusable bt_event_window scenario snapshots")
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--phase", required=True, choices=sorted(ALLOWED_PHASES))
    parser.add_argument("--market", default="US")
    parser.add_argument("--notes", default="")
    parser.add_argument("--source-json", default="")
    parser.add_argument("--source-jsonl", default="")
    parser.add_argument("--source-csv", default="")
    parser.add_argument("--copy-from-scenario-id", default="")
    args = parser.parse_args()

    source_flags = [bool(args.source_json), bool(args.source_jsonl), bool(args.source_csv), bool(args.copy_from_scenario_id)]
    if sum(source_flags) != 1:
        raise SystemExit("Choose exactly one of --source-json, --source-jsonl, --source-csv, --copy-from-scenario-id")

    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)

    with session_factory() as session:
        ensure_manifest_table(session, cfg.schema)
        if args.source_json:
            source_kind = "import-json"
            rows = load_rows_from_file(Path(args.source_json), scenario_id=args.scenario_id, market=args.market)
            source_path = str(Path(args.source_json))
            copied_from = None
        elif args.source_jsonl:
            source_kind = "import-jsonl"
            rows = load_rows_from_file(Path(args.source_jsonl), scenario_id=args.scenario_id, market=args.market)
            source_path = str(Path(args.source_jsonl))
            copied_from = None
        elif args.source_csv:
            source_kind = "import-csv"
            rows = load_rows_from_file(Path(args.source_csv), scenario_id=args.scenario_id, market=args.market)
            source_path = str(Path(args.source_csv))
            copied_from = None
        else:
            source_kind = "copy"
            copied_from = args.copy_from_scenario_id
            rows = [_normalize_row(r, scenario_id=args.scenario_id, market=args.market) for r in load_existing_rows(session, schema=cfg.schema, source_scenario_id=copied_from)]
            source_path = None

        if source_kind not in ALLOWED_SOURCE_KINDS:
            raise SystemExit(f"Unsupported source_kind={source_kind}")

        manifest = compute_manifest(
            scenario_id=args.scenario_id,
            phase=args.phase,
            source_kind=source_kind,
            market=args.market,
            rows=rows,
            notes=args.notes,
            source_path=source_path,
            copied_from_scenario_id=copied_from,
        )
        replace_scenario_rows(session, schema=cfg.schema, scenario_id=args.scenario_id, rows=rows)
        upsert_manifest(session, manifest)
        session.commit()

    print(json.dumps({k: v for k, v in manifest.items() if k != "symbols"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
