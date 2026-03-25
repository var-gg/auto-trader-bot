from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy import create_engine, text

from backtest_app.db.local_session import LocalBacktestDbConfig, guard_backtest_local_only

MODES = {"init-full", "refresh-reference", "refresh-market", "resync-full"}


def load_config(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def get_local_url() -> str:
    cfg = LocalBacktestDbConfig.from_env()
    return guard_backtest_local_only(cfg.url)


def source_url() -> str:
    value = os.getenv("SOURCE_DB_URL", "").strip()
    if not value:
        raise SystemExit("SOURCE_DB_URL is required")
    return value


def tables_for_mode(config: dict, mode: str) -> list[dict]:
    tables = config["mirror_tables"]
    if mode == "init-full":
        return tables
    if mode == "resync-full":
        return tables
    if mode == "refresh-reference":
        return [t for t in tables if t.get("refresh_group") == "reference"]
    if mode == "refresh-market":
        return [t for t in tables if t.get("refresh_group") == "market"]
    raise ValueError(f"unsupported mode: {mode}")


def ensure_meta_tables(local_conn) -> None:
    bootstrap_files = [
        Path("db/sql/bootstrap/000_meta_sql_patch_log.sql"),
        Path("db/sql/patches/202603252345_local_mirror_state.sql"),
    ]
    for path in bootstrap_files:
        sql = path.read_text(encoding="utf-8")
        local_conn.connection.cursor().execute(sql)


def get_state(local_conn, table_name: str) -> dict | None:
    row = local_conn.execute(
        text("SELECT table_name, last_cursor_text, last_refreshed_at, last_mode, row_count FROM meta.local_mirror_state WHERE table_name = :table_name"),
        {"table_name": table_name},
    ).fetchone()
    return dict(row._mapping) if row else None


def resolve_cursor_column(spec: dict) -> str | None:
    return spec.get("cursor_column") or spec.get("fallback_cursor_column")


def build_source_sql(spec: dict, mode: str, state: dict | None) -> tuple[str, dict[str, Any]]:
    base_sql = spec["source_sql"].strip().rstrip(";")
    params: dict[str, Any] = {}
    if mode in {"init-full", "resync-full"}:
        return base_sql, params
    strategy = spec.get("refresh_strategy", "full")
    cursor_column = resolve_cursor_column(spec)
    if strategy in {"cursor_upsert", "resync"} and state and state.get("last_cursor_text") and cursor_column:
        sql = f"{base_sql} WHERE {cursor_column} > :last_cursor ORDER BY {cursor_column}"
        params["last_cursor"] = state["last_cursor_text"]
        return sql, params
    return base_sql, params


def truncate_if_needed(local_conn, spec: dict, mode: str) -> None:
    if mode not in {"init-full", "resync-full"}:
        return
    truncate_sql = spec.get("truncate_sql")
    if truncate_sql:
        local_conn.execute(text(truncate_sql))


def record_run_start(local_conn, mode: str, table_name: str) -> int:
    row = local_conn.execute(
        text("INSERT INTO meta.local_mirror_run_log(mode, table_name) VALUES (:mode, :table_name) RETURNING run_id"),
        {"mode": mode, "table_name": table_name},
    ).fetchone()
    return int(row._mapping["run_id"])


def record_run_finish(local_conn, run_id: int, *, status: str, rows_copied: int, notes: str = "") -> None:
    local_conn.execute(
        text("UPDATE meta.local_mirror_run_log SET finished_at = NOW(), status = :status, rows_copied = :rows_copied, notes = :notes WHERE run_id = :run_id"),
        {"run_id": run_id, "status": status, "rows_copied": rows_copied, "notes": notes},
    )


def upsert_state(local_conn, *, table_name: str, last_cursor_text: str | None, mode: str, row_count: int) -> None:
    local_conn.execute(
        text("""
        INSERT INTO meta.local_mirror_state(table_name, last_cursor_text, last_refreshed_at, last_mode, row_count)
        VALUES (:table_name, :last_cursor_text, NOW(), :last_mode, :row_count)
        ON CONFLICT (table_name) DO UPDATE
        SET last_cursor_text = EXCLUDED.last_cursor_text,
            last_refreshed_at = EXCLUDED.last_refreshed_at,
            last_mode = EXCLUDED.last_mode,
            row_count = EXCLUDED.row_count
        """),
        {"table_name": table_name, "last_cursor_text": last_cursor_text, "last_mode": mode, "row_count": row_count},
    )


def last_cursor_from_rows(rows: list[dict], spec: dict) -> str | None:
    cursor_column = resolve_cursor_column(spec)
    if not cursor_column or not rows:
        return None
    values = [r.get(cursor_column) for r in rows if r.get(cursor_column) is not None]
    if not values:
        return None
    value = max(values)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def refresh_table(source_conn, local_conn, spec: dict, mode: str) -> dict[str, Any]:
    table_name = spec["name"]
    state = get_state(local_conn, table_name)
    sql, params = build_source_sql(spec, mode, state)
    run_id = record_run_start(local_conn, mode, table_name)
    try:
        truncate_if_needed(local_conn, spec, mode)
        rows = [dict(r._mapping) for r in source_conn.execute(text(sql), params)]
        if rows:
            local_conn.execute(text(spec["insert_sql"]), rows)
        cursor_text = last_cursor_from_rows(rows, spec) or (state or {}).get("last_cursor_text")
        upsert_state(local_conn, table_name=table_name, last_cursor_text=cursor_text, mode=mode, row_count=len(rows))
        record_run_finish(local_conn, run_id, status="OK", rows_copied=len(rows), notes=spec["target_table"])
        return {"table": table_name, "rows": len(rows), "cursor": cursor_text, "status": "OK"}
    except Exception as exc:
        record_run_finish(local_conn, run_id, status="FAILED", rows_copied=0, notes=str(exc))
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh local trading schema mirror from production/proxy source")
    parser.add_argument("mode", choices=sorted(MODES))
    parser.add_argument("--config", default="config/local_trading_mirror.json")
    args = parser.parse_args()

    config = load_config(args.config)
    source_engine = create_engine(source_url(), future=True)
    local_engine = create_engine(get_local_url(), future=True)
    selected = tables_for_mode(config, args.mode)
    results: list[dict[str, Any]] = []

    with source_engine.connect() as source_conn, local_engine.begin() as local_conn:
        ensure_meta_tables(local_conn)
        for spec in selected:
            results.append(refresh_table(source_conn, local_conn, spec, args.mode))

    print(json.dumps({"mode": args.mode, "tables": results}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
