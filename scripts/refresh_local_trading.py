from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from backtest_app.db.local_session import LocalBacktestDbConfig, guard_backtest_local_only

MODES = {"init-full", "refresh-reference", "refresh-market", "resync-full"}
SELECT_PREFIX_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
ORDER_BY_RE = re.compile(r"\border\s+by\b", re.IGNORECASE)
WHERE_RE = re.compile(r"\bwhere\b", re.IGNORECASE)


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


def split_schema_table(qualified_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in qualified_name.split(".") if part.strip()]
    if len(parts) != 2:
        raise ValueError(f"Expected schema-qualified table name, got: {qualified_name}")
    return parts[0], parts[1]


def qualify_table(name: str) -> str:
    schema, table = split_schema_table(name)
    return f'{schema}.{table}'


def table_exists(conn, qualified_name: str) -> bool:
    schema, table = split_schema_table(qualified_name)
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table
            """
        ),
        {"schema": schema, "table": table},
    ).fetchone()
    return row is not None


def ensure_select_only(sql: str, *, field_name: str = "source_sql") -> str:
    normalized = sql.strip().rstrip(";")
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if not SELECT_PREFIX_RE.match(normalized):
        raise ValueError(f"{field_name} must be SELECT-only")
    if ";" in normalized:
        raise ValueError(f"{field_name} must contain exactly one SELECT statement")
    return normalized


def ensure_not_excluded(spec: dict, exclude_prefixes: list[str]) -> None:
    source_table = spec["source_table"]
    for prefix in exclude_prefixes:
        if source_table.startswith(prefix):
            raise ValueError(f"Mirror source_table is blocked by exclude_prefixes: {source_table} (prefix {prefix})")


def append_incremental_cursor(base_sql: str, cursor_column: str) -> str:
    sql = ensure_select_only(base_sql)
    order_clause = f" ORDER BY {cursor_column}"
    if ORDER_BY_RE.search(sql):
        core_sql = ORDER_BY_RE.split(sql, maxsplit=1)[0].rstrip()
    else:
        core_sql = sql
    if WHERE_RE.search(core_sql):
        return f"{core_sql} AND {cursor_column} > :last_cursor{order_clause}"
    return f"{core_sql} WHERE {cursor_column} > :last_cursor{order_clause}"


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
        text(
            "SELECT table_name, last_cursor_text, last_refreshed_at, last_mode, row_count FROM meta.local_mirror_state WHERE table_name = :table_name"
        ),
        {"table_name": table_name},
    ).fetchone()
    return dict(row._mapping) if row else None


def resolve_cursor_column(spec: dict) -> str | None:
    return spec.get("cursor_column") or spec.get("fallback_cursor_column")


def build_source_sql(spec: dict, mode: str, state: dict | None) -> tuple[str, dict[str, Any]]:
    base_sql = ensure_select_only(spec["source_sql"])
    params: dict[str, Any] = {}
    if mode in {"init-full", "resync-full"}:
        return base_sql, params
    strategy = spec.get("refresh_strategy", "full")
    cursor_column = resolve_cursor_column(spec)
    if strategy in {"cursor_upsert", "resync"} and state and state.get("last_cursor_text") and cursor_column:
        sql = append_incremental_cursor(base_sql, cursor_column)
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


def record_run_finish(
    local_conn,
    run_id: int,
    *,
    status: str,
    rows_copied: int,
    cursor_text: str | None = None,
    notes: str = "",
) -> None:
    local_conn.execute(
        text(
            """
            UPDATE meta.local_mirror_run_log
            SET finished_at = NOW(),
                status = :status,
                rows_copied = :rows_copied,
                notes = :notes
            WHERE run_id = :run_id
            """
        ),
        {"run_id": run_id, "status": status, "rows_copied": rows_copied, "notes": f"cursor={cursor_text or ''}; {notes}".strip()},
    )


def upsert_state(local_conn, *, table_name: str, last_cursor_text: str | None, mode: str, row_count: int) -> None:
    local_conn.execute(
        text(
            """
        INSERT INTO meta.local_mirror_state(table_name, last_cursor_text, last_refreshed_at, last_mode, row_count)
        VALUES (:table_name, :last_cursor_text, NOW(), :last_mode, :row_count)
        ON CONFLICT (table_name) DO UPDATE
        SET last_cursor_text = EXCLUDED.last_cursor_text,
            last_refreshed_at = EXCLUDED.last_refreshed_at,
            last_mode = EXCLUDED.last_mode,
            row_count = EXCLUDED.row_count
        """
        ),
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


def expected_row_count(source_conn, sql: str, params: dict[str, Any]) -> int:
    row = source_conn.execute(text(f"SELECT COUNT(*) AS row_count FROM ({sql}) AS mirror_source"), params).fetchone()
    return int(row._mapping["row_count"])


def preflight_spec(source_conn, local_conn, spec: dict, exclude_prefixes: list[str]) -> dict[str, Any]:
    ensure_not_excluded(spec, exclude_prefixes)
    source_sql = ensure_select_only(spec["source_sql"])
    source_table = spec["source_table"]
    target_table = spec["target_table"]
    source_exists = table_exists(source_conn, source_table)
    target_exists = table_exists(local_conn, target_table)
    if not source_exists:
        raise RuntimeError(f"Source table does not exist: {source_table}")
    if not target_exists:
        raise RuntimeError(
            f"Target table does not exist in local mirror DB: {target_table}. Apply SQL bootstrap first with scripts/db_apply_sql.py"
        )
    return {
        "name": spec["name"],
        "source_table": source_table,
        "target_table": target_table,
        "source_sql": source_sql,
        "source_exists": source_exists,
        "target_exists": target_exists,
    }


def run_preflight(source_conn, local_conn, selected: list[dict], exclude_prefixes: list[str]) -> list[dict[str, Any]]:
    source_conn.execute(text("SELECT 1"))
    local_conn.execute(text("SELECT 1"))
    return [preflight_spec(source_conn, local_conn, spec, exclude_prefixes) for spec in selected]


def dry_run_table(source_conn, local_conn, spec: dict, mode: str) -> dict[str, Any]:
    state = get_state(local_conn, spec["name"])
    sql, params = build_source_sql(spec, mode, state)
    return {
        "table": spec["name"],
        "source_table": spec["source_table"],
        "target_table": spec["target_table"],
        "mode": mode,
        "estimated_rows": expected_row_count(source_conn, sql, params),
        "cursor_column": resolve_cursor_column(spec),
        "cursor_before": (state or {}).get("last_cursor_text"),
        "sql": sql,
        "params": params,
    }


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
        record_run_finish(
            local_conn,
            run_id,
            status="OK",
            rows_copied=len(rows),
            cursor_text=cursor_text,
            notes=f"target={spec['target_table']}",
        )
        return {
            "table": table_name,
            "rows": len(rows),
            "cursor_before": (state or {}).get("last_cursor_text"),
            "cursor_after": cursor_text,
            "status": "OK",
            "target_table": spec["target_table"],
        }
    except Exception as exc:
        record_run_finish(local_conn, run_id, status="FAILED", rows_copied=0, cursor_text=(state or {}).get("last_cursor_text"), notes=str(exc))
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh local trading schema mirror from production/proxy source")
    parser.add_argument("mode", choices=sorted(MODES))
    parser.add_argument("--config", default="config/local_trading_mirror.json")
    parser.add_argument("--preflight", action="store_true", help="Run fail-closed connection/spec/table checks and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print per-table expected row counts and cursor state without local writes")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    selected = tables_for_mode(config, args.mode)
    exclude_prefixes = config.get("exclude_prefixes", [])
    source_engine = create_engine(source_url(), future=True)
    local_engine = create_engine(get_local_url(), future=True)

    with source_engine.connect() as source_conn, local_engine.begin() as local_conn:
        ensure_meta_tables(local_conn)
        preflight = run_preflight(source_conn, local_conn, selected, exclude_prefixes)
        if args.preflight:
            print(json.dumps({"mode": args.mode, "preflight": preflight}, ensure_ascii=False, indent=2, default=str))
            return 0
        if args.dry_run:
            dry_results = [dry_run_table(source_conn, local_conn, spec, args.mode) for spec in selected]
            print(json.dumps({"mode": args.mode, "preflight": preflight, "tables": dry_results}, ensure_ascii=False, indent=2, default=str))
            local_conn.rollback()
            return 0
        results = [refresh_table(source_conn, local_conn, spec, args.mode) for spec in selected]

    print(json.dumps({"mode": args.mode, "preflight": preflight, "tables": results}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
