from __future__ import annotations

import argparse
import hashlib
import time
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text

PATCH_LOG_BOOTSTRAP = Path("db/sql/bootstrap/000_meta_sql_patch_log.sql")
VALID_ROOTS = ("bootstrap", "patches", "verify")


def sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def sorted_sql_files(root: Path, groups: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for group in groups:
        group_dir = root / group
        if not group_dir.exists():
            continue
        files.extend(sorted(p for p in group_dir.glob("*.sql") if p.is_file()))
    return files


def ensure_patch_log(conn) -> None:
    sql = PATCH_LOG_BOOTSTRAP.read_text(encoding="utf-8")
    conn.connection.cursor().execute(sql)


def patch_group_for(path: Path) -> str:
    return path.parent.name


def patch_already_applied(conn, patch_name: str, checksum: str) -> bool:
    row = conn.execute(
        text("SELECT checksum_sha256 FROM meta.sql_patch_log WHERE patch_name = :patch_name AND success = TRUE"),
        {"patch_name": patch_name},
    ).fetchone()
    if not row:
        return False
    prior = row._mapping["checksum_sha256"]
    if prior != checksum:
        raise RuntimeError(f"Patch name already applied with different checksum: {patch_name}")
    return True


def apply_patch(engine, path: Path, *, dry_run: bool = False) -> str:
    sql = path.read_text(encoding="utf-8")
    checksum = sha256_text(sql)
    patch_name = path.name
    patch_group = patch_group_for(path)
    with engine.begin() as conn:
        ensure_patch_log(conn)
        if patch_group == "verify":
            if dry_run:
                return f"verify-dry-run {patch_name}"
            result = conn.execute(text(sql))
            rows = result.fetchall() if result.returns_rows else []
            return f"verified {patch_name}: {len(rows)} row(s)"
        if patch_already_applied(conn, patch_name, checksum):
            return f"skipped {patch_name}"
        if dry_run:
            return f"dry-run {patch_name}"
        started = time.perf_counter()
        conn.connection.cursor().execute(sql)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        conn.execute(
            text("""
                INSERT INTO meta.sql_patch_log(
                    patch_name, patch_group, checksum_sha256, execution_ms, notes
                ) VALUES (
                    :patch_name, :patch_group, :checksum_sha256, :execution_ms, :notes
                )
            """),
            {
                "patch_name": patch_name,
                "patch_group": patch_group,
                "checksum_sha256": checksum,
                "execution_ms": elapsed_ms,
                "notes": str(path),
            },
        )
        return f"applied {patch_name} ({elapsed_ms} ms)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply SQL-first bootstrap/patch/verify files to a target DB")
    parser.add_argument("--db-url", required=True, help="Target DB URL")
    parser.add_argument("--root", default="db/sql", help="SQL root directory")
    parser.add_argument("--groups", nargs="+", default=["bootstrap", "patches"], choices=list(VALID_ROOTS))
    parser.add_argument("--include", nargs="*", default=[], help="Specific sql files relative to root")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    files = [root / rel for rel in args.include] if args.include else sorted_sql_files(root, args.groups)
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        raise SystemExit(f"Missing SQL files: {', '.join(missing)}")

    engine = create_engine(args.db_url, future=True)
    for path in files:
        print(apply_patch(engine, path, dry_run=args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
