# SQL-first DB management

## Principles
- DDL is managed only by checked-in SQL files under `db/sql/`.
- Python executes SQL; Python does not generate schema.
- ORM is mapping-only.
- `create_all`, autogenerate, runtime DDL, and Alembic are forbidden for app/runtime paths.
- Production/live paths must not implicitly create schemas or tables.

## Why not Alembic
- We need SQL files to remain the full, reviewable source of truth.
- Local backtest/research schema must be reconstructable without hidden migration state.
- Explicit SQL patches make the live/local split easier to audit than autogenerate workflows.
- The goal here is deterministic schema application, not runtime schema evolution convenience.

## Directory layout
- `db/sql/bootstrap/` — prerequisite SQL that must exist before normal patches
- `db/sql/patches/` — ordered schema/data patches
- `db/sql/verify/` — read-only verification SQL

## Naming rules
- Use lexicographically sortable names.
- Format: `NNN_short_description.sql` for bootstrap/verify, and `YYYYMMDDHHMM_short_description.sql` or `NNN_short_description.sql` for patches.
- Patch filename is the canonical patch id stored in `meta.sql_patch_log.patch_name`.
- Never rename an already-applied patch file.

## Apply rules
1. Bootstrap first.
2. Patches second.
3. Verify optionally, last.
4. Patch SQL should be idempotent when practical (`IF NOT EXISTS`, guarded inserts, safe updates).
5. If a patch cannot be naturally idempotent, make its preconditions explicit and let the runner guard with `sql_patch_log`.
6. One patch file should represent one coherent rollback unit.

## Patch log
`meta.sql_patch_log` fields:
- `patch_name` TEXT PRIMARY KEY
- `patch_group` TEXT NOT NULL
- `checksum_sha256` TEXT NOT NULL
- `applied_at` TIMESTAMPTZ NOT NULL DEFAULT NOW()
- `applied_by` TEXT NOT NULL DEFAULT CURRENT_USER
- `tool_name` TEXT NOT NULL DEFAULT 'scripts/db_apply_sql.py'
- `success` BOOLEAN NOT NULL DEFAULT TRUE
- `execution_ms` INTEGER
- `notes` TEXT

## Runner
Apply bootstrap + patches:
```bash
python scripts/db_apply_sql.py --db-url "$DB_URL"
```

Apply only verify SQL:
```bash
python scripts/db_apply_sql.py --db-url "$DB_URL" --groups verify
```

Apply a specific patch list:
```bash
python scripts/db_apply_sql.py --db-url "$DB_URL" --include bootstrap/000_meta_sql_patch_log.sql patches/202603252315_sql_first_cutover.sql
```

## Rollback principle
- Failed patch execution rolls back the active transaction; the patch is not logged.
- Rollback is SQL-first too: ship an explicit compensating patch rather than editing history.
- Do not delete or rewrite rows in `meta.sql_patch_log` to fake rollback.

## ORM rule
- SQLAlchemy models exist only for mapping/querying existing tables.
- New tables/columns/indexes must be introduced through `db/sql/*.sql`, never through ORM metadata side effects.
