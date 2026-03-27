-- Purpose: persist reusable bt_event_window scenario snapshots outside run_id naming.
-- Scope: local + production SQL-first schema evolution.

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.bt_scenario_snapshot_manifest (
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
    copied_from_scenario_id TEXT,
    CONSTRAINT ck_bt_scenario_snapshot_manifest_phase CHECK (phase IN ('discovery', 'holdout')),
    CONSTRAINT ck_bt_scenario_snapshot_manifest_source_kind CHECK (source_kind IN ('import-json', 'import-jsonl', 'import-csv', 'copy'))
);

CREATE INDEX IF NOT EXISTS ix_bt_scenario_snapshot_manifest_phase_market
    ON meta.bt_scenario_snapshot_manifest(phase, market, window_start, window_end);

CREATE INDEX IF NOT EXISTS ix_bt_scenario_snapshot_manifest_created_at
    ON meta.bt_scenario_snapshot_manifest(created_at DESC);
