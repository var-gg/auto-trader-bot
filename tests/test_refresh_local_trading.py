import json
from pathlib import Path

from scripts import refresh_local_trading as mod


def test_tables_for_mode_filters_groups(tmp_path):
    cfg = {
        "mirror_tables": [
            {"name": "ticker", "refresh_group": "reference"},
            {"name": "ohlcv_daily", "refresh_group": "market"},
        ]
    }
    assert [t["name"] for t in mod.tables_for_mode(cfg, "refresh-reference")] == ["ticker"]
    assert [t["name"] for t in mod.tables_for_mode(cfg, "refresh-market")] == ["ohlcv_daily"]
    assert len(mod.tables_for_mode(cfg, "init-full")) == 2


def test_build_source_sql_uses_incremental_cursor():
    spec = {
        "source_sql": "SELECT * FROM trading.ohlcv_daily",
        "refresh_strategy": "cursor_upsert",
        "cursor_column": "updated_at",
    }
    sql, params = mod.build_source_sql(spec, "refresh-market", {"last_cursor_text": "2026-03-25T00:00:00+00:00"})
    assert "WHERE updated_at > :last_cursor" in sql
    assert params["last_cursor"] == "2026-03-25T00:00:00+00:00"
