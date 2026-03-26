import pytest

from scripts import refresh_local_trading as mirror


class FakeResult:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, existing_tables=None, counts=None):
        self.existing_tables = existing_tables or set()
        self.counts = counts or {}
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append((sql, params))
        if "information_schema.tables" in sql:
            key = f"{params['schema']}.{params['table']}"
            row = {"exists": 1} if key in self.existing_tables else None
            return FakeResult(type("Row", (), {"_mapping": row})() if row else None)
        if "COUNT(*) AS row_count" in sql:
            row_count = self.counts.get(sql, 0)
            return FakeResult(type("Row", (), {"_mapping": {"row_count": row_count}})())
        if "SELECT table_name, last_cursor_text" in sql:
            return FakeResult(None)
        return FakeResult(None)


def test_source_sql_must_be_select_only():
    with pytest.raises(ValueError):
        mirror.ensure_select_only("DELETE FROM trading.ticker")

    with pytest.raises(ValueError):
        mirror.ensure_select_only("SELECT * FROM trading.ticker; DELETE FROM trading.ticker")


def test_exclude_prefixes_are_enforced():
    spec = {"source_table": "trading.order_live", "name": "blocked"}
    with pytest.raises(ValueError):
        mirror.ensure_not_excluded(spec, ["trading.order_", "trading.fill"])


def test_incremental_cursor_appends_after_existing_where_and_replaces_order_by():
    sql = "SELECT * FROM trading.ohlcv_daily WHERE symbol = 'AAPL' ORDER BY updated_at DESC"
    built = mirror.append_incremental_cursor(sql, "updated_at")
    assert "WHERE symbol = 'AAPL' AND updated_at > :last_cursor" in built
    assert built.endswith("ORDER BY updated_at")
    assert "DESC" not in built


def test_build_source_sql_uses_incremental_cursor_with_existing_where():
    spec = {
        "source_sql": "SELECT * FROM trading.ohlcv_daily WHERE symbol = 'AAPL' ORDER BY updated_at DESC",
        "refresh_strategy": "cursor_upsert",
        "cursor_column": "updated_at",
    }
    sql, params = mirror.build_source_sql(spec, "refresh-market", {"last_cursor_text": "2026-01-01T00:00:00+00:00"})
    assert "updated_at > :last_cursor" in sql
    assert params["last_cursor"] == "2026-01-01T00:00:00+00:00"


def test_preflight_fails_when_target_table_missing():
    source_conn = FakeConn(existing_tables={"trading.ticker"})
    local_conn = FakeConn(existing_tables=set())
    spec = {
        "name": "ticker",
        "source_table": "trading.ticker",
        "target_table": "trading.bt_mirror_ticker",
        "source_sql": "SELECT id, symbol FROM trading.ticker",
    }
    with pytest.raises(RuntimeError, match="Target table does not exist"):
        mirror.preflight_spec(source_conn, local_conn, spec, [])


def test_dry_run_reports_estimated_rows_and_cursor_state(monkeypatch):
    source_conn = FakeConn(counts={"SELECT COUNT(*) AS row_count FROM (SELECT * FROM trading.ticker) AS mirror_source": 42})
    local_conn = FakeConn()
    monkeypatch.setattr(mirror, "get_state", lambda *_args, **_kwargs: {"last_cursor_text": "abc"})
    spec = {
        "name": "ticker",
        "source_table": "trading.ticker",
        "target_table": "trading.bt_mirror_ticker",
        "source_sql": "SELECT * FROM trading.ticker",
        "refresh_strategy": "full",
    }
    result = mirror.dry_run_table(source_conn, local_conn, spec, "refresh-reference")
    assert result["estimated_rows"] == 42
    assert result["cursor_before"] == "abc"
    assert result["target_table"] == "trading.bt_mirror_ticker"
