from scripts import refresh_local_trading as mod


def test_tables_for_mode_only_returns_whitelisted_entries():
    cfg = {
        "mirror_tables": [
            {"name": "ticker", "refresh_group": "reference"},
            {"name": "ohlcv_daily", "refresh_group": "market"},
        ],
        "exclude_prefixes": ["trading.order_", "trading.execution"],
    }
    selected = mod.tables_for_mode(cfg, "refresh-reference")
    names = [t["name"] for t in selected]
    assert names == ["ticker"]
    assert "order_fill" not in names


def test_refresh_table_uses_only_spec_sql(monkeypatch):
    executed = []

    class SourceConn:
        def execute(self, stmt, params=None):
            executed.append((str(stmt), params))
            return []

    class LocalConn:
        connection = type("Dummy", (), {"cursor": lambda self: type("Cur", (), {"execute": lambda self, sql: None})()})()

        def execute(self, stmt, params=None):
            text_stmt = str(stmt)
            if text_stmt.startswith("SELECT table_name"):
                return type("Res", (), {"fetchone": lambda self: None})()
            if "RETURNING run_id" in text_stmt:
                return type("Res", (), {"fetchone": lambda self: type("Row", (), {"_mapping": {"run_id": 1}})()})()
            return type("Res", (), {})()

    spec = {
        "name": "ticker",
        "source_sql": "SELECT id, symbol FROM trading.ticker",
        "insert_sql": "INSERT INTO trading.bt_mirror_ticker(ticker_id, symbol) VALUES (:id, :symbol)",
        "target_table": "trading.bt_mirror_ticker",
    }
    out = mod.refresh_table(SourceConn(), LocalConn(), spec, "refresh-reference")
    assert out["table"] == "ticker"
    assert executed[0][0].strip().startswith("SELECT id, symbol FROM trading.ticker")
    assert all("order" not in sql.lower() for sql, _ in executed)
