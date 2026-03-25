from pathlib import Path

import pytest

from scripts import db_apply_sql as mod


class DummyCursor:
    def __init__(self, parent):
        self.parent = parent

    def execute(self, sql):
        self.parent.executed_sql.append(sql)


class DummyDBConn:
    def __init__(self, parent):
        self.parent = parent

    def cursor(self):
        return DummyCursor(self.parent)


class DummyConn:
    def __init__(self):
        self.executed_sql = []
        self.patch_rows = {}
        self.inserted = []
        self.connection = DummyDBConn(self)

    def execute(self, stmt, params=None):
        text_stmt = str(stmt)
        if "SELECT checksum_sha256 FROM meta.sql_patch_log" in text_stmt:
            patch_name = params["patch_name"]
            checksum = self.patch_rows.get(patch_name)
            if checksum is None:
                return DummyResult(None)
            return DummyResult({"checksum_sha256": checksum})
        if "INSERT INTO meta.sql_patch_log" in text_stmt:
            self.patch_rows[params["patch_name"]] = params["checksum_sha256"]
            self.inserted.append(params["patch_name"])
            return DummyResult(None)
        if text_stmt.strip().startswith("SELECT"):
            return DummyResult(None, returns_rows=True, rows=[])
        return DummyResult(None)


class DummyResult:
    def __init__(self, row, returns_rows=False, rows=None):
        self._row = row
        self.returns_rows = returns_rows
        self._rows = rows or []

    def fetchone(self):
        if self._row is None:
            return None
        return type("Row", (), {"_mapping": self._row})()

    def fetchall(self):
        return self._rows


class DummyBegin:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return DummyBegin(self.conn)


def test_sorted_sql_files_orders_groups(tmp_path):
    root = tmp_path / "db" / "sql"
    (root / "bootstrap").mkdir(parents=True)
    (root / "patches").mkdir(parents=True)
    (root / "bootstrap" / "001_a.sql").write_text("select 1", encoding="utf-8")
    (root / "patches" / "002_b.sql").write_text("select 2", encoding="utf-8")
    files = mod.sorted_sql_files(root, ["bootstrap", "patches"])
    assert [p.name for p in files] == ["001_a.sql", "002_b.sql"]


def test_apply_patch_skips_duplicate(monkeypatch, tmp_path):
    patch = tmp_path / "001_test.sql"
    patch.write_text("SELECT 1;", encoding="utf-8")
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(mod, "PATCH_LOG_BOOTSTRAP", patch)
    first = mod.apply_patch(engine, patch)
    second = mod.apply_patch(engine, patch)
    assert first.startswith("applied 001_test.sql")
    assert second == "skipped 001_test.sql"
    assert conn.inserted == ["001_test.sql"]


def test_apply_patch_detects_checksum_mismatch(monkeypatch, tmp_path):
    bootstrap = tmp_path / "000_bootstrap.sql"
    bootstrap.write_text("SELECT 1;", encoding="utf-8")
    patch = tmp_path / "001_test.sql"
    patch.write_text("SELECT 1;", encoding="utf-8")
    conn = DummyConn()
    engine = DummyEngine(conn)
    monkeypatch.setattr(mod, "PATCH_LOG_BOOTSTRAP", bootstrap)
    mod.apply_patch(engine, patch)
    patch.write_text("SELECT 2;", encoding="utf-8")
    with pytest.raises(RuntimeError):
        mod.apply_patch(engine, patch)
