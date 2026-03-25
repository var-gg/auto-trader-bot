from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import URL, create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

LOCAL_HOST_ALLOWLIST = {"localhost", "127.0.0.1"}
BACKTEST_SEARCH_PATH = "trading,bt_result,meta,public"


@dataclass(frozen=True)
class LocalBacktestDbConfig:
    url: str
    schema: str = "trading"
    search_path: str = BACKTEST_SEARCH_PATH

    @classmethod
    def from_env(cls) -> "LocalBacktestDbConfig":
        db_url = os.getenv("BACKTEST_DB_URL", "").strip()
        if db_url:
            return cls(
                url=db_url,
                schema=os.getenv("BACKTEST_DB_SCHEMA", "trading"),
                search_path=os.getenv("BACKTEST_DB_SEARCH_PATH", BACKTEST_SEARCH_PATH),
            )

        user = os.getenv("BACKTEST_DB_USER", "postgres")
        password = os.getenv("BACKTEST_DB_PASSWORD", "")
        host = os.getenv("BACKTEST_DB_HOST", "127.0.0.1")
        port = int(os.getenv("BACKTEST_DB_PORT", "5432"))
        name = os.getenv("BACKTEST_DB_NAME", "auto_trader_backtest")
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=name,
        ).render_as_string(hide_password=False)
        return cls(url=url, schema=os.getenv("BACKTEST_DB_SCHEMA", "trading"), search_path=os.getenv("BACKTEST_DB_SEARCH_PATH", BACKTEST_SEARCH_PATH))


def guard_backtest_local_only(db_url: str) -> str:
    lowered = db_url.lower().strip()
    if not lowered:
        raise ValueError("BACKTEST_DB_URL is required for local-db mode")
    if "/cloudsql/" in lowered or "googleapis.com" in lowered:
        raise ValueError("Cloud SQL URLs are forbidden for backtest_app local-db mode")
    if any(k in lowered for k in ("instance_connection_name", "db_user", "db_pass")):
        raise ValueError("Live DB wiring markers detected in BACKTEST_DB_URL")
    if lowered.startswith("postgresql") and "@" in lowered:
        host_part = lowered.split("@", 1)[1].split("/", 1)[0]
        host = host_part.split(":", 1)[0]
        require_local = os.getenv("BACKTEST_DB_REQUIRE_LOCAL", "true").lower() == "true"
        if require_local and host not in LOCAL_HOST_ALLOWLIST:
            raise ValueError(f"Non-local host forbidden for backtest_app: {host}")
    if os.getenv("INSTANCE_CONNECTION_NAME") and os.getenv("BACKTEST_ALLOW_LIVE_ENV", "false").lower() != "true":
        raise ValueError("Live Cloud SQL env detected during backtest; refusing local-db session creation")
    return db_url


def create_backtest_engine(config: LocalBacktestDbConfig | None = None) -> Engine:
    cfg = config or LocalBacktestDbConfig.from_env()
    db_url = guard_backtest_local_only(cfg.url)
    engine = create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        connect_args={"options": f"-c search_path={cfg.search_path} -c default_transaction_read_only=on"},
    )

    @event.listens_for(engine, "connect")
    def _set_readonly_session(dbapi_connection, connection_record):  # pragma: no cover
        with dbapi_connection.cursor() as cur:
            cur.execute(f"SET search_path TO {cfg.search_path};")
            cur.execute("SET default_transaction_read_only = on;")

    return engine


def create_backtest_session_factory(config: LocalBacktestDbConfig | None = None) -> sessionmaker[Session]:
    engine = create_backtest_engine(config)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def local_session_scope(config: LocalBacktestDbConfig | None = None):
    session_factory = create_backtest_session_factory(config)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
