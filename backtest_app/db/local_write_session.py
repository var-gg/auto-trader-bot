from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .local_session import BACKTEST_SEARCH_PATH, LocalBacktestDbConfig, guard_backtest_local_only


def create_backtest_write_engine(config: LocalBacktestDbConfig | None = None) -> Engine:
    from sqlalchemy import create_engine

    cfg = config or LocalBacktestDbConfig.from_env()
    db_url = guard_backtest_local_only(cfg.url)
    engine = create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        connect_args={"options": f"-c search_path={cfg.search_path or BACKTEST_SEARCH_PATH}"},
    )

    @event.listens_for(engine, "connect")
    def _set_session_config(dbapi_connection, connection_record):  # pragma: no cover
        with dbapi_connection.cursor() as cur:
            cur.execute(f"SET search_path TO {cfg.search_path or BACKTEST_SEARCH_PATH};")

    return engine


def create_backtest_write_session_factory(config: LocalBacktestDbConfig | None = None) -> sessionmaker[Session]:
    engine = create_backtest_write_engine(config)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def local_write_session_scope(config: LocalBacktestDbConfig | None = None):
    session_factory = create_backtest_write_session_factory(config)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
