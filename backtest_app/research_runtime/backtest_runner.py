from __future__ import annotations

from backtest_app.runner.cli import run_backtest as _legacy_run_backtest


def run_research_backtest(*args, **kwargs):
    return _legacy_run_backtest(*args, **kwargs)
