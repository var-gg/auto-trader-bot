from __future__ import annotations


def execute_research_backtest(*args, **kwargs):
    from backtest_app.runner import cli as legacy_cli
    return legacy_cli.run_backtest(*args, **kwargs)
