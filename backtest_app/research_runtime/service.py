from __future__ import annotations

from backtest_app.research_runtime.engine import run_backtest
from backtest_app.research_runtime.optuna_runner import OptunaResearchRunner
from backtest_app.validation import run_fold_validation


def execute_research_backtest(*args, **kwargs):
    return run_backtest(*args, **kwargs)


def execute_research_study(*, request, runner_fn=run_backtest, validation_fn=run_fold_validation, **kwargs):
    output_dir = kwargs.get("output_dir") or "."
    return OptunaResearchRunner(output_dir).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_path=kwargs.get("data_path"), data_source=kwargs.get("data_source", "local-db"), strategy_mode=kwargs.get("strategy_mode", "research_similarity_v2"))
