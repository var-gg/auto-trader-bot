from __future__ import annotations

import hashlib
import json
from typing import Any

from backtest_app.configs.models import BacktestConfig, BacktestScenario, build_research_manifest


def build_data_snapshot_id(*, scenario: BacktestScenario, config: BacktestConfig, data_source: str, historical_metadata: dict[str, Any] | None = None) -> str:
    payload = {
        "scenario_id": scenario.scenario_id,
        "market": scenario.market,
        "start_date": scenario.start_date,
        "end_date": scenario.end_date,
        "symbols": list(scenario.symbols),
        "data_source": data_source,
        "research_spec": (config.research_spec.to_dict() if config.research_spec else None),
        "historical_source": dict(historical_metadata or {}),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]


def run_research_backtest(run_callable, *, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=True):
    return run_callable(request=request, data_path=data_path, data_source=data_source, scenario_id=scenario_id, strategy_mode=strategy_mode, enable_validation=enable_validation)


def ensure_manifest(*, request, data_source: str, historical_metadata: dict[str, Any] | None = None):
    if request.config.manifest is not None:
        return request.config.manifest
    snapshot_id = build_data_snapshot_id(scenario=request.scenario, config=request.config, data_source=data_source, historical_metadata=historical_metadata)
    return build_research_manifest(scenario=request.scenario, config=request.config, data_snapshot_id=snapshot_id)
