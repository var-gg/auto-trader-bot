from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("backtest_app.structured")


def emit_backtest_event(event_type: str, **fields: Any) -> Dict[str, Any]:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        **fields,
    }
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
    return payload


def build_backtest_run_log(
    *,
    scenario_id: str,
    data_range: str,
    parameter_hash: str,
    score_summary: Dict[str, Any] | None = None,
    fill_summary: Dict[str, Any] | None = None,
    strategy_version: str | None = None,
    feature_version: str | None = None,
    seed: int | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return emit_backtest_event(
        "backtest_run",
        scenario_id=scenario_id,
        data_range=data_range,
        parameter_hash=parameter_hash,
        score_summary=score_summary or {},
        fill_summary=fill_summary or {},
        strategy_version=strategy_version,
        feature_version=feature_version,
        seed=seed,
        extra=extra or {},
    )
