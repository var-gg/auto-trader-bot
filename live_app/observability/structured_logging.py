from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("live_app.structured")


def emit_live_event(event_type: str, **fields: Any) -> Dict[str, Any]:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        **fields,
    }
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
    return payload


def build_live_run_log(
    *,
    run_id: str,
    slot: str,
    command: str,
    strategy_version: str | None = None,
    decision_summary: Dict[str, Any] | None = None,
    risk_reject_reason: str | None = None,
    order_batch_id: str | None = None,
    order_plan_id: str | None = None,
    broker_request_id: str | None = None,
    broker_response_id: str | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return emit_live_event(
        "live_run",
        run_id=run_id,
        slot=slot,
        command=command,
        strategy_version=strategy_version,
        decision_summary=decision_summary or {},
        risk_reject_reason=risk_reject_reason,
        order_batch_id=order_batch_id,
        order_plan_id=order_plan_id,
        broker_request_id=broker_request_id,
        broker_response_id=broker_response_id,
        extra=extra or {},
    )
