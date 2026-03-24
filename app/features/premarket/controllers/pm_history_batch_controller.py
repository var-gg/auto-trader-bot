from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.db import get_db
from live_app.application.context import RunContext
from live_app.application.history_commands import BackfillUnfilledReasonsCommand, ComputeOutcomesCommand, RunHistoryPostprocessCommand
from live_app.observability.structured_logging import build_live_run_log

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/premarket/history",
    tags=["premarket"],
)


class PMHistoryBatchRequest(BaseModel):
    lookback_days: int = Field(default=7, ge=1, le=90)
    limit: int = Field(default=2000, ge=1, le=20000)


class PMHistoryOutcomeRequest(BaseModel):
    lookback_days: int = Field(default=14, ge=1, le=180)
    limit: int = Field(default=5000, ge=1, le=50000)


class PMHistoryPostprocessRequest(BaseModel):
    backfill_lookback_days: int = Field(default=7, ge=1, le=90)
    backfill_limit: int = Field(default=2000, ge=1, le=20000)
    outcome_lookback_days: int = Field(default=14, ge=1, le=180)
    outcome_limit: int = Field(default=5000, ge=1, le=50000)


def require_internal_scheduler_auth(
    x_scheduler_token: str | None = Header(default=None, alias="X-Scheduler-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    configured = os.getenv("SCHEDULER_INTERNAL_TOKEN") or os.getenv("INTERNAL_API_TOKEN")
    if not configured:
        logger.error("Scheduler auth token is not configured")
        raise HTTPException(status_code=503, detail="scheduler auth is not configured")
    bearer = authorization[7:].strip() if authorization and authorization.lower().startswith("bearer ") else None
    provided = x_scheduler_token or bearer
    if not provided or not hmac.compare_digest(str(provided), str(configured)):
        raise HTTPException(status_code=401, detail="unauthorized")


def _ctx(command: str) -> RunContext:
    return RunContext(actor="scheduler", channel="internal", metadata={"slot": "HOUSEKEEPING", "command": command, "strategy_version": "pm-core-v2"})


@router.post("/backfill-unfilled-reasons")
def backfill_unfilled_reasons(
    request: PMHistoryBatchRequest = Body(default=PMHistoryBatchRequest()),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    try:
        summary = BackfillUnfilledReasonsCommand(db).execute(lookback_days=request.lookback_days, limit=request.limit, ctx=_ctx("history.backfill_unfilled"))
        build_live_run_log(
            run_id="housekeeping-backfill",
            slot="HOUSEKEEPING",
            command="history.backfill_unfilled",
            strategy_version="pm-core-v2",
            decision_summary={"processed": summary.scanned, "updated": summary.updated, "skipped": summary.unresolved},
        )
        return {"processed": summary.scanned, "updated": summary.updated, "skipped": summary.unresolved, "errors": 0}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PM history backfill failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="pm history backfill failed")


@router.post("/compute-outcomes")
def compute_outcomes(
    request: PMHistoryOutcomeRequest = Body(default=PMHistoryOutcomeRequest()),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    try:
        summary = ComputeOutcomesCommand(db).execute(lookback_days=request.lookback_days, limit=request.limit, ctx=_ctx("history.compute_outcomes"))
        build_live_run_log(
            run_id="housekeeping-outcomes",
            slot="HOUSEKEEPING",
            command="history.compute_outcomes",
            strategy_version="pm-core-v2",
            decision_summary={"processed": summary.scanned, "updated": summary.upserted, "skipped": summary.skipped_missing_price},
        )
        return {"processed": summary.scanned, "updated": summary.upserted, "skipped": summary.skipped_missing_price, "errors": 0}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PM history outcome compute failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="pm history outcome compute failed")


@router.post("/postprocess")
def run_postprocess(
    request: PMHistoryPostprocessRequest = Body(default=PMHistoryPostprocessRequest()),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    try:
        summary = RunHistoryPostprocessCommand(db).execute(
            backfill_lookback_days=request.backfill_lookback_days,
            backfill_limit=request.backfill_limit,
            outcome_lookback_days=request.outcome_lookback_days,
            outcome_limit=request.outcome_limit,
            ctx=_ctx("history.postprocess"),
        )
        build_live_run_log(
            run_id="housekeeping-postprocess",
            slot="HOUSEKEEPING",
            command="history.postprocess",
            strategy_version="pm-core-v2",
            decision_summary={
                "unfilled_processed": summary.unfilled.scanned,
                "unfilled_updated": summary.unfilled.updated,
                "outcomes_processed": summary.outcomes.scanned,
                "outcomes_updated": summary.outcomes.upserted,
            },
        )
        return {
            "ok": True,
            "unfilled": {"processed": summary.unfilled.scanned, "updated": summary.unfilled.updated, "skipped": summary.unfilled.unresolved},
            "outcomes": {"processed": summary.outcomes.scanned, "updated": summary.outcomes.upserted, "skipped": summary.outcomes.skipped_missing_price},
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PM history postprocess failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="pm history postprocess failed")
