from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.premarket.services.pm_history_batch_service import PMHistoryBatchService

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
    """Scheduler internal endpoint guard (shared-token style)."""
    configured = os.getenv("SCHEDULER_INTERNAL_TOKEN") or os.getenv("INTERNAL_API_TOKEN")

    if not configured:
        logger.error("Scheduler auth token is not configured")
        raise HTTPException(status_code=503, detail="scheduler auth is not configured")

    bearer = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization[7:].strip()

    provided = x_scheduler_token or bearer

    if not provided or not hmac.compare_digest(str(provided), str(configured)):
        raise HTTPException(status_code=401, detail="unauthorized")


@router.post("/backfill-unfilled-reasons")
def backfill_unfilled_reasons(
    request: PMHistoryBatchRequest = Body(default=PMHistoryBatchRequest()),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    try:
        svc = PMHistoryBatchService(db)
        summary = svc.backfill_unfilled_reasons(
            lookback_days=request.lookback_days,
            limit=request.limit,
        )
        return {
            "processed": summary.scanned,
            "updated": summary.updated,
            "skipped": summary.unresolved,
            "errors": 0,
        }
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
        svc = PMHistoryBatchService(db)
        summary = svc.compute_tplus_outcomes(
            lookback_days=request.lookback_days,
            limit=request.limit,
        )
        return {
            "processed": summary.scanned,
            "updated": summary.upserted,
            "skipped": summary.skipped_missing_price,
            "errors": 0,
        }
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
        svc = PMHistoryBatchService(db)
        summary = svc.run_postprocess(
            backfill_lookback_days=request.backfill_lookback_days,
            backfill_limit=request.backfill_limit,
            outcome_lookback_days=request.outcome_lookback_days,
            outcome_limit=request.outcome_limit,
        )
        return {
            "ok": True,
            "unfilled": {
                "processed": summary.unfilled.scanned,
                "updated": summary.unfilled.updated,
                "skipped": summary.unfilled.unresolved,
            },
            "outcomes": {
                "processed": summary.outcomes.scanned,
                "updated": summary.outcomes.upserted,
                "skipped": summary.outcomes.skipped_missing_price,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("PM history postprocess failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="pm history postprocess failed")
