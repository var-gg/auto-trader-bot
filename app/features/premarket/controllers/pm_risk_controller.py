from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.premarket.controllers.pm_history_batch_controller import require_internal_scheduler_auth
from app.features.premarket.services.headline_risk_service import HeadlineRiskService

router = APIRouter(prefix="/api/premarket/risk", tags=["premarket"])


@router.post("/refresh")
def refresh_risk_snapshot(
    scope: str = Query(default="GLOBAL", pattern="^(KR|US|GLOBAL)$"),
    window_minutes: int = Query(default=720, ge=60, le=1440),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    try:
        svc = HeadlineRiskService(db)
        out = svc.refresh_snapshot(scope=scope, window_minutes=window_minutes)
        return {"ok": True, **out}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"risk refresh failed: {e}")


@router.get("/latest")
def get_latest_risk_snapshot(
    scope: str = Query(default="GLOBAL", pattern="^(KR|US|GLOBAL)$"),
    _auth: None = Depends(require_internal_scheduler_auth),
    db: Session = Depends(get_db),
):
    svc = HeadlineRiskService(db)
    row = svc.get_latest_active_snapshot(scope=scope)
    return {"ok": True, "scope": scope, "snapshot": row}
