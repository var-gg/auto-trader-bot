from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
import logging

from app.core.db import get_db
from app.features.kis_test.models.kis_test_models import BootstrapRequest, BootstrapResponse
from live_app.application.bootstrap_commands import RunBootstrapCommand
from live_app.application.context import RunContext

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/kis-test/bootstrap", tags=["KIS Bootstrap"])


@router.get("", response_model=BootstrapResponse, summary="장전 기초데이터 일괄 갱신")
async def run_bootstrap(
    skip_token_refresh: bool = Query(False, description="토큰 갱신 단계 스킵 여부"),
    skip_fred_ingest: bool = Query(False, description="FRED 데이터 수집 단계 스킵 여부"),
    skip_yahoo_ingest: bool = Query(False, description="Yahoo Finance 데이터 수집 단계 스킵 여부"),
    skip_risk_refresh: bool = Query(False, description="프리마켓 리스크 스냅샷 갱신 단계 스킵 여부"),
    skip_signal_update: bool = Query(False, description="시그널 갱신 단계 스킵 여부"),
    token_threshold_hours: int = Query(24, ge=1, le=168, description="토큰 갱신 임계 시간 (시간 단위)"),
    fred_lookback_days: int = Query(30, ge=1, le=365, description="FRED 데이터 수집 기간 (일 단위)"),
    yahoo_period: str = Query("1mo", description="Yahoo Finance 데이터 수집 기간"),
    db: Session = Depends(get_db),
):
    try:
        request = BootstrapRequest(
            skip_token_refresh=skip_token_refresh,
            skip_fred_ingest=skip_fred_ingest,
            skip_yahoo_ingest=skip_yahoo_ingest,
            skip_risk_refresh=skip_risk_refresh,
            skip_signal_update=skip_signal_update,
            token_threshold_hours=token_threshold_hours,
            fred_lookback_days=fred_lookback_days,
            yahoo_period=yahoo_period,
        )
        ctx = RunContext(
            actor="http",
            channel="live_app.api",
            metadata={
                "route": "/kis-test/bootstrap",
                "slot": "US_PREOPEN",
                "strategy_version": "pm-core-v2",
            },
        )
        logger.info("🚀 장전 기초데이터 일괄 갱신 요청 수신")
        return await RunBootstrapCommand(db).execute(request, ctx)
    except Exception as e:
        logger.error(f"❌ 장전 기초데이터 일괄 갱신 중 오류: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Bootstrap 실행 중 오류가 발생했습니다: {str(e)}")


@router.get("/health", summary="Bootstrap 서비스 상태 확인")
async def health_check():
    return {
        "status": "healthy",
        "service": "bootstrap",
        "description": "장전 기초데이터 일괄 갱신 서비스",
        "steps": [
            "1. KIS 토큰 갱신",
            "2. FRED 매크로 데이터 수집",
            "3. Yahoo Finance 데이터 수집",
            "4. 프리마켓 리스크 스냅샷 갱신",
            "5. 프리마켓 시그널 갱신",
        ],
    }
