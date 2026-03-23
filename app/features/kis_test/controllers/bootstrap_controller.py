# app/features/kis_test/controllers/bootstrap_controller.py

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.kis_test.models.kis_test_models import (
    BootstrapRequest,
    BootstrapResponse,
)
from app.features.kis_test.services.bootstrap_service import BootstrapService
import logging

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
    """
    장전 기초데이터 일괄 갱신

    장전(pre-market)에 필요한 모든 기초 데이터를 순차적으로 갱신합니다.
    **인자 없이 호출하면 모든 단계를 디폴트 설정으로 실행합니다.**

    **실행 순서:**
    1. **KIS 토큰 갱신** (`/kis-test/token/refresh`)
       - 만료 임박 토큰을 자동으로 갱신
       - 기본값: 24시간 이내 만료 예정 토큰

    2. **FRED 매크로 데이터 수집** (`/macro/ingest/fred/bootstrap`)
       - 미국 경제 지표 수집 (인플레이션, 노동, 성장, 금리 등)
       - 기본값: 최근 30일치 데이터

    3. **Yahoo Finance 데이터 수집** (`/yahoo-finance/ingest`)
       - S&P 500 티커들의 시세 데이터 수집
       - 기본값: 최근 1개월(1mo) 데이터

    4. **프리마켓 리스크 스냅샷 갱신** (`/api/premarket/risk/refresh` 내부 서비스 호출)
       - KR/US 헤드라인 리스크 snapshot 생성
       - PM BUY discount / PM SELL markup 계산 근거 갱신

    5. **프리마켓 시그널 갱신** (`/api/premarket/signals/update` 내부 서비스 호출)
       - 최신 데이터 기반 매매 시그널 생성
       - 미국/한국 전체 종목 처리

    **Query Parameters (모두 선택사항):**
    - skip_token_refresh: 토큰 갱신 단계 스킵 (기본값: false)
    - skip_fred_ingest: FRED 데이터 수집 단계 스킵 (기본값: false)
    - skip_yahoo_ingest: Yahoo Finance 데이터 수집 단계 스킵 (기본값: false)
    - skip_risk_refresh: 프리마켓 리스크 스냅샷 갱신 단계 스킵 (기본값: false)
    - skip_signal_update: 시그널 갱신 단계 스킵 (기본값: false)
    - token_threshold_hours: 토큰 갱신 임계 시간 (기본값: 24시간)
    - fred_lookback_days: FRED 데이터 수집 기간 (기본값: 30일)
    - yahoo_period: Yahoo Finance 데이터 수집 기간 (기본값: 1mo)
    """
    try:
        bootstrap_request = BootstrapRequest(
            skip_token_refresh=skip_token_refresh,
            skip_fred_ingest=skip_fred_ingest,
            skip_yahoo_ingest=skip_yahoo_ingest,
            skip_risk_refresh=skip_risk_refresh,
            skip_signal_update=skip_signal_update,
            token_threshold_hours=token_threshold_hours,
            fred_lookback_days=fred_lookback_days,
            yahoo_period=yahoo_period,
        )

        logger.info("🚀 장전 기초데이터 일괄 갱신 요청 수신")
        logger.info(f"  - 토큰 갱신: {'스킵' if skip_token_refresh else '실행'}")
        logger.info(f"  - FRED 수집: {'스킵' if skip_fred_ingest else f'{fred_lookback_days}일'}")
        logger.info(f"  - Yahoo 수집: {'스킵' if skip_yahoo_ingest else yahoo_period}")
        logger.info(f"  - 리스크 갱신: {'스킵' if skip_risk_refresh else '실행'}")
        logger.info(f"  - 시그널 갱신: {'스킵' if skip_signal_update else '실행'}")

        service = BootstrapService(db)
        response = await service.run_bootstrap(bootstrap_request)

        if response.overall_success:
            logger.info(
                f"✅ 장전 기초데이터 일괄 갱신 완료 - 성공: {response.successful_steps}, "
                f"실패: {response.failed_steps}, 스킵: {response.skipped_steps}, "
                f"소요시간: {response.total_duration_seconds:.2f}초"
            )
        else:
            logger.warning(
                f"⚠️ 장전 기초데이터 일괄 갱신 완료 (일부 실패) - 성공: {response.successful_steps}, "
                f"실패: {response.failed_steps}, 스킵: {response.skipped_steps}"
            )

        return response

    except Exception as e:
        logger.error(f"❌ 장전 기초데이터 일괄 갱신 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Bootstrap 실행 중 오류가 발생했습니다: {str(e)}")


@router.get("/health", summary="Bootstrap 서비스 상태 확인")
async def health_check():
    """Bootstrap 서비스 상태 확인"""
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
