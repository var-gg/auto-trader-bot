# app/features/signals/controllers/backtest_controller.py
"""
백테스팅 컨트롤러
- vec40 테이블 기반 백테스팅 API
"""
from __future__ import annotations
import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.signals.models.similarity_models import (
    BacktestRequest,
    BacktestResponse
)
from app.features.signals.services.backtest_service import BacktestService


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/signals",
    tags=["signals-backtest"]
)


@router.post("/backtest-vec40", response_model=BacktestResponse)
def backtest_vec40_post(
    request: BacktestRequest,
    db: Session = Depends(get_db)
) -> BacktestResponse:
    """
    🎯 Vec40 테이블 기반 백테스팅 API
    
    - ticker_id와 from_date를 입력받아 백테스팅 수행
    - 10기간씩 슬라이딩하며 벡터화하여 vec40 테이블과 유사도 검색
    - 상위 10개가 모두 UP일 때 매수 시그널
    - 15일 후 종가에 매도
    
    **요청 파라미터:**
    - ticker_id: 분석할 티커 ID (필수)
    - from_date: 백테스팅 시작일 (필수)
    - lookback: 벡터화할 기간 (기본값: 10)
    - top_k: 유사도 비교할 시그널 개수 (기본값: 10)
    - exit_window: 청산 윈도우 (기본값: 15)
    - peak_threshold: 상승 피크 감지 임계값 (기본값: 0.05)
    
    **응답:**
    - total_signals: 총 매수 시그널 발생 횟수
    - correct_direction_count: 방향 적중 횟수 (청산가 > 매수가)
    - peak_gain_5pct_count: 윈도우 기간 중 5% 이상 상승 경험 횟수
    - total_profit: 총 수익
    - return_rate: 수익률
    - win_rate: 승률
    - peak_experience_rate: 피크 경험률
    """
    try:
        service = BacktestService(db)
        result = service.backtest_vec40(request)
        return result
    except ValueError as e:
        logger.error(f"Backtest validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Backtest error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"백테스팅 중 오류가 발생했습니다: {str(e)}")


@router.get("/backtest-vec40", response_model=BacktestResponse)
def backtest_vec40_get(
    ticker_id: int = Query(..., description="분석할 티커 ID", gt=0),
    from_date: date = Query(..., description="백테스팅 시작일"),
    lookback: int = Query(10, description="벡터화할 기간 (캔들 개수)", ge=3, le=20),
    top_k: int = Query(10, description="유사도 비교할 시그널 개수", ge=1, le=50),
    exit_window: int = Query(15, description="매수 후 청산까지 기간", ge=1, le=30),
    peak_threshold: float = Query(0.05, description="상승 피크 감지 임계값 (5% = 0.05)", ge=0.01, le=0.20),
    db: Session = Depends(get_db)
) -> BacktestResponse:
    """
    🎯 Vec40 테이블 기반 백테스팅 API (GET 버전)
    
    **쿼리 파라미터:**
    - ticker_id: 분석할 티커 ID (필수)
    - from_date: 백테스팅 시작일 (필수)
    - lookback: 벡터화할 기간 (기본값: 10)
    - top_k: 유사도 비교할 시그널 개수 (기본값: 10)
    - exit_window: 청산 윈도우 (기본값: 15)
    - peak_threshold: 상승 피크 감지 임계값 (기본값: 0.05)
    
    **예시:**
    ```
    GET /api/signals/backtest-vec40?ticker_id=1&from_date=2023-01-01
    GET /api/signals/backtest-vec40?ticker_id=1&from_date=2023-01-01&lookback=10&exit_window=20
    ```
    """
    request = BacktestRequest(
        ticker_id=ticker_id,
        from_date=from_date,
        lookback=lookback,
        top_k=top_k,
        exit_window=exit_window,
        peak_threshold=peak_threshold
    )
    
    try:
        service = BacktestService(db)
        result = service.backtest_vec40(request)
        return result
    except ValueError as e:
        logger.error(f"Backtest validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Backtest error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"백테스팅 중 오류가 발생했습니다: {str(e)}")

