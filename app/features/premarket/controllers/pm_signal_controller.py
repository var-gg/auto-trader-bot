# app/features/premarket/controllers/pm_signal_controller.py
"""
Pre-market Best Signal API Controller
"""
from __future__ import annotations
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.premarket.services.pm_signal_service import PMSignalService
from app.features.premarket.services.pm_signal_service_v2 import PMSignalServiceV2
from app.features.premarket.models.pm_signal_models import (
    UpdatePMSignalsRequest,
    UpdatePMSignalsResponse,
    GetPMSignalsResponse,
    TestPMSignalRequest,
    TestPMSignalResponse
)
from live_app.application.context import RunContext
from live_app.application.pm_signal_commands import (
    GetPMSignalsQuery,
    TestPMSignalQuery,
    UpdatePMSignalsCommand,
)


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/premarket/signals",
    tags=["premarket"]
)


@router.post("/update", response_model=UpdatePMSignalsResponse)
def update_pm_signals_post(
    request: UpdatePMSignalsRequest,
    db: Session = Depends(get_db)
) -> UpdatePMSignalsResponse:
    """
    📊 장전 신호 계산 및 DB 저장 (POST)
    
    - 전체 티커 또는 특정 티커만 처리
    - dry_run=true면 DB 저장 없이 테스트만
    - country 필터로 US/KR 종목만 선택 가능
    
    **Request Body:**
    ```json
    {
      "tickers": ["NVDA", "AAPL"],  // 선택적, 없으면 전체
      "country": "US",               // US, KR, 없으면 전체
      "dry_run": false,              // true면 저장 안 함
      "anchor_date": "2025-10-20"    // 선택적, 없으면 오늘
    }
    ```
    
    **Response:**
    ```json
    {
      "success": true,
      "config_id": 4,
      "anchor_date": "2025-10-20",
      "results": {
        "total": 500,
        "success": 498,
        "failed": 2,
        "no_signal": 0
      },
      "elapsed_seconds": 152.3,
      "samples": [...]
    }
    ```
    """
    try:
        ctx = RunContext(actor="http", channel="live_app.api", metadata={"route": "/api/premarket/signals/update", "method": "POST"})
        command = UpdatePMSignalsCommand(db)
        return command.execute(request, ctx)
    except ValueError as e:
        logger.error(f"PM signal update validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"PM signal update error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"신호 계산 중 오류가 발생했습니다: {str(e)}")


@router.get("/update", response_model=UpdatePMSignalsResponse)
def update_pm_signals_get(
    tickers: Optional[str] = Query(None, description="티커 심볼 (쉼표로 구분, 예: NVDA,AAPL)"),
    country: Optional[str] = Query(None, description="국가 필터 (US, KR, 없으면 전체)"),
    dry_run: bool = Query(False, description="True면 저장 안 함 (테스트용)"),
    anchor_date: Optional[str] = Query(None, description="기준일자 (YYYY-MM-DD, 없으면 오늘)"),
    db: Session = Depends(get_db)
) -> UpdatePMSignalsResponse:
    """
    📊 장전 신호 계산 및 DB 저장 (GET)
    
    **Query Parameters:**
    - tickers: 티커 심볼 (쉼표로 구분, 예: NVDA,AAPL)
    - country: 국가 필터 (US, KR, 없으면 전체)
    - dry_run: True면 저장 안 함 (기본값: false)
    - anchor_date: 기준일자 (YYYY-MM-DD, 없으면 오늘)
    
    **예시:**
    ```
    # 미국 종목 전체 처리
    GET /api/premarket/signals/update?country=US
    
    # 특정 종목만 처리 (dry_run)
    GET /api/premarket/signals/update?tickers=NVDA,AAPL&dry_run=true
    
    # 한국 종목만 처리
    GET /api/premarket/signals/update?country=KR
    ```
    """
    # 🔍 디버깅: 파라미터 확인
    logger.info(f"GET /update called - tickers={tickers}, country={country}, dry_run={dry_run}")
    
    # tickers를 리스트로 변환
    ticker_list = [t.strip() for t in tickers.split(',')] if tickers else None
    
    request = UpdatePMSignalsRequest(
        tickers=ticker_list,
        country=country,
        dry_run=dry_run,
        anchor_date=anchor_date
    )
    
    logger.info(f"Created request object - country={request.country}")
    
    try:
        ctx = RunContext(actor="http", channel="live_app.api", metadata={"route": "/api/premarket/signals/update", "method": "GET"})
        command = UpdatePMSignalsCommand(db)
        return command.execute(request, ctx)
    except ValueError as e:
        logger.error(f"PM signal update validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"PM signal update error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"신호 계산 중 오류가 발생했습니다: {str(e)}")


@router.get("", response_model=GetPMSignalsResponse)
def get_pm_signals(
    limit: int = Query(100, description="개수 제한 (기본: 100)", ge=1, le=500),
    min_signal: Optional[float] = Query(None, description="최소 신호값 (예: 0.5)"),
    max_signal: Optional[float] = Query(None, description="최대 신호값 (예: -0.5)"),
    order: str = Query("signal_desc", description="정렬 방식 (signal_desc, signal_asc, updated_desc)"),
    db: Session = Depends(get_db)
) -> GetPMSignalsResponse:
    """
    📋 저장된 신호 조회
    
    **Query Parameters:**
    - limit: 개수 제한 (기본: 100)
    - min_signal: 최소 신호값 (예: 0.5)
    - max_signal: 최대 신호값 (예: -0.5)
    - order: 정렬 방식 (signal_desc, signal_asc, updated_desc)
    
    **예시:**
    ```
    # 상위 100개 (신호값 높은 순)
    GET /api/premarket/signals?limit=100
    
    # 신호값 0.5 이상만
    GET /api/premarket/signals?min_signal=0.5
    
    # 신호값 -0.5 이하만 (하락)
    GET /api/premarket/signals?max_signal=-0.5
    
    # 최근 업데이트 순
    GET /api/premarket/signals?order=updated_desc
    ```
    
    **Response:**
    ```json
    {
      "success": true,
      "count": 100,
      "signals": [
        {
          "ticker_id": 348,
          "symbol": "NVDA",
          "company_name": "NVIDIA Corporation",
          "signal_1d": 0.87,
          "best_target_id": 115239,
          "updated_at": "2025-10-20T08:30:15Z"
        }
      ]
    }
    ```
    """
    try:
        ctx = RunContext(actor="http", channel="live_app.api", metadata={"route": "/api/premarket/signals", "method": "GET"})
        query = GetPMSignalsQuery(db)
        return query.execute(limit=limit, min_signal=min_signal, max_signal=max_signal, order=order, ctx=ctx)
    except Exception as e:
        logger.error(f"PM signal query error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"신호 조회 중 오류가 발생했습니다: {str(e)}")


@router.get("/test", response_model=TestPMSignalResponse)
def test_pm_signal(
    ticker_id: int = Query(..., description="대상 티커 ID"),
    anchor_date: Optional[str] = Query(None, description="기준일자 (YYYY-MM-DD, 없으면 오늘)"),
    topN: int = Query(70, description="ANN 검색 개수 (기본: 70)", ge=1, le=200),
    use_ann: bool = Query(True, description="True=ANN 사용, False=전체 스캔 (정확)"),
    db: Session = Depends(get_db)
) -> TestPMSignalResponse:
    """
    🧪 테스트용 신호 계산 (DB 저장 없음, 상세 매칭 결과 리턴)
    
    **Query Parameters:**
    - ticker_id: 대상 티커 ID (필수)
    - anchor_date: 기준일자 (YYYY-MM-DD, 없으면 오늘)
    - topN: ANN 검색 개수 (기본: 70)
    - use_ann: True=ANN 사용 (빠름), False=전체 스캔 (정확, 느림)
    
    **예시:**
    ```
    # NVDA (ticker_id=348) ANN 테스트 (기본)
    GET /api/premarket/signals/test?ticker_id=348
    
    # 전체 스캔으로 정확한 결과 확인 (느림)
    GET /api/premarket/signals/test?ticker_id=348&use_ann=false
    
    # 특정 날짜로 테스트
    GET /api/premarket/signals/test?ticker_id=348&anchor_date=2025-10-20
    
    # 검색 개수 조정
    GET /api/premarket/signals/test?ticker_id=348&topN=100
    ```
    
    **Response:**
    ```json
    {
      "success": true,
      "ticker_id": 348,
      "symbol": "NVDA",
      "country": "US",
      "signal_1d": 0.87,
      "p_up": 0.935,
      "p_down": 0.065,
      "best_direction": "UP",
      "up_matches": [...],  // TOP N 매칭 결과
      "down_matches": [...],
      "up_reranked_top10": [...],  // 재랭킹 TOP 10
      "down_reranked_top10": [...],
      "stats": {
        "search_mode": "ANN (pgvector index)",  // 또는 "Full Scan (exact)"
        "up_count": 70,
        "down_count": 70,
        "up_country_stats": {"US": 35, "KR": 35},  // 국가별 통계
        "down_country_stats": {"US": 40, "KR": 30}
      }
    }
    ```
    
    **주요 확인 포인트:**
    - **stats.search_mode**: ANN vs Full Scan 확인
    - **up_country_stats / down_country_stats**: 매칭된 종목의 국가별 분포
      - ANN(use_ann=true)과 Full Scan(use_ann=false) 결과를 비교하면 ANN 왜곡 여부 확인 가능
      - 예: ANN은 {"KR": 68, "US": 2}인데 Full Scan은 {"US": 50, "KR": 20}이면 → ANN 인덱스 문제
    - **up_matches / down_matches**: 실제 매칭된 종목 리스트 (country 포함)
    """
    try:
        request = TestPMSignalRequest(
            ticker_id=ticker_id,
            anchor_date=anchor_date,
            topN=topN,
            use_ann=use_ann
        )
        
        ctx = RunContext(actor="http", channel="live_app.api", metadata={"route": "/api/premarket/signals/test", "method": "GET"})
        query = TestPMSignalQuery(db)
        return query.execute(request, ctx)
    except ValueError as e:
        logger.error(f"PM signal test validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"PM signal test error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"신호 테스트 중 오류가 발생했습니다: {str(e)}")


@router.get("/update/v2")
def update_pm_signals_v2(
    tickers: Optional[str] = Query(None, description="티커 심볼 (쉼표로 구분)"),
    country: Optional[str] = Query(None, description="국가 필터 (US, KR)"),
    dry_run: bool = Query(True, description="True면 저장 안 함 (테스트용)"),
    anchor_date: Optional[str] = Query(None, description="기준일자 (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    📊 장전 신호 계산 v2 (GET, 개선된 버전)
    
    주요 개선사항:
    - signal_1d 산출식 통일 (테스트=배치)
    - Shape/Context 분해 로그 (진단용)
    - 상승/하락 비율 통계
    - 컨텍스트 과가중 방지 (breadth 축소, β gating, TopK)
    
    **Query Parameters:**
    - tickers: 티커 심볼 (쉼표로 구분, 예: NVDA,AAPL)
    - country: 국가 필터 (US, KR, 없으면 전체)
    - dry_run: True면 저장 안 함 (기본값: true)
    - anchor_date: 기준일자 (YYYY-MM-DD, 없으면 오늘)
    
    **예시:**
    ```
    # 미국 종목 테스트
    GET /api/premarket/signals/update/v2?country=US&dry_run=true
    
    # 한국 종목 전체 (저장 포함)
    GET /api/premarket/signals/update/v2?country=KR&dry_run=false
    ```
    """
    try:
        ticker_list = [t.strip() for t in tickers.split(',')] if tickers else None
        
        service = PMSignalServiceV2(db)
        result = service.update_signals_v2(
            tickers=ticker_list,
            country=country,
            anchor_date=anchor_date,
            dry_run=dry_run
        )
        return result
    except ValueError as e:
        logger.error(f"[V2 GET] Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[V2 GET] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"신호 계산 중 오류: {str(e)}")

