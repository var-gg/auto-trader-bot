# app/features/signals/controllers/signal_controller.py
from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.signals.models.signal_models import (
    SignalDetectionRequest,
    SignalDetectionResponse,
    SignalDirection,
    SaveOption,
    AlgorithmVersion,
    IntradaySignalRequest,
    IntradaySignalResponse
)
from app.features.signals.models.similarity_models import (
    SimilaritySearchRequest,
    SimilaritySearchResponse,
    IntradaySimilaritySearchRequest,
    IntradaySimilaritySearchResponse,
    CountryFilter
)
from app.features.signals.services.signal_detection_service import SignalDetectionService
from app.features.signals.services.intraday_signal_service import IntradaySignalService


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/signals",
    tags=["signals"]
)


@router.post("/detect", response_model=SignalDetectionResponse)
def detect_signals(
    request: SignalDetectionRequest,
    db: Session = Depends(get_db)
) -> SignalDetectionResponse:
    """
    🎯 시그널 탐지 API
    
    - 티커ID, 기간, 최소앞캔들을 입력받아 상승 개시점을 탐지합니다.
    - 데이터가 부족하면 자동으로 추가 적재합니다.
    - 탐지된 시그널 중 최소앞캔들 조건을 만족하는 것만 반환합니다.
    - ticker_id가 없으면 전체 티커 배치 처리 (SAVE 모드에서만 가능)
    
    **요청 파라미터:**
    - ticker_id: 티커 ID (빈값이면 전체 티커 처리, SAVE 모드에서만)
    - days: 분석할 기간 (거래일 수, 기본값: 100)
    - direction: 시그널 방향 (UP: 상승, DOWN: 하락, 기본값: UP)
    - save_option: 저장 옵션 (NONE: 저장 안함, SAVE: DB 저장)
    - lookback: 직전 구간 확인 기간 (기본값: 5, 범위: 3~10) - 최소 앞 캔들 자동 보장
    - future_window: 이후 구간 평가 기간 (기본값: 15, 범위: 7~24)
    - min_change: 최소 변동률 (기본값: 0.05, 범위: 0.03~0.10)
    - max_reverse: 반대 방향 최대 허용폭 (기본값: 0.05, 범위: 0.03~0.07)
    - flatness_k: 평탄성 허용치 ATR 배수 (기본값: 1.0, 범위: 0.5~1.5)
    
    **응답:**
    - 단일 티커: 탐지된 시그널 목록 (날짜, 가격, 지표 값, 앞캔들 개수 등)
    - 배치 처리: 처리 결과 요약 (총 티커 수, 성공/실패, 시그널 통계)
    """
    try:
        service = SignalDetectionService(db)
        result = service.detect_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Signal detection validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Signal detection error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"시그널 탐지 중 오류가 발생했습니다: {str(e)}")


@router.get("/detect", response_model=SignalDetectionResponse)
def detect_signals_get(
    ticker_id: Optional[int] = Query(None, description="티커 ID (빈값이면 전체 티커 처리, SAVE 모드에서만)", gt=0),
    days: int = Query(100, description="분석할 기간 (거래일 수)", ge=20, le=500),
    direction: SignalDirection = Query(SignalDirection.UP, description="시그널 방향 (UP: 상승, DOWN: 하락)"),
    save_option: SaveOption = Query(SaveOption.NONE, description="저장 옵션 (NONE: 저장 안함, SAVE: DB 저장)"),
    version: AlgorithmVersion = Query(AlgorithmVersion.V1, description="알고리즘 버전 (v1: 평탄성기반, v2: 지속형, v3: 혼합형)"),
    lookback: int = Query(10, description="직전 구간 확인 기간 (최소 앞 캔들 자동 보장)", ge=3, le=20),
    future_window: int = Query(15, description="이후 구간 평가 기간", ge=3, le=15),
    min_change: float = Query(0.10, description="최소 변동률 (UP: 상승률, DOWN: 하락률)", ge=0.03, le=0.20),
    max_reverse: float = Query(0.03, description="반대 방향 최대 허용폭", ge=0.01, le=0.10),
    flatness_k: float = Query(1.0, description="평탄성 허용치 ATR 배수", ge=0.5, le=1.5),
    db: Session = Depends(get_db)
) -> SignalDetectionResponse:
    """
    🎯 시그널 탐지 API (GET 버전)
    
    - 쿼리 파라미터로 간단하게 호출할 수 있는 버전입니다.
    - POST /api/signals/detect와 동일한 기능을 수행합니다.
    - ticker_id가 없으면 전체 티커 배치 처리 (SAVE 모드에서만 가능)
    
    **쿼리 파라미터:**
    - ticker_id: 티커 ID (빈값이면 전체 티커 처리, SAVE 모드에서만)
    - days: 분석할 기간 (기본값: 100)
    - direction: 시그널 방향 (기본값: UP)
    - save_option: 저장 옵션 (기본값: NONE)
    - lookback: 직전 구간 확인 기간 (기본값: 5)
    - future_window: 이후 구간 평가 기간 (기본값: 15)
    - min_change: 최소 변동률 (기본값: 0.05)
    - max_reverse: 반대 방향 최대 허용폭 (기본값: 0.05)
    - flatness_k: 평탄성 허용치 ATR 배수 (기본값: 1.0)
    
    **예시:**
    ```
    # 단일 티커 상승 시그널 탐지
    GET /api/signals/detect?ticker_id=1&direction=UP&min_change=0.07
    
    # 전체 티커 배치 처리 (SAVE 모드)
    GET /api/signals/detect?save_option=SAVE&direction=UP&min_change=0.07
    ```
    """
    request = SignalDetectionRequest(
        ticker_id=ticker_id,
        days=days,
        direction=direction,
        save_option=save_option,
        version=version,
        lookback=lookback,
        future_window=future_window,
        min_change=min_change,
        max_reverse=max_reverse,
        flatness_k=flatness_k
    )
    
    try:
        service = SignalDetectionService(db)
        result = service.detect_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Signal detection validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Signal detection error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"시그널 탐지 중 오류가 발생했습니다: {str(e)}")


@router.post("/similarity", response_model=SimilaritySearchResponse)
def search_similar_signals_post(
    request: SimilaritySearchRequest,
    db: Session = Depends(get_db)
) -> SimilaritySearchResponse:
    """
    🔍 형태 유사도 검색 API
    
    - 티커와 기준일자를 입력받아 해당 기간의 캔들을 벡터화
    - DB에 저장된 시그널 벡터들과 비교하여 유사도 TOP K 반환
    - save=true일 때 분석 결과(p_up, p_down, exp_up, exp_down)를 DB에 저장
    - save=true + ticker_id=null일 때 전체 티커 배치 처리
    
    **요청 파라미터:**
    - ticker_id: 분석할 티커 ID (save=False일 때 필수, save=True일 때 빈값이면 전체 티커 배치 처리)
    - reference_date: 기준일자 (빈값이면 오늘)
    - lookback: 벡터화할 기간 (기본값: 10)
    - top_k: 반환할 유사 시그널 개수 (기본값: 10)
    - direction_filter: 필터링할 방향 (UP/DOWN, 빈값이면 전체)
    - save: 분석 결과 저장 여부 (기본값: false)
    - country: 국가 필터 (기본값: ALL, 배치 모드 전용)
    
    **응답:**
    - 단일 티커: 유사도 높은 순으로 시그널 목록 반환
    - 배치 처리: 처리 결과 요약 (총 티커 수, 성공/실패, 저장 개수)
    """
    # Validation: save=False일 때 ticker_id 필수
    if not request.save and request.ticker_id is None:
        raise HTTPException(status_code=400, detail="ticker_id는 save=False일 때 필수입니다.")
    
    try:
        service = SignalDetectionService(db)
        result = service.search_similar_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Similarity search validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Similarity search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"유사도 검색 중 오류가 발생했습니다: {str(e)}")


@router.get("/similarity", response_model=SimilaritySearchResponse)
def search_similar_signals_get(
    ticker_id: Optional[int] = Query(None, description="분석할 티커 ID (save=False일 때 필수, save=True일 때 빈값이면 전체 티커 배치 처리)", gt=0),
    reference_date: Optional[date] = Query(None, description="기준일자 (빈값이면 오늘)"),
    lookback: int = Query(10, description="벡터화할 기간 (캔들 개수)", ge=3, le=20),
    top_k: int = Query(10, description="반환할 유사 시그널 개수", ge=1, le=50),
    direction_filter: Optional[SignalDirection] = Query(None, description="필터링할 방향 (빈값이면 전체)"),
    version: AlgorithmVersion = Query(AlgorithmVersion.V3, description="알고리즘 버전 (조회군 제한)"),
    save: bool = Query(False, description="분석 결과 저장 여부 (True: DB 저장, False: 저장 안함)"),
    country: CountryFilter = Query(CountryFilter.ALL, description="국가 필터 (배치 모드 전용, ALL: 전체, US: 미국, KR: 한국)"),
    db: Session = Depends(get_db)
) -> SimilaritySearchResponse:
    """
    🔍 형태 유사도 검색 API (GET 버전)
    
    **쿼리 파라미터:**
    - ticker_id: 분석할 티커 ID (save=False일 때 필수, save=True일 때 빈값이면 전체 티커 배치 처리)
    - reference_date: 기준일자 (기본값: 오늘)
    - lookback: 벡터화할 기간 (기본값: 10)
    - top_k: 반환할 유사 시그널 개수 (기본값: 10)
    - direction_filter: 필터링할 방향 (기본값: 전체)
    - version: 알고리즘 버전 (기본값: v3, 조회군 제한)
    - save: 분석 결과 저장 여부 (기본값: false)
    - country: 국가 필터 (기본값: ALL, 배치 모드 전용)
    
    **예시:**
    ```
    # 단일 티커 검색
    GET /api/signals/similarity?ticker_id=1&version=v3
    GET /api/signals/similarity?ticker_id=1&reference_date=2024-09-15&direction_filter=UP&version=v2
    
    # 단일 티커 + 분석 결과 저장
    GET /api/signals/similarity?ticker_id=1&save=true
    
    # 전체 티커 배치 처리 (save=true 필수)
    GET /api/signals/similarity?save=true&version=v3
    
    # 미국 종목만 배치 처리
    GET /api/signals/similarity?save=true&country=US&version=v3
    
    # 한국 종목만 배치 처리
    GET /api/signals/similarity?save=true&country=KR&version=v3
    ```
    """
    # Validation: save=False일 때 ticker_id 필수
    if not save and ticker_id is None:
        raise HTTPException(status_code=400, detail="ticker_id는 save=False일 때 필수입니다.")
    
    # Validation: save=False일 때는 배치 처리 불가
    if not save and ticker_id is None:
        raise HTTPException(status_code=400, detail="전체 티커 배치 처리는 save=True일 때만 가능합니다.")
    
    request = SimilaritySearchRequest(
        ticker_id=ticker_id,
        reference_date=reference_date,
        lookback=lookback,
        top_k=top_k,
        direction_filter=direction_filter,
        version=version,
        save=save,
        country=country
    )
    
    try:
        service = SignalDetectionService(db)
        result = service.search_similar_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Similarity search validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Similarity search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"유사도 검색 중 오류가 발생했습니다: {str(e)}")


@router.post("/detect-intraday", response_model=IntradaySignalResponse)
def detect_intraday_signals(
    request: IntradaySignalRequest,
    db: Session = Depends(get_db)
) -> IntradaySignalResponse:
    """
    🎯 5분봉 시그널 탐지 API
    
    - KIS API를 통해 5분봉 데이터를 실시간 수집하여 시그널 탐지
    - 메모리에서만 처리 (DB 저장 없음)
    - 일봉 탐지 알고리즘과 동일한 로직 사용
    
    **요청 파라미터:**
    - ticker_id: 티커 ID (필수)
    - candles: 분석할 캔들 개수 (기본값: 100, 범위: 7~500)
    - direction: 시그널 방향 (UP: 상승, DOWN: 하락, ALL: 둘 다)
    - version: 알고리즘 버전 (v1/v2/v3)
    - lookback: 직전 구간 확인 기간
    - future_window: 이후 구간 평가 기간
    - min_change: 최소 변동률
    - max_reverse: 반대 방향 최대 허용폭
    - flatness_k: 평탄성 허용치
    
    **데이터 수집:**
    - 해외주식: 5분봉 직접 조회 (KEYB 페이징)
    - 국내주식: 2분봉 조회 → 5분봉 리샘플링
    
    **응답:**
    - 탐지된 시그널 목록
    - 각 시그널의 날짜/시간, 가격, 지표 값
    """
    try:
        service = IntradaySignalService(db)
        result = service.detect_intraday_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Intraday signal detection validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Intraday signal detection error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"5분봉 시그널 탐지 중 오류가 발생했습니다: {str(e)}")


@router.get("/detect-intraday", response_model=IntradaySignalResponse)
def detect_intraday_signals_get(
    ticker_id: Optional[int] = Query(None, description="티커 ID (없으면 전체 티커 배치 처리)", gt=0),
    candles: int = Query(100, description="분석할 캔들 개수 (5분봉)", ge=7, le=100000),
    direction: SignalDirection = Query(SignalDirection.UP, description="시그널 방향"),
    version: AlgorithmVersion = Query(AlgorithmVersion.V1, description="알고리즘 버전"),
    save_option: SaveOption = Query(SaveOption.NONE, description="저장 옵션"),
    lookback: int = Query(10, description="직전 구간 확인 기간", ge=3, le=20),
    future_window: int = Query(15, description="이후 구간 평가 기간", ge=7, le=24),
    min_change: float = Query(0.015, description="최소 변동률 (소수점 3자리: 0.001~0.200)", ge=0.001, le=0.200),
    max_reverse: float = Query(0.005, description="반대 방향 최대 허용폭 (소수점 3자리: 0.001~0.100)", ge=0.001, le=0.100),
    flatness_k: float = Query(1.0, description="평탄성 허용치", ge=0.5, le=1.5),
    db: Session = Depends(get_db)
) -> IntradaySignalResponse:
    """
    🎯 5분봉 시그널 탐지 API (GET 버전)
    
    **쿼리 파라미터:**
    - ticker_id: 티커 ID (필수)
    - candles: 분석할 캔들 개수 (기본값: 100)
    - direction: 시그널 방향 (기본값: UP)
    - version: 알고리즘 버전 (기본값: v1)
    
    **예시:**
    ```
    GET /api/signals/detect-intraday?ticker_id=1&candles=200&version=v2
    GET /api/signals/detect-intraday?ticker_id=1&direction=DOWN&min_change=0.05
    ```
    """
    request = IntradaySignalRequest(
        ticker_id=ticker_id,
        candles=candles,
        direction=direction,
        version=version,
        save_option=save_option,
        lookback=lookback,
        future_window=future_window,
        min_change=min_change,
        max_reverse=max_reverse,
        flatness_k=flatness_k
    )
    
    try:
        service = IntradaySignalService(db)
        result = service.detect_intraday_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Intraday signal detection validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Intraday signal detection error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"5분봉 시그널 탐지 중 오류가 발생했습니다: {str(e)}")


@router.post("/similarity-intraday", response_model=IntradaySimilaritySearchResponse)
def search_intraday_similar_signals_post(
    request: IntradaySimilaritySearchRequest,
    db: Session = Depends(get_db)
) -> IntradaySimilaritySearchResponse:
    """
    🔍 분봉 형태 유사도 검색 API (POST)
    
    - 티커와 기준일시를 입력받아 해당 기간의 5분봉을 API로 수집하여 벡터화
    - DB에 저장된 분봉 시그널 벡터들과 비교하여 유사도 TOP K 반환
    """
    try:
        service = IntradaySignalService(db)
        result = service.search_intraday_similar_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Intraday similarity search validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Intraday similarity search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"분봉 유사도 검색 중 오류가 발생했습니다: {str(e)}")


@router.get("/similarity-intraday", response_model=IntradaySimilaritySearchResponse)
def search_intraday_similar_signals_get(
    ticker_id: int = Query(..., description="분석할 티커 ID", gt=0),
    reference_datetime: Optional[str] = Query(None, description="기준일시 (YYYYMMDD HHMMSS)"),
    lookback: int = Query(10, description="벡터화할 기간", ge=3, le=20),
    top_k: int = Query(10, description="반환할 유사 시그널 개수", ge=1, le=50),
    direction_filter: Optional[SignalDirection] = Query(None, description="필터링할 방향"),
    version: AlgorithmVersion = Query(AlgorithmVersion.V1, description="알고리즘 버전"),
    db: Session = Depends(get_db)
) -> IntradaySimilaritySearchResponse:
    """
    🔍 분봉 형태 유사도 검색 API (GET)
    
    **쿼리 파라미터:**
    - ticker_id: 분석할 티커 ID (필수)
    - reference_datetime: 기준일시 (빈값이면 현재)
    - lookback: 벡터화할 기간 (기본값: 10)
    - top_k: 반환할 유사 시그널 개수 (기본값: 10)
    - direction_filter: 필터링할 방향
    - version: 알고리즘 버전
    
    **예시:**
    ```
    GET /api/signals/similarity-intraday?ticker_id=1&version=v1
    GET /api/signals/similarity-intraday?ticker_id=1&reference_datetime=20251010 143000&direction_filter=UP
    ```
    """
    request = IntradaySimilaritySearchRequest(
        ticker_id=ticker_id,
        reference_datetime=reference_datetime,
        lookback=lookback,
        top_k=top_k,
        direction_filter=direction_filter,
        version=version
    )
    
    try:
        service = IntradaySignalService(db)
        result = service.search_intraday_similar_signals(request)
        return result
    except ValueError as e:
        logger.error(f"Intraday similarity search validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Intraday similarity search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"분봉 유사도 검색 중 오류가 발생했습니다: {str(e)}")