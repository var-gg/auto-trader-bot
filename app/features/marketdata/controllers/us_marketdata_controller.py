# app/features/marketdata/controllers/us_marketdata_controller.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.marketdata.services.us_daily_ingestor import USDailyIngestor
from app.features.marketdata.services.us_market_holiday_service import USMarketHolidayService
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n

router = APIRouter(prefix="/marketdata", tags=["[미국주식] marketdata"])

# -------- 1) 티커ID 기반 기간별 데이터 수집 --------

class TickerIdDailyRequest(BaseModel):
    """티커ID 기반 기간별 데이터 수집 요청"""
    ticker_ids: List[int] = Field(..., description="수집할 티커 ID 목록")
    days: int = Field(..., gt=0, description="조회할 거래일 수 (최근 N일)")

@router.post(
    "/sync/daily",
    summary="티커ID 기반 기간별 데이터 수집",
    description="""
    지정된 티커 ID들의 일봉 데이터를 KIS API를 통해 수집합니다.
    
    **처리 과정:**
    1. 티커 존재 여부 확인: 데이터베이스에서 요청된 티커 ID들이 모두 존재하는지 검증
    2. 누락 티커 확인: 요청된 티커 ID 중 존재하지 않는 ID들을 식별하여 오류 반환
    3. 거래소 코드 변환: 티커 테이블의 거래소 코드(Yahoo Finance 기준)를 KIS API 코드로 자동 변환
    4. KIS API 호출: USDailyIngestor 서비스를 통해 지정된 기간의 일봉 데이터 수집
    5. 데이터 upsert: 수집된 데이터를 ohlcv_daily 테이블에 upsert 처리
    6. 결과 집계: 종목별 수집된 데이터 건수를 집계하여 반환
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **지원 거래소:** 미국 주식 거래소 (NMS, NYQ, NAS 등)
    
    **사용 예시:**
    - `POST /marketdata/sync/daily` with body: `{"ticker_ids": [123, 456], "days": 21}`
    """,
    response_description="수집된 종목별 데이터 건수와 상태 정보를 반환합니다."
)
def sync_daily(req: TickerIdDailyRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
    # 티커 존재 여부 확인
    tickers = db.query(Ticker).filter(Ticker.id.in_(req.ticker_ids)).all()
    if not tickers:
        raise HTTPException(status_code=404, detail="지정된 티커 ID들을 찾을 수 없습니다.")
    
    # 누락된 티커 ID 확인
    found_ids = {t.id for t in tickers}
    missing_ids = set(req.ticker_ids) - found_ids
    if missing_ids:
        raise HTTPException(status_code=404, detail=f"다음 티커 ID들을 찾을 수 없습니다: {sorted(missing_ids)}")
    
    ingestor = USDailyIngestor(db)
    counts = ingestor.sync_for_ticker_ids(req.ticker_ids, req.days)
    return {"status": "ok", "upserted": counts}

@router.post(
    "/sync/daily/all",
    summary="전체 티커 일봉 데이터 수집 ★★★",
    description="""
    모든 활성 티커에 대해 50일치 일봉 데이터를 자동으로 수집합니다.
    
    **처리 과정:**
    1. 활성 티커 조회: 데이터베이스에서 모든 활성 상태의 미국 주식 티커들을 조회
    2. 배치 처리 설정: 50일치 일봉 데이터 수집을 위한 고정 기간 설정
    3. 거래소 코드 변환: 각 티커의 Yahoo Finance 코드를 KIS API 코드로 자동 변환
    4. 순차 데이터 수집: USDailyIngestor 서비스를 통해 각 티커별로 일봉 데이터 수집
    5. 데이터 upsert: 수집된 데이터를 ohlcv_daily 테이블에 upsert 처리
    6. 결과 집계: 전체 티커 수, 성공/실패 건수, 요약 정보를 생성하여 반환
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **처리 방식:** 배치 처리 (대량 처리)
    **수집 기간:** 50일 (고정)
    
    **사용 예시:**
    - `POST /marketdata/sync/daily/all` (인자 없이 실행)
    """,
    response_description="수집된 종목별 데이터 건수와 전체 요약 정보를 반환합니다."
)
def sync_daily_all(db: Session = Depends(get_db)) -> Dict[str, Any]:
    ingestor = USDailyIngestor(db)
    counts = ingestor.sync_all_tickers(days=50)
    return {"status": "ok", "upserted": counts}

# -------- 3) 마켓 휴일 정보 동기화 --------

@router.post(
    "/holidays/sync",
    summary="마켓 휴일 정보 동기화 ★★★",
    description="""
    Finnhub API를 통해 US 거래소의 휴일 정보를 조회하여 DB에 저장합니다.
    
    **처리 과정:**
    1. 거래소 설정: US 거래소의 휴일 정보 수집을 위한 설정
    2. Finnhub API 호출: USMarketHolidayService를 통해 Finnhub API에서 휴일 데이터 조회
    3. 데이터 검증: 수집된 휴일 데이터의 유효성 검증 및 정제
    4. 중복 제거: 기존 DB 데이터와 중복되지 않는 새로운 휴일 정보만 선별
    5. 데이터베이스 저장: market_holiday 테이블에 휴일 정보 저장
    6. 결과 집계: 동기화된 휴일 건수와 상태 정보를 반환
    
    **데이터 소스:** Finnhub API
    **저장 테이블:** market_holiday
    **대상 거래소:** US (미국)
    **처리 방식:** 전체 동기화
    
    **사용 예시:**
    - `POST /marketdata/holidays/sync` (인자 없이 실행)
    """,
    response_description="동기화 결과와 처리된 데이터 건수를 반환합니다."
)
def sync_holidays(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = USMarketHolidayService(db)
    result = service.sync_holidays_for_exchange("US")
    return result

# -------- 4) 현재 휴장 여부 확인 --------

@router.get(
    "/is-market-closed",
    summary="현재 휴장 여부 확인",
    description="""
    현재 날짜가 주말이거나 완전휴장(부분개장 제외)인지 판별하여 boolean으로 반환합니다.
    
    **처리 과정:**
    1. 현재 시간 조회: 뉴욕 시간(ET) 기준 현재 날짜 및 시간 정보 획득
    2. 주말 체크: 뉴욕 시간 기준 현재 날짜가 토요일 또는 일요일인지 확인
    3. 휴일 DB 조회: market_holiday 테이블에서 현재 날짜의 휴일 정보 조회
    4. 휴장 판별: 주말 또는 완전휴장(부분개장 제외) 조건을 종합하여 휴장 여부 판단
    5. 결과 반환: boolean 값으로 휴장 여부를 반환
    
    **판별 조건:**
    - 주말 (토요일, 일요일) - 뉴욕 시간 기준
    - 완전휴장일 (부분개장 제외)
    
    **데이터 소스:** market_holiday 테이블
    **대상 거래소:** US (미국)
    **시간 기준:** 뉴욕 시간 (America/New_York)
    **응답 형식:** boolean (true: 휴장, false: 개장)
    
    **사용 예시:**
    - `GET /marketdata/is-market-closed`
    """,
    response_description="휴장 여부와 뉴욕 시간 정보를 포함한 상세 정보를 반환합니다."
)
def is_market_closed(db: Session = Depends(get_db)) -> Dict[str, Any]:
    service = USMarketHolidayService(db)
    is_closed = service.is_market_closed_now()
    
    # 뉴욕 시간 정보 추가
    from datetime import datetime
    import pytz
    
    ny_tz = pytz.timezone('America/New_York')
    ny_now = datetime.now(ny_tz)
    
    return {
        "is_market_closed": is_closed,
        "exchange": "US",
        "timezone": "America/New_York",
        "current_time_ny": ny_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "current_date_ny": ny_now.strftime("%Y-%m-%d"),
        "weekday_ny": ny_now.strftime("%A"),
        "description": "미장 휴장 여부 확인 (뉴욕 시간 기준)"
    }


