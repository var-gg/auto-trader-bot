# app/features/marketdata/controllers/us_price_detail_controller.py
from fastapi import APIRouter, Depends
from typing import Dict, Any
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.marketdata.services.us_price_detail_ingestor import USPriceDetailIngestor

router = APIRouter(prefix="/marketdata", tags=["[미국주식] marketdata"])

@router.get(
    "/sync/price-detail/{ticker_id}",
    summary="해외주식현재가상세 수집 (단일 티커ID)",
    description="""
    지정된 티커 ID의 현재가상세 데이터를 KIS API를 통해 수집합니다.
    
    **처리 과정:**
    1. 티커 ID 검증: 요청된 티커 ID가 존재하는지 확인
    2. 거래소 코드 변환: 티커 테이블의 거래소 코드(Yahoo Finance 기준)를 KIS API 코드로 자동 변환
    3. KIS API 호출: USPriceDetailIngestor 서비스를 통해 해외주식현재가상세 API 호출
    4. 실시간 데이터 수집: 현재 시점의 실시간 현재가 정보 (OHLCV) 수집
    5. 데이터 변환: KIS API 응답을 ohlcv_daily 테이블 형식에 맞게 변환
    6. 데이터 upsert: 수집된 현재가 데이터를 기존 일봉 테이블에 upsert 처리
    7. 결과 반환: 저장된 캔들 데이터와 상태 정보를 반환
    
    **수집 데이터:**
    - 시가 (open)
    - 고가 (high) 
    - 저가 (low)
    - 종가 (close)
    - 거래량 (volume)
    - 거래일 (trade_date)
    - 데이터 소스 정보 (source, source_symbol, source_exchange)
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **데이터 타입:** 실시간 현재가 (KIS_CURRENT_PRICE)
    **처리 방식:** upsert (기존 데이터 업데이트 또는 신규 삽입)
    
    **사용 예시:**
    - `GET /marketdata/sync/price-detail/317`
    """,
    response_description="수집된 캔들 데이터를 반환합니다."
)
def sync_price_detail(ticker_id: int, db: Session = Depends(get_db)) -> Dict[str, Any]:
    ingestor = USPriceDetailIngestor(db)
    result = ingestor.sync_price_detail_for_ticker_id(ticker_id)
    
    # 서비스에서 반환된 결과를 그대로 반환
    return result
