# app/features/marketdata/controllers/kr_price_detail_controller.py
from fastapi import APIRouter, Depends
from typing import Dict, Any
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.marketdata.services.kr_price_detail_ingestor import KRPriceDetailIngestor

router = APIRouter(prefix="/marketdata", tags=["[국내주식] marketdata"])

@router.get(
    "/kr/sync/price-detail/{ticker_id}",
    summary="국내주식현재가 시세 수집 (단일 티커ID)",
    description="""
    지정된 티커 ID의 국내주식 현재가 시세 데이터를 KIS API를 통해 수집합니다.
    
    **처리 과정:**
    1. 티커 ID 검증: 요청된 티커 ID가 존재하고 국내주식(KOE 거래소)인지 확인
    2. KIS API 호출: KRPriceDetailIngestor 서비스를 통해 국내주식현재가 시세 API 호출
    3. 실시간 데이터 수집: 현재 시점의 실시간 현재가 정보 (OHLCV) 수집
    4. 데이터 변환: KIS API 응답을 ohlcv_daily 테이블 형식에 맞게 변환
    5. 데이터 upsert: 수집된 현재가 데이터를 기존 일봉 테이블에 upsert 처리
    6. 결과 반환: 저장된 캔들 데이터와 상태 정보를 반환
    
    **수집 데이터:**
    - 시가 (stck_oprc)
    - 고가 (stck_hgpr) 
    - 저가 (stck_lwpr)
    - 종가 (stck_prpr)
    - 거래량 (acml_vol)
    - 거래일 (trade_date)
    - 전일대비 (prdy_vrss)
    - 전일대비율 (prdy_ctrt)
    - 데이터 소스 정보 (source, source_symbol, source_exchange)
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **데이터 타입:** 실시간 현재가 (KIS_KR_CURRENT_PRICE)
    **지원 거래소:** KOE (코스피)만
    **처리 방식:** upsert (기존 데이터 업데이트 또는 신규 삽입)
    
    **사용 예시:**
    - `GET /marketdata/kr/sync/price-detail/123` (국내주식 티커 ID 123)
    """,
    response_description="수집된 캔들 데이터를 반환합니다."
)
def sync_kr_price_detail(ticker_id: int, db: Session = Depends(get_db)) -> Dict[str, Any]:
    ingestor = KRPriceDetailIngestor(db)
    result = ingestor.sync_price_detail_for_ticker_id(ticker_id)
    
    # 서비스에서 반환된 결과를 그대로 반환
    return result
