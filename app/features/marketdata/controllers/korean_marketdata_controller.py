# app/features/marketdata/controllers/korean_marketdata_controller.py
from __future__ import annotations
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.marketdata.services.kr_kospi_parser_service import KrKospiParserService
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.features.marketdata.services.kr_daily_ingestor import KRDailyIngestor
from app.features.marketdata.services.kr_market_holiday_service import KRMarketHolidayService
from app.core.kis_client import KISClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/marketdata", tags=["[국내주식] marketdata"])

# -------- KOSPI 마스터 파일 파싱 --------

@router.post(
    "/kospi/stocks",
    summary="KOSPI200 종목 파싱 및 저장",
    description="""
    KOSPI 마스터 파일을 파싱하여 KOSPI200 구성종목을 데이터베이스에 저장합니다.
    
    **처리 과정:**
    1. 마스터 파일 로드: KOSPI 마스터 파일(.mst)을 로드하여 파싱 준비
    2. 종목 필터링: KRX='Y' 조건으로 실제 주식만 선별 (파생상품 제외)
    3. KOSPI200 선별: KOSPI200섹터업종이 0이 아닌 종목만 선별 (실제 구성종목)
    4. 파생상품 제외: ETN, ETF, ELW, SPAC, 리츠, REITs 등 파생상품 키워드 제외
    5. 중복 확인: 기존 DB에 존재하는 종목인지 확인 (symbol + exchange 기준)
    6. 데이터베이스 저장: 
       - ticker 테이블: symbol(단축코드), exchange(KOE), country(KR), type(stock)
       - ticker_i18n 테이블: lang_code(ko), name(한글명)
    7. 결과 집계: 저장된 종목 수, 건너뛴 종목 수를 집계하여 반환
    
    **데이터 소스:** KOSPI 마스터 파일 (kospi_code.mst)
    **저장 테이블:** ticker, ticker_i18n
    **대상 거래소:** KOE (코스피)
    **필터 조건:** 
    - KRX='Y' (실제 주식만)
    - KOSPI200섹터업종 ≠ 0 (실제 KOSPI200 구성종목)
    - 파생상품 키워드 제외
    
    **사용 예시:**
    - `POST /marketdata/kospi/stocks`
    """,
    response_description="저장된 종목 수와 결과 정보를 반환합니다."
)
def save_kospi_stocks(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    📊 KOSPI200 종목 파싱 및 저장 API
    
    KOSPI 마스터 파일(.mst)을 파싱하여 KOSPI200 구성종목을 데이터베이스에 저장:
    - KRX='Y' 조건으로 실제 주식만 선별
    - KOSPI200섹터업종이 0이 아닌 종목만 선별 (실제 KOSPI200 구성종목)
    - ticker 테이블: symbol(단축코드), exchange(KOE), country(KR), type(stock)
    - ticker_i18n 테이블: lang_code(ko), name(한글명)
    """
    try:
        logger.info("KOSPI200 종목 파싱 및 저장 API 호출")
        
        # KOSPI 파서 서비스 실행
        parser = KrKospiParserService()
        stocks = parser.get_stocks_only()
        
        logger.info(f"파싱 완료 - {len(stocks)}개 KOSPI200 종목")
        
        # 데이터베이스에 저장
        saved_count = 0
        skipped_count = 0
        
        for stock in stocks:
            symbol = stock['symbol']
            name_kr = stock['name_kr']
            
            # 이미 존재하는지 확인
            existing_ticker = db.query(Ticker).filter(
                Ticker.symbol == symbol,
                Ticker.exchange == 'KOE'
            ).first()
            
            if existing_ticker:
                logger.debug(f"이미 존재하는 종목 건너뛰기: {symbol} ({name_kr})")
                skipped_count += 1
                continue
            
            try:
                # ticker 테이블에 저장
                new_ticker = Ticker(
                    symbol=symbol,
                    exchange='KOE',
                    country='KR',
                    type='stock'
                )
                db.add(new_ticker)
                db.flush()  # ID를 얻기 위해 flush
                
                # ticker_i18n 테이블에 저장
                new_ticker_i18n = TickerI18n(
                    ticker_id=new_ticker.id,
                    lang_code='ko',
                    name=name_kr
                )
                db.add(new_ticker_i18n)
                
                saved_count += 1
                logger.debug(f"저장 완료: {symbol} ({name_kr})")
                
            except Exception as e:
                logger.error(f"저장 실패: {symbol} ({name_kr}) - {e}")
                db.rollback()
                continue
        
        # 커밋
        db.commit()
        
        logger.info(f"저장 완료! 신규 저장: {saved_count}개, 건너뛴 종목: {skipped_count}개")
        
        return {
            "status": "ok",
            "total_parsed": len(stocks),
            "saved_count": saved_count,
            "skipped_count": skipped_count,
            "description": "KOSPI200 종목 데이터베이스 저장 완료",
            "metadata": {
                "parsed_at": "2024-01-01T00:00:00Z",  # 실제로는 현재 시간
                "source_file": "kospi_code.mst",
                "filter_applied": [
                    "KRX='Y' 조건으로 실제 주식만 선별",
                    "KOSPI200섹터업종이 0이 아닌 종목만 선별",
                    "명백한 파생상품 키워드 제외: ETN, ETF, ELW, SPAC, 리츠, REITs"
                ]
            }
        }
        
    except FileNotFoundError as e:
        logger.error(f"파일 없음: {e}")
        raise HTTPException(
            status_code=404, 
            detail=f"KOSPI 마스터 파일을 찾을 수 없습니다: {str(e)}"
        )
    except Exception as e:
        logger.error(f"API 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"KOSPI 데이터 파싱 중 오류가 발생했습니다: {str(e)}"
        )

# -------- 국내주식 현재가 일자별 테스트 API --------

@router.get(
    "/test/daily-price/{stock_code}",
    summary="국내주식 현재가 일자별 테스트",
    description="""
    KIS API를 통해 국내주식의 일자별 현재가 정보를 조회합니다.
    
    **처리 과정:**
    1. 파라미터 검증: stock_code, period_div_code, org_adj_prc 파라미터 유효성 확인
    2. KIS 클라이언트 초기화: KISClient 인스턴스를 통한 API 연결 설정
    3. KIS API 호출: domestic_daily_price 메서드를 통해 국내주식 일자별 데이터 조회
    4. 응답 상태 확인: KIS API 응답의 rt_cd 필드를 통한 성공/실패 여부 확인
    5. 결과 반환: 원본 KIS API 응답과 함께 요청 파라미터 정보를 포함하여 반환
    
    **파라미터 설명:**
    - stock_code: 종목코드 (예: 005930)
    - period_div_code: 기간 분류 코드 (D: 일, W: 주, M: 월)
    - org_adj_prc: 수정주가 원주가 가격 (0: 수정주가미반영, 1: 수정주가반영)
    
    **데이터 소스:** KIS API (한국투자증권)
    **API 용도:** 테스트 및 디버깅
    **응답 형식:** KIS API 원본 응답
    
    **사용 예시:**
    - `GET /marketdata/test/daily-price/005930?period_div_code=D&org_adj_prc=1`
    """,
    response_description="국내주식 일자별 현재가 데이터를 반환합니다."
)
def test_domestic_daily_price(
    stock_code: str,
    period_div_code: str = "D",
    org_adj_prc: str = "1",
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    📊 국내주식 현재가 일자별 테스트 API
    
    KIS API를 통해 국내주식의 일자별 현재가 정보를 조회:
    - stock_code: 종목코드 (예: 005930)
    - period_div_code: 기간 분류 코드 (D: 일, W: 주, M: 월)
    - org_adj_prc: 수정주가 원주가 가격 (0: 수정주가미반영, 1: 수정주가반영)
    """
    try:
        logger.info(f"국내주식 현재가 일자별 테스트 API 호출: {stock_code}")
        
        # KIS 클라이언트를 통한 API 호출
        kis_client = KISClient(db)
        result = kis_client.domestic_daily_price(stock_code, period_div_code, org_adj_prc)
        
        logger.debug(f"KIS API 응답 상태: {result.get('rt_cd', 'unknown')}")
        
        return {
            "status": "ok",
            "stock_code": stock_code,
            "period_div_code": period_div_code,
            "org_adj_prc": org_adj_prc,
            "kis_response": result,
            "description": "국내주식 현재가 일자별 조회 완료"
        }
        
    except Exception as e:
        logger.error(f"API 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 현재가 일자별 조회 중 오류가 발생했습니다: {str(e)}"
        )

# -------- 국내휴장일조회 테스트 API --------

@router.get(
    "/test/holiday-check/{bass_dt}",
    summary="국내휴장일조회 테스트",
    description="""
    KIS API를 통해 국내휴장일 정보를 조회합니다.
    
    **처리 과정:**
    1. 날짜 형식 검증: bass_dt 파라미터가 YYYYMMDD 형식인지 확인 (8자리 숫자)
    2. KIS 클라이언트 초기화: KISClient 인스턴스를 통한 API 연결 설정
    3. KIS API 호출: domestic_holiday_check 메서드를 통해 국내휴장일 정보 조회
    4. 응답 상태 확인: KIS API 응답의 rt_cd 필드를 통한 성공/실패 여부 확인
    5. 결과 반환: 원본 KIS API 응답과 함께 요청 파라미터 정보를 포함하여 반환
    
    **파라미터 설명:**
    - bass_dt: 기준일자 (YYYYMMDD 형식)
    
    **조회 정보:**
    - 영업일여부(bzdy_yn): 금융기관이 업무를 하는 날
    - 거래일여부(tr_day_yn): 증권 업무가 가능한 날
    - 개장일여부(opnd_yn): 주식시장이 개장되는 날 (주문 가능 여부)
    - 결제일여부(sttl_day_yn): 주식 거래에서 실제로 주식을 인수하고 돈을 지불하는 날
    
    **데이터 소스:** KIS API (한국투자증권)
    **API 용도:** 테스트 및 디버깅
    **제한사항:** 모의투자에서는 지원하지 않음 (실전 환경에서만 사용 가능)
    
    **사용 예시:**
    - `GET /marketdata/test/holiday-check/20241201`
    """,
    response_description="국내휴장일 정보를 반환합니다."
)
def test_domestic_holiday_check(
    bass_dt: str,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    📅 국내휴장일조회 테스트 API
    
    KIS API를 통해 국내휴장일 정보를 조회:
    - bass_dt: 기준일자 (YYYYMMDD 형식)
    - 영업일여부(bzdy_yn): 금융기관이 업무를 하는 날
    - 거래일여부(tr_day_yn): 증권 업무가 가능한 날
    - 개장일여부(opnd_yn): 주식시장이 개장되는 날 (주문 가능 여부)
    - 결제일여부(sttl_day_yn): 주식 거래에서 실제로 주식을 인수하고 돈을 지불하는 날
    """
    try:
        logger.info(f"국내휴장일조회 테스트 API 호출: {bass_dt}")
        
        # 날짜 형식 검증
        if len(bass_dt) != 8 or not bass_dt.isdigit():
            raise HTTPException(
                status_code=400, 
                detail="날짜 형식이 올바르지 않습니다. YYYYMMDD 형식으로 입력해주세요."
            )
        
        # KIS 클라이언트를 통한 API 호출
        kis_client = KISClient(db)
        result = kis_client.domestic_holiday_check(bass_dt)
        
        logger.debug(f"KIS API 응답 상태: {result.get('rt_cd', 'unknown')}")
        
        return {
            "status": "ok",
            "bass_dt": bass_dt,
            "kis_response": result,
            "description": "국내휴장일조회 완료",
            "note": "모의투자에서는 지원하지 않습니다. 실전 환경에서만 사용 가능합니다."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"국내휴장일조회 중 오류가 발생했습니다: {str(e)}"
        )

# -------- 국내주식 마켓휴일 정보 동기화 API --------

@router.post(
    "/kr/holidays/sync",
    summary="국내주식 마켓휴일 정보 동기화 ★★★",
    description="""
    KIS 국내휴장일조회 API를 통해 국내주식 휴일 정보를 조회하여 DB에 저장합니다.
    
    **처리 과정:**
    1. 현재 날짜 조회: KST 기준 오늘 날짜를 기준일로 설정
    2. KIS API 호출: KRMarketHolidayService를 통해 KIS 국내휴장일조회 API 호출
    3. 휴일 데이터 수집: 오늘 날짜 기준으로 휴일 정보 조회 (영업일, 거래일, 개장일, 결제일)
    4. 데이터 변환: KIS API 응답을 market_holiday 테이블 형식에 맞게 변환
    5. 중복 제거: 기존 DB 데이터와 중복되지 않는 새로운 휴일 정보만 선별
    6. 데이터베이스 저장: market_holiday 테이블에 exchange='KR'로 저장
    7. 결과 집계: 동기화된 휴일 건수와 상태 정보를 반환
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** market_holiday
    **대상 거래소:** KR (국내)
    **매핑 정보:**
    - 개장일여부(opnd_yn) → is_open 필드
    - exchange='KR'로 저장
    
    **사용 예시:**
    - `POST /marketdata/kr/holidays/sync`
    """,
    response_description="동기화 결과와 처리된 데이터 건수를 반환합니다."
)
def sync_kr_holidays(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    📅 국내주식 마켓휴일 정보 동기화 API
    
    KIS 국내휴장일조회 API를 통해 국내주식 휴일 정보를 수집:
    - 오늘 날짜 기준으로 휴일 정보를 동기화
    - 기존 market_holiday 테이블에 저장 (exchange='KR')
    - 개장일여부(opnd_yn)를 is_open 필드에 매핑
    """
    try:
        logger.info("국내주식 마켓휴일 정보 동기화 API 호출")
        
        service = KRMarketHolidayService(db)
        result = service.sync_holidays_for_today()
        
        logger.debug(f"동기화 결과: {result.get('status', 'unknown')}")
        
        return result
        
    except Exception as e:
        logger.error(f"API 오류: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 마켓휴일 동기화 중 오류가 발생했습니다: {str(e)}"
        )

# -------- 국내주식 현재 휴장 여부 확인 API --------

@router.get(
    "/kr/is-market-closed",
    summary="국내주식 현재 휴장 여부 확인",
    description="""
    현재 날짜가 주말이거나 국내주식 완전휴장(부분개장 제외)인지 판별하여 boolean으로 반환합니다.
    
    **처리 과정:**
    1. 현재 시간 조회: KST 기준 현재 날짜 및 시간 정보 획득
    2. 주말 체크: 현재 날짜가 토요일 또는 일요일인지 확인
    3. 휴일 DB 조회: market_holiday 테이블에서 exchange='KR' 조건으로 현재 날짜의 휴일 정보 조회
    4. 휴장 판별: 주말 또는 국내주식 완전휴장(부분개장 제외) 조건을 종합하여 휴장 여부 판단
    5. 결과 반환: boolean 값으로 국내주식 휴장 여부를 반환
    
    **판별 조건:**
    - 주말 (토요일, 일요일)
    - 국내주식 완전휴장일 (부분개장 제외)
    
    **데이터 소스:** market_holiday 테이블
    **대상 거래소:** KR (국내)
    **응답 형식:** boolean (true: 휴장, false: 개장)
    
    **사용 예시:**
    - `GET /marketdata/kr/is-market-closed`
    """,
    response_description="국내주식 휴장 여부를 boolean으로 반환합니다."
)
def is_kr_market_closed(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    📅 국내주식 현재 휴장 여부 확인 API
    
    현재 날짜가 국내주식 휴장인지 판별:
    - 주말 (토요일, 일요일) 체크
    - 국내주식 완전휴장 (부분개장 제외) 체크
    - exchange='KR' 데이터 기준으로 판별
    """
    try:
        logger.info("국내주식 현재 휴장 여부 확인 API 호출")
        
        logger.debug("KRMarketHolidayService 초기화 시작")
        service = KRMarketHolidayService(db)
        logger.debug("KRMarketHolidayService 초기화 완료")
        
        logger.debug("is_market_closed_now() 호출 시작")
        is_closed = service.is_market_closed_now()
        logger.info(f"국내주식 휴장 여부 확인 완료: {is_closed}")
        
        return {
            "is_market_closed": is_closed,
            "exchange": "KR",
            "description": "국내주식 휴장 여부 확인 완료"
        }
        
    except Exception as e:
        logger.error(f"API 오류: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 휴장 여부 확인 중 오류가 발생했습니다: {str(e)}"
        )

# -------- 국내주식 일봉 데이터 수집 API --------

class TickerIdDailyRequest(BaseModel):
    """국내주식 티커ID 기반 기간별 데이터 수집 요청"""
    ticker_ids: List[int] = Field(..., description="수집할 국내주식 티커 ID 목록")
    days: int = Field(..., gt=0, le=30, description="조회할 거래일 수 (최근 N일, 최대 30일)")

@router.post(
    "/kr/sync/daily",
    summary="국내주식 티커ID 기반 기간별 데이터 수집",
    description="""
    지정된 국내주식 티커 ID들의 일봉 데이터를 KIS API를 통해 수집합니다.
    
    **처리 과정:**
    1. 국내주식 티커 확인: 데이터베이스에서 요청된 티커 ID들이 모두 존재하고 exchange='KOE'인지 검증
    2. 누락 티커 확인: 요청된 티커 ID 중 존재하지 않거나 KOE 거래소가 아닌 ID들을 식별하여 오류 반환
    3. 기간 제한 확인: days 파라미터가 1~30일 범위 내에 있는지 검증 (최대 30일 제한)
    4. KIS API 호출: KRDailyIngestor 서비스를 통해 지정된 기간의 국내주식 일봉 데이터 수집
    5. 데이터 upsert: 수집된 데이터를 ohlcv_daily 테이블에 upsert 처리
    6. 결과 집계: 종목별 수집된 데이터 건수를 집계하여 반환
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **지원 거래소:** KOE (코스피)만 지원
    **기간 제한:** 최대 30일
    
    **사용 예시:**
    - `POST /marketdata/kr/sync/daily` with body: `{"ticker_ids": [123, 456], "days": 21}`
    """,
    response_description="수집된 종목별 데이터 건수와 상태 정보를 반환합니다."
)
def sync_kr_daily(req: TickerIdDailyRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
    # 국내주식 티커 존재 여부 확인
    tickers = db.query(Ticker).filter(
        Ticker.id.in_(req.ticker_ids),
        Ticker.exchange == "KOE"
    ).all()
    if not tickers:
        raise HTTPException(status_code=404, detail="지정된 국내주식 티커 ID들을 찾을 수 없습니다.")
    
    # 누락된 티커 ID 확인
    found_ids = {t.id for t in tickers}
    missing_ids = set(req.ticker_ids) - found_ids
    if missing_ids:
        raise HTTPException(status_code=404, detail=f"다음 국내주식 티커 ID들을 찾을 수 없습니다: {sorted(missing_ids)}")
    
    ingestor = KRDailyIngestor(db)
    counts = ingestor.sync_for_ticker_ids(req.ticker_ids, req.days)
    return {"status": "ok", "upserted": counts}

@router.post(
    "/kr/sync/daily/all",
    summary="전체 국내주식 티커 일봉 데이터 수집 ★★★",
    description="""
    모든 활성 국내주식 티커에 대해 30일치 일봉 데이터를 자동으로 수집합니다.
    
    **처리 과정:**
    1. 활성 국내주식 티커 조회: 데이터베이스에서 모든 활성 상태의 국내주식 티커들(exchange='KOE')을 조회
    2. 배치 처리 설정: 30일치 일봉 데이터 수집을 위한 고정 기간 설정
    3. KIS API 호출: KRDailyIngestor 서비스를 통해 각 국내주식 티커별로 일봉 데이터 수집
    4. 데이터 upsert: 수집된 데이터를 ohlcv_daily 테이블에 upsert 처리
    5. 결과 집계: 전체 티커 수, 성공/실패 건수, 요약 정보를 생성하여 반환
    
    **데이터 소스:** KIS API (한국투자증권)
    **저장 테이블:** ohlcv_daily
    **처리 방식:** 배치 처리 (대량 처리)
    **수집 기간:** 30일 (고정)
    **대상 거래소:** KOE (코스피)만
    
    **사용 예시:**
    - `POST /marketdata/kr/sync/daily/all` (인자 없이 실행)
    """,
    response_description="수집된 종목별 데이터 건수와 전체 요약 정보를 반환합니다."
)
def sync_kr_daily_all(db: Session = Depends(get_db)) -> Dict[str, Any]:
    ingestor = KRDailyIngestor(db)
    counts = ingestor.sync_all_tickers(days=30)
    return {"status": "ok", "upserted": counts}
