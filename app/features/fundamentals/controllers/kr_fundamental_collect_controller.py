"""
펀더멘털 데이터 수집 컨트롤러
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any

from app.core.db import get_db
from app.features.fundamentals.services.kr_kis_fundamental_service import KISFundamentalService

router = APIRouter(prefix="/api/fundamentals/collect", tags=["[국내주식] Fundamental Collection"])


@router.post(
    "/ticker/{ticker_id}",
    summary="특정 종목 펀더멘털 데이터 수집",
    description="""
    지정된 ticker_id의 한국 주식 펀더멘털 데이터를 KIS API를 통해 수집합니다.
    
    **처리 과정:**
    1. 티커 정보 조회: ticker_id로 한국 주식 여부 확인
    2. KIS API 호출: 3개 API를 순차적으로 호출
       - 주식기본조회: 상장주식수, 전일종가 수집
       - 재무비율: EPS, BPS, 부채비율 수집
       - 예탁원배당일정: 최근 2년 배당이력 수집
    3. 데이터 계산: 시가총액, PER, PBR 계산
    4. DB 저장: 펀더멘털 스냅샷과 배당이력 upsert
    
    **수집 데이터:**
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 부채비율
    - 배당이력: 최근 2년간의 배당 지급 이력 (KRW 기준)
    
    **지원 대상:**
    - 한국 주식만 지원 (country='KR')
    - KOSPI, KOSDAQ 거래소 종목
    - KIS API에서 지원하는 모든 한국 상장 기업
    """,
    response_description="수집된 펀더멘털 데이터와 처리된 배당이력 건수를 반환합니다."
)
async def collect_fundamentals_for_ticker(
    ticker_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        service = KISFundamentalService(db)
        result = service.collect_fundamentals(ticker_id)
        
        return {
            "success": True,
            "message": f"Fundamentals collected successfully for ticker_id: {ticker_id}",
            "data": result
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect fundamentals: {str(e)}")


@router.post(
    "/ticker/{ticker_id}/fundamental-snapshot",
    summary="펀더멘털 스냅샷만 수집",
    description="""
    지정된 ticker_id의 펀더멘털 스냅샷만 수집합니다 (배당이력 제외).
    
    **처리 과정:**
    1. 티커 정보 조회: ticker_id로 한국 주식 여부 확인
    2. KIS API 호출: 2개 API를 순차적으로 호출
       - 주식기본조회: 상장주식수, 전일종가 수집
       - 재무비율: EPS, BPS, 부채비율 수집
    3. 데이터 계산: 시가총액, PER, PBR 계산
    4. DB 저장: 펀더멘털 스냅샷만 upsert
    
    **수집 데이터:**
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 부채비율
    - 배당이력: 수집하지 않음
    
    **사용 목적:**
    - 빠른 펀더멘털 지표 업데이트가 필요한 경우
    - 배당이력은 별도로 관리하고 싶은 경우
    - API 호출 횟수를 줄이고 싶은 경우
    
    **지원 대상:**
    - 한국 주식만 지원 (country='KR')
    - KOSPI, KOSDAQ 거래소 종목
    """,
    response_description="수집된 펀더멘털 스냅샷 데이터를 반환합니다."
)
async def collect_fundamental_snapshot_only(
    ticker_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        service = KISFundamentalService(db)
        
        # ticker 정보 조회
        from app.shared.models.ticker import Ticker
        ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise HTTPException(status_code=404, detail=f"Ticker ID {ticker_id} not found")
        
        if ticker.country != "KR":
            raise HTTPException(status_code=400, detail=f"Only Korean stocks are supported. Ticker {ticker.symbol} is from {ticker.country}")
        
        # symbol 추출
        symbol = ticker.symbol
        if "." in symbol:
            symbol = symbol.split(".")[0]
        
        # API 호출
        stock_basic_result = service.kis_client.stock_basic_info(symbol, "300")
        financial_ratio_result = service.kis_client.financial_ratio(symbol, "0")
        
        # API 응답 검증
        if stock_basic_result.get("rt_cd") != "0":
            raise HTTPException(status_code=400, detail=f"Stock basic info API failed: {stock_basic_result.get('msg1', 'Unknown error')}")
        
        if financial_ratio_result.get("rt_cd") != "0":
            raise HTTPException(status_code=400, detail=f"Financial ratio API failed: {financial_ratio_result.get('msg1', 'Unknown error')}")
        
        # 펀더멘털 스냅샷 저장
        snapshot = service._parse_and_save_fundamental_snapshot(ticker_id, stock_basic_result, financial_ratio_result)
        
        return {
            "success": True,
            "message": f"Fundamental snapshot collected successfully for ticker_id: {ticker_id}",
            "data": {
                "ticker_id": ticker_id,
                "symbol": symbol,
                "snapshot_created": snapshot is not None
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect fundamental snapshot: {str(e)}")


@router.post(
    "/ticker/{ticker_id}/dividend-history",
    summary="배당이력만 수집",
    description="""
    지정된 ticker_id의 배당이력만 수집합니다 (펀더멘털 스냅샷 제외).
    
    **처리 과정:**
    1. 티커 정보 조회: ticker_id로 한국 주식 여부 확인
    2. KIS API 호출: 예탁원배당일정 API 호출
       - 조회 기간: 최근 2년 (오늘 기준)
       - 조회 타입: 배당전체 (결산배당 + 중간배당)
    3. 데이터 파싱: 배당지급일, 주당배당금, 배당률 추출
    4. DB 저장: 배당이력만 upsert (KRW 통화 기준)
    
    **수집 데이터:**
    - 배당이력: 최근 2년간의 배당 지급 이력
      - 배당지급일: YYYY/MM/DD 형식 지원
      - 주당배당금: 현금배당금 (KRW)
      - 배당률: 현금배당률 (%)
    - 펀더멘털 스냅샷: 수집하지 않음
    
    **사용 목적:**
    - 배당 정책 변화 추적이 필요한 경우
    - 배당이력만 별도 업데이트가 필요한 경우
    - 펀더멘털 지표는 최신이고 배당이력만 보완하는 경우
    
    **지원 대상:**
    - 한국 주식만 지원 (country='KR')
    - KOSPI, KOSDAQ 거래소 종목
    """,
    response_description="수집된 배당이력 데이터와 처리된 레코드 건수를 반환합니다."
)
async def collect_dividend_history_only(
    ticker_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        service = KISFundamentalService(db)
        
        # ticker 정보 조회
        from app.shared.models.ticker import Ticker
        ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise HTTPException(status_code=404, detail=f"Ticker ID {ticker_id} not found")
        
        if ticker.country != "KR":
            raise HTTPException(status_code=400, detail=f"Only Korean stocks are supported. Ticker {ticker.symbol} is from {ticker.country}")
        
        # symbol 추출
        symbol = ticker.symbol
        if "." in symbol:
            symbol = symbol.split(".")[0]
        
        # 배당일정 조회 (최근 2년)
        from datetime import datetime, timedelta
        two_years_ago = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
        today = datetime.now().strftime("%Y%m%d")
        
        dividend_result = service.kis_client.dividend_schedule("0", two_years_ago, today, symbol)
        
        # API 응답 검증
        import logging
        logger = logging.getLogger(__name__)
        
        if dividend_result.get("rt_cd") != "0":
            logger.warning(f"Dividend schedule API failed: {dividend_result.get('msg1', 'Unknown error')}")
            dividend_result = {"output1": []}
        
        # 배당이력 저장
        dividend_count = service._parse_and_save_dividend_history(ticker_id, dividend_result)
        
        return {
            "success": True,
            "message": f"Dividend history collected successfully for ticker_id: {ticker_id}",
            "data": {
                "ticker_id": ticker_id,
                "symbol": symbol,
                "dividend_records_processed": dividend_count
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to collect dividend history: {str(e)}")


@router.get(
    "/ticker/{ticker_id}/status",
    summary="펀더멘털 데이터 상태 조회",
    description="""
    지정된 ticker_id의 펀더멘털 데이터 상태를 조회합니다.
    
    **처리 과정:**
    1. 펀더멘털 스냅샷 조회: DB에서 해당 ticker의 최신 스냅샷 확인
    2. 배당이력 개수 조회: 해당 ticker의 배당이력 레코드 수 확인
    3. 데이터 상태 분석: 각 필드별 데이터 존재 여부 확인
    4. 상태 정보 반환: 구조화된 상태 정보 제공
    
    **조회 정보:**
    - 펀더멘털 스냅샷 상태:
      - 존재 여부 및 최근 업데이트 일시
      - PER, PBR, 시가총액, 부채비율 데이터 보유 여부
    - 배당이력 상태:
      - 총 배당이력 레코드 수
    - 전체 데이터 상태 요약
    
    **사용 목적:**
    - 데이터 수집 전 현재 상태 확인
    - 누락된 데이터 필드 파악
    - 데이터 품질 검증
    - 수집 작업 계획 수립
    
    **지원 대상:**
    - 모든 ticker_id (미국/한국 주식 구분 없음)
    - 펀더멘털 데이터가 수집된 모든 종목
    """,
    response_description="펀더멘털 스냅샷과 배당이력의 현재 상태 정보를 반환합니다."
)
async def get_fundamental_status(
    ticker_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        from app.features.fundamentals.models.fundamental_snapshot import FundamentalSnapshot
        from app.features.fundamentals.models.dividend_history import DividendHistory
        
        # 펀더멘털 스냅샷 조회
        snapshot = db.query(FundamentalSnapshot).filter(
            FundamentalSnapshot.ticker_id == ticker_id
        ).first()
        
        # 배당이력 개수 조회
        dividend_count = db.query(DividendHistory).filter(
            DividendHistory.ticker_id == ticker_id
        ).count()
        
        return {
            "success": True,
            "ticker_id": ticker_id,
            "fundamental_snapshot": {
                "exists": snapshot is not None,
                "updated_at": snapshot.updated_at.isoformat() if snapshot else None,
                "has_per": snapshot.per is not None if snapshot else False,
                "has_pbr": snapshot.pbr is not None if snapshot else False,
                "has_market_cap": snapshot.market_cap is not None if snapshot else False,
                "has_debt_ratio": snapshot.debt_ratio is not None if snapshot else False
            },
            "dividend_history": {
                "count": dividend_count
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get fundamental status: {str(e)}")


@router.post(
    "/sync/all",
    summary="전체 한국 주식 펀더멘털 동기화 ★★★",
    description="""
    KOE 거래소의 모든 한국 주식 펀더멘털을 일괄 동기화합니다.
    
    **처리 과정:**
    1. 대상 티커 조회: KOE 거래소의 모든 한국 주식
    2. 순차 처리: 각 종목별로 개별 처리
       - KIS API 호출: 3개 API (주식기본조회, 재무비율, 예탁원배당일정)
       - 데이터 계산: 시가총액, PER, PBR 계산
       - DB 저장: 펀더멘털 스냅샷과 배당이력 upsert
    3. 진행 상황 로깅: 10개 종목마다 진행률 출력
    4. 결과 집계: 성공/실패 통계와 상세 결과 반환
    
    **수집 데이터:**
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 부채비율
    - 배당이력: 최근 2년간의 배당 지급 이력 (KRW 기준)
    
    **성능 특성:**
    - 대량의 KIS API 호출 발생 (종목당 3회 호출)
    - 순차 처리로 안정성 확보
    - 실행 시간이 오래 걸릴 수 있음 (종목 수에 비례)
    
    **지원 대상:**
    - KOE 거래소의 모든 한국 주식
    - KIS API에서 지원하는 모든 한국 상장 기업
    
    **주의사항:**
    - 대량의 API 호출로 인한 실행 시간 지연 가능
    - KIS API 호출 제한에 주의 필요
    """,
    response_description="전체 동기화 결과, 성공/실패 통계, 처리 시간 정보를 반환합니다."
)
async def sync_all_korean_fundamentals(
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 DEBUG: Starting Korean fundamentals sync API call")
        service = KISFundamentalService(db)
        result = service.sync_all_korean_fundamentals()
        
        logger.info(f"✅ DEBUG: Korean fundamentals sync completed - {result['successful']}/{result['total_tickers']} successful, {result['total_dividend_records']} dividend records")
        
        return {
            "success": True,
            "message": f"Korean fundamentals sync completed: {result['successful']}/{result['total_tickers']} successful",
            "data": result
        }
        
    except Exception as e:
        logger.error(f"❌ DEBUG: Korean fundamentals sync failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to sync all Korean fundamentals: {str(e)}")
