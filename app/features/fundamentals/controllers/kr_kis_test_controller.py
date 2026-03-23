"""
KIS API 테스트 컨트롤러
- 국내주식 재무비율 API 테스트
- 예탁원정보(배당일정) API 테스트
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from app.core.db import get_db
from app.core.kis_client import KISClient

router = APIRouter(prefix="/api/fundamentals/kis-test", tags=["[국내주식] KIS API Test"])


@router.get(
    "/financial-ratio/{stock_code}",
    summary="국내주식 재무비율 API 테스트",
    description="""
    KIS API의 국내주식 재무비율 조회 기능을 테스트합니다.
    
    **처리 과정:**
    1. KIS API 호출: 국내주식 재무비율 API (TR_ID: FHKST66430300)
    2. 파라미터 전송: 종목코드, 분류구분코드 전달
    3. 응답 검증: API 응답 상태 및 데이터 구조 확인
    4. 결과 반환: 원본 API 응답 데이터 반환
    
    **수집 데이터:**
    - EPS (주당순이익)
    - BPS (주당순자산)
    - 부채비율
    - 기타 재무비율 지표들
    
    **파라미터:**
    - stock_code: 종목코드 (6자리, 예: 000660)
    - div_cls_code: 분류구분코드 (0: 년간, 1: 분기)
    
    **지원 대상:**
    - 한국 주식 (KOSPI, KOSDAQ)
    - KIS API에서 지원하는 모든 한국 상장 기업
    """,
    response_description="KIS API 응답 데이터와 처리 상태를 반환합니다."
)
async def test_financial_ratio(
    stock_code: str,
    div_cls_code: str = "0",
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        kis_client = KISClient(db)
        result = kis_client.financial_ratio(stock_code, div_cls_code)
        
        return {
            "success": True,
            "stock_code": stock_code,
            "div_cls_code": div_cls_code,
            "api_response": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KIS API 호출 실패: {str(e)}")


@router.get("/dividend-schedule")
async def test_dividend_schedule(
    query_type: str = "0",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    stock_code: str = "",
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    예탁원정보(배당일정) API 테스트
    - query_type: 조회구분 (0: 배당전체, 1: 결산배당, 2: 중간배당)
    - from_date: 조회일자From (YYYYMMDD, 기본값: 1개월 전)
    - to_date: 조회일자To (YYYYMMDD, 기본값: 오늘)
    - stock_code: 종목코드 (빈값: 전체)
    """
    try:
        # 기본 날짜 설정
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y%m%d")
        
        kis_client = KISClient(db)
        result = kis_client.dividend_schedule(query_type, from_date, to_date, stock_code)
        
        # API 응답 상태 확인
        rt_cd = result.get("rt_cd", "unknown")
        msg_cd = result.get("msg_cd", "unknown")
        msg1 = result.get("msg1", "unknown")
        
        return {
            "success": rt_cd == "0",
            "query_type": query_type,
            "from_date": from_date,
            "to_date": to_date,
            "stock_code": stock_code,
            "api_status": {
                "rt_cd": rt_cd,
                "msg_cd": msg_cd,
                "msg1": msg1
            },
            "api_response": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KIS API 호출 실패: {str(e)}")


@router.get("/dividend-schedule-debug")
async def test_dividend_schedule_debug(
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    배당일정 API 디버깅용 - 여러 옵션으로 테스트
    """
    try:
        kis_client = KISClient(db)
        
        # 여러 테스트 케이스
        test_cases = [
            {
                "name": "전체 배당 (최근 3개월)",
                "params": {
                    "query_type": "0",
                    "from_date": (datetime.now() - timedelta(days=90)).strftime("%Y%m%d"),
                    "to_date": datetime.now().strftime("%Y%m%d"),
                    "stock_code": ""
                }
            },
            {
                "name": "삼성전자 배당 (최근 1년)",
                "params": {
                    "query_type": "0",
                    "from_date": (datetime.now() - timedelta(days=365)).strftime("%Y%m%d"),
                    "to_date": datetime.now().strftime("%Y%m%d"),
                    "stock_code": "005930"
                }
            },
            {
                "name": "결산배당만 (최근 1년)",
                "params": {
                    "query_type": "1",
                    "from_date": (datetime.now() - timedelta(days=365)).strftime("%Y%m%d"),
                    "to_date": datetime.now().strftime("%Y%m%d"),
                    "stock_code": ""
                }
            }
        ]
        
        results = []
        for test_case in test_cases:
            try:
                result = kis_client.dividend_schedule(**test_case["params"])
                results.append({
                    "test_name": test_case["name"],
                    "params": test_case["params"],
                    "success": result.get("rt_cd") == "0",
                    "response": result
                })
            except Exception as e:
                results.append({
                    "test_name": test_case["name"],
                    "params": test_case["params"],
                    "success": False,
                    "error": str(e)
                })
        
        return {
            "success": True,
            "test_results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KIS API 호출 실패: {str(e)}")


@router.get("/stock-basic-info/{stock_code}")
async def test_stock_basic_info(
    stock_code: str,
    product_type: str = "300",
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    주식기본조회 API 테스트 (v1_국내주식-067)
    - stock_code: 종목번호 (6자리, 예: 005930)
    - product_type: 상품유형코드 (300: 주식/ETF/ETN/ELW, 301: 선물옵션, 302: 채권, 306: ELS)
    """
    try:
        kis_client = KISClient(db)
        result = kis_client.stock_basic_info(stock_code, product_type)
        
        # API 응답 상태 확인
        rt_cd = result.get("rt_cd", "unknown")
        msg_cd = result.get("msg_cd", "unknown")
        msg1 = result.get("msg1", "unknown")
        
        return {
            "success": rt_cd == "0",
            "stock_code": stock_code,
            "product_type": product_type,
            "api_status": {
                "rt_cd": rt_cd,
                "msg_cd": msg_cd,
                "msg1": msg1
            },
            "api_response": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KIS API 호출 실패: {str(e)}")


@router.get("/test-both/{stock_code}")
async def test_both_apis(
    stock_code: str,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    두 API 모두 테스트 (삼성전자 기준)
    - stock_code: 종목코드 (예: 005930)
    """
    try:
        kis_client = KISClient(db)
        
        # 재무비율 조회 (년간)
        financial_ratio_result = kis_client.financial_ratio(stock_code, "0")
        
        # 배당일정 조회 (최근 3개월)
        three_months_ago = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        today = datetime.now().strftime("%Y%m%d")
        dividend_result = kis_client.dividend_schedule("0", three_months_ago, today, stock_code)
        
        return {
            "success": True,
            "stock_code": stock_code,
            "financial_ratio": {
                "api_response": financial_ratio_result
            },
            "dividend_schedule": {
                "from_date": three_months_ago,
                "to_date": today,
                "api_response": dividend_result
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KIS API 호출 실패: {str(e)}")
