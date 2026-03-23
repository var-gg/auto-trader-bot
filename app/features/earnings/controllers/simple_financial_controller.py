# app/features/earnings/controllers/simple_financial_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
import logging
import requests

from app.core.db import get_db
from app.core.config import get_settings
from app.features.earnings.services.simple_financial_service import SimpleFinancialService
from app.features.earnings.models.simple_financial_model import (
    SimpleFinancialRequest,
    SimpleFinancialResponse
)

router = APIRouter(prefix="/api/earnings/simple-financial", tags=["Earnings - Simple Financial Data"])
logger = logging.getLogger(__name__)


@router.get("/data/{corp_code}/{bsns_year}", response_model=SimpleFinancialResponse)
async def get_simple_financial_data(
    corp_code: str,
    bsns_year: str,
    fs_div: str = Query(default="CFS", description="개별/연결구분 (OFS:재무제표, CFS:연결재무제표)"),
    db: Session = Depends(get_db)
):
    """
    기업의 분기별 재무 데이터 조회 (DART 원본 데이터 그대로)
    
    Args:
        corp_code: 기업고유번호 (8자리, 예: 00126380)
        bsns_year: 사업연도 (4자리, 예: 2023)
        fs_div: 개별/연결구분 (OFS: 재무제표, CFS: 연결재무제표)
    
    Returns:
        SimpleFinancialResponse: DART 원본 재무 데이터 (계산 없음)
        
    Data Fields:
        - gross_profit: 매출총이익
        - net_income: 당기순이익(손실)
        - basic_eps: 기본주당이익(손실)
        - diluted_eps: 희석주당이익(손실)
    """
    try:
        logger.info(f"Starting financial data collection - corp_code: {corp_code}, bsns_year: {bsns_year}, fs_div: {fs_div}")
        
        # 요청 검증
        try:
            request = SimpleFinancialRequest(corp_code=corp_code, bsns_year=bsns_year, fs_div=fs_div)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # 데이터 수집 서비스 실행
        service = SimpleFinancialService(db)
        result = service.get_financial_data_for_corporation(request)
        
        if not result.success:
            status_code = 422 if "찾을 수 없습니다" in result.message else 500
            raise HTTPException(status_code=status_code, detail=result.message)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in financial data collection: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/test/samsung-2023")
async def test_samsung_financial_data(db: Session = Depends(get_db)):
    """
    삼성전자 2023년 재무 데이터 테스트
    - 빠른 검증을 위한 테스트 엔드포인트
    """
    try:
        logger.info("Testing Samsung Electronics financial data for 2023")
        
        service = SimpleFinancialService(db)
        request = SimpleFinancialRequest(
            corp_code="00126380",  # 삼성전자
            bsns_year="2023",
            fs_div="CFS"  # 연결재무제표
        )
        
        result = service.get_financial_data_for_corporation(request)
        
        return {
            "test_info": {
                "corporation": "삼성전자",
                "corp_code": "00126380",
                "stock_code": "005930",
                "analysis_year": "2023",
                "description": "DART 원본 재무 데이터 조회 (계정명 4개)"
            },
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Error in Samsung financial data test: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Samsung financial data test error: {str(e)}"
        )


@router.get("/debug/raw-dart-response")
async def debug_raw_dart_response(
    corp_code: str = Query(default="00126380", description="기업고유번호"),
    bsns_year: str = Query(default="2023", description="사업연도"),
    reprt_code: str = Query(default="11011", description="보고서코드 (11011:사업보고서)"),
    fs_div: str = Query(default="CFS", description="개별/연결구분"),
    db: Session = Depends(get_db)
):
    """
    DART 원본 응답 디버깅용
    - 실제 계정명들을 확인하여 매칭 문제 해결
    """
    try:
        settings = get_settings()
        
        params = {
            "crtfc_key": settings.DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
            "fs_div": fs_div
        }
        
        response = requests.get(
            "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
            params=params,
            timeout=30
        )
        
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}", "response": response.text}
        
        data = response.json()
        
        if data.get("status") != "000":
            return {
                "error": f"DART API Error: {data.get('status')} - {data.get('message')}",
                "full_response": data
            }
        
        account_list = data.get("list", [])
        
        # 계정명 분석
        all_account_names = []
        target_accounts = []
        
        for account in account_list:
            account_nm = account.get("account_nm", "")
            all_account_names.append(account_nm)
            
            # 우리가 찾는 계정들 체크
            if any(keyword in account_nm for keyword in ["매출총이익", "당기순이익", "기본주당이익", "희석주당이익"]):
                target_accounts.append({
                    "account_nm": account_nm,
                    "sj_div": account.get("sj_div"),
                    "sj_nm": account.get("sj_nm"),
                    "thstrm_amount": account.get("thstrm_amount"),
                    "thstrm_add_amount": account.get("thstrm_add_amount")
                })
        
        # 계정명 분석 강화
        target_accounts_detailed = []
        for account in account_list:
            account_nm = account.get("account_nm", "")
            if any(keyword in account_nm for keyword in ["매출총이익", "당기순이익", "기본주당이익", "희석주당이익"]):
                target_accounts_detailed.append({
                    "account_nm": account_nm,
                    "account_id": account.get("account_id"),
                    "sj_div": account.get("sj_div"),
                    "sj_nm": account.get("sj_nm"),
                    "thstrm_amount": account.get("thstrm_amount"),
                    "thstrm_add_amount": account.get("thstrm_add_amount"),
                    "frmtrm_amount": account.get("frmtrm_amount"),
                    "currency": account.get("currency")
                })

        return {
            "summary": {
                "status": "success",
                "total_accounts": len(account_list),
                "target_accounts_found": len(target_accounts_detailed)
            },
            "target_accounts": target_accounts_detailed,
            **data  # 전체 DART 응답 포함
        }
        
    except Exception as e:
        return {"error": str(e)}


@router.get("/info/data-explanation")
async def get_data_explanation():
    """
    데이터 필드 설명
    
    ## 추출 계정들
    
    ### 1. 매출총이익 (gross_profit)
    - **의미**: 매출액에서 매출원가를 뺀 금액
    - **계정명**: "매출총이익"
    - **단위**: 원 (금액)
    
    ### 2. 당기순이익(손실) (net_income)
    - **의미**: 모든 수익과 비용을 차감한 최종 순익
    - **계정명**: "당기순이익(손실)"
    - **단위**: 원 (금액)
    
    ### 3. 기본주당이익(손실) (basic_eps)
    - **의미**: 보통주 기준 주당 순이익
    - **계정명**: "기본주당이익(손실)"
    - **단위**: 원 (주당)
    
    ### 4. 희석주당이익(손실) (diluted_eps)
    - **의미**: 가능한 주식까지 고려한 주당 순이익
    - **계정명**: "희석주당이익(손실)"
    - **단위**: 원 (주당)
    
    ## 금액 필드 설명
    
    - **thstrm_amount**: 당기금액 (해당 분기 금액)
    - **thstrm_add_amount**: 당기누적금액 (연초부터 누적)
    - **frmtrm_amount**: 전기금액 (전년 동분기 금액)
    - **frmtrm_add_amount**: 전기누적금액 (전년 누적)
    - **bfefrmtrm_amount**: 전전기금액 (사업보고서만 제공)
    
    ## 보고서 구분
    
    - **11013**: 1분기보고서
    - **11012**: 반기보고서 (상반기)
    - **11014**: 3분기보고서 (1-3분기)
    - **11011**: 사업보고서 (연간)
    """
    return {
        "api_info": {
            "endpoint": "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
            "description": "DART 단일회사 전체 재무제표 API",
            "method": "GET with corp_code, bsns_year, reprt_code, fs_div"
        },
        "extracted_accounts": {
            "매출총이익": {
                "field_name": "gross_profit",
                "meaning": "매출액에서 매출원가를 뺀 금액",
                "units": "원 (금액)"
            },
            "당기순이익(손실)": {
                "field_name": "net_income",
                "meaning": "모든 수익과 비용을 차감한 최종 순익",
                "units": "원 (금액)"
            },
            "기본주당이익(손실)": {
                "field_name": "basic_eps",
                "meaning": "보통주 기준 주당 순이익",
                "units": "원 (주당)"
            },
            "희석주당이익(손실)": {
                "field_name": "diluted_eps",
                "meaning": "가능한 주식까지 고려한 주당 순이익",
                "units": "원 (주당)"
            }
        },
        "amount_fields": {
            "thstrm_amount": "당기금액 (해당 분기 금액)",
            "thstrm_add_amount": "당기누적금액 (연초부터 누적)",
            "frmtrm_amount": "전기금액 (전년 동분기 금액)",
            "frmtrm_add_amount": "전기누적금액 (전년 누적)",
            "bfefrmtrm_amount": "전전기금액 (사업보고서에만 제공)",
            "currency": "통화 단위 (보통 KRW)"
        },
        "sample_corporations": [
            {"corp_code": "00126380", "name": "삼성전자", "stock_code": "005930"},
            {"corp_code": "00164728", "name": "SK하이닉스", "stock_code": "000660"},
            {"corp_code": "00163879", "name": "LG전자", "stock_code": "066570"},
            {"corp_code": "00356361", "name": "NAVER", "stock_code": "035420"},
        ]
    }
