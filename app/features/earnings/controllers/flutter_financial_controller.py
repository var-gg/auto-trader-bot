# app/features/earnings/controllers/flutter_financial_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
import logging

from app.core.db import get_db
from app.features.earnings.services.flutter_financial_service import FlutterFinancialService
from app.features.earnings.models.flutter_financial_model import (
    FlutterFinancialRequest,
    FlutterFinancialTestResponse
)

router = APIRouter(prefix="/api/earnings/flutter-financial", tags=["Earnings - Financial Statement (DART)"])
logger = logging.getLogger(__name__)


@router.get("/test/{corp_code}/{bsns_year}/{reprt_code}", response_model=FlutterFinancialTestResponse)
async def test_financial_statement(
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
    db: Session = Depends(get_db)
):
    """
    DART 정기보고서 재무정보 테스트 API
    - corp_code: 고유번호 (8자리, 예: 00126380)
    - bsns_year: 사업연도 (4자리, 예: 2023)
    - reprt_code: 보고서코드 (11011:사업보고서, 11012:반기보고서, 11013:1분기보고서, 11014:3분기보고서)
    
    ※ 받은 정보를 그대로 리턴합니다 (데이터 양식 확인용)
    """
    try:
        logger.info(f"Testing financial statement API - corp_code: {corp_code}, bsns_year: {bsns_year}, reprt_code: {reprt_code}")
        
        service = FlutterFinancialService(db)
        result = service.get_financial_statement_test(corp_code, bsns_year, reprt_code)
        
        if result is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve financial statement test data"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in test_financial_statement: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/raw/{corp_code}/{bsns_year}/{reprt_code}")
async def get_raw_financial_data(
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
    db: Session = Depends(get_db)
):
    """
    DART API 원본 응답 조회 (디버깅용)
    - 응답 구조 분석을 위한 원본 데이터 반환
    """
    try:
        logger.info(f"Getting raw financial data - corp_code: {corp_code}, bsns_year: {bsns_year}, reprt_code: {reprt_code}")
        
        service = FlutterFinancialService(db)
        result = service.get_raw_api_response(corp_code, bsns_year, reprt_code)
        
        if result is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve raw financial data"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_raw_financial_data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/samples")
async def test_sample_corporations(db: Session = Depends(get_db)):
    """
    샘플 기업들의 재무정보 테스트
    - 삼성전자, SK하이닉스, LG전자, NAVER 등의 데이터로 테스트
    - 최근 3년간 사업보고서 정보 확인
    """
    try:
        logger.info("Testing financial statement API with sample corporations")
        
        service = FlutterFinancialService(db)
        result = service.get_sample_corporations_test()
        
        if result is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve sample corporations test data"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in test_sample_corporations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/analyze/{corp_code}/{bsns_year}/{reprt_code}")
async def analyze_financial_statement(
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
    db: Session = Depends(get_db)
):
    """
    재무정보 데이터 분석
    - 재무상태표와 손익계산서 분리
    - 개별/연결 구분
    - 주요 지표 분석
    """
    try:
        logger.info(f"Analyzing financial statement - corp_code: {corp_code}, bsns_year: {bsns_year}, reprt_code: {reprt_code}")
        
        service = FlutterFinancialService(db)
        
        # 원본 데이터 조회
        request = FlutterFinancialRequest(
            corp_code=corp_code,
            bsns_year=bsns_year,
            reprt_code=reprt_code
        )
        
        raw_data = service.get_financial_statement_raw(request)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve raw financial data for analysis"
            )
        
        # 데이터 분석
        analysis = service.analyze_financial_data(raw_data)
        
        return {
            "request_info": {
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "report_type": {
                    "11011": "사업보고서",
                    "11012": "반기보고서", 
                    "11013": "1분기보고서",
                    "11014": "3분기보고서"
                }.get(reprt_code, "알 수 없음")
            },
            "api_response": raw_data,
            "analysis": analysis
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in analyze_financial_statement: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/info/report-codes")
async def get_report_codes_info():
    """
    보고서 코드 정보 조회
    """
    return {
        "report_codes": {
            "11011": {
                "name": "사업보고서",
                "description": "연간 사업보고서",
                "period": "연간"
            },
            "11012": {
                "name": "반기보고서",
                "description": "반기 보고서",
                "period": "상반기"
            },
            "11013": {
                "name": "1분기보고서", 
                "description": "1분기 보고서",
                "period": "1분기"
            },
            "11014": {
                "name": "3분기보고서",
                "description": "3분기 보고서", 
                "period": "3분기"
            }
        },
        "sample_corporations": [
            {"corp_code": "00126380", "name": "삼성전자", "stock_code": "005930"},
            {"corp_code": "00164728", "name": "SK하이닉스", "stock_code": "000660"},
            {"corp_code": "00163879", "name": "LG전자", "stock_code": "066570"},
            {"corp_code": "00356361", "name": "NAVER", "stock_code": "035420"},
        ],
        "api_endpoint": "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json",
        "description": "DART 정기보고서 재무정보 API 테스트용 엔드포인트"
    }


@router.get("/quick-test")
async def quick_test(db: Session = Depends(get_db)):
    """
    빠른 테스트용 엔드포인트
    - 삼성전자 2023년 사업보고서로 간단 테스트
    """
    try:
        # 삼성전자 2023년 사업보고서 테스트
        corp_code = "00126380"  # 삼성전자
        bsns_year = "2023"
        reprt_code = "11011"   # 사업보고서
        
        logger.info("Quick test: Samsung Electronics 2023 Annual Report")
        
        service = FlutterFinancialService(db)
        result = service.get_financial_statement_test(corp_code, bsns_year, reprt_code)
        
        return {
            "test_info": {
                "corporation": "삼성전자",
                "corp_code": corp_code,
                "period": "2023년 사업보고서",
                "description": "간단한 API 응답 구조 테스트"
            },
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Error in quick_test: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Quick test error: {str(e)}"
        )
