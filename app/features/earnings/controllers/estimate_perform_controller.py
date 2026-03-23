# app/features/earnings/controllers/estimate_perform_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from app.core.db import get_db
from app.features.earnings.services.estimate_perform_service import EstimatePerformService
from app.features.earnings.models.estimate_perform_model import EstimatePerformResponse

router = APIRouter(prefix="/api/earnings/estimate-perform", tags=["Earnings - Estimate Perform"])
logger = logging.getLogger(__name__)


@router.get("/{symbol}", response_model=EstimatePerformResponse)
async def get_estimate_perform(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    국내주식 실적추정 데이터 조회
    - symbol: 종목코드 (예: 005930, 삼성전자)
    """
    try:
        logger.info(f"Getting estimate perform data for symbol: {symbol}")
        
        service = EstimatePerformService(db)
        result = service.get_estimate_perform(symbol)
        
        if result is None:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to fetch estimate perform data for symbol: {symbol}"
            )
        
            if result.rt_cd != '0':
                logger.error(f"KIS API error for {symbol}: {result.msg_cd} - {result.msg1}")
                return result  # 에러 상황도 응답으로 반환 (응답 형태 분석용)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_estimate_perform: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/analyze/{symbol}")
async def analyze_estimate_perform(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    분석된 실적추정 데이터 조회
    - symbol: 종목코드 (예: 005930, 삼성현자)
    - 응답형태 분석을 위한 상세 분석 데이터 제공
    """
    try:
        logger.info(f"Analyzing estimate perform data for symbol: {symbol}")
        
        service = EstimatePerformService(db)
        result = service.analyze_estimate_perform(symbol)
        
        if result is None:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to analyze estimate perform data for symbol: {symbol}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in analyze_estimate_perform: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/comparison/multiple")
async def compare_multiple_symbols(
    symbols: str = Query(..., description="Comma-separated list of symbols (e.g., '005930,000660,035420')"),
    db: Session = Depends(get_db)
):
    """
    여러 종목의 실적추정 데이터 비교 분석
    - symbols: 쉼표로 구분된 종목코드 리스트 (예: 005930,000660,035420)
    """
    try:
        # 문자열을 리스트로 변환
        symbol_list = [s.strip() for s in symbols.split(',') if s.strip()]
        
        if len(symbol_list) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one symbol must be provided"
            )
        
        if len(symbol_list) > 10:
            raise HTTPException(
                status_code=400,
                detail="Maximum 10 symbols allowed per request"
            )
        
        logger.info(f"Comparing estimate perform data for {len(symbol_list)} symbols: {symbol_list}")
        
        service = EstimatePerformService(db)
        result = service.get_multiple_symbols_analysis(symbol_list)
        
        if result is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to perform comparison analysis"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in compare_multiple_symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
                detail=f"Internal server error: {str(e)}"
        )


@router.get("/debug/{symbol}")
async def debug_estimate_perform(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    실적추정 API 원시 응답 디버그용 엔드포인트
    - 응답 구조 분석을 위한 엔드포인트
    """
    try:
        logger.info(f"Debug mode: Getting raw estimate perform data for symbol: {symbol}")
        
        service = EstimatePerformService(db)
        # 원시 응답을 직접 반환 (모델 검증 없이)
        raw_response = service.kis_client.estimate_perform(symbol)
        
        if raw_response is None:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to fetch raw estimate perform data for symbol: {symbol}"
            )
        
        logger.info(f"Debug response for {symbol}: {raw_response}")
        return raw_response
        
    except Exception as e:
        logger.error(f"Unexpected error in debug_estimate_perform: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/test/sample")
async def test_sample_symbols(db: Session = Depends(get_db)):
    """
    샘플 종목들로 실적추정 API 응답 형태 테스트
    - 삼성전자(005930), SK하이닉스(000660), NAVER(035420) 종목 데이터로 테스트
    """
    try:
        sample_symbols = ["005930", "000660", "035420"]
        sample_names = ["삼성전자", "SK하이닉스", "NAVER"]
        
        logger.info(f"Testing estimate perform API with sample symbols: {sample_symbols}")
        
        service = EstimatePerformService(db)
        results = {}
        
        for symbol, name in zip(sample_symbols, sample_names):
            try:
                result = service.analyze_estimate_perform(symbol)
                results[symbol] = {
                    "symbol": symbol,
                    "name": name,
                    "status": "SUCCESS" if result else "FAILED",
                    "data": result
                }
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol} ({name}): {str(e)}")
                results[symbol] = {
                    "symbol": symbol,
                    "name": name,
                    "status": "FAILED",
                    "error": str(e),
                    "data": None
                }
        
        return {
            "status": "COMPLETED",
            "description": "Sample symbols estimate perform test completed",
            "results": results,
            "summary": {
                "total_symbols": len(sample_symbols),
                "successful": len([r for r in results.values() if r["status"] == "SUCCESS"]),
                "failed": len([r for r in results.values() if r["status"] == "FAILED"])
            }
        }
        
    except Exception as e:
        logger.error(f"Error in test_sample_symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
