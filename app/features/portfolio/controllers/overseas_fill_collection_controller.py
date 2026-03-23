# app/features/portfolio/controllers/overseas_fill_collection_controller.py

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.portfolio.services.overseas_fill_collection_service import OverseasFillCollectionService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/overseas-fill-collection", tags=["Fill Collection"])


@router.post("/collect")
async def collect_overseas_fills(
    days_back: int = Query(default=7, ge=1, le=30, description="조회할 일수 (1-30일)"),
    db: Session = Depends(get_db)
):
    """
    해외주식 체결정보 수집
    
    KIS API를 통해 해외주식 체결정보를 수집하고 order_fill 테이블에 upsert합니다.
    주기적으로 실행되어 체결상태를 업데이트합니다.
    
    **주요 기능:**
    - KIS 해외주식 주문체결내역 API 호출
    - broker_order와 매핑하여 order_fill upsert
    - 체결상태 자동 결정 (UNFILLED/PARTIAL/FULL/CANCELLED/REJECTED)
    
    **체결상태 매핑:**
    - '00' 접수 → UNFILLED
    - '06' 부분체결 → PARTIAL  
    - '01' 완전체결 → FULL
    - '04' 취소 → CANCELLED
    - '03' 거부 → REJECTED
    
    **매핑 정보:**
    - order_number = odno
    - ord_gno_brno = routing_org_code
    - fill_qty = ft_ccld_qty
    - fill_price = ft_ccld_amt3
    - filled_at = dmst_ord_dt + thco_ord_tmd
    
    **주의사항:**
    - 주기적으로 실행되는 배치 작업용 API입니다
    - 기존 체결정보는 업데이트되고, 새로운 체결정보는 생성됩니다
    - broker_order가 존재하지 않는 경우 무시됩니다
    """
    try:
        logger.info(f"해외주식 체결정보 수집 요청 - {days_back}일간")
        
        service = OverseasFillCollectionService(db)
        result = await service.collect_overseas_fills(days_back)
        
        if not result.get("success", False):
            raise HTTPException(
                status_code=500, 
                detail=result.get("error", "체결정보 수집 중 오류가 발생했습니다.")
            )
        
        logger.info(f"해외주식 체결정보 수집 완료 - 처리: {result.get('processed_count', 0)}건, 업서트: {result.get('upserted_count', 0)}건")
        
        return {
            "success": True,
            "message": result.get("message", "체결정보 수집 완료"),
            "processed_count": result.get("processed_count", 0),
            "upserted_count": result.get("upserted_count", 0),
            "days_back": days_back
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"해외주식 체결정보 수집 API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"체결정보 수집 중 오류가 발생했습니다: {str(e)}")


@router.get("/stats")
async def get_collection_stats(db: Session = Depends(get_db)):
    """
    체결정보 수집 통계 조회
    
    최근 7일간의 체결정보 수집 통계를 조회합니다.
    체결상태별 건수와 전체 통계를 제공합니다.
    
    **응답 데이터:**
    - period: 조회 기간
    - total_fills: 전체 체결 건수
    - status_counts: 체결상태별 건수
      - UNFILLED: 미체결
      - PARTIAL: 부분체결
      - FULL: 완전체결
      - CANCELLED: 취소
      - REJECTED: 거부
    """
    try:
        logger.info("체결정보 수집 통계 조회 요청")
        
        service = OverseasFillCollectionService(db)
        stats = await service.get_collection_stats()
        
        if "error" in stats:
            raise HTTPException(status_code=500, detail=stats["error"])
        
        logger.info("체결정보 수집 통계 조회 완료")
        
        return {
            "success": True,
            "data": stats
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"체결정보 수집 통계 조회 API 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"통계 조회 중 오류가 발생했습니다: {str(e)}")


@router.get("/health")
async def health_check():
    """
    해외주식 체결정보 수집 서비스 상태 확인
    
    서비스의 현재 상태를 확인합니다.
    """
    return {
        "status": "healthy",
        "service": "overseas-fill-collection",
        "description": "해외주식 체결정보 수집 서비스"
    }
