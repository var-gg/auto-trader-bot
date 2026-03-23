# app/features/earnings/controllers/dart_corp_code_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import logging

from app.core.db import get_db
from app.features.earnings.services.dart_corp_code_service import DartCorpCodeService
from app.shared.models.dart_corp_code import DartCorpCode

router = APIRouter(prefix="/api/earnings/dart-corp-code", tags=["Earnings - DART Corp Code"])
logger = logging.getLogger(__name__)


@router.post("/sync")
async def sync_corp_codes(db: Session = Depends(get_db)):
    """
    DART에서 최신 기업코드 데이터 동기화
    - ZIP 파일 다운로드 및 파싱
    - 데이터베이스 업데이트/생성
    """
    try:
        logger.info("Starting manual corp code sync...")
        
        service = DartCorpCodeService(db)
        result = service.sync_corp_codes()
        
        if result["status"] != "success":
            raise HTTPException(
                status_code=500,
                detail=f"Sync failed: {result.get('message', 'Unknown error')}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in sync_corp_codes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/stats")
async def get_corp_code_stats(db: Session = Depends(get_db)):
    """
    기업코드 데이터베이스 현황 조회
    """
    try:
        service = DartCorpCodeService(db)
        result = service.get_corp_code_stats()
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_corp_code_stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/lookup")
async def lookup_corp_code(
    corp_code: Optional[str] = Query(None, description="기업 고유번호 (8자리)"),
    stock_code: Optional[str] = Query(None, description="종목코드 (6자리)"),
    db: Session = Depends(get_db)
):
    """
    기업코드 또는 종목코드로 기업 정보 조회
    """
    try:
        if not corp_code and not stock_code:
            raise HTTPException(
                status_code=400,
                detail="Either corp_code or stock_code must be provided"
            )
        
        service = DartCorpCodeService(db)
        corp = service.get_corp_code_lookup(corp_code, stock_code)
        
        if not corp:
            return {
                "status": "not_found",
                "message": "Corporation not found",
                "data": None
            }


        
        return {
            "status": "success",
            "message": "Corporation found",
            "data": corp.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in lookup_corp_code: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/search")
async def search_corp_codes(
    name: str = Query(..., description="검색할 기업명 (부분 검색)"),
    limit: int = Query(10, ge=1, le=100, description="결과 개수 제한"),
    stock_listed_only: bool = Query(False, description="상장기업만 검색"),
    db: Session = Depends(get_db)
):
    """
    기업명으로 기업 검색
    """
    try:
        query = db.query(DartCorpCode).filter(
            DartCorpCode.is_active == True,
            DartCorpCode.corp_name.like(f"%{name}%")
        )
        
        if stock_listed_only:
            query = query.filter(DartCorpCode.is_stock_listed == True)
        
        results = query.limit(limit).all()
        
        return {
            "status": "success",
            "message": f"Found {len(results)} corporations",
            "data": [corp.to_dict() for corp in results],
            "search_params": {
                "name": name,
                "limit": limit,
                "stock_listed_only": stock_listed_only
            }
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in search_corp_codes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/samples")
async def get_sample_corporations(db: Session = Depends(get_db)):
    """
    주요 기업 샘플 데이터 조회 (테스트용)
    """
    try:
        # 주요 기업들의 샘플 데이터
        sample_corp_codes = ["00126380", "00164728", "00163879", "00356361"]  # 삼성전자, SK하이닉스, LG전자, NAVER
        sample_stock_codes = ["005930", "000660", "066570", "035420"]
        
        service = DartCorpCodeService(db)
        sample_data = []
        
        for corp_code, stock_code in zip(sample_corp_codes, sample_stock_codes):
            corp = service.get_corp_code_lookup(corp_code=corp_code)
            if corp:
                sample_data.append(corp.to_dict())
            else:
                # 데이터가 없으면 빈 정보로 표시
                sample_data.append({
                    "corp_code": corp_code,
                    "stock_code": stock_code,
                    "corp_name": "정보 없음",
                    "status": "not_found"
                })
        
        return {
            "status": "success",
            "message": "Sample corporation data retrieved",
            "data": sample_data,
            "description": "주요 상장기업 샘플 데이터 (삼성전자, SK하이닉스, LG전자, NAVER)"
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in get_sample_corporations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/validate")
async def validate_corp_code_mapping(
    corp_code: str = Query(..., description="기업 고유번호 (8자리)"),
    stock_code: str = Query(..., description="종목코드 (6자리)"),
    db: Session = Depends(get_db)
):
    """
    고유번호와 종목코드 매핑 검증
    """
    try:
        service = DartCorpCodeService(db)
        
        # 고유번호로 조회
        corp_by_corp_code = service.get_corp_code_lookup(corp_code=corp_code)
        
        # 종목코드로 조회
        corp_by_stock_code = service.get_corp_code_lookup(stock_code=stock_code)
        
        if not corp_by_corp_code and not corp_by_stock_code:
            return {
                "status": "both_not_found",
                "message": "Both corporation codes not found",
                "validation": {
                    "corp_code_exists": False,
                    "stock_code_exists": False,
                    "mapping_valid": False
                }
            }
        
        elif corp_by_corp_code and corp_by_stock_code:
            # 둘 다 있으면 매핑이 올바른지 확인
            mapping_valid = (
                corp_by_corp_code.stock_code == stock_code and
                corp_by_stock_code.corp_code == corp_code
            )
            
            return {
                "status": "found",
                "message": "Both corporation codes found",
                "validation": {
                    "corp_code_exists": True,
                    "stock_code_exists": True,
                    "mapping_valid": mapping_valid,
                },
                "data": {
                    "corp_by_corp_code": corp_by_corp_code.to_dict() if corp_by_corp_code else None,
                    "corp_by_stock_code": corp_by_stock_code.to_dict() if corp_by_stock_code else None
                }
            }
        
        else:
            # 하나만 있는 경우
            return {
                "status": "partial",
                "message": "Only one corporation code found",
                "validation": {
                    "corp_code_exists": corp_by_corp_code is not None,
                    "stock_code_exists": corp_by_stock_code is not None,
                    "mapping_valid": False,
                },
                "data": {
                    "found_corp": corp_by_corp_code.to_dict() if corp_by_corp_code else None,
                    "not_found_type": "stock_code" if corp_by_corp_code else "corp_code"
                }
            }
        
    except Exception as e:
        logger.error(f"Unexpected error in validate_corp_code_mapping: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/list")
async def list_corp_codes(
    page: int = Query(1, ge=1, description="페이지 번호"),
    page_size: int = Query(20, ge=1, le=100, description="페이지당 항목 수"),
    stock_listed_only: bool = Query(False, description="상장기업만 조회"),
    db: Session = Depends(get_db)
):
    """
    기업코드 목록 조회 (페이지네이션)
    """
    try:
        query = db.query(DartCorpCode).filter(DartCorpCode.is_active == True)
        
        if stock_listed_only:
            query = query.filter(DartCorpCode.is_stock_listed == True)
        
        # 총 개수
        total_count = query.count()
        
        # 페이지네이션
        offset = (page - 1) * page_size
        results = query.offset(offset).limit(page_size).all()
        
        return {
            "status": "success",
            "message": f"Retrieved {len(results)} corporations",
            "data": [corp.to_dict() for corp in results],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": (total_count + page_size - 1) // page_size,
                "has_next": offset + page_size < total_count,
                "has_prev": page > 1
            },
            "filters": {
                "stock_listed_only": stock_listed_only
            }
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in list_corp_codes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
