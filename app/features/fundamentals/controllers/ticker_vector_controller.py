# app/features/fundamentals/controllers/ticker_vector_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

from app.core.db import get_db
from app.features.fundamentals.services.ticker_vector_service import TickerVectorService
from app.features.fundamentals.services.ticker_source_text_service import TickerSourceTextService
from app.features.fundamentals.models.ticker_vector import TickerVector
from app.shared.models.ticker import Ticker

# Pydantic 모델들
class SourceTextUpdateRequest(BaseModel):
    source_text: str

class BatchCreateRequest(BaseModel):
    batch_size: int = 10
    force_update: bool = False

router = APIRouter(prefix="/ticker/vector", tags=["ticker-vector"])

@router.post("/generate-source-text/{ticker_id}")
def generate_source_text(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    """티커의 소스텍스트 생성"""
    service = TickerSourceTextService(db)
    result = service.generate_source_text_for_ticker(ticker_id)
    
    if not result:
        raise HTTPException(status_code=400, detail=f"티커 ID {ticker_id}의 소스텍스트 생성에 실패했습니다.")
    
    return {
        "status": "success",
        "ticker_id": ticker_id,
        "ticker_symbol": result["ticker_symbol"],
        "company_name": result["company_name"],
        "country": result["country"],
        "source_text": result["source_text"]
    }

@router.post("/create/{ticker_id}")
def create_ticker_vector(
    ticker_id: int,
    force_update: bool = Query(False, description="기존 벡터가 있어도 강제 업데이트"),
    db: Session = Depends(get_db)
):
    """티커 벡터 생성 (소스텍스트 생성 → 임베딩 생성 → DB 저장)"""
    service = TickerVectorService(db)
    result = service.create_ticker_vector(ticker_id, force_update)
    
    if not result:
        raise HTTPException(status_code=400, detail=f"티커 ID {ticker_id}의 벡터 생성에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"티커 ID {ticker_id}의 벡터가 성공적으로 생성되었습니다.",
        "vector": result.to_dict()
    }

@router.put("/update/{ticker_id}")
def update_ticker_vector(
    ticker_id: int,
    request: SourceTextUpdateRequest,
    db: Session = Depends(get_db)
):
    """티커 벡터 업데이트 (외부에서 받은 소스텍스트로 임베딩 재생성)"""
    service = TickerVectorService(db)
    result = service.update_ticker_vector(ticker_id, request.source_text)
    
    if not result:
        raise HTTPException(status_code=404, detail=f"티커 ID {ticker_id}의 벡터를 찾을 수 없거나 업데이트에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"티커 ID {ticker_id}의 벡터가 성공적으로 업데이트되었습니다.",
        "vector": result.to_dict()
    }

@router.get("/{ticker_id}")
def get_ticker_vector(ticker_id: int, db: Session = Depends(get_db)):
    """티커 벡터 조회"""
    service = TickerVectorService(db)
    vector = service.get_ticker_vector(ticker_id)
    
    if not vector:
        raise HTTPException(status_code=404, detail=f"티커 ID {ticker_id}의 벡터를 찾을 수 없습니다.")
    
    return {
        "status": "success",
        "vector": vector.to_dict()
    }

@router.get("/without-vector")
def get_tickers_without_vector(
    limit: int = Query(100, description="조회할 최대 개수", le=1000),
    db: Session = Depends(get_db)
):
    """벡터가 없는 티커 목록 조회"""
    service = TickerVectorService(db)
    tickers = service.get_tickers_without_vector(limit)
    
    return {
        "status": "success",
        "count": len(tickers),
        "tickers": [
            {
                "id": ticker.id,
                "symbol": ticker.symbol,
                "name": ticker.name,
                "exchange": ticker.exchange
            }
            for ticker in tickers
        ]
    }

@router.post("/batch-create")
async def batch_create_vectors_async(
    request: BatchCreateRequest,
    db: Session = Depends(get_db)
):
    """비동기 배치로 티커 벡터 생성 (벡터가 없는 티커들 대상)"""
    service = TickerVectorService(db)
    result = await service.batch_create_vectors_async(
        batch_size=request.batch_size,
        force_update=request.force_update
    )
    
    return {
        "status": "success",
        "result": result
    }

@router.get("/stats/summary")
def get_vector_stats(db: Session = Depends(get_db)):
    """벡터 통계 조회"""
    service = TickerVectorService(db)
    stats = service.get_vector_stats()
    
    return {
        "status": "success",
        "stats": stats
    }

@router.delete("/{ticker_id}")
def delete_ticker_vector(ticker_id: int, db: Session = Depends(get_db)):
    """티커 벡터 삭제"""
    service = TickerVectorService(db)
    success = service.delete_ticker_vector(ticker_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"티커 ID {ticker_id}의 벡터를 찾을 수 없거나 삭제에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"티커 ID {ticker_id}의 벡터가 성공적으로 삭제되었습니다."
    }

