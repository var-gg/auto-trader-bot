# app/features/news/controllers/news_embedding_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any

from app.core.db import get_db
from app.features.news.services.news_embedding_service import NewsEmbeddingService
from app.features.news.models.news_vector import NewsVector

router = APIRouter(prefix="/news/embedding", tags=["news-embedding"])

@router.post("/create/{news_id}")
def create_embedding(
    news_id: int,
    force_update: bool = Query(False, description="기존 임베딩이 있어도 강제 업데이트"),
    db: Session = Depends(get_db)
):
    """특정 뉴스에 대한 임베딩 생성"""
    service = NewsEmbeddingService(db)
    result = service.create_embedding_for_news(news_id, force_update)
    
    if not result:
        raise HTTPException(status_code=400, detail=f"뉴스 ID {news_id}의 임베딩 생성에 실패했습니다.")
    
    return {
        "status": "success",
        "news_id": news_id,
        "vector_id": result.id,
        "model_name": result.model_name,
        "vector_dimension": result.vector_dimension,
        "status": result.status
    }

@router.post("/create-batch")
def create_embeddings_batch(
    news_ids: List[int],
    force_update: bool = Query(False, description="기존 임베딩이 있어도 강제 업데이트"),
    db: Session = Depends(get_db)
):
    """여러 뉴스에 대한 임베딩을 배치로 생성"""
    if len(news_ids) > 100:
        raise HTTPException(status_code=400, detail="한 번에 최대 100개의 뉴스만 처리할 수 있습니다.")
    
    service = NewsEmbeddingService(db)
    results = service.create_embeddings_batch(news_ids, force_update)
    
    return {
        "status": "completed",
        "results": results
    }

@router.get("/without-embedding")
def get_news_without_embedding(
    limit: int = Query(100, description="조회할 최대 개수", le=1000),
    db: Session = Depends(get_db)
):
    """임베딩이 없는 뉴스 목록 조회"""
    service = NewsEmbeddingService(db)
    news_list = service.get_news_without_embedding(limit)
    
    return {
        "status": "success",
        "count": len(news_list),
        "news": [
            {
                "id": news.id,
                "title": news.title,
                "source": news.source,
                "published_at": news.published_at,
                "content_length": len(news.content) if news.content else 0
            }
            for news in news_list
        ]
    }

@router.get("/{news_id}")
def get_embedding(
    news_id: int,
    db: Session = Depends(get_db)
):
    """특정 뉴스의 임베딩 조회"""
    service = NewsEmbeddingService(db)
    vector = service.get_embedding_by_news_id(news_id)
    
    if not vector:
        raise HTTPException(status_code=404, detail=f"뉴스 ID {news_id}의 임베딩을 찾을 수 없습니다.")
    
    return {
        "status": "success",
        "news_id": news_id,
        "vector_id": vector.id,
        "model_name": vector.model_name,
        "vector_dimension": vector.vector_dimension,
        "text_length": vector.text_length,
        "status": vector.status,
        "created_at": vector.created_at,
        "updated_at": vector.updated_at
    }

@router.get("/stats/summary")
def get_embedding_stats(db: Session = Depends(get_db)):
    """임베딩 통계 조회"""
    service = NewsEmbeddingService(db)
    
    # 전체 뉴스 수
    total_news = db.query(NewsVector).count()
    
    # 성공한 임베딩 수
    success_count = db.query(NewsVector).filter(NewsVector.status == "SUCCESS").count()
    
    # 실패한 임베딩 수
    failed_count = db.query(NewsVector).filter(NewsVector.status == "FAILED").count()
    
    # 임베딩이 없는 뉴스 수
    news_without_embedding = service.get_news_without_embedding(limit=10000)
    without_embedding_count = len(news_without_embedding)
    
    return {
        "status": "success",
        "stats": {
            "total_embeddings": total_news,
            "successful_embeddings": success_count,
            "failed_embeddings": failed_count,
            "news_without_embedding": without_embedding_count,
            "success_rate": round(success_count / total_news * 100, 2) if total_news > 0 else 0
        },
        "model_info": service.get_model_info()
    }

@router.delete("/{news_id}")
def delete_embedding(
    news_id: int,
    db: Session = Depends(get_db)
):
    """특정 뉴스의 임베딩 삭제"""
    vector = db.query(NewsVector).filter(NewsVector.news_id == news_id).first()
    
    if not vector:
        raise HTTPException(status_code=404, detail=f"뉴스 ID {news_id}의 임베딩을 찾을 수 없습니다.")
    
    db.delete(vector)
    db.commit()
    
    return {
        "status": "success",
        "message": f"뉴스 ID {news_id}의 임베딩이 삭제되었습니다."
    }
