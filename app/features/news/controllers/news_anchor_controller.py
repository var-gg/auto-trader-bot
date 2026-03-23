# app/features/news/controllers/news_anchor_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

from app.core.db import get_db
from app.features.news.services.news_anchor_service import NewsAnchorService
from app.features.news.models.news_anchor_vector import NewsAnchorVector

# Pydantic 모델들
class AnchorCreateRequest(BaseModel):
    code: str
    name_ko: str
    description: str
    anchor_text: str

class AnchorUpdateRequest(BaseModel):
    name_ko: Optional[str] = None
    description: Optional[str] = None
    anchor_text: Optional[str] = None

router = APIRouter(prefix="/news/anchor", tags=["news-anchor"])

@router.post("/create")
def create_anchor(
    request: AnchorCreateRequest,
    db: Session = Depends(get_db)
):
    """새로운 앵커 벡터 생성"""
    service = NewsAnchorService(db)
    result = service.create_anchor(
        code=request.code,
        name_ko=request.name_ko,
        description=request.description,
        anchor_text=request.anchor_text
    )
    
    if not result:
        raise HTTPException(status_code=400, detail=f"앵커 '{request.code}' 생성에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"앵커 '{request.code}'가 성공적으로 생성되었습니다.",
        "anchor": result.to_dict()
    }

@router.put("/update/{code}")
def update_anchor(
    code: str,
    request: AnchorUpdateRequest,
    db: Session = Depends(get_db)
):
    """기존 앵커 벡터 업데이트"""
    service = NewsAnchorService(db)
    result = service.update_anchor(
        code=code,
        name_ko=request.name_ko,
        description=request.description,
        anchor_text=request.anchor_text
    )
    
    if not result:
        raise HTTPException(status_code=404, detail=f"앵커 '{code}'를 찾을 수 없거나 업데이트에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"앵커 '{code}'가 성공적으로 업데이트되었습니다.",
        "anchor": result.to_dict()
    }

@router.get("/list")
def get_all_anchors(db: Session = Depends(get_db)):
    """모든 앵커 목록 조회"""
    service = NewsAnchorService(db)
    anchors = service.get_all_anchors()
    
    return {
        "status": "success",
        "count": len(anchors),
        "anchors": [anchor.to_dict() for anchor in anchors]
    }

@router.get("/{code}")
def get_anchor_by_code(code: str, db: Session = Depends(get_db)):
    """코드로 특정 앵커 조회"""
    service = NewsAnchorService(db)
    anchor = service.get_anchor_by_code(code)
    
    if not anchor:
        raise HTTPException(status_code=404, detail=f"앵커 '{code}'를 찾을 수 없습니다.")
    
    return {
        "status": "success",
        "anchor": anchor.to_dict()
    }

@router.delete("/{code}")
def delete_anchor(code: str, db: Session = Depends(get_db)):
    """앵커 삭제"""
    service = NewsAnchorService(db)
    success = service.delete_anchor(code)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"앵커 '{code}'를 찾을 수 없거나 삭제에 실패했습니다.")
    
    return {
        "status": "success",
        "message": f"앵커 '{code}'가 성공적으로 삭제되었습니다."
    }

@router.get("/stats/summary")
def get_anchor_stats(db: Session = Depends(get_db)):
    """앵커 통계 조회"""
    service = NewsAnchorService(db)
    stats = service.get_anchor_stats()
    
    return {
        "status": "success",
        "stats": stats
    }

@router.post("/batch-create")
def create_anchors_batch(
    anchors: List[AnchorCreateRequest],
    db: Session = Depends(get_db)
):
    """여러 앵커를 배치로 생성"""
    if len(anchors) > 50:
        raise HTTPException(status_code=400, detail="한 번에 최대 50개의 앵커만 생성할 수 있습니다.")
    
    service = NewsAnchorService(db)
    results = {
        "total": len(anchors),
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    for anchor_request in anchors:
        try:
            result = service.create_anchor(
                code=anchor_request.code,
                name_ko=anchor_request.name_ko,
                description=anchor_request.description,
                anchor_text=anchor_request.anchor_text
            )
            
            if result:
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(f"앵커 '{anchor_request.code}': 생성 실패")
                
        except Exception as e:
            results["failed"] += 1
            results["errors"].append(f"앵커 '{anchor_request.code}': {str(e)}")
    
    return {
        "status": "completed",
        "results": results
    }
