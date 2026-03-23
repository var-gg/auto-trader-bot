from fastapi import APIRouter
from app.services.rss_service import get_all_rss

router = APIRouter()

@router.get("/rss")
def get_rss():
    """모든 타겟 뉴스 소스에서 기사 수집"""
    data = get_all_rss()
    return {
        "sources": {name: [item.dict() for item in items] for name, items in data.items()}
    }
