from fastapi import APIRouter, Depends
from fastapi.params import Query
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.news.repositories.news_repository import NewsRepository
from app.features.news.services.news_ingestion_service import NewsIngestionService
from app.features.news.services.news_filter_service import NewsFilterService
from app.features.news.services.news_content_service import NewsContentService
from app.features.news.services.news_analysis_service import NewsAnalysisService

router = APIRouter(prefix="/news", tags=["news"])

@router.post(
    "/ingest",
    summary="RSS 뉴스 수집",
    description="설정된 RSS 소스들에서 뉴스를 수집하여 데이터베이스에 저장합니다. 중복 링크는 자동으로 제외됩니다.",
    response_description="수집된 뉴스 건수와 상태 정보를 반환합니다."
)
def ingest(db: Session = Depends(get_db)):
    repo = NewsRepository(db)
    counts = NewsIngestionService(repo).ingest_all()
    return {"status": "ok", "inserted": counts}

@router.post(
    "/filter",
    summary="뉴스 관련성 필터링",
    description="수집된 뉴스들을 GPT를 사용하여 자본시장 관련성을 분석하고 0~1 점수를 부여합니다. 점수가 높은 뉴스만 다음 단계로 진행됩니다.",
    response_description="필터링된 뉴스 건수와 상태 정보를 반환합니다."
)
def filter_related(db: Session = Depends(get_db)):
    repo = NewsRepository(db)
    n = NewsFilterService(repo).run(limit=200)
    return {"status": "ok", "filtered": n}

@router.post(
    "/content",
    summary="뉴스 본문 크롤링",
    description="필터링된 뉴스들의 본문을 크롤링하여 저장합니다. 재시도 로직이 포함되어 있어 실패한 경우 자동으로 재시도합니다.",
    response_description="크롤링 성공/실패 건수와 상태 정보를 반환합니다."
)
def fetch_content(db: Session = Depends(get_db)):
    repo = NewsRepository(db)
    result = NewsContentService(repo).run(limit=200)
    return {"status": "ok", **result}

@router.post(
    "/analyze",
    summary="뉴스 AI 분석",
    description="크롤링된 뉴스들을 GPT를 사용하여 한국어 요약, 테마 분류, 티커 매핑을 수행합니다. 18개 고정 테마 중 관련 테마를 자동으로 선택합니다.",
    response_description="분석된 뉴스 건수, 테마 매핑 수, 티커 매핑 수와 상태 정보를 반환합니다."
)
def analyze(db: Session = Depends(get_db)):
    repo = NewsRepository(db)
    result = NewsAnalysisService(db, repo).run(limit=60)
    return {"status": "ok", **result}

@router.post(
    "/run",
    summary="뉴스 파이프라인 전체 실행 ★★★",
    description="뉴스 처리의 전체 파이프라인을 순차적으로 실행합니다: RSS 수집 → 필터링 → 본문 크롤링 → AI 분석. 배치 처리용으로 사용됩니다.",
    response_description="각 단계별 처리 결과와 상태 정보를 반환합니다."
)
def run_all(db: Session = Depends(get_db)):
    repo = NewsRepository(db)
    ing = NewsIngestionService(repo).ingest_all()
    fil = NewsFilterService(repo).run(limit=200)
    cont = NewsContentService(repo).run(limit=200)
    ana = NewsAnalysisService(db, repo).run(limit=60)
    return {"status": "ok", "ingest": ing, "filter": fil, "content": cont, "analyze": ana}
