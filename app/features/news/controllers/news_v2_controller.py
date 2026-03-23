# app/features/news/controllers/news_v2_controller.py
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.db import get_db
from app.core.kis_client import KISClient
from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.repositories.kis_news_repository import KisNewsRepository
from app.features.news.services.news_ingestion_service import NewsIngestionService  # 기존 RSS 수집 재사용
from app.features.news.services.news_v2_content_service import NewsV2ContentService
from app.features.news.services.news_v2_vector_service import NewsV2VectorService
from app.features.news.services.news_v2_economic_service import NewsV2EconomicService
from app.features.news.services.news_v2_ticker_service import NewsV2TickerService
from app.features.news.services.news_v2_summary_service import NewsV2SummaryService
from app.features.news.services.news_v2_reevaluation_service import NewsV2ReevaluationService
from app.features.news.services.kis_news_service import KisNewsService

router = APIRouter(prefix="/news_v2", tags=["news-v2"])
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

@router.post(
    "/run",
    summary="뉴스 V2 파이프라인 전체 실행 ★★★",
    description="뉴스 V2 처리의 전체 파이프라인을 순차적으로 실행합니다: KIS 뉴스 수집 → RSS 수집 → 본문 크롤링 → 벡터 생성 → 경제관련 분류 → 티커 매핑 → GPT-5 재평가(본문 기반) → 요약 생성(0.8 이상만). 새로운 벡터 기반 접근법을 사용합니다.",
    response_description="각 단계별 처리 결과와 상태 정보를 반환합니다."
)
def run_all_v2(
          enable_kis_news: bool = Query(True, description="KIS 뉴스 수집 활성화 여부"),
          content_limit: int = Query(14, description="본문 크롤링 대상 뉴스 수 (1일 이내 뉴스 중 벡터가 없는 것들, 언론사별로 균등 배분)"),
          vector_limit: int = Query(14, description="벡터 생성 대상 뉴스 수 (본문은 있지만 벡터가 없는 뉴스들)"),
          economic_limit: int = Query(14, description="경제관련 분류 대상 뉴스 수 (벡터는 있지만 분류가 안된 뉴스들)"),
          ticker_limit: int = Query(14, description="티커 매핑 대상 뉴스 수 (경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들)"),
          reevaluation_limit: int = Query(14, description="GPT-5 재평가 대상 뉴스 수 (티커 매핑 완료되었지만 요약이 없는 뉴스들)"),
          summary_limit: int = Query(14, description="요약 생성 대상 뉴스 수 (재평가 완료 & confidence >= 0.8인 티커가 있고 요약이 없는 뉴스들)"),
    db: Session = Depends(get_db)
):
    """뉴스 V2 파이프라인 전체 실행"""
    try:
        # 리포지토리 초기화
        repo = NewsV2Repository(db)
        
        # 0. KIS 뉴스 수집 (해외 + 국내)
        kis_overseas_result = {"success": 0, "skipped": 0, "errors": 0}
        kis_domestic_result = {"success": 0, "skipped": 0, "errors": 0}
        
        if enable_kis_news:
            logger.info("=== 0단계: KIS 뉴스 수집 시작 ===")
            try:
                kis_repo = KisNewsRepository(db)
                kis_service = KisNewsService(kis_repo)
                kis_client = KISClient(db)
                
                # 현재 시각 가져오기
                now = datetime.now(KST)
                date_str = now.strftime("%Y%m%d")
                time_str = now.strftime("%H%M%S")
                
                # 0-1. 해외 뉴스 수집
                logger.info("=== 0-1단계: KIS 해외 뉴스 수집 ===")
                try:
                    overseas_response = kis_client.overseas_news_test(
                        INFO_GB="t",
                        CLASS_CD="04",
                        NATION_CD="US",
                        EXCHANGE_CD="",
                        SYMB="",
                        DATA_DT=date_str,
                        DATA_TM=time_str,
                        CTS="",
                    )
                    if overseas_response and overseas_response.get("rt_cd") == "0":
                        outblock1 = overseas_response.get("outblock1", [])
                        if outblock1:
                            kis_overseas_result = kis_service.ingest_overseas_news(outblock1)
                            logger.info(f"KIS 해외 뉴스 수집 완료: {kis_overseas_result}")
                except Exception as e:
                    logger.error(f"KIS 해외 뉴스 수집 실패: {e}")
                
                # 0-2. 국내 뉴스 수집
                logger.info("=== 0-2단계: KIS 국내 뉴스 수집 ===")
                try:
                    domestic_response = kis_client.domestic_news_test(
                        FID_NEWS_OFER_ENTP_CODE="",
                        FID_COND_MRKT_CLS_CODE="",
                        FID_INPUT_ISCD="",
                        FID_TITL_CNTT="",
                        FID_INPUT_DATE_1=date_str,
                        FID_INPUT_HOUR_1=time_str,
                        FID_RANK_SORT_CLS_CODE="",
                        FID_INPUT_SRNO="",
                    )
                    if domestic_response and domestic_response.get("rt_cd") == "0":
                        output = domestic_response.get("output", [])
                        if output:
                            kis_domestic_result = kis_service.ingest_domestic_news(output)
                            logger.info(f"KIS 국내 뉴스 수집 완료: {kis_domestic_result}")
                except Exception as e:
                    logger.error(f"KIS 국내 뉴스 수집 실패: {e}")
                    
            except Exception as e:
                logger.error(f"KIS 뉴스 수집 중 오류 발생: {e}")
        
        # 1. RSS 수집 (기존 서비스 재사용)
        logger.info("=== 1단계: RSS 수집 시작 ===")
        ingestion_service = NewsIngestionService(repo)
        ingest_result = ingestion_service.ingest_all()
        
        # 2. 본문 크롤링 (1일 이내 뉴스 중 news_vector가 없는 것들)
        logger.info("=== 2단계: 본문 크롤링 시작 ===")
        content_service = NewsV2ContentService(repo)
        content_result = content_service.run(limit=content_limit)
        
        # 3. 벡터 생성 (본문이 있지만 벡터가 없는 뉴스들)
        logger.info("=== 3단계: 벡터 생성 시작 ===")
        vector_service = NewsV2VectorService(repo)
        vector_result = vector_service.run(limit=vector_limit)
        
        # 4. 경제관련 분류 (벡터는 있지만 분류가 안된 뉴스들)
        logger.info("=== 4단계: 경제관련 분류 시작 ===")
        economic_service = NewsV2EconomicService(repo)
        economic_result = economic_service.run(limit=economic_limit)
        
        # 5. 티커 매핑 (경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들)
        logger.info("=== 5단계: 티커 매핑 시작 ===")
        ticker_service = NewsV2TickerService(repo)
        ticker_result = ticker_service.run(limit=ticker_limit)
        
        # 6. GPT-5 재평가 (티커 매핑 완료되었지만 요약이 없는 뉴스들, 본문 기반)
        logger.info("=== 6단계: GPT-5 재평가 시작 ===")
        reevaluation_service = NewsV2ReevaluationService(repo)
        reevaluation_result = reevaluation_service.run(limit=reevaluation_limit)
        
        # 7. 요약 생성 (재평가 완료 & confidence >= 0.8인 티커가 있고 요약이 없는 뉴스들)
        logger.info("=== 7단계: 요약 생성 시작 ===")
        summary_service = NewsV2SummaryService(repo)
        summary_result = summary_service.run(limit=summary_limit)
        
        # 전체 결과 반환
        return {
            "status": "ok",
            "pipeline": "news_v2",
            "results": {
                "kis_news_overseas": kis_overseas_result,
                "kis_news_domestic": kis_domestic_result,
                "ingest": ingest_result,
                "content": content_result,
                "vector": vector_result,
                "economic": economic_result,
                "ticker": ticker_result,
                "reevaluation": reevaluation_result,
                "summary": summary_result
            }
        }
        
    except Exception as e:
        logger.error(f"뉴스 V2 파이프라인 실행 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"파이프라인 실행 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/content",
    summary="뉴스 V2 본문 크롤링",
    description="1일 이내 뉴스 중 news_vector가 없는 것들의 본문을 크롤링합니다.",
    response_description="크롤링 성공/실패 건수와 상태 정보를 반환합니다."
)
def fetch_content_v2(
    limit: int = Query(14, description="본문 크롤링 대상 뉴스 수 (1일 이내 뉴스 중 벡터가 없는 것들, 언론사별로 균등 배분)", le=1000),
    db: Session = Depends(get_db)
):
    """뉴스 V2 본문 크롤링"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2ContentService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"본문 크롤링 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"본문 크롤링 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/vector",
    summary="뉴스 V2 벡터 생성",
    description="본문이 있지만 벡터가 없는 뉴스들의 벡터를 생성합니다.",
    response_description="벡터 생성 성공/실패 건수와 상태 정보를 반환합니다."
)
def generate_vectors_v2(
    limit: int = Query(14, description="벡터 생성 대상 뉴스 수 (본문은 있지만 벡터가 없는 뉴스들)", le=500),
    db: Session = Depends(get_db)
):
    """뉴스 V2 벡터 생성"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2VectorService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"벡터 생성 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"벡터 생성 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/economic",
    summary="뉴스 V2 경제관련 분류",
    description="벡터는 있지만 경제관련 분류가 안된 뉴스들의 경제관련 여부를 판단합니다.",
    response_description="경제관련 분류 성공/실패 건수와 상태 정보를 반환합니다."
)
def classify_economic_v2(
    limit: int = Query(14, description="경제관련 분류 대상 뉴스 수 (벡터는 있지만 분류가 안된 뉴스들)", le=500),
    db: Session = Depends(get_db)
):
    """뉴스 V2 경제관련 분류"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2EconomicService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"경제관련 분류 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"경제관련 분류 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/ticker",
    summary="뉴스 V2 티커 매핑",
    description="경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들의 티커를 매핑합니다.",
    response_description="티커 매핑 성공/실패 건수와 매핑된 티커 수를 반환합니다."
)
def map_tickers_v2(
    limit: int = Query(14, description="티커 매핑 대상 뉴스 수 (경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들)", le=500),
    db: Session = Depends(get_db)
):
    """뉴스 V2 티커 매핑"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2TickerService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"티커 매핑 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"티커 매핑 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/summary",
    summary="뉴스 V2 요약 생성",
    description="재평가 완료 & confidence >= 0.8인 티커가 있고 요약이 없는 뉴스들의 요약을 생성합니다.",
    response_description="요약 생성 성공/실패 건수와 상태 정보를 반환합니다."
)
def generate_summary_v2(
    limit: int = Query(14, description="요약 생성 대상 뉴스 수 (재평가 완료 & confidence >= 0.8인 티커가 있고 요약이 없는 뉴스들)", le=500),
    db: Session = Depends(get_db)
):
    """뉴스 V2 요약 생성"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2SummaryService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"요약 생성 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"요약 생성 중 오류가 발생했습니다: {str(e)}")

@router.post(
    "/reevaluation",
    summary="뉴스 V2 GPT-5 재평가",
    description="티커 매핑 완료되었지만 요약이 없는 뉴스들의 본문을 GPT-5에 전달하여 티커 신뢰도를 재평가합니다.",
    response_description="재평가 성공/실패 건수와 업데이트된 티커 수를 반환합니다."
)
def reevaluate_confidence_v2(
    limit: int = Query(14, description="재평가 대상 뉴스 수 (티커 매핑 완료되었지만 요약이 없는 뉴스들)", le=100),
    db: Session = Depends(get_db)
):
    """뉴스 V2 GPT-5 재평가"""
    try:
        repo = NewsV2Repository(db)
        service = NewsV2ReevaluationService(repo)
        result = service.run(limit=limit)
        return {"status": "ok", **result}
    except Exception as e:
        logger.error(f"GPT-5 재평가 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"GPT-5 재평가 중 오류가 발생했습니다: {str(e)}")
