import logging
import requests
import trafilatura
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.news.repositories.news_repository import NewsRepository
from app.features.news.services.news_prompt_service import NewsPromptService

logger = logging.getLogger("news_test")

router = APIRouter(prefix="/news/test", tags=["news-test"])

@router.get(
    "/extract",
    summary="RSS 링크 본문 추출 테스트",
    description="URL을 입력받아 해당 페이지의 본문을 추출하여 반환합니다. 뉴스 내용 크롤링 기능의 테스트용 API입니다.",
    response_description="추출된 본문 내용과 메타 정보를 반환합니다."
)
def test_content_extraction(url: str = Query(..., description="테스트할 RSS 링크 URL")):
    """
    RSS 링크의 본문 추출을 테스트하는 단순한 API
    news_content_service.py의 _fetch_content 메서드와 동일한 로직 사용
    """
    try:
        # news_content_service.py의 _fetch_content와 동일한 로직
        resp = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        
        if resp.status_code != 200:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"HTTP {resp.status_code}: 페이지에 접근할 수 없습니다",
                    "url": url
                }
            )
        
        # trafilatura를 사용한 본문 추출
        extracted_content = trafilatura.extract(resp.text)
        
        if not extracted_content:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "본문을 추출할 수 없습니다. 페이지가 본문 추출에 적합하지 않을 수 있습니다",
                    "url": url
                }
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "url": url,
                "content_length": len(extracted_content),
                "content_preview": extracted_content[:500] + "..." if len(extracted_content) > 500 else extracted_content,
                "full_content": extracted_content
            }
        )
        
    except requests.Timeout:
        return JSONResponse(
            status_code=408,
            content={
                "success": False,
                "error": "요청 타임아웃: URL에 접근하는데 너무 오래 걸립니다",
                "url": url
            }
        )
    except requests.ConnectionError:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "error": "접속 불가: URL에 연결할 수 없습니다",
                "url": url
            }
        )
    except Exception as e:
        logger.error(f"RSS 링크 테스트 중 오류 발생: {url}, 오류: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"처리 중 오류가 발생했습니다: {str(e)}",
                "url": url
            }
        )


@router.get(
    "/classify-prompt",
    response_class=PlainTextResponse,
    summary="뉴스 관련성 분류 프롬프트 테스트",
    description="""
    특정 뉴스 ID에 대해 실제 GPT로 전송되는 관련성 분류 프롬프트를 반환합니다.
    
    **사용 목적:**
    - 뉴스 관련성 분류 프롬프트 디버깅 및 테스트
    - 실제 GPT API 호출 전 프롬프트 검증
    - 프롬프트 품질 개선을 위한 분석
    
    **반환 데이터:**
    - 실제 GPT로 전송되는 프롬프트 텍스트 (PlainTextResponse)
    """,
    response_description="GPT 관련성 분류 프롬프트 텍스트를 직접 반환합니다."
)
def test_classify_finance_relevance_prompt(
    news_id: int = Query(..., description="테스트할 뉴스 ID"),
    db: Session = Depends(get_db)
):
    """뉴스 관련성 분류 프롬프트를 테스트합니다."""
    try:
        logger.info(f"Testing classify finance relevance prompt for news_id: {news_id}")
        
        # 뉴스 데이터 조회
        repo = NewsRepository(db)
        news = repo.get_by_id(news_id)
        
        if not news:
            raise HTTPException(
                status_code=404,
                detail=f"News not found: {news_id}"
            )
        
        # 뉴스 요약 조회 (한국어 요약만)
        news_summary = repo.get_summary_by_news_id_and_lang(news_id, "ko")
        summary_text = news_summary.summary_text if news_summary else None
        
        # 프롬프트 생성
        schema, prompt = NewsPromptService.generate_classify_finance_relevance_prompt(
            title=news.title,
            summary=summary_text
        )
        
        # 스키마와 프롬프트를 합쳐서 완전한 프롬프트 생성
        import json
        schema_text = json.dumps(schema, ensure_ascii=False, indent=2)
        full_prompt = f"### [SYSTEM PROMPT]\n{prompt}\n\n### [OUTPUT SCHEMA]\n{schema_text}"
        
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing classify prompt for news_id {news_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"프롬프트 테스트 실패: {str(e)}"
        )


@router.get(
    "/summarize-prompt",
    response_class=PlainTextResponse,
    summary="뉴스 요약 및 테마 태깅 프롬프트 테스트",
    description="""
    특정 뉴스 ID에 대해 실제 GPT로 전송되는 요약 및 테마 태깅 프롬프트를 반환합니다.
    
    **사용 목적:**
    - 뉴스 요약 및 테마 태깅 프롬프트 디버깅 및 테스트
    - 실제 GPT API 호출 전 프롬프트 검증
    - 프롬프트 품질 개선을 위한 분석
    
    **반환 데이터:**
    - 실제 GPT로 전송되는 프롬프트 텍스트 (PlainTextResponse)
    """,
    response_description="GPT 요약 및 테마 태깅 프롬프트 텍스트를 직접 반환합니다."
)
def test_summarize_and_tag_themes_prompt(
    news_id: int = Query(..., description="테스트할 뉴스 ID"),
    max_themes: int = Query(5, description="최대 테마 개수", ge=1, le=10),
    db: Session = Depends(get_db)
):
    """뉴스 요약 및 테마 태깅 프롬프트를 테스트합니다."""
    try:
        logger.info(f"Testing summarize and tag themes prompt for news_id: {news_id}")
        
        # 뉴스 데이터 조회
        repo = NewsRepository(db)
        news = repo.get_by_id(news_id)
        
        if not news:
            raise HTTPException(
                status_code=404,
                detail=f"News not found: {news_id}"
            )
        
        if not news.content:
            raise HTTPException(
                status_code=400,
                detail=f"News content not available for news_id: {news_id}"
            )
        
        # 프롬프트 생성
        schema, prompt = NewsPromptService.generate_summarize_and_tag_themes_prompt(
            title=news.title,
            content=news.content,
            max_themes=max_themes
        )
        
        # 스키마와 프롬프트를 합쳐서 완전한 프롬프트 생성
        import json
        schema_text = json.dumps(schema, ensure_ascii=False, indent=2)
        full_prompt = f"### [SYSTEM PROMPT]\n{prompt}\n\n### [OUTPUT SCHEMA]\n{schema_text}"
        
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing summarize prompt for news_id {news_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"프롬프트 테스트 실패: {str(e)}"
        )


@router.get(
    "/ticker-scope-prompt",
    response_class=PlainTextResponse,
    summary="티커/스코프 제안 프롬프트 테스트",
    description="""
    특정 뉴스 ID에 대해 실제 GPT로 전송되는 티커/스코프 제안 프롬프트를 반환합니다.
    
    **사용 목적:**
    - 티커/스코프 제안 프롬프트 디버깅 및 테스트
    - 실제 GPT API 호출 전 프롬프트 검증
    - 프롬프트 품질 개선을 위한 분석
    
    **반환 데이터:**
    - 실제 GPT로 전송되는 프롬프트 텍스트 (PlainTextResponse)
    """,
    response_description="GPT 티커/스코프 제안 프롬프트 텍스트를 직접 반환합니다."
)
def test_suggest_tickers_or_scope_prompt(
    news_id: int = Query(..., description="테스트할 뉴스 ID"),
    max_tickers: int = Query(10, description="최대 티커 개수", ge=1, le=20),
    db: Session = Depends(get_db)
):
    """티커/스코프 제안 프롬프트를 테스트합니다."""
    try:
        logger.info(f"Testing ticker/scope suggestion prompt for news_id: {news_id}")
        
        # 뉴스 데이터 조회
        repo = NewsRepository(db)
        news = repo.get_by_id(news_id)
        
        if not news:
            raise HTTPException(
                status_code=404,
                detail=f"News not found: {news_id}"
            )
        
        # 뉴스 요약 조회 (한국어)
        news_summary = repo.get_summary_by_news_id_and_lang(news_id, "ko")
        
        if not news_summary:
            raise HTTPException(
                status_code=400,
                detail=f"Korean summary not found for news_id: {news_id}. Please run summarize first."
            )
        
        # 뉴스 테마 조회
        news_themes = repo.get_themes_by_news_id(news_id)
        themes = [{"theme_id": theme.theme_id} for theme in news_themes]
        
        if not themes:
            raise HTTPException(
                status_code=400,
                detail=f"Themes not found for news_id: {news_id}. Please run analysis first."
            )
        
        # 프롬프트 생성
        schema, prompt = NewsPromptService.generate_suggest_tickers_or_scope_prompt(
            title_ko=news_summary.title_localized,
            summary_ko=news_summary.summary_text,
            themes=themes,
            max_tickers=max_tickers,
            db=db
        )
        
        # 스키마와 프롬프트를 합쳐서 완전한 프롬프트 생성
        import json
        schema_text = json.dumps(schema, ensure_ascii=False, indent=2)
        full_prompt = f"### [SYSTEM PROMPT]\n{prompt}\n\n### [OUTPUT SCHEMA]\n{schema_text}"
        
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing ticker/scope prompt for news_id {news_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"프롬프트 테스트 실패: {str(e)}"
        )
