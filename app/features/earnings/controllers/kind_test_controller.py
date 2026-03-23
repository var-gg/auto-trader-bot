# app/features/earnings/controllers/kind_test_controller.py

from fastapi import APIRouter, HTTPException, status as http_status
from app.features.earnings.services.kind_crawling_service import KindCrawlingService
from app.features.earnings.models.kind_test_model import (
    KindCrawlingRequest, 
    KindCrawlingResponse, 
    KindConnectionTestResponse,
    KindAdvancedCrawlingRequest,
    KindAdvancedCrawlingResponse,
    CompanyData
)
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/earnings/kind-test", tags=["Kind IR Test"])

kind_service = KindCrawlingService()


@router.post("/crawl", response_model=KindCrawlingResponse)
async def crawl_ir_schedule(request: KindCrawlingRequest):
    """
    KIND IR 게시판 크롤링 테스트 API
    
    KIND 증권거래소 IR 게시판에서 특정 종목의 IR 일정을 크롤링합니다.
    제공받은 헤더와 페이로드 정보를 사용하여 실제 웹사이트에 요청을 보냅니다.
    """
    try:
        logger.info(f"KIND IR 크롤링 요청 수신 - 종목코드: {request.search_code}")
        
        # KIND 크롤링 서비스 호출
        html_content = await kind_service.crawl_ir_schedule(
            search_code=request.search_code,
            from_date=request.from_date,
            to_date=request.to_date,
            page_size=request.page_size,
            page_index=request.page_index
        )
        
        return KindCrawlingResponse(
            success=True,
            message="KIND IR 게시판 크롤링 성공",
            html_content=html_content,
            search_info={
                "search_code": request.search_code,
                "from_date": request.from_date,
                "to_date": request.to_date,
                "page_size": request.page_size,
                "page_index": request.page_index
            }
        )
        
    except Exception as e:
        logger.error(f"KIND IR 크롤링 실패: {str(e)}")
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"KIND IR 크롤링 실패: {str(e)}"
        )


@router.get("/crawl/{search_code}", response_model=KindCrawlingResponse)
async def crawl_ir_schedule_quick(search_code: str):
    """
    KIND IR 게시판 빠른 크롤링 테스트 API (기본값 사용)
    
    종목 코드만 제공하면 기본 검색 조건으로 크롤링을 수행합니다.
    """
    default_request = KindCrawlingRequest(search_code=search_code)
    
    try:
        logger.info(f"KIND IR 빠른 크롤링 요청 수신 - 종목코드: {search_code}")
        
        html_content = await kind_service.crawl_ir_schedule(
            search_code=search_code,
            from_date=default_request.from_date,
            to_date=default_request.to_date,
            page_size=default_request.page_size,
            page_index=default_request.page_index
        )
        
        return KindCrawlingResponse(
            success=True,
            message="KIND IR 게시판 빠른 크롤링 성공",
            html_content=html_content,
            search_info={
                "search_code": search_code,
                "from_date": default_request.from_date,
                "to_date": default_request.to_date,
                "page_size": default_request.page_size,
                "page_index": default_request.page_index
            }
        )
        
    except Exception as e:
        logger.error(f"KIND IR 빠른 크롤링 실패: {str(e)}")
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"KIND IR 빠른 크롤링 실패: {str(e)}"
        )


@router.get("/test-connection", response_model=KindConnectionTestResponse)
async def test_kind_connection():
    """
    KIND 웹사이트 연결 테스트 API
    
    KIND 증권거래소 웹사이트 접근 가능성을 테스트합니다.
    """
    try:
        logger.info("KIND 웹사이트 연결 테스트 시작")
        
        test_result = await kind_service.test_connection()
        
        return KindConnectionTestResponse(**test_result)
        
    except Exception as e:
        logger.error(f"KIND 연결 테스트 실패: {str(e)}")
        return KindConnectionTestResponse(
            status="error",
            status_code=None,
            message=f"KIND 연결 테스트 실패: {str(e)}",
            accessible=False
        )


@router.post("/crawl-advanced", response_model=KindAdvancedCrawlingResponse)
async def crawl_ir_schedule_advanced(request: KindAdvancedCrawlingRequest):
    """
    KIND IR 게시판 고급 크롤링 API
    
    제목 키워드로 검색하여 모든 페이지를 탐색하고, 
    종목코드와 IR 일정 날짜를 추출합니다.
    
    - 전체 건수를 확인하여 페이징 처리
    - HTML에서 종목코드와 날짜 자동 추출
    - 6자리 종목코드로 자동 변환
    """
    try:
        logger.info(f"KIND IR 고급 크롤링 요청 수신 - 제목: {request.title}, 기간: {request.from_date} ~ {request.to_date}")
        
        # KIND 고급 크롤링 서비스 호출
        result = await kind_service.crawl_ir_schedule_advanced(
            title=request.title,
            from_date=request.from_date,
            to_date=request.to_date,
            current_page_size=request.current_page_size
        )
        
        # CompanyData 객체로 변환
        company_data_list = []
        for item in result.get('results', []):
            company_data_list.append(CompanyData(
                company_code=item['company_code'],
                date=item['date'],
                company_name=item['company_name']
            ))
        
        return KindAdvancedCrawlingResponse(
            success=result['success'],
            total_count=result['total_count'],
            extracted_count=result['extracted_count'],
            page_count=result['page_count'],
            results=company_data_list,
            search_params=result['search_params'],
            error=result.get('error')
        )
        
    except Exception as e:
        logger.error(f"KIND IR 고급 크롤링 실패: {str(e)}")
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"KIND IR 고급 크롤링 실패: {str(e)}"
        )


@router.get("/crawl-advanced/{title}")
async def crawl_ir_schedule_advanced_quick(
    title: str,
    from_date: str = "2025-04-05",
    to_date: str = "2025-10-05",
    current_page_size: int = 15
):
    """
    KIND IR 게시판 빠른 고급 크롤링 API
    
    URL 파라미터로 간단하게 크롤링을 수행합니다.
    """
    request = KindAdvancedCrawlingRequest(
        title=title,
        from_date=from_date,
        to_date=to_date,
        current_page_size=current_page_size
    )
    
    return await crawl_ir_schedule_advanced(request)


@router.get("/health")
async def health_check():
    """서비스 상태 확인 API"""
    return {
        "status": "healthy",
        "service": "kind_ir_test",
        "message": "KIND IR 테스트 서비스가 정상적으로 작동중입니다."
    }
