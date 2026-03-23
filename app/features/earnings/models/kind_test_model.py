# app/features/earnings/models/kind_test_model.py

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class KindCrawlingRequest(BaseModel):
    """KIND 크롤링 요청 모델"""
    search_code: str = Field(default="034220", description="검색할 종목 코드 (예: 034220)")
    from_date: str = Field(default="2025-10-04", description="검색 시작 날짜 (YYYY-MM-DD)")
    to_date: str = Field(default="2026-01-04", description="검색 종료 날짜 (YYYY-MM-DD)")
    page_size: int = Field(default=15, description="페이지당 항목 수", ge=1, le=100)
    page_index: int = Field(default=1, description="페이지 인덱스", ge=1)

    class Config:
        schema_extra = {
            "example": {
                "search_code": "034220",
                "from_date": "2025-10-04",
                "to_date": "2026-01-04",
                "page_size": 15,
                "page_index": 1
            }
        }


class KindCrawlingResponse(BaseModel):
    """KIND 크롤링 응답 모델"""
    success: bool = Field(description="크롤링 성공 여부")
    message: str = Field(description="응답 메시지")
    html_content: Optional[str] = Field(description="크롤링된 HTML 내용", default=None)
    search_info: Optional[dict] = Field(description="검색 정보", default=None)
    timestamp: datetime = Field(default_factory=datetime.now, description="응답 시간")


class KindConnectionTestResponse(BaseModel):
    """KIND 연결 테스트 응답 모델"""
    status: str = Field(description="상태 (success/error)")
    status_code: Optional[int] = Field(description="HTTP 상태 코드")
    message: str = Field(description="응답 메시지")
    accessible: bool = Field(description="접근 가능 여부")
    timestamp: datetime = Field(default_factory=datetime.now, description="테스트 시간")


class KindAdvancedCrawlingRequest(BaseModel):
    """KIND 고급 크롤링 요청 모델"""
    title: str = Field(description="검색할 제목 키워드 (예: '실적 발표')")
    from_date: str = Field(description="검색 시작 날짜 (YYYY-MM-DD)")
    to_date: str = Field(description="검색 종료 날짜 (YYYY-MM-DD)")
    current_page_size: int = Field(default=15, description="페이지당 항목 수", ge=1, le=100)

    class Config:
        schema_extra = {
            "example": {
                "title": "실적 발표",
                "from_date": "2025-04-05",
                "to_date": "2025-10-05",
                "current_page_size": 15
            }
        }


class CompanyData(BaseModel):
    """회사 데이터 모델"""
    company_code: str = Field(description="종목코드 (6자리)")
    date: str = Field(description="IR 일정 날짜 (YYYY-MM-DD)")
    company_name: str = Field(description="회사명")


class KindAdvancedCrawlingResponse(BaseModel):
    """KIND 고급 크롤링 응답 모델"""
    success: bool = Field(description="크롤링 성공 여부")
    total_count: int = Field(description="전체 검색 결과 건수")
    extracted_count: int = Field(description="실제 추출된 데이터 건수")
    page_count: int = Field(description="처리된 페이지 수")
    results: List[CompanyData] = Field(description="추출된 회사 데이터 목록")
    search_params: Dict[str, Any] = Field(description="검색 파라미터")
    error: Optional[str] = Field(description="오류 메시지", default=None)
    timestamp: datetime = Field(default_factory=datetime.now, description="응답 시간")
