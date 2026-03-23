# app/features/yahoo_finance/models/yahoo_finance_models.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date


class YahooIndexIngestRequest(BaseModel):
    """야후 파이낸스 지수/환율 데이터 수집 요청 모델"""
    
    period: str = Field(
        default="1mo",
        description="조회 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"
    )


class YahooIndexDataPoint(BaseModel):
    """지수/환율 데이터 포인트"""
    
    d: date = Field(description="날짜")
    value: float = Field(description="값 (종가 또는 환율)")


class YahooIndexIngestResult(BaseModel):
    """개별 심볼 수집 결과"""
    
    symbol: str = Field(description="심볼 코드")
    success: bool = Field(description="성공 여부")
    data_count: int = Field(description="수집된 데이터 개수")
    inserted_count: int = Field(description="DB에 삽입된 데이터 개수")
    updated_count: int = Field(description="DB에 업데이트된 데이터 개수")
    error: Optional[str] = Field(default=None, description="에러 메시지")


class YahooIndexIngestResponse(BaseModel):
    """야후 파이낸스 지수/환율 데이터 수집 응답 모델"""
    
    success: bool = Field(description="전체 성공 여부")
    period: str = Field(description="조회 기간")
    total_symbols: int = Field(description="처리한 심볼 개수")
    successful_symbols: int = Field(description="성공한 심볼 개수")
    failed_symbols: int = Field(description="실패한 심볼 개수")
    results: List[YahooIndexIngestResult] = Field(description="심볼별 수집 결과")
    error: Optional[str] = Field(default=None, description="전체 에러 메시지")


class YahooIndexQueryRequest(BaseModel):
    """야후 파이낸스 지수/환율 데이터 조회 요청 모델"""
    
    symbol: str = Field(
        description="심볼 코드 (^GSPC, ^KS200, KRW=X)"
    )
    start_date: Optional[date] = Field(
        default=None,
        description="시작 날짜 (미입력시 전체)"
    )
    end_date: Optional[date] = Field(
        default=None,
        description="종료 날짜 (미입력시 전체)"
    )


class YahooIndexQueryResponse(BaseModel):
    """야후 파이낸스 지수/환율 데이터 조회 응답 모델"""
    
    success: bool = Field(description="성공 여부")
    symbol: str = Field(description="심볼 코드")
    name: Optional[str] = Field(default=None, description="지수/환율 이름")
    data_count: int = Field(description="조회된 데이터 개수")
    data: List[YahooIndexDataPoint] = Field(description="데이터 배열")
    error: Optional[str] = Field(default=None, description="에러 메시지")

