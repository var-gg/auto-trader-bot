# app/features/yahoo_finance_test/models/yahoo_finance_test_models.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum


class YFSymbol(str, Enum):
    """지원하는 야후 파이낸스 심볼"""
    SP500 = "^GSPC"  # S&P 500
    KOSPI = "^KS11"  # KOSPI
    KOSPI200 = "^KS200"  # KOSPI200
    USDKRW = "KRW=X"  # USD/KRW


class YFPeriod(str, Enum):
    """조회 기간 옵션"""
    ONE_DAY = "1d"
    FIVE_DAYS = "5d"
    ONE_MONTH = "1mo"
    THREE_MONTHS = "3mo"
    SIX_MONTHS = "6mo"
    ONE_YEAR = "1y"
    TWO_YEARS = "2y"
    FIVE_YEARS = "5y"
    TEN_YEARS = "10y"
    YTD = "ytd"
    MAX = "max"


class YFInterval(str, Enum):
    """데이터 간격 옵션"""
    ONE_DAY = "1d"
    FIVE_DAYS = "5d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
    THREE_MONTHS = "3mo"


class YFDailyPriceRequest(BaseModel):
    """야후 파이낸스 일봉 데이터 요청 모델
    
    야후 파이낸스 API를 통해 지수 및 환율 일봉 데이터를 조회하기 위한 요청 모델입니다.
    """
    
    symbol: str = Field(
        default="^GSPC",
        description="심볼 코드 (^GSPC: S&P 500, ^KS11: KOSPI, ^KS200: KOSPI200, KRW=X: USD/KRW)"
    )
    period: str = Field(
        default="1mo",
        description="조회 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"
    )
    interval: str = Field(
        default="1d",
        description="데이터 간격 (1d: 일봉, 5d: 5일봉, 1wk: 주봉, 1mo: 월봉, 3mo: 분기봉)"
    )


class YFDailyPriceData(BaseModel):
    """야후 파이낸스 일봉 데이터 개별 항목"""
    
    date: str = Field(description="날짜 (YYYY-MM-DD)")
    open: Optional[float] = Field(default=None, description="시가")
    high: Optional[float] = Field(default=None, description="고가")
    low: Optional[float] = Field(default=None, description="저가")
    close: Optional[float] = Field(default=None, description="종가")
    adj_close: Optional[float] = Field(default=None, description="조정 종가 (Adjusted Close)")
    volume: Optional[int] = Field(default=None, description="거래량")


class YFDailyPriceResponse(BaseModel):
    """야후 파이낸스 일봉 데이터 응답 모델
    
    야후 파이낸스 API의 일봉 데이터 응답을 담는 모델입니다.
    """
    
    success: bool = Field(description="성공 여부")
    symbol: str = Field(description="심볼 코드")
    period: str = Field(description="조회 기간")
    interval: str = Field(description="데이터 간격")
    data: Optional[List[YFDailyPriceData]] = Field(
        default=None,
        description="일봉 데이터 배열"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="메타데이터 (시간대, 통화, 심볼 정보 등)"
    )
    error: Optional[str] = Field(
        default=None,
        description="에러 메시지 (실패시)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None,
        description="원본 응답 데이터 (완전한 원본 보존)"
    )


class YFMultiSymbolRequest(BaseModel):
    """야후 파이낸스 다중 심볼 조회 요청 모델
    
    여러 심볼을 한 번에 조회하기 위한 요청 모델입니다.
    """
    
    symbols: List[str] = Field(
        default=["^GSPC", "^KS11", "KRW=X"],
        description="심볼 코드 배열 (예: ['^GSPC', '^KS11', 'KRW=X'])"
    )
    period: str = Field(
        default="1mo",
        description="조회 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"
    )
    interval: str = Field(
        default="1d",
        description="데이터 간격 (1d: 일봉, 5d: 5일봉, 1wk: 주봉, 1mo: 월봉, 3mo: 분기봉)"
    )


class YFMultiSymbolResponse(BaseModel):
    """야후 파이낸스 다중 심볼 조회 응답 모델
    
    여러 심볼의 일봉 데이터 응답을 담는 모델입니다.
    """
    
    success: bool = Field(description="전체 성공 여부")
    period: str = Field(description="조회 기간")
    interval: str = Field(description="데이터 간격")
    results: Dict[str, YFDailyPriceResponse] = Field(
        description="심볼별 조회 결과 딕셔너리"
    )
    error: Optional[str] = Field(
        default=None,
        description="전체 에러 메시지 (실패시)"
    )


class YFServiceInfo(BaseModel):
    """야후 파이낸스 테스트 서비스 정보"""
    
    service_name: str = Field(description="서비스 이름")
    description: str = Field(description="서비스 설명")
    supported_symbols: List[Dict[str, str]] = Field(
        description="지원하는 심볼 목록"
    )
    supported_periods: List[str] = Field(
        description="지원하는 조회 기간 목록"
    )
    supported_intervals: List[str] = Field(
        description="지원하는 데이터 간격 목록"
    )
    version: str = Field(description="버전")

