# app/features/signals/models/similarity_models.py
from __future__ import annotations
from datetime import date, datetime
from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field

from app.features.signals.models.signal_models import SignalDirection, AlgorithmVersion


class CountryFilter(str, Enum):
    """
    국가 필터
    """
    ALL = "ALL"  # 전체
    US = "US"    # 미국
    KR = "KR"    # 한국


class SimilaritySearchRequest(BaseModel):
    """
    유사도 검색 요청 모델
    """
    ticker_id: Optional[int] = Field(None, description="분석할 티커 ID (save=False일 때 필수, save=True일 때 빈값이면 전체 티커 배치 처리)", gt=0)
    reference_date: Optional[date] = Field(None, description="기준일자 (빈값이면 오늘)")
    lookback: int = Field(default=10, description="벡터화할 기간 (캔들 개수)", ge=3, le=20)
    top_k: int = Field(default=10, description="반환할 유사 시그널 개수", ge=1, le=50)
    direction_filter: Optional[SignalDirection] = Field(None, description="필터링할 방향 (빈값이면 전체)")
    version: AlgorithmVersion = Field(default=AlgorithmVersion.V3, description="알고리즘 버전 (v1: 평탄성기반, v2: 지속형, v3: 혼합형)")
    save: bool = Field(default=False, description="분석 결과 저장 여부 (True: DB 저장, False: 저장 안함)")
    country: CountryFilter = Field(default=CountryFilter.ALL, description="국가 필터 (배치 모드 전용, ALL: 전체, US: 미국, KR: 한국)")


class SimilarSignal(BaseModel):
    """
    유사 시그널 정보
    """
    result_id: int = Field(..., description="결과 ID")
    ticker_id: int = Field(..., description="티커 ID")
    symbol: str = Field(..., description="종목 심볼")
    exchange: str = Field(..., description="거래소 코드")
    signal_date: date = Field(..., description="시그널 발생 날짜")
    direction: str = Field(..., description="시그널 방향 (UP/DOWN)")
    close: float = Field(..., description="종가")
    change_7_24d: float = Field(..., description="이후 변동률")
    similarity: float = Field(..., description="유사도 (0~1, 높을수록 유사)")
    config_id: int = Field(..., description="설정 ID")


class SimilaritySearchResponse(BaseModel):
    """
    유사도 검색 응답 모델
    """
    query_ticker_id: Optional[int] = Field(None, description="조회한 티커 ID (배치 모드일 때는 null)")
    query_symbol: Optional[str] = Field(None, description="조회한 종목 심볼 (배치 모드일 때는 null)")
    query_exchange: Optional[str] = Field(None, description="조회한 거래소 (배치 모드일 때는 null)")
    reference_date: date = Field(..., description="기준일자")
    lookback: int = Field(..., description="사용한 lookback")
    vector_dim: Optional[int] = Field(None, description="생성된 벡터 차원 (배치 모드일 때는 null)")
    total_compared: int = Field(default=0, description="비교한 시그널 총 개수")
    similar_signals: List[SimilarSignal] = Field(default_factory=list, description="유사 시그널 목록 (유사도 높은 순)")
    
    # 배치 처리 결과 (save=True + ticker_id=None일 때만 사용)
    is_batch: bool = Field(default=False, description="배치 처리 모드 여부")
    total_tickers: int = Field(default=0, description="처리 대상 티커 총 개수")
    success_count: int = Field(default=0, description="성공한 티커 수")
    error_count: int = Field(default=0, description="실패한 티커 수")
    saved_count: int = Field(default=0, description="저장된 분석 결과 수")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query_ticker_id": 1,
                "query_symbol": "AAPL",
                "query_exchange": "NMS",
                "reference_date": "2024-10-11",
                "lookback": 10,
                "vector_dim": 17,
                "total_compared": 150,
                "similar_signals": [
                    {
                        "result_id": 42,
                        "ticker_id": 5,
                        "symbol": "MSFT",
                        "exchange": "NMS",
                        "signal_date": "2024-09-15",
                        "direction": "UP",
                        "close": 420.50,
                        "change_7_24d": 0.085,
                        "similarity": 0.95,
                        "config_id": 1
                    }
                ]
            }
        }


class IntradaySimilaritySearchRequest(BaseModel):
    """
    분봉 유사도 검색 요청 모델
    """
    ticker_id: int = Field(..., description="분석할 티커 ID", gt=0)
    reference_datetime: Optional[str] = Field(None, description="기준일시 (YYYYMMDD HHMMSS, 빈값이면 현재시각)")
    lookback: int = Field(default=10, description="벡터화할 기간 (캔들 개수)", ge=3, le=20)
    top_k: int = Field(default=10, description="반환할 유사 시그널 개수", ge=1, le=50)
    direction_filter: Optional[SignalDirection] = Field(None, description="필터링할 방향 (빈값이면 전체)")
    version: AlgorithmVersion = Field(default=AlgorithmVersion.V1, description="알고리즘 버전")


class IntradaySimilarSignal(BaseModel):
    """
    분봉 유사 시그널 정보
    """
    result_id: int = Field(..., description="결과 ID")
    ticker_id: int = Field(..., description="티커 ID")
    symbol: str = Field(..., description="종목 심볼")
    exchange: str = Field(..., description="거래소 코드")
    signal_datetime: str = Field(..., description="시그널 발생 날짜+시간 (YYYYMMDD HHMMSS)")
    direction: str = Field(..., description="시그널 방향 (UP/DOWN)")
    close: float = Field(..., description="종가")
    change_7_24d: float = Field(..., description="이후 변동률")
    similarity: float = Field(..., description="유사도 (0~1, 높을수록 유사)")
    config_id: int = Field(..., description="설정 ID")


class IntradaySimilaritySearchResponse(BaseModel):
    """
    분봉 유사도 검색 응답 모델
    """
    query_ticker_id: int = Field(..., description="조회한 티커 ID")
    query_symbol: str = Field(..., description="조회한 종목 심볼")
    query_exchange: str = Field(..., description="조회한 거래소")
    reference_datetime: str = Field(..., description="기준일시 (YYYYMMDD HHMMSS)")
    lookback: int = Field(..., description="사용한 lookback")
    vector_dim: int = Field(..., description="생성된 벡터 차원")
    total_compared: int = Field(..., description="비교한 시그널 총 개수")
    similar_signals: List[IntradaySimilarSignal] = Field(default_factory=list, description="유사 시그널 목록")


class BacktestRequest(BaseModel):
    """
    백테스팅 요청 모델
    """
    ticker_id: int = Field(..., description="분석할 티커 ID", gt=0)
    from_date: date = Field(..., description="백테스팅 시작일")
    lookback: int = Field(default=10, description="벡터화할 기간 (캔들 개수)", ge=3, le=20)
    top_k: int = Field(default=10, description="유사도 비교할 시그널 개수", ge=1, le=50)
    exit_window: int = Field(default=15, description="매수 후 청산까지 기간", ge=1, le=30)
    peak_threshold: float = Field(default=0.05, description="상승 피크 감지 임계값 (5% = 0.05)", ge=0.01, le=0.20)


class BacktestResponse(BaseModel):
    """
    백테스팅 응답 모델
    """
    ticker_id: int = Field(..., description="티커 ID")
    symbol: str = Field(..., description="종목 심볼")
    exchange: str = Field(..., description="거래소")
    from_date: date = Field(..., description="백테스팅 시작일")
    to_date: date = Field(..., description="백테스팅 종료일")
    lookback: int = Field(..., description="사용한 lookback")
    top_k: int = Field(..., description="유사도 비교한 시그널 개수")
    exit_window: int = Field(..., description="청산 윈도우 (일)")
    
    # 백테스팅 결과
    total_signals: int = Field(..., description="총 매수 시그널 발생 횟수")
    correct_direction_count: int = Field(..., description="방향 적중 횟수 (청산가 > 매수가)")
    peak_gain_5pct_count: int = Field(..., description="윈도우 기간 중 5% 이상 상승 경험 횟수")
    total_profit: float = Field(..., description="총 수익 (절대값)")
    return_rate: float = Field(..., description="수익률 (총 수익 / 총 투자금)")
    
    # 통계
    win_rate: float = Field(..., description="승률 (방향 적중 / 총 시그널)")
    peak_experience_rate: float = Field(..., description="피크 경험률 (5% 이상 상승 / 총 시그널)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "ticker_id": 1,
                "symbol": "AAPL",
                "exchange": "NMS",
                "from_date": "2023-01-01",
                "to_date": "2024-10-15",
                "lookback": 10,
                "top_k": 10,
                "exit_window": 15,
                "total_signals": 25,
                "correct_direction_count": 18,
                "peak_gain_5pct_count": 22,
                "total_profit": 1250.50,
                "return_rate": 0.125,
                "win_rate": 0.72,
                "peak_experience_rate": 0.88
            }
        }
