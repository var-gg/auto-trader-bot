# app/features/signals/models/signal_models.py
from __future__ import annotations
from datetime import date
from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


class SignalDirection(str, Enum):
    """
    시그널 방향
    """
    UP = "UP"      # 상승 시그널
    DOWN = "DOWN"  # 하락 시그널
    ALL = "ALL"    # 상승 + 하락 모두


class SaveOption(str, Enum):
    """
    저장 옵션
    """
    NONE = "NONE"  # 저장 안함
    SAVE = "SAVE"  # 저장


class AlgorithmVersion(str, Enum):
    """
    알고리즘 버전
    """
    V1 = "v1"  # 평탄성 기반 (직전 구간 평탄 + 이후 변동)
    V2 = "v2"  # 지속형 (이후 구간만 평가)
    V3 = "v3"  # 혼합형 (v1 + v2 병합, 벡터는 v2)
    V4 = "v4"  # 안정적 돌파 (윈도우 기반, 시작부 평탄 + 중간에 저점/고점 깨지 않음)


class SignalDetectionRequest(BaseModel):
    """
    시그널 탐지 요청 모델
    """
    ticker_id: Optional[int] = Field(None, description="티커 ID (빈값이면 전체 티커 처리, SAVE 모드에서만 가능)", gt=0)
    days: int = Field(default=100, description="분석할 기간 (거래일 수)", ge=20, le=500)
    direction: SignalDirection = Field(default=SignalDirection.UP, description="시그널 방향 (UP: 상승, DOWN: 하락)")
    save_option: SaveOption = Field(default=SaveOption.NONE, description="저장 옵션 (NONE: 저장 안함, SAVE: DB 저장)")
    version: AlgorithmVersion = Field(default=AlgorithmVersion.V1, description="알고리즘 버전 (v1: 평탄성기반, v2: 지속형, v3: 혼합형)")
    
    # 알고리즘 파라미터
    lookback: int = Field(default=10, description="직전 구간 확인 기간 (시그널 이전 최소 캔들 수 보장)", ge=3, le=20)
    future_window: int = Field(default=15, description="이후 구간 평가 기간 (상승/하락 확인)", ge=3, le=15)
    min_change: float = Field(default=0.10, description="최소 변동률 (UP: 최소 상승률, DOWN: 최소 하락률)", ge=0.03, le=0.20)
    max_reverse: float = Field(default=0.03, description="반대 방향 최대 허용폭 (UP: 하락폭 제한, DOWN: 상승폭 제한)", ge=0.01, le=0.10)
    flatness_k: float = Field(default=1.0, description="직전 구간 평탄성 허용치 (ATR 배수)", ge=0.5, le=1.5)


class SignalPoint(BaseModel):
    """
    개별 시그널 포인트
    """
    signal_date: date = Field(..., description="시그널 발생 날짜")
    direction: str = Field(..., description="시그널 방향 (UP/DOWN)")
    close: float = Field(..., description="종가")
    change_7_24d: float = Field(..., description="이후 7~24일 최대 변동률 (UP: 상승률, DOWN: 하락률)")
    past_slope: float = Field(..., description="직전 구간 기울기 (양수면 상승, 음수면 하락)")
    past_std: float = Field(..., description="직전 구간 표준편차 (평탄성)")
    atr: Optional[float] = Field(None, description="평균 변동성 (ATR)")
    prior_candles: int = Field(..., description="시그널 이전 캔들 개수")


class SignalDetectionResponse(BaseModel):
    """
    시그널 탐지 응답 모델
    """
    ticker_id: Optional[int] = Field(None, description="티커 ID")
    symbol: Optional[str] = Field(None, description="종목 심볼")
    exchange: Optional[str] = Field(None, description="거래소 코드")
    requested_direction: str = Field(..., description="요청한 방향 (UP/DOWN/ALL)")
    total_candles: int = Field(..., description="분석에 사용된 총 캔들 수")
    total_signals: int = Field(..., description="탐지된 시그널 총 개수")
    up_signals: int = Field(default=0, description="상승 시그널 개수")
    down_signals: int = Field(default=0, description="하락 시그널 개수")
    lookback: int = Field(..., description="적용된 lookback (시그널 이전 최소 캔들 보장)")
    data_start_date: Optional[date] = Field(None, description="데이터 시작 날짜")
    data_end_date: Optional[date] = Field(None, description="데이터 종료 날짜")
    signals: List[SignalPoint] = Field(default_factory=list, description="탐지된 시그널 목록")
    
    # 배치 처리용 필드
    is_batch: bool = Field(default=False, description="배치 처리 여부")
    batch_summary: Optional[Dict[str, Any]] = Field(None, description="배치 처리 요약 정보")
    
    class Config:
        json_schema_extra = {
            "example": {
                "ticker_id": 1,
                "symbol": "AAPL",
                "exchange": "NMS",
                "requested_direction": "ALL",
                "total_candles": 100,
                "total_signals": 8,
                "up_signals": 5,
                "down_signals": 3,
                "lookback": 10,
                "data_start_date": "2024-07-01",
                "data_end_date": "2024-10-11",
                "signals": [
                    {
                        "signal_date": "2024-09-15",
                        "direction": "UP",
                        "close": 150.25,
                        "change_7_24d": 0.065,
                        "past_slope": -0.15,
                        "past_std": 1.2,
                        "atr": 2.5,
                        "prior_candles": 45
                    }
                ]
            }
        }


class IntradaySignalPoint(BaseModel):
    """
    5분봉 개별 시그널 포인트
    """
    signal_datetime: str = Field(..., description="시그널 발생 날짜+시간 (YYYYMMDD HHMMSS)")
    direction: str = Field(..., description="시그널 방향 (UP/DOWN)")
    close: float = Field(..., description="종가")
    change_7_24d: float = Field(..., description="이후 변동률")
    past_slope: float = Field(..., description="직전 구간 기울기")
    past_std: float = Field(..., description="직전 구간 표준편차")
    atr: Optional[float] = Field(None, description="평균 변동성 (ATR)")
    prior_candles: int = Field(..., description="시그널 이전 캔들 개수")


class IntradaySignalRequest(BaseModel):
    """
    5분봉 시그널 탐지 요청 모델
    """
    ticker_id: Optional[int] = Field(None, description="티커 ID (없으면 전체 티커 배치 처리)", gt=0)
    candles: int = Field(default=100, description="분석할 캔들 개수 (5분봉)", ge=7, le=100000)
    direction: SignalDirection = Field(default=SignalDirection.UP, description="시그널 방향 (UP: 상승, DOWN: 하락)")
    version: AlgorithmVersion = Field(default=AlgorithmVersion.V1, description="알고리즘 버전 (v1: 평탄성기반, v2: 지속형, v3: 혼합형)")
    save_option: SaveOption = Field(default=SaveOption.NONE, description="저장 옵션 (NONE: 저장안함, SAVE: DB저장)")
    
    # 알고리즘 파라미터 (5분봉 특화: 더 작은 변동률 지원)
    lookback: int = Field(default=10, description="직전 구간 확인 기간", ge=3, le=20)
    future_window: int = Field(default=15, description="이후 구간 평가 기간", ge=7, le=24)
    min_change: float = Field(default=0.015, description="최소 변동률 (소수점 3자리: 0.001~0.200)", ge=0.001, le=0.200)
    max_reverse: float = Field(default=0.005, description="반대 방향 최대 허용폭 (소수점 3자리: 0.001~0.100)", ge=0.001, le=0.100)
    flatness_k: float = Field(default=1.0, description="평탄성 허용치 (ATR 배수)", ge=0.5, le=1.5)


class IntradaySignalResponse(BaseModel):
    """
    5분봉 시그널 탐지 응답 모델
    """
    ticker_id: Optional[int] = Field(None, description="티커 ID (배치 처리 시 None)")
    symbol: Optional[str] = Field(None, description="종목 심볼 (배치 처리 시 None)")
    exchange: Optional[str] = Field(None, description="거래소 코드 (배치 처리 시 None)")
    requested_direction: str = Field(..., description="요청한 방향")
    total_candles: int = Field(default=0, description="수집된 총 캔들 수")
    total_signals: int = Field(default=0, description="탐지된 시그널 총 개수")
    up_signals: int = Field(default=0, description="상승 시그널 개수")
    down_signals: int = Field(default=0, description="하락 시그널 개수")
    version: str = Field(..., description="사용된 알고리즘 버전")
    signals: List[IntradaySignalPoint] = Field(default_factory=list, description="탐지된 시그널 목록")
    is_batch: bool = Field(default=False, description="배치 처리 여부")
    batch_summary: Optional[Dict[str, Any]] = Field(None, description="배치 처리 결과 요약")
    skipped: bool = Field(default=False, description="티커 스킵 여부")
    skip_reason: Optional[str] = Field(None, description="스킵 사유")

