# app/features/premarket/models/pm_signal_models.py
"""
Pre-market Best Signal API의 Request/Response 모델
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date


class UpdatePMSignalsRequest(BaseModel):
    """장전 신호 업데이트 요청"""
    tickers: Optional[List[str]] = Field(None, description="대상 티커 심볼 리스트 (없으면 전체)")
    country: Optional[str] = Field(None, description="국가 필터 (US, KR, 없으면 전체)")
    dry_run: bool = Field(False, description="True면 저장 안 함 (테스트용)")
    anchor_date: Optional[str] = Field(None, description="기준일자 (YYYY-MM-DD, 없으면 오늘)")


class SignalSample(BaseModel):
    """개별 신호 샘플"""
    ticker_id: int
    symbol: str
    company_name: Optional[str]
    best_target_id: int
    signal_1d: float = Field(..., description="방향성 점수 (-1.0 ~ +1.0)")
    reason: str = Field(..., description="신호 이유 (CONFIDENT, LOW_CONF, TOO_FEW)")


class UpdatePMSignalsResponse(BaseModel):
    """장전 신호 업데이트 응답"""
    success: bool
    config_id: int
    anchor_date: str
    results: dict = Field(..., description="처리 결과 통계 (total, success, failed, no_signal)")
    elapsed_seconds: float
    samples: List[SignalSample] = Field(default_factory=list, description="샘플 결과 (최대 5개)")
    errors: Optional[List[dict]] = Field(None, description="에러 목록")


class PMSignalItem(BaseModel):
    """저장된 신호 조회 아이템"""
    ticker_id: int
    symbol: str
    company_name: Optional[str]
    signal_1d: float
    best_target_id: int
    updated_at: str


class GetPMSignalsResponse(BaseModel):
    """저장된 신호 조회 응답"""
    success: bool
    count: int
    signals: List[PMSignalItem]


# ========== 테스트 API 모델 ==========

class ANNMatchItem(BaseModel):
    """ANN 매칭 개별 아이템"""
    symbol: str
    country: Optional[str] = None
    anchor_date: str
    cos_shape: float = Field(..., description="Shape 코사인 유사도")
    cos_ctx: Optional[float] = Field(None, description="Context 코사인 유사도")
    score: Optional[float] = Field(None, description="재랭킹 점수 (alpha*shape + beta*ctx)")
    tb_label: Optional[str] = None
    iae_1_3: Optional[float] = None


class TestPMSignalRequest(BaseModel):
    """테스트 신호 계산 요청"""
    ticker_id: int = Field(..., description="대상 티커 ID")
    anchor_date: Optional[str] = Field(None, description="기준일자 (YYYY-MM-DD, 없으면 오늘)")
    topN: Optional[int] = Field(70, description="ANN 검색 개수 (기본: 70)", ge=1, le=200)
    use_ann: bool = Field(True, description="True=pgvector ANN 사용, False=전체 스캔 (정확)")


class TestPMSignalResponse(BaseModel):
    """테스트 신호 계산 응답"""
    success: bool
    ticker_id: int
    symbol: str
    company_name: Optional[str]
    country: Optional[str]
    config_id: int
    anchor_date: str
    signal_1d: float
    p_up: float
    p_down: float
    best_direction: str
    best_target: dict
    up_matches: List[ANNMatchItem] = Field(..., description="UP 방향 매칭 결과 (TOP N)")
    down_matches: List[ANNMatchItem] = Field(..., description="DOWN 방향 매칭 결과 (TOP N)")
    up_reranked_top10: List[ANNMatchItem] = Field(..., description="UP 재랭킹 TOP 10")
    down_reranked_top10: List[ANNMatchItem] = Field(..., description="DOWN 재랭킹 TOP 10")
    stats: dict = Field(..., description="통계 정보")
    error: Optional[str] = None