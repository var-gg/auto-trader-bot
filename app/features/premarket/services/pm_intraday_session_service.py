# app/features/premarket/services/pm_intraday_session_service.py
"""
PM 신호 기반 장중(Intraday) 서비스
- PM: 오늘 매수 대상 종목 풀 선정 (signal_1d > 0.3, SHORT 뉴스 제외)
- 5분봉: 실제 가격 결정 (intraday_signal_detection_result 기반 임계값)
- 리밸런싱: 5분봉 기반 (기존 방식 유지)
"""
from __future__ import annotations
from typing import Dict, Any, List
import logging
from sqlalchemy.orm import Session

from app.features.premarket.services.pm_active_set_service import PMActiveSetService

logger = logging.getLogger(__name__)


def get_pm_intraday_active_set(
    db: Session,
    country: str,
    min_signal: float = 0.3,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    PM 신호 기반 장중 액티브 셋 선정
    
    ⚠️ 역할: 오늘 매수할 종목 풀만 제공
    - 실제 가격은 5분봉 intraday_signal_detection_result로 결정
    
    Args:
        db: DB 세션
        country: 국가 (KR/US)
        min_signal: 최소 신호 강도 (기본 0.3)
        limit: 최대 종목 수
    
    Returns:
        [
            {
                "ticker_id": int,
                "symbol": str,
                "company_name": str,
                "signal_1d": float,
                "current_price": float,
                "atr_pct": float
            },
            ...
        ]
    """
    pm_service = PMActiveSetService(db)
    candidates, _ = pm_service.get_pm_active_candidates(
        country=country,
        min_signal=min_signal,
        limit=limit,
        exclude_short=True,
        mode="BUY"  # 양수 신호
    )
    
    logger.info(f"🎯 PM 장중 액티브 풀: {len(candidates)}개 (signal_1d > {min_signal})")
    
    return candidates



