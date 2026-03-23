# app/features/premarket/repositories/optuna_repository.py
"""
Optuna 최적화 결과 조회 Repository
- 최신 promoted 설정 조회
- 타겟 벡터 조회 (TB 라벨, IAE 포함)
"""
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_

from app.features.premarket.models.optuna_models import (
    OptunaVectorConfig,
    OptunaTargetVector,
    PMBestSignal
)


class OptunaRepository:
    """Optuna 최적화 결과 조회"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_latest_promoted_config(self) -> Optional[OptunaVectorConfig]:
        """
        최신 promoted 상태의 벡터 설정 조회
        
        Returns:
            OptunaVectorConfig 또는 None
        """
        return self.db.query(OptunaVectorConfig).filter(
            OptunaVectorConfig.status == 'promoted'
        ).order_by(
            desc(OptunaVectorConfig.promoted_at),
            desc(OptunaVectorConfig.id)
        ).first()
    
    def get_config_by_id(self, config_id: int) -> Optional[OptunaVectorConfig]:
        """설정 ID로 조회"""
        return self.db.query(OptunaVectorConfig).filter(
            OptunaVectorConfig.id == config_id
        ).first()
    
    def get_target_vector(self, target_id: int) -> Optional[OptunaTargetVector]:
        """
        타겟 벡터 조회 (TB 라벨, IAE 포함)
        
        Args:
            target_id: optuna_target_vectors.id
        
        Returns:
            OptunaTargetVector 또는 None
        """
        return self.db.query(OptunaTargetVector).filter(
            OptunaTargetVector.id == target_id
        ).first()
    
    def get_pm_best_signals(
        self,
        country: str,
        min_signal: float = 0.5,
        limit: int = 10,
        exclude_short_positions: bool = True,
        mode: str = "BUY"  # "BUY" or "SELL"
    ) -> List[Dict[str, Any]]:
        """
        PM 베스트 신호 조회
        
        ⚠️ 중요: signal_1d 방향 (순추세)
        - BUY: signal_1d > 0 (양수, 상승 예상 → 매수)
        - SELL: signal_1d < 0 (음수, 하락 예상 → 매도)
        
        Args:
            country: 국가 (KR, US)
            min_signal: 최소 신호 강도 (절댓값, 기본 0.5)
            limit: 최대 개수
            exclude_short_positions: SHORT 포지션 추천 제외 여부
            mode: "BUY" (양수 신호) or "SELL" (음수 신호)
        
        Returns:
            [
                {
                    "ticker_id": int,
                    "symbol": str,
                    "company_name": str,
                    "signal_1d": float,       # 원본 (BUY는 양수, SELL은 음수)
                    "signal_strength": float, # abs(signal_1d)
                    "best_target_id": int,
                    "tb_label": str,        # UP_FIRST, DOWN_FIRST, TIMEOUT
                    "iae_1_3": float,       # 초기 역행 폭
                    "direction": str,       # UP, DOWN
                    "updated_at": datetime,
                    "reverse_breach_day": int or None,  # 역돌파 일자
                    "reverse_breach_date": date or None  # 역돌파 날짜
                },
                ...
            ]
        """
        from app.shared.models.ticker import Ticker
        from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation
        from sqlalchemy import case
        
        # LONG 추천 서브쿼리 (LEFT JOIN용)
        long_subq = self.db.query(
            AnalystRecommendation.ticker_id,
            AnalystRecommendation.position_type
        ).filter(
            and_(
                AnalystRecommendation.is_latest == True,
                AnalystRecommendation.position_type == 'LONG'
            )
        ).subquery()
        
        query = self.db.query(
            PMBestSignal.ticker_id,
            PMBestSignal.symbol,
            PMBestSignal.company_name,
            PMBestSignal.signal_1d,
            PMBestSignal.best_target_id,
            PMBestSignal.updated_at,
            PMBestSignal.reverse_breach_day,
            PMBestSignal.reverse_breach_date,
            OptunaTargetVector.tb_label,
            OptunaTargetVector.iae_1_3,
            OptunaTargetVector.direction,
            case(
                (long_subq.c.position_type == 'LONG', True),
                else_=False
            ).label('has_long_recommendation')
        ).join(
            Ticker,
            Ticker.id == PMBestSignal.ticker_id
        ).outerjoin(
            OptunaTargetVector,
            OptunaTargetVector.id == PMBestSignal.best_target_id
        ).outerjoin(
            long_subq,
            long_subq.c.ticker_id == PMBestSignal.ticker_id
        ).filter(
            Ticker.country == country
        )
        
        # ⚠️ 신호 방향 필터링
        if mode == "BUY":
            # BUY: signal_1d > 0 (양수), min_signal 이상
            query = query.filter(PMBestSignal.signal_1d > min_signal)
        else:  # SELL
            # SELL: signal_1d < 0 (음수), 절댓값이 min_signal 이상
            query = query.filter(PMBestSignal.signal_1d < -min_signal)
        
        # SHORT 포지션 추천 제외 (기본)
        if exclude_short_positions:
            # 서브쿼리: 최신 SHORT 추천이 있는 종목 제외
            short_subq = self.db.query(
                AnalystRecommendation.ticker_id
            ).filter(
                and_(
                    AnalystRecommendation.is_latest == True,
                    AnalystRecommendation.position_type == 'SHORT'
                )
            ).subquery()
            
            query = query.filter(
                ~PMBestSignal.ticker_id.in_(short_subq)
            )
        
        # 정렬: 절댓값이 큰 순서 (강한 신호 우선)
        if mode == "BUY":
            # BUY는 양수이므로 내림차순 (큰 양수 = 강한 신호)
            query = query.order_by(PMBestSignal.signal_1d.desc())
        else:  # SELL
            # SELL은 음수이므로 오름차순 (작은 음수 = 강한 신호)
            query = query.order_by(PMBestSignal.signal_1d.asc())
        
        query = query.limit(limit)
        
        results = []
        for row in query.all():
            signal_raw = float(row.signal_1d)
            results.append({
                "ticker_id": row.ticker_id,
                "symbol": row.symbol,
                "company_name": row.company_name,
                "signal_1d": signal_raw,  # 원본 (음수 or 양수)
                "signal_strength": abs(signal_raw),  # 절댓값 (0~1)
                "best_target_id": row.best_target_id,
                "tb_label": row.tb_label,
                "iae_1_3": float(row.iae_1_3) if row.iae_1_3 is not None else None,
                "direction": row.direction,
                "updated_at": row.updated_at,
                "reverse_breach_day": row.reverse_breach_day,
                "reverse_breach_date": row.reverse_breach_date,
                "has_long_recommendation": bool(row.has_long_recommendation)  # ✅ LONG 추천 여부
            })
        
        return results
    
    def get_ladder_params(self, config_id: Optional[int] = None) -> Optional[Dict[str, Dict]]:
        """
        최적화된 래더 파라미터 조회
        
        Args:
            config_id: 설정 ID (없으면 최신 promoted 사용)
        
        Returns:
            {
                "buy": {
                    "MIN_TICK_GAP": 1,
                    "ADAPTIVE_BASE_LEGS": 2,
                    ...
                },
                "sell": {
                    "MIN_TICK_GAP": 2,
                    "ADAPTIVE_BASE_LEGS": 3,
                    ...
                }
            }
            또는 None
        """
        if config_id:
            config = self.get_config_by_id(config_id)
        else:
            config = self.get_latest_promoted_config()
        
        if not config:
            return None
        
        # ladder_params 속성이 있는지 확인 (안전하게)
        ladder_params = getattr(config, 'ladder_params', None)
        
        if not ladder_params:
            return None
        
        return ladder_params

