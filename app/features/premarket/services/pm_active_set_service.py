# app/features/premarket/services/pm_active_set_service.py
"""
PM 신호 기반 액티브 셋 선정 서비스
- pm_best_signal 테이블에서 상위 종목 조회
- TB 메타라벨 + IAE + 현재가 정보 통합
- 래더 파라미터는 optuna_vector_config (promoted)에서 조회
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, func
from datetime import date, timedelta

from app.features.premarket.repositories.optuna_repository import OptunaRepository
from app.features.marketdata.models.ohlcv_daily import OhlcvDaily
from app.shared.models.ticker import Ticker

logger = logging.getLogger(__name__)


class PMActiveSetService:
    """PM 신호 기반 액티브 셋 선정"""
    
    def __init__(self, db: Session):
        self.db = db
        self.optuna_repo = OptunaRepository(db)
    
    def get_pm_active_candidates(
        self,
        country: str,
        min_signal: float = 0.5,
        limit: int = 10,
        exclude_short: bool = True,
        mode: str = "BUY"  # "BUY" or "SELL"
    ) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Dict]]]:
        """
        PM 신호 기반 액티브 후보 조회
        
        ⚠️ 중요: signal_1d 방향 (순추세)
        - BUY: signal_1d > 0 (양수, 상승 예상 → 매수)
        - SELL: signal_1d < 0 (음수, 하락 예상 → 매도)
        
        Args:
            country: 국가 (KR, US)
            min_signal: 최소 신호 강도 (절댓값, 기본 0.5)
            limit: 최대 개수 (기본 10)
            exclude_short: SHORT 포지션 추천 제외 (기본 True)
            mode: "BUY" (양수 신호) or "SELL" (음수 신호)
        
        Returns:
            (candidates, ladder_params)
            candidates: [
                {
                    "ticker_id": int,
                    "symbol": str,
                    "company_name": str,
                    "signal_1d": float,        # 원본 (BUY는 양수, SELL은 음수)
                    "signal_strength": float,  # abs(signal_1d)
                    "best_target_id": int,
                    "tb_label": str,           # UP_FIRST, DOWN_FIRST, TIMEOUT
                    "iae_1_3": float,          # 초기 역행 폭
                    "direction": str,          # UP, DOWN
                    "current_price": float,    # 현재가 (최근 종가)
                    "atr_pct": float,          # ATR % (추정, 옵션)
                    "updated_at": datetime,
                    "reverse_breach_day": int or None,  # 역돌파 일자
                    "reverse_breach_date": date or None  # 역돌파 날짜
                },
                ...
            ]
            ladder_params: {"buy": {...}, "sell": {...}} 또는 None
        """
        # 1) PM 베스트 신호 조회
        pm_signals = self.optuna_repo.get_pm_best_signals(
            country=country,
            min_signal=min_signal,
            limit=limit,
            exclude_short_positions=exclude_short,
            mode=mode
        )
        
        if not pm_signals:
            logger.warning(f"❌ PM 신호 없음: country={country}, mode={mode}, |signal|≥{min_signal}")
            return [], None
        
        logger.info(f"✅ PM 신호 조회: {len(pm_signals)}개 (country={country}, mode={mode}, |signal|≥{min_signal})")
        
        # 2) 현재가 + ATR 정보 추가
        ticker_ids = [s["ticker_id"] for s in pm_signals]
        price_map = self._get_latest_prices(ticker_ids)
        atr_map = self._estimate_atr(ticker_ids)
        
        candidates = []
        for sig in pm_signals:
            tid = sig["ticker_id"]
            sym = sig["symbol"]
            
            # 현재가 없으면 스킵
            if tid not in price_map or price_map[tid] is None:
                logger.warning(f"⚠️ [{sym}] 현재가 없음 → 스킵")
                continue
            
            candidates.append({
                "ticker_id": tid,
                "symbol": sym,
                "company_name": sig.get("company_name"),
                "signal_1d": sig["signal_1d"],          # 원본 (음수 or 양수)
                "signal_strength": sig["signal_strength"],  # abs(signal_1d)
                "best_target_id": sig["best_target_id"],
                "tb_label": sig.get("tb_label"),
                "iae_1_3": sig.get("iae_1_3"),
                "direction": sig.get("direction"),
                "current_price": price_map[tid],
                "atr_pct": atr_map.get(tid, 0.05),  # 기본 5%
                "updated_at": sig.get("updated_at"),
                "reverse_breach_day": sig.get("reverse_breach_day"),
                "reverse_breach_date": sig.get("reverse_breach_date"),
                "has_long_recommendation": sig.get("has_long_recommendation", False)  # ✅ LONG 추천 여부
            })
        
        logger.info(f"🎯 PM 액티브 후보: {len(candidates)}개 (가격정보 병합 완료)")
        
        # 3) 래더 파라미터 조회 (최신 promoted 설정)
        ladder_params = self.optuna_repo.get_ladder_params()
        if not ladder_params:
            logger.warning("⚠️ 래더 파라미터 없음 → 기본값 사용")
            # 기본값 제공 (DB에 데이터가 없을 때)
            ladder_params = {
                "buy": {
                    "MIN_TICK_GAP": 1,
                    "ADAPTIVE_BASE_LEGS": 2,
                    "ADAPTIVE_LEG_BOOST": 0.6,
                    "MIN_TOTAL_SPREAD_PCT": 0.01,
                    "ADAPTIVE_STRENGTH_SCALE": 0.1,
                    "FIRST_LEG_BASE_PCT": 0.012,
                    "FIRST_LEG_MIN_PCT": 0.006,
                    "FIRST_LEG_MAX_PCT": 0.050,
                    "FIRST_LEG_GAIN_WEIGHT": 0.6,
                    "FIRST_LEG_ATR_WEIGHT": 0.5,
                    "FIRST_LEG_REQ_FLOOR_PCT": 0.012,
                    "ADAPTIVE_MAX_STEP_PCT": 0.060,
                    "ADAPTIVE_FRAC_ALPHA": 1.25,
                    "ADAPTIVE_GAIN_SCALE": 0.10,
                    "MIN_LOT_QTY": 1
                },
                "sell": {
                    "MIN_TICK_GAP": 2,
                    "ADAPTIVE_BASE_LEGS": 3,
                    "ADAPTIVE_LEG_BOOST": 1.0,
                    "MIN_TOTAL_SPREAD_PCT": 0.012,
                    "ADAPTIVE_STRENGTH_SCALE": 0.19,
                    "FIRST_LEG_BASE_PCT": 0.015,
                    "FIRST_LEG_MIN_PCT": 0.01,
                    "FIRST_LEG_MAX_PCT": 0.060,
                    "FIRST_LEG_GAIN_WEIGHT": 0.6,
                    "FIRST_LEG_ATR_WEIGHT": 0.5,
                    "FIRST_LEG_REQ_FLOOR_PCT": 0.0,
                    "ADAPTIVE_MAX_STEP_PCT": 0.060,
                    "ADAPTIVE_FRAC_ALPHA": 1.25,
                    "ADAPTIVE_GAIN_SCALE": 0.10,
                    "MIN_LOT_QTY": 1
                }
            }
        else:
            logger.info(f"✅ 래더 파라미터 로드: buy={ladder_params.get('buy', {})}, sell={ladder_params.get('sell', {})}")
        
        return candidates, ladder_params
    
    def _get_latest_prices(self, ticker_ids: List[int]) -> Dict[int, Optional[float]]:
        """
        최근 종가 조회 (당일 또는 전일)
        
        Args:
            ticker_ids: 티커 ID 리스트
        
        Returns:
            {ticker_id: close_price}
        """
        if not ticker_ids:
            return {}
        
        # 최근 5일 범위에서 각 종목별 최신 종가 조회
        cutoff_date = date.today() - timedelta(days=5)
        
        # 서브쿼리: 각 ticker_id별 최신 거래일
        latest_dates_subq = self.db.query(
            OhlcvDaily.ticker_id,
            func.max(OhlcvDaily.trade_date).label('latest_date')
        ).filter(
            OhlcvDaily.ticker_id.in_(ticker_ids),
            OhlcvDaily.trade_date >= cutoff_date
        ).group_by(
            OhlcvDaily.ticker_id
        ).subquery()
        
        # 최신 종가 조회
        results = self.db.query(
            OhlcvDaily.ticker_id,
            OhlcvDaily.close
        ).join(
            latest_dates_subq,
            and_(
                OhlcvDaily.ticker_id == latest_dates_subq.c.ticker_id,
                OhlcvDaily.trade_date == latest_dates_subq.c.latest_date
            )
        ).all()
        
        price_map = {}
        for tid, close in results:
            price_map[tid] = float(close) if close is not None else None
        
        # 가격 없는 종목은 None으로
        for tid in ticker_ids:
            if tid not in price_map:
                price_map[tid] = None
        
        return price_map
    
    def _estimate_atr(self, ticker_ids: List[int], window: int = 14) -> Dict[int, float]:
        """
        ATR % 추정 (간이 버전)
        
        Args:
            ticker_ids: 티커 ID 리스트
            window: ATR 계산 윈도우 (기본 14일)
        
        Returns:
            {ticker_id: atr_pct}
        """
        if not ticker_ids:
            return {}
        
        cutoff_date = date.today() - timedelta(days=window + 5)
        
        atr_map = {}
        
        for tid in ticker_ids:
            try:
                # 최근 N일 데이터 조회
                rows = self.db.query(OhlcvDaily).filter(
                    OhlcvDaily.ticker_id == tid,
                    OhlcvDaily.trade_date >= cutoff_date
                ).order_by(
                    OhlcvDaily.trade_date.desc()
                ).limit(window).all()
                
                if len(rows) < 5:
                    atr_map[tid] = 0.05  # 기본 5%
                    continue
                
                # 간이 ATR: 최근 N일 (high - low) / close 평균
                ranges = []
                for row in rows:
                    if row.high and row.low and row.close and row.close > 0:
                        range_pct = (row.high - row.low) / row.close
                        ranges.append(range_pct)
                
                if ranges:
                    atr_pct = sum(ranges) / len(ranges)
                    atr_map[tid] = max(0.01, min(atr_pct, 0.20))  # 1~20% 범위
                else:
                    atr_map[tid] = 0.05
                
            except Exception as e:
                logger.debug(f"ATR 추정 실패 (ticker_id={tid}): {e}")
                atr_map[tid] = 0.05
        
        return atr_map

