# app/features/signals/services/backtest_service.py
"""
백테스팅 서비스
- vec40 테이블 기반 유사도 검색 백테스팅
"""
from __future__ import annotations
import logging
from typing import Dict, Any
from sqlalchemy.orm import Session

import pandas as pd
import numpy as np

from app.shared.models.ticker import Ticker
from app.features.signals.repositories.signal_repository import SignalRepository
from app.features.signals.repositories.vec40_repository import Vec40Repository
from app.features.signals.models.similarity_models import (
    BacktestRequest,
    BacktestResponse
)


logger = logging.getLogger(__name__)


class BacktestService:
    """
    백테스팅 서비스
    - vec40 테이블 기반 유사도 검색
    - 슬라이딩 윈도우 백테스팅
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = SignalRepository(db)
        self.vec40_repo = Vec40Repository(db)
    
    def backtest_vec40(self, request: BacktestRequest) -> BacktestResponse:
        """
        vec40 테이블 기반 백테스팅
        
        Args:
            request: 백테스팅 요청
            
        Returns:
            백테스팅 결과
        """
        from datetime import datetime, timedelta
        
        ticker_id = request.ticker_id
        from_date = request.from_date
        lookback = request.lookback
        top_k = request.top_k
        exit_window = request.exit_window
        peak_threshold = request.peak_threshold
        
        logger.info(f"백테스팅 시작 - 티커ID: {ticker_id}, 시작일: {from_date}, lookback: {lookback}")
        
        # 📊 Step 1: 티커 정보 조회
        ticker = self._get_ticker(ticker_id)
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        logger.debug(f"티커 정보: {ticker.symbol}:{ticker.exchange} ({ticker.country})")
        
        # 📦 Step 2: 일봉 데이터 조회
        daily_data = self.repo.get_daily_data_as_dict(ticker_id, order_desc=False)
        
        if len(daily_data) < lookback + exit_window:
            raise ValueError(f"데이터가 부족합니다. (현재: {len(daily_data)}건, 최소: {lookback + exit_window}건 필요)")
        
        # DataFrame으로 변환
        df = pd.DataFrame(daily_data)
        df = df.sort_values('date').reset_index(drop=True)
        
        # from_date 이후 데이터만 필터링
        df = df[df['date'] >= from_date].reset_index(drop=True)
        
        if len(df) < lookback + exit_window:
            raise ValueError(f"from_date 이후 데이터가 부족합니다. (현재: {len(df)}건)")
        
        logger.debug(f"백테스팅 데이터: {len(df)}건 (from: {df['date'].min()}, to: {df['date'].max()})")
        
        # 벡터 생성 함수 로드 (40차원 벡터 생성용)
        from app.features.signals.utils.shape_vector import make_shape_vector_v2
        
        # 백테스팅 결과 저장
        total_signals = 0
        correct_direction_count = 0
        peak_gain_5pct_count = 0
        total_investment = 0.0
        total_profit = 0.0
        
        # 🔄 Step 4: 슬라이딩 윈도우로 백테스팅
        for i in range(lookback, len(df) - exit_window):
            # 현재 시점 기준 lookback 기간 데이터
            window_data = df.iloc[i-lookback:i]
            
            try:
                # OHLCV 딕셔너리 리스트로 변환
                ohlcv_data = window_data.to_dict('records')
                
                # v2/v4 알고리즘으로 벡터 생성 (40차원)
                # signal_detection_service와 동일한 설정 사용
                # m=5: 2*m(가격,거래량) + 4*m(캔들) + 7(메타) + 3(캔들메타) = 40차원
                query_vector = make_shape_vector_v2(
                    ohlcv_data=ohlcv_data,
                    m=5,  # PAA 리샘플링 길이
                    w_price=1.0,
                    w_volume=1.0,
                    w_candle=1.0,
                    w_meta=0.5,  # ✅ v4 기본값과 동일
                    meta_scaler="tanh",
                    candle_mode="diff",
                    include_candle_meta=True  # ✅ 40차원 맞추기 (3개 추가)
                )
                
                logger.debug(f"벡터 생성 완료 - 차원: {len(query_vector)}")
                
                # Python list로 변환
                query_vector_list = query_vector.tolist() if hasattr(query_vector, 'tolist') else list(query_vector)
                
                # vec40 테이블에서 유사도 검색
                top_similarities = self.vec40_repo.search_similar_vectors(
                    query_vector=query_vector_list,
                    direction_filter=None,  # 전체 검색
                    top_k=top_k
                )
                
                # 조건 1: 가장 높은 유사도가 0.7 이상이어야 함
                if len(top_similarities) == 0 or top_similarities[0]['similarity'] < 0.7:
                    continue
                
                # 조건 2: 상위 top_k개 중 최소 (top_k - 1)개는 UP이어야 함
                up_count = sum(1 for item in top_similarities if item['direction'] == 'UP')
                if up_count < top_k:
                    continue
                
                # 🎯 매수 시그널 발생!
                total_signals += 1
                
                # 매수가
                buy_price = float(df.iloc[i+1]['open'])
                total_investment += buy_price
                
                # exit_window일 후 매도가
                sell_price = float(df.iloc[i + exit_window]['close'])
                
                # 수익 계산
                profit = sell_price - buy_price
                total_profit += profit
                
                # 방향 적중 여부 (매도가 > 매수가)
                if sell_price > buy_price:
                    correct_direction_count += 1
                
                # 윈도우 기간 중 최고가 확인 (5% 이상 상승 경험 여부)
                window_highs = df.iloc[i:i+exit_window+1]['high'].tolist()
                max_high = max(window_highs)
                max_gain_rate = (max_high - buy_price) / buy_price
                
                if max_gain_rate >= peak_threshold:
                    peak_gain_5pct_count += 1
                
                logger.debug(f"시그널 {total_signals}: {df.iloc[i]['date']} | 매수: {buy_price:.2f}, 매도: {sell_price:.2f}, 수익: {profit:.2f}, 최대상승: {max_gain_rate*100:.2f}%")
                
            except Exception as e:
                logger.warning(f"벡터 생성/검색 실패 (i={i}): {e}")
                continue
        
        # 📊 Step 5: 결과 집계
        if total_signals == 0:
            win_rate = 0.0
            peak_experience_rate = 0.0
            return_rate = 0.0
        else:
            win_rate = correct_direction_count / total_signals
            peak_experience_rate = peak_gain_5pct_count / total_signals
            return_rate = total_profit / total_investment if total_investment > 0 else 0.0
        
        to_date = df['date'].max()
        
        logger.info(f"백테스팅 완료 - 시그널: {total_signals}, 승률: {win_rate:.2%}, 피크경험률: {peak_experience_rate:.2%}, 수익률: {return_rate:.2%}")
        
        return BacktestResponse(
            ticker_id=ticker_id,
            symbol=ticker.symbol,
            exchange=ticker.exchange,
            from_date=from_date,
            to_date=to_date,
            lookback=lookback,
            top_k=top_k,
            exit_window=exit_window,
            total_signals=total_signals,
            correct_direction_count=correct_direction_count,
            peak_gain_5pct_count=peak_gain_5pct_count,
            total_profit=total_profit,
            return_rate=return_rate,
            win_rate=win_rate,
            peak_experience_rate=peak_experience_rate
        )
    
    def _get_ticker(self, ticker_id: int):
        """티커 조회"""
        return self.db.query(Ticker).filter(Ticker.id == ticker_id).first()

