# app/features/marketdata/services/marketdata_prompt_common_service.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from datetime import date, timedelta
import pandas as pd

from app.features.marketdata.models.ohlcv_daily import OhlcvDaily
from app.features.marketdata.services.technical_indicators import calculate_technical_indicators

class MarketdataPromptCommonService:
    """공용 마켓데이터 프롬프트 서비스 - 미국주식과 국내주식 모두 지원"""
    
    def __init__(self, db: Session):
        self.db = db
    
    @staticmethod
    def build_ticker_prompt_static(db: Session, ticker_id: int, days: int = 21) -> Dict[str, Any]:
        """
        특정 티커의 프롬프트 데이터를 생성 (정적 메서드 - 기존 함수 호환성 유지)
        
        Args:
            db: 데이터베이스 세션
            ticker_id: 티커 ID
            days: 조회할 거래일 수
            
        Returns:
            Dict: 프롬프트 데이터
        """
        # 최근 거래일 데이터 조회
        end_d = date.today()
        start_d = end_d - timedelta(days=days + 3)  # 주말/휴장 여유
        
        rows = (
            db.query(OhlcvDaily)
            .filter(OhlcvDaily.ticker_id == ticker_id)
            .filter(OhlcvDaily.trade_date >= start_d)
            .order_by(OhlcvDaily.trade_date.desc())  # 최신순으로 정렬
            .all()
        )
        
        if not rows:
            return {
                "error": "No data available",
                "ticker_id": ticker_id,
                "days": days
            }
        
        # 최신 가격 (첫 번째 행)
        latest_row = rows[0]
        current_price = latest_row.close
        
        # 최근 캔들 데이터 (최대 5일) - OHLCV 포함
        recent_closes = []
        for i, row in enumerate(rows[:5]):  # 최대 5일
            recent_closes.append({
                "d": row.trade_date.isoformat(),
                "o": row.open,      # 시가
                "h": row.high,      # 고가
                "l": row.low,       # 저가
                "c": row.close,     # 종가
                "v": row.volume or 0
            })
        
        # 기술지표 계산
        technical_summary = {}
        
        if len(rows) >= 5:  # 최소 5일 데이터가 있으면 기술지표 계산
            try:
                # DataFrame 생성 (날짜 오름차순으로 정렬)
                df_rows = sorted(rows, key=lambda x: x.trade_date)
                df = pd.DataFrame([{
                    'trade_date': r.trade_date,
                    'open': r.open,
                    'high': r.high,
                    'low': r.low,
                    'close': r.close,
                    'volume': r.volume or 0
                } for r in df_rows])
                
                # 기술지표 계산
                indicators_result = calculate_technical_indicators(df, min_periods=5)
                
                if indicators_result.get("success"):
                    indicators = indicators_result.get("indicators", {})
                    
                    # 이동평균
                    if 'ma' in indicators and 'error' not in indicators['ma']:
                        ma = indicators['ma']
                        technical_summary['ma20'] = ma['ma_20']['current']
                        technical_summary['ma50'] = ma['ma_50']['current']
                    
                    # RSI
                    if 'rsi' in indicators and 'error' not in indicators['rsi']:
                        rsi = indicators['rsi']
                        technical_summary['rsi14'] = rsi['current']
                        technical_summary['rsi_status'] = rsi['signal']
                    
                    # ATR
                    if 'atr' in indicators and 'error' not in indicators['atr']:
                        atr = indicators['atr']
                        technical_summary['atr'] = atr['current']
                        technical_summary['atr_percentage'] = atr['atr_percentage']
                        technical_summary['volatility_level'] = atr['volatility_level']
                    
                    # MACD
                    if 'macd' in indicators and 'error' not in indicators['macd']:
                        macd = indicators['macd']
                        technical_summary['macd_line'] = macd['macd_line']
                        technical_summary['macd_signal'] = macd['signal_line']
                        technical_summary['macd_histogram'] = macd['histogram']
                        technical_summary['macd_signal_type'] = macd['signal']
                    
                    # 가격 변화율
                    if 'price_change' in indicators and 'error' not in indicators['price_change']:
                        pc = indicators['price_change']
                        technical_summary['change_pct'] = pc['change_rate']
                        
            except Exception as e:
                technical_summary = {"error": f"Technical analysis failed: {str(e)}"}
        else:
            technical_summary = {"error": f"Insufficient data: {len(rows)} days"}
        
        return {
            "current_price": current_price,
            "recent_closes": recent_closes,
            "technical_summary": technical_summary
        }
    
    def build_ticker_prompt(self, ticker_id: int, days: int = 21) -> Dict[str, Any]:
        """
        특정 티커의 프롬프트 데이터를 생성 (미국주식/국내주식 공용)
        
        Args:
            ticker_id: 티커 ID
            days: 조회할 거래일 수
            
        Returns:
            Dict: 프롬프트 데이터
        """
        # 정적 메서드 호출
        return self.build_ticker_prompt_static(self.db, ticker_id, days)
