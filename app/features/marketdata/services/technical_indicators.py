# app/features/marketdata/services/technical_indicators.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import date

def calculate_technical_indicators(
    df: pd.DataFrame,
    min_periods: int = 20
) -> Dict[str, any]:
    """
    주가 데이터에서 기술지표 계산
    - MA(20, 50): 이동평균
    - RSI(14): 상대강도지수
    - Bollinger Bands: 볼린저밴드
    - Volume Ratio: 거래량 비율
    
    Args:
        df: OHLCV 데이터프레임 (columns: trade_date, open, high, low, close, volume)
        min_periods: 최소 필요 기간 수
        
    Returns:
        Dict: 계산된 기술지표들
    """
    if len(df) < min_periods:
        return {
            "error": f"Insufficient data: {len(df)} < {min_periods}",
            "available_periods": len(df),
            "indicators": {}
        }
    
    # 데이터 정렬 (날짜 오름차순)
    df = df.sort_values('trade_date').reset_index(drop=True)
    
    indicators = {}
    
    # 1. 이동평균 (MA)
    try:
        df['ma_20'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['ma_50'] = df['close'].rolling(window=50, min_periods=1).mean()
        
        indicators['ma'] = {
            "ma_20": {
                "current": float(df['ma_20'].iloc[-1]) if not pd.isna(df['ma_20'].iloc[-1]) else None,
                "trend": "up" if len(df) >= 2 and df['ma_20'].iloc[-1] > df['ma_20'].iloc[-2] else "down"
            },
            "ma_50": {
                "current": float(df['ma_50'].iloc[-1]) if not pd.isna(df['ma_50'].iloc[-1]) else None,
                "trend": "up" if len(df) >= 2 and df['ma_50'].iloc[-1] > df['ma_50'].iloc[-2] else "down"
            }
        }
    except Exception as e:
        indicators['ma'] = {"error": str(e)}
    
    # 2. RSI(14)
    try:
        if len(df) >= 14:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            current_rsi = float(df['rsi'].iloc[-1]) if not pd.isna(df['rsi'].iloc[-1]) else None
            prev_rsi = float(df['rsi'].iloc[-2]) if len(df) >= 2 and not pd.isna(df['rsi'].iloc[-2]) else None
            
            indicators['rsi'] = {
                "current": current_rsi,
                "change_rate": ((current_rsi - prev_rsi) / prev_rsi * 100) if prev_rsi and prev_rsi != 0 else None,
                "signal": "overbought" if current_rsi and current_rsi > 70 else "oversold" if current_rsi and current_rsi < 30 else "neutral"
            }
        else:
            indicators['rsi'] = {"error": "Insufficient data for RSI(14)"}
    except Exception as e:
        indicators['rsi'] = {"error": str(e)}
    
    # 3. 볼린저밴드
    try:
        if len(df) >= 20:
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            bb_std = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle'] * 100
            
            current_close = float(df['close'].iloc[-1])
            current_upper = float(df['bb_upper'].iloc[-1]) if not pd.isna(df['bb_upper'].iloc[-1]) else None
            current_lower = float(df['bb_lower'].iloc[-1]) if not pd.isna(df['bb_lower'].iloc[-1]) else None
            current_width = float(df['bb_width'].iloc[-1]) if not pd.isna(df['bb_width'].iloc[-1]) else None
            
            indicators['bollinger'] = {
                "upper": current_upper,
                "middle": float(df['bb_middle'].iloc[-1]) if not pd.isna(df['bb_middle'].iloc[-1]) else None,
                "lower": current_lower,
                "width": current_width,
                "position": "above_upper" if current_upper and current_close > current_upper else "below_lower" if current_lower and current_close < current_lower else "within_bands",
                "squeeze": "yes" if current_width and current_width < 10 else "no"  # 밴드폭이 좁으면 스퀴즈
            }
        else:
            indicators['bollinger'] = {"error": "Insufficient data for Bollinger Bands"}
    except Exception as e:
        indicators['bollinger'] = {"error": str(e)}
    
    # 4. 거래량 이동평균 대비 비율
    try:
        if len(df) >= 20:
            df['volume_ma_20'] = df['volume'].rolling(window=20, min_periods=1).mean()
            current_volume = float(df['volume'].iloc[-1])
            avg_volume = float(df['volume_ma_20'].iloc[-1]) if not pd.isna(df['volume_ma_20'].iloc[-1]) else None
            
            indicators['volume'] = {
                "current": current_volume,
                "avg_20": avg_volume,
                "ratio": (current_volume / avg_volume) if avg_volume and avg_volume != 0 else None,
                "signal": "high" if avg_volume and current_volume > avg_volume * 1.5 else "low" if avg_volume and current_volume < avg_volume * 0.5 else "normal"
            }
        else:
            indicators['volume'] = {"error": "Insufficient data for volume analysis"}
    except Exception as e:
        indicators['volume'] = {"error": str(e)}
    
    # 5. ATR(14) - Average True Range
    try:
        if len(df) >= 14:
            # True Range 계산
            df['prev_close'] = df['close'].shift(1)
            df['tr1'] = df['high'] - df['low']
            df['tr2'] = abs(df['high'] - df['prev_close'])
            df['tr3'] = abs(df['low'] - df['prev_close'])
            df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
            
            # ATR(14) 계산 (지수이동평균 사용)
            df['atr_14'] = df['true_range'].ewm(span=14, min_periods=14).mean()
            
            current_atr = float(df['atr_14'].iloc[-1]) if not pd.isna(df['atr_14'].iloc[-1]) else None
            current_close = float(df['close'].iloc[-1])
            atr_percentage = (current_atr / current_close * 100) if current_atr and current_close else None
            
            indicators['atr'] = {
                "current": current_atr,
                "atr_percentage": atr_percentage,
                "volatility_level": "high" if atr_percentage and atr_percentage > 3 else "medium" if atr_percentage and atr_percentage > 1.5 else "low"
            }
        else:
            indicators['atr'] = {"error": "Insufficient data for ATR(14)"}
    except Exception as e:
        indicators['atr'] = {"error": str(e)}
    
    # 6. MACD(12,26,9) - Moving Average Convergence Divergence
    try:
        if len(df) >= 26:
            # EMA 계산
            df['ema_12'] = df['close'].ewm(span=12, min_periods=12).mean()
            df['ema_26'] = df['close'].ewm(span=26, min_periods=26).mean()
            
            # MACD Line = EMA(12) - EMA(26)
            df['macd_line'] = df['ema_12'] - df['ema_26']
            
            # Signal Line = EMA(9) of MACD Line
            df['macd_signal'] = df['macd_line'].ewm(span=9, min_periods=9).mean()
            
            # MACD Histogram = MACD Line - Signal Line
            df['macd_histogram'] = df['macd_line'] - df['macd_signal']
            
            current_macd = float(df['macd_line'].iloc[-1]) if not pd.isna(df['macd_line'].iloc[-1]) else None
            current_signal = float(df['macd_signal'].iloc[-1]) if not pd.isna(df['macd_signal'].iloc[-1]) else None
            current_histogram = float(df['macd_histogram'].iloc[-1]) if not pd.isna(df['macd_histogram'].iloc[-1]) else None
            prev_histogram = float(df['macd_histogram'].iloc[-2]) if len(df) >= 2 and not pd.isna(df['macd_histogram'].iloc[-2]) else None
            
            # MACD 신호 판단
            signal_type = "neutral"
            if current_macd and current_signal:
                if current_macd > current_signal and current_histogram > prev_histogram:
                    signal_type = "bullish"
                elif current_macd < current_signal and current_histogram < prev_histogram:
                    signal_type = "bearish"
                elif current_macd > current_signal:
                    signal_type = "bullish_weak"
                elif current_macd < current_signal:
                    signal_type = "bearish_weak"
            
            indicators['macd'] = {
                "macd_line": current_macd,
                "signal_line": current_signal,
                "histogram": current_histogram,
                "signal": signal_type,
                "ema_12": float(df['ema_12'].iloc[-1]) if not pd.isna(df['ema_12'].iloc[-1]) else None,
                "ema_26": float(df['ema_26'].iloc[-1]) if not pd.isna(df['ema_26'].iloc[-1]) else None
            }
        else:
            indicators['macd'] = {"error": "Insufficient data for MACD(12,26,9)"}
    except Exception as e:
        indicators['macd'] = {"error": str(e)}
    
    # 7. 추가 지표: 가격 변화율
    try:
        if len(df) >= 2:
            current_price = float(df['close'].iloc[-1])
            prev_price = float(df['close'].iloc[-2])
            price_change = ((current_price - prev_price) / prev_price * 100) if prev_price != 0 else None
            
            indicators['price_change'] = {
                "current": current_price,
                "change_rate": price_change,
                "change_amount": current_price - prev_price if prev_price else None
            }
    except Exception as e:
        indicators['price_change'] = {"error": str(e)}
    
    return {
        "success": True,
        "available_periods": len(df),
        "indicators": indicators
    }

def format_indicators_for_prompt(indicators_result: Dict) -> str:
    """
    기술지표를 프롬프트용 텍스트로 포맷팅
    """
    if not indicators_result.get("success"):
        return f"기술지표 계산 실패: {indicators_result.get('error', 'Unknown error')}"
    
    indicators = indicators_result.get("indicators", {})
    periods = indicators_result.get("available_periods", 0)
    
    prompt_text = f"📊 기술지표 분석 (기간: {periods}일)\n\n"
    
    # 이동평균
    if 'ma' in indicators and 'error' not in indicators['ma']:
        ma = indicators['ma']
        prompt_text += f"📈 이동평균:\n"
        prompt_text += f"  - MA20: {ma['ma_20']['current']:.2f} ({ma['ma_20']['trend']})\n"
        prompt_text += f"  - MA50: {ma['ma_50']['current']:.2f} ({ma['ma_50']['trend']})\n\n"
    
    # RSI
    if 'rsi' in indicators and 'error' not in indicators['rsi']:
        rsi = indicators['rsi']
        prompt_text += f"📊 RSI(14): {rsi['current']:.2f}"
        if rsi.get('change_rate'):
            prompt_text += f" (변화율: {rsi['change_rate']:+.2f}%)"
        prompt_text += f" [{rsi['signal']}]\n\n"
    
    # 볼린저밴드
    if 'bollinger' in indicators and 'error' not in indicators['bollinger']:
        bb = indicators['bollinger']
        prompt_text += f"📈 볼린저밴드:\n"
        prompt_text += f"  - 상단: {bb['upper']:.2f}\n"
        prompt_text += f"  - 중간: {bb['middle']:.2f}\n"
        prompt_text += f"  - 하단: {bb['lower']:.2f}\n"
        prompt_text += f"  - 폭: {bb['width']:.2f}% ({bb['squeeze']})\n"
        prompt_text += f"  - 위치: {bb['position']}\n\n"
    
    # 거래량
    if 'volume' in indicators and 'error' not in indicators['volume']:
        vol = indicators['volume']
        prompt_text += f"📊 거래량 분석:\n"
        prompt_text += f"  - 현재: {vol['current']:,.0f}\n"
        prompt_text += f"  - 평균(20일): {vol['avg_20']:,.0f}\n"
        prompt_text += f"  - 비율: {vol['ratio']:.2f}x ({vol['signal']})\n\n"
    
    # ATR
    if 'atr' in indicators and 'error' not in indicators['atr']:
        atr = indicators['atr']
        prompt_text += f"📊 ATR(14):\n"
        prompt_text += f"  - 현재값: {atr['current']:.4f}\n"
        if atr.get('atr_percentage'):
            prompt_text += f"  - 변동성: {atr['atr_percentage']:.2f}% ({atr['volatility_level']})\n\n"
    
    # MACD
    if 'macd' in indicators and 'error' not in indicators['macd']:
        macd = indicators['macd']
        prompt_text += f"📈 MACD(12,26,9):\n"
        prompt_text += f"  - MACD Line: {macd['macd_line']:.4f}\n"
        prompt_text += f"  - Signal Line: {macd['signal_line']:.4f}\n"
        prompt_text += f"  - Histogram: {macd['histogram']:.4f}\n"
        prompt_text += f"  - 신호: {macd['signal']}\n"
        prompt_text += f"  - EMA12: {macd['ema_12']:.2f}\n"
        prompt_text += f"  - EMA26: {macd['ema_26']:.2f}\n\n"
    
    # 가격 변화
    if 'price_change' in indicators and 'error' not in indicators['price_change']:
        pc = indicators['price_change']
        prompt_text += f"💰 가격 변화:\n"
        prompt_text += f"  - 현재가: {pc['current']:.2f}\n"
        if pc.get('change_rate'):
            prompt_text += f"  - 변화율: {pc['change_rate']:+.2f}%\n"
        if pc.get('change_amount'):
            prompt_text += f"  - 변화액: {pc['change_amount']:+.2f}\n"
    
    return prompt_text
