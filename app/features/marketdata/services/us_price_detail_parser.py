# app/features/marketdata/services/us_price_detail_parser.py
from __future__ import annotations
from datetime import datetime, date
from typing import Dict, List, Optional

def parse_kis_price_detail_payload(payload: Dict, symbol: str, exchange: str, ticker_id: int) -> Optional[Dict]:
    """
    KIS 해외주식현재가상세 응답 파싱
    - 현재가, 전일대비, 거래량 등 상세 정보 추출
    - 기존 일봉 데이터와 동일한 형태로 변환
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Parsing KIS price detail payload for {symbol}:{exchange} (ticker_id: {ticker_id})")
        logger.debug(f"Full payload: {payload}")
        
        # API 응답 상태 확인
        rt_cd = payload.get("rt_cd")
        msg_cd = payload.get("msg_cd")
        msg1 = payload.get("msg1")
        
        logger.info(f"API Response - rt_cd: {rt_cd}, msg_cd: {msg_cd}, msg1: {msg1}")
        
        if rt_cd != "0":
            logger.error(f"KIS API error for {symbol}:{exchange} - rt_cd: {rt_cd}, msg: {msg1}")
            return None
        
        # output 또는 output1에서 기본 정보 추출
        output_data = payload.get("output", {}) or payload.get("output1", {})
        logger.debug(f"output data: {output_data}")
        
        if not output_data:
            logger.warning(f"No output/output1 found in payload for {symbol}:{exchange}")
            return None

        # 해외거래소 기준 정확한 키 사용 (KIS 해외주식현재가상세 API 문서 기준)
        current_price = _parse_float(output_data.get("last"))  # 현재가
        logger.debug(f"Current price (last): {current_price}")
        
        if current_price is None:
            logger.warning(f"No current price found for {symbol}:{exchange} in output data: {output_data}")
            return None

        # 전일 종가
        prev_close = _parse_float(output_data.get("base"))  # 전일종가
        logger.debug(f"Previous close (base): {prev_close}")
        
        # 거래량
        volume = _parse_int(output_data.get("tvol"))  # 거래량
        logger.debug(f"Volume (tvol): {volume}")
        
        # 시가, 고가, 저가
        open_price = _parse_float(output_data.get("open"))   # 시가
        high_price = _parse_float(output_data.get("high"))   # 고가
        low_price = _parse_float(output_data.get("low"))     # 저가
        
        logger.debug(f"OHLC - Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {current_price}")

        # 거래일 (오늘 날짜 사용)
        trade_date = datetime.now().date()
        logger.debug(f"Trade date: {trade_date}")

        # 장마감 여부 판단 (모든 OHLCV 값이 있고 거래량이 있으면 마감된 것으로 간주)
        is_final = (
            open_price is not None and 
            high_price is not None and 
            low_price is not None and 
            current_price is not None and 
            volume is not None and 
            volume > 0
        )
        logger.debug(f"Is final: {is_final} (OHLCV check: O={open_price}, H={high_price}, L={low_price}, C={current_price}, V={volume})")

        row = {
            "ticker_id": ticker_id,
            "trade_date": trade_date,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": current_price,
            "volume": volume or 0,
            "is_final": is_final,
            "source": "KIS_CURRENT_PRICE",
            "source_symbol": symbol,
            "source_exchange": exchange,
            "source_payload": {
                "output": output_data,
                "output2": payload.get("output2", {}),
            },
        }

        logger.info(f"Successfully parsed price detail for {symbol}:{exchange} - Price: {current_price}, Volume: {volume}")
        return row

    except Exception as e:
        logger.error(f"Error parsing price detail payload for {symbol}:{exchange}: {e}")
        logger.debug(f"Payload: {payload}")
        return None

def _parse_float(value: any) -> Optional[float]:
    """문자열을 float로 변환"""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None

def _parse_int(value: any) -> Optional[int]:
    """문자열을 int로 변환"""
    if value is None:
        return None
    try:
        return int(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None
