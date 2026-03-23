# app/core/finnhub_client.py

import os
import logging
import requests
from typing import Dict, List, Any
from datetime import datetime

from app.core.config import FINNHUB_BASE_URL, FINNHUB_API_KEY

logger = logging.getLogger(__name__)

def get(endpoint: str, params: dict = None):
    if params is None:
        params = {}
    params["token"] = FINNHUB_API_KEY
    url = f"{FINNHUB_BASE_URL}/{endpoint}"
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

class FinnhubClient:
    """
    Finnhub API 클라이언트
    - Market Holiday API 지원
    """
    
    def __init__(self):
        self.base_url = FINNHUB_BASE_URL
        self.api_key = FINNHUB_API_KEY
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """공통 API 요청 메서드"""
        if params is None:
            params = {}
        params["token"] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    
    def get_market_holidays(self, exchange: str = "US") -> List[Dict[str, Any]]:
        """
        마켓 휴일 정보 조회
        - exchange: 거래소 코드 (기본값: US)
        - 반환: 휴일 정보 리스트
        """
        endpoint = "stock/market-holiday"
        params = {"exchange": exchange}
        
        try:
            result = self._make_request(endpoint, params)
            return result.get("data", [])
        except Exception as e:
            logger.error(f"Error fetching market holidays for {exchange}: {e}")
            return []
    
    def get_multiple_exchange_holidays(self, exchanges: List[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        여러 거래소의 휴일 정보를 한번에 조회
        - exchanges: 거래소 코드 리스트 (기본값: ["US", "KR", "JP"])
        - 반환: {exchange: [holidays]}
        """
        if exchanges is None:
            exchanges = ["US", "KR", "JP"]
        
        results = {}
        for exchange in exchanges:
            holidays = self.get_market_holidays(exchange)
            results[exchange] = holidays
        
        return results
