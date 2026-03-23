# app/features/news/services/kis_news_service.py

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
import logging

from app.features.news.repositories.kis_news_repository import KisNewsRepository
from app.features.news.models.kis_news import KisNews

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


class KisNewsService:
    def __init__(self, repo: KisNewsRepository):
        self.repo = repo

    @staticmethod
    def parse_kis_dt(dt: str, tm: str) -> datetime:
        """
        KIS 날짜/시간을 KST timezone-aware datetime으로 변환
        dt: 'YYYYMMDD'
        tm: 'HHMMSS' (6자리, 부족하면 0으로 채움)
        """
        tm_padded = tm.zfill(6)
        return datetime.strptime(dt + tm_padded, "%Y%m%d%H%M%S").replace(tzinfo=KST)

    def map_overseas_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        해외 뉴스 아이템을 kis_news 테이블 포맷으로 변환
        """
        symb = (item.get("symb") or "").strip()
        exchange_cd = (item.get("exchange_cd") or "").strip()
        
        # ticker_id 매핑 (심볼만으로 조회)
        ticker_id = None
        if symb:
            ticker_id = self.repo.find_ticker_by_symbol(symb)
        
        if not ticker_id:
            logger.warning(f"Ticker not found for overseas symbol: {symb}, exchange: {exchange_cd}")
            return None

        return {
            "source_type": "overseas",
            "source_key": item["news_key"],
            "ticker_id": ticker_id,
            "title": item["title"].strip(),
            "published_at": self.parse_kis_dt(item["data_dt"], item["data_tm"]),
            "publisher": item.get("source"),
            "class_cd": item.get("class_cd"),
            "class_name": item.get("class_name"),
            "nation_cd": item.get("nation_cd"),
            "exchange_cd": exchange_cd or None,
            "symbol": symb or None,
            "symbol_name": item.get("symb_name"),
            "kr_iscd": None,
            "lang": "ko",
            "raw_json": item,
        }

    def map_domestic_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        국내 뉴스 아이템을 kis_news 테이블 포맷으로 변환
        """
        iscd1 = (item.get("iscd1") or "").strip()
        
        # ticker_id 매핑
        ticker_id = None
        if iscd1:
            ticker_id = self.repo.find_ticker_by_kr_code(iscd1)
        
        if not ticker_id:
            logger.warning(f"Ticker not found for domestic code: {iscd1}")
            return None

        return {
            "source_type": "domestic",
            "source_key": item["cntt_usiq_srno"],
            "ticker_id": ticker_id,
            "title": item["hts_pbnt_titl_cntt"].strip(),
            "published_at": self.parse_kis_dt(item["data_dt"], item["data_tm"]),
            "publisher": item.get("dorg"),
            "class_cd": item.get("news_lrdv_code"),
            "class_name": None,
            "nation_cd": None,
            "exchange_cd": None,
            "symbol": None,
            "symbol_name": None,
            "kr_iscd": iscd1 or None,
            "lang": "ko",
            "raw_json": item,
        }

    def ingest_overseas_news(self, items: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        해외 뉴스 리스트 적재
        """
        success_count = 0
        skip_count = 0
        error_count = 0
        errors = []

        for item in items:
            try:
                mapped = self.map_overseas_item(item)
                if mapped:
                    self.repo.upsert_kis_news(mapped)
                    success_count += 1
                else:
                    skip_count += 1
            except Exception as e:
                error_count += 1
                errors.append({"item": item.get("news_key"), "error": str(e)})
                logger.error(f"Failed to ingest overseas news: {e}", exc_info=True)

        return {
            "success": success_count,
            "skipped": skip_count,
            "errors": error_count,
            "error_details": errors,
        }

    def ingest_domestic_news(self, items: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        국내 뉴스 리스트 적재
        """
        success_count = 0
        skip_count = 0
        error_count = 0
        errors = []

        for item in items:
            try:
                mapped = self.map_domestic_item(item)
                if mapped:
                    self.repo.upsert_kis_news(mapped)
                    success_count += 1
                else:
                    skip_count += 1
            except Exception as e:
                error_count += 1
                errors.append({"item": item.get("cntt_usiq_srno"), "error": str(e)})
                logger.error(f"Failed to ingest domestic news: {e}", exc_info=True)

        return {
            "success": success_count,
            "skipped": skip_count,
            "errors": error_count,
            "error_details": errors,
        }

    def get_recent_news(self, limit: int = 100) -> list[KisNews]:
        """최근 뉴스 목록 조회"""
        return self.repo.list_recent(limit)

    def get_news_by_ticker(self, ticker_id: int, limit: int = 50) -> list[KisNews]:
        """특정 티커의 뉴스 조회"""
        return self.repo.list_by_ticker(ticker_id, limit)

