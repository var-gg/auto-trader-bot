# app/features/news/services/news_v2_content_service.py
import logging
import requests
import trafilatura
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.core.config import MAX_CONTENT_RETRY

logger = logging.getLogger(__name__)

class NewsV2ContentService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo

    def _fetch_content(self, url: str) -> str | None:
        """본문 크롤링 - 기존 로직 재사용"""
        try:
            resp = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return None
            return trafilatura.extract(resp.text)
        except Exception as e:
            logger.warning(f"본문 크롤링 실패: {url}, 오류: {str(e)}")
            return None

    def run(self, limit: int = 14) -> Dict[str, Any]:
        """
        1일 이내 뉴스 중 news_vector가 없는 것들의 본문 크롤링 (언론사별로 균등 배분)
        """
        logger.info(f"NewsV2 본문 크롤링 시작 - 최대 {limit}개 처리 (언론사별 균등 배분)")
        
        items = self.repo.list_for_content_fetch_v2(limit=limit, max_retry=MAX_CONTENT_RETRY)
        
        if not items:
            logger.info("처리할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0, "sources": {}}
        
        # 언론사별 카운트
        source_count = {}
        for news in items:
            source_count[news.source] = source_count.get(news.source, 0) + 1
        
        logger.info(f"총 {len(items)}개 뉴스 본문 크롤링 시작 - 언론사별: {source_count}")
        
        success_count = 0
        failed_count = 0
        
        for news in items:
            try:
                content = self._fetch_content(news.link)
                if content and len(content.strip()) > 100:  # 최소 길이 체크
                    self.repo.mark_content_success(news, content)
                    success_count += 1
                    logger.debug(f"본문 크롤링 성공: {news.id} - {news.title[:50]}...")
                else:
                    self.repo.mark_content_failed(news, max_retry=MAX_CONTENT_RETRY)
                    failed_count += 1
                    logger.debug(f"본문 크롤링 실패: {news.id} - {news.title[:50]}...")
                    
            except Exception as e:
                self.repo.mark_content_failed(news, max_retry=MAX_CONTENT_RETRY)
                failed_count += 1
                logger.error(f"본문 크롤링 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items),
            "sources": source_count
        }
        
        logger.info(f"NewsV2 본문 크롤링 완료: 성공 {success_count}개, 실패 {failed_count}개 - 언론사별: {source_count}")
        return result
