import logging
from app.features.news.repositories.news_repository import NewsRepository
from .news_ai_service import NewsAIService

logger = logging.getLogger("news_filter_service")

class NewsFilterService:
    def __init__(self, repo: NewsRepository): self.repo = repo

    def run(self, limit: int = 150) -> int:
        items = self.repo.list_for_filtering(limit=limit)
        updated = 0
        for news in items:
            is_related, score, model = NewsAIService.classify_finance_relevance(title=news.title, summary=None)
            self.repo.mark_filtered(news, is_related=is_related, score=score, model=model)
            updated += 1
        return updated
