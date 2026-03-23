import logging, requests, trafilatura
from app.core.config import MAX_CONTENT_RETRY
from app.features.news.repositories.news_repository import NewsRepository

logger = logging.getLogger("news_content_service")

class NewsContentService:
    def __init__(self, repo: NewsRepository): self.repo = repo

    def _fetch_content(self, url: str) -> str | None:
        try:
            resp = requests.get(url, timeout=12, headers={"User-Agent":"Mozilla/5.0"})
            if resp.status_code != 200: return None
            return trafilatura.extract(resp.text)
        except Exception:
            return None

    def run(self, limit: int = 200) -> dict:
        items = self.repo.list_for_content_fetch(limit=limit, max_retry=MAX_CONTENT_RETRY)
        ok, fail = 0, 0
        for n in items:
            content = self._fetch_content(n.link)
            if content:
                self.repo.mark_content_success(n, content=content)
                ok += 1
            else:
                self.repo.mark_content_failed(n, max_retry=MAX_CONTENT_RETRY)
                fail += 1
        return {"success": ok, "failed": fail}
