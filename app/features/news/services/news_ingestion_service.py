import pytz, time
from datetime import datetime
from dateutil import parser as date_parser
from app.core.config import KST_ZONE
import feedparser, logging
from app.core.config import RSS_SOURCES
from app.features.news.dto.news_item_dto import NewsItemDTO
from app.features.news.repositories.news_repository import NewsRepository

logger = logging.getLogger("news_ingestion_service")

def parse_to_kst(pub_date, fallback_tz: str | None) -> tuple[datetime | None, str | None]:
    """
    pub_date: str(일반적), or time tuple
    반환: (published_at_kst, published_date_kst_str 'YYYY-MM-DD')
    """
    if not pub_date:
        return None, None
    dt = date_parser.parse(pub_date) if isinstance(pub_date, str) else datetime.fromtimestamp(time.mktime(pub_date))
    if dt.tzinfo is None:
        # tz 없는 경우 피드 선언 타임존으로 로컬라이즈
        local_tz = pytz.timezone(fallback_tz or "UTC")
        dt = local_tz.localize(dt)
    kst = dt.astimezone(pytz.timezone(KST_ZONE))
    return kst, kst.date().isoformat()

class NewsIngestionService:
    def __init__(self, repo: NewsRepository):
        self.repo = repo

    def ingest_all(self) -> dict:
        counts = {}
        for source in RSS_SOURCES:
            feed = feedparser.parse(source["url"])
            inserted = 0
            for entry in feed.entries:
                title = entry.get("title") or ""
                link  = entry.get("link") or ""
                if not link: continue
                dt_kst, date_kst = parse_to_kst(entry.get("published") or entry.get("updated"), source.get("timezone"))
                dto = NewsItemDTO(title=title, link=link, published_at=dt_kst, source=source["name"], summary=entry.get("summary"))
                self.repo.create_if_not_exists(
                    title=dto.title,
                    link=dto.link,
                    published_at=dto.published_at,     # tz-aware KST
                    published_date_kst=date_kst,       # 'YYYY-MM-DD'
                    source=dto.source,
                )
                inserted += 1
            counts[source["name"]] = inserted
        return counts
