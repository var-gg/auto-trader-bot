from pydantic import BaseModel
from datetime import datetime

class NewsItemDTO(BaseModel):
    title: str
    link: str
    published_at: datetime | None = None
    source: str
    summary: str | None = None
