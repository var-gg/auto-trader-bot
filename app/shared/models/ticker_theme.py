from sqlalchemy import Column, Integer, ForeignKey
from app.core.db import Base
from app.core.config import DB_SCHEMA

class TickerTheme(Base):
    __tablename__ = "ticker_theme"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="티커 테마 매핑 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), nullable=False, comment="티커 ID")
    theme_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.theme.id", ondelete="CASCADE"), nullable=False, comment="테마 ID")
