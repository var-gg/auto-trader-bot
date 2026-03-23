from sqlalchemy import Column, Integer, String, ForeignKey
from app.core.db import Base
from app.core.config import DB_SCHEMA

class TickerI18n(Base):
    __tablename__ = "ticker_i18n"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="티커 다국어 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), nullable=False, comment="티커 ID")
    lang_code = Column(String, nullable=False, comment="언어 코드")   # "en", "ko"
    name = Column(String, nullable=False, comment="종목명")        # 종목명 다국어
