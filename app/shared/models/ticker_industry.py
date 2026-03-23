from sqlalchemy import Column, Integer, ForeignKey
from app.core.db import Base
from app.core.config import DB_SCHEMA

class TickerIndustry(Base):
    __tablename__ = "ticker_industry"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True, comment="티커 산업 매핑 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), nullable=False, comment="티커 ID")
    industry_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.industry.id", ondelete="CASCADE"), nullable=False, comment="산업 ID")
