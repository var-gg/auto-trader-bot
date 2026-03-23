from sqlalchemy import Column, Integer, String
from app.core.db import Base

class Sector(Base):
    __tablename__ = "sector"
    __table_args__ = {"schema": "trading"}

    id = Column(Integer, primary_key=True, comment="섹터 ID")
    code = Column(String, nullable=False, unique=True, comment="섹터 코드")  # 예: "Technology"
    system = Column(String, nullable=False, default="YF", comment="시스템 구분")  # 고정: Yahoo Finance 
