from sqlalchemy import Column, Integer, String, ForeignKey
from app.core.db import Base

class SectorI18n(Base):
    __tablename__ = "sector_i18n"
    __table_args__ = {"schema": "trading"}

    id = Column(Integer, primary_key=True, comment="섹터 다국어 ID")
    sector_id = Column(Integer, ForeignKey("trading.sector.id"), comment="섹터 ID")
    lang_code = Column(String, nullable=False, comment="언어 코드")  # "en", "ko"
    name = Column(String, nullable=False, comment="섹터명")       # "Technology", "기술주"
    description = Column(String, nullable=True, comment="설명")
