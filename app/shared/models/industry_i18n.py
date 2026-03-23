from sqlalchemy import Column, Integer, String, ForeignKey
from app.core.db import Base

class IndustryI18n(Base):
    __tablename__ = "industry_i18n"
    __table_args__ = {"schema": "trading"}

    id = Column(Integer, primary_key=True)
    industry_id = Column(Integer, ForeignKey("trading.industry.id"))
    lang_code = Column(String, nullable=False)
    name = Column(String, nullable=False)        # "Medical Devices", "의료기기"
    description = Column(String, nullable=True)
