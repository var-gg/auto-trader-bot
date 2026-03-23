from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from app.core.db import Base

class Industry(Base):
    __tablename__ = "industry"
    __table_args__ = (
        UniqueConstraint("code", "sector_id", name="uq_industry_code_sector"),
        {"schema": "trading"}  # ✅ dict는 반드시 마지막!
    )

    id = Column(Integer, primary_key=True, comment="산업 ID")
    code = Column(String, nullable=False, comment="산업 코드")
    sector_id = Column(Integer, ForeignKey("trading.sector.id"), comment="섹터 ID")
    system = Column(String, nullable=False, default="YF", comment="시스템 구분")
