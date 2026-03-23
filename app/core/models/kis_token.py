from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

class KISToken(Base):
    __tablename__ = "kis_token"
    __table_args__ = (
        UniqueConstraint("appkey_hash", "tr_id", name="uq_kis_token_appkey_trid"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="KIS 토큰 ID")
    provider = Column(String(16), nullable=False, default="KIS", comment="제공업체")     # 확장 대비
    base_url = Column(String(256), nullable=False, comment="기본 URL")                   # 환경 구분용
    appkey_hash = Column(String(128), nullable=False, index=True, comment="앱키 해시")    # appkey+secret 해시
    tr_id = Column(String(32), nullable=False, comment="거래 ID")                       # 용도별 구분(선택)

    access_token = Column(String(4096), nullable=False, comment="액세스 토큰")
    expires_at = Column(DateTime(timezone=True), nullable=False, comment="만료일시")     # UTC

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), comment="수정일시")
