# app/features/fred/models/macro_group.py
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, DateTime, Text, Boolean, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.core.db import Base

class MacroGroup(Base):
    __tablename__ = "macro_group"
    __table_args__ = (
        UniqueConstraint("code", name="uq_macro_group_code"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, comment="매크로 그룹 ID")
    code: Mapped[str] = mapped_column(String(64), nullable=False, comment="그룹 코드 (예: INFLATION, LABOR)")
    name: Mapped[str] = mapped_column(String(128), nullable=False, comment="그룹 이름 (예: Inflation, Labor)")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="그룹 설명")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="사용유무")
    sort_order: Mapped[Optional[int]] = mapped_column(Integer, comment="정렬 순서")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, comment="생성일시")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, comment="수정일시")
