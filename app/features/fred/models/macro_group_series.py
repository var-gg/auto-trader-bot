# app/features/fred/models/macro_group_series.py
from datetime import datetime
from sqlalchemy import Integer, DateTime, Boolean, UniqueConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from app.core.db import Base

class MacroGroupSeries(Base):
    __tablename__ = "macro_group_series"
    __table_args__ = (
        UniqueConstraint("group_id", "series_id", name="uq_macro_group_series"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, comment="그룹-시리즈 관계 ID")
    group_id: Mapped[int] = mapped_column(ForeignKey("trading.macro_group.id", ondelete="CASCADE"), nullable=False, comment="그룹 ID")
    series_id: Mapped[int] = mapped_column(ForeignKey("trading.macro_data_series.id", ondelete="CASCADE"), nullable=False, comment="시리즈 ID")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="사용유무")
    sort_order: Mapped[int] = mapped_column(Integer, default=0, comment="그룹 내 정렬 순서")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, comment="생성일시")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, comment="수정일시")
