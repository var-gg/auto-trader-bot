# app/features/fred/models/macro_data_series.py
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Integer, DateTime, Text, Boolean, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.core.db import Base

class MacroDataSeries(Base):
    __tablename__ = "macro_data_series"
    __table_args__ = (
        UniqueConstraint("fred_series_id", name="uq_macro_series_fred_id"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, comment="매크로 시리즈 ID")
    fred_series_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="FRED 시리즈 ID")
    title: Mapped[Optional[str]] = mapped_column(String(512), comment="시리즈 제목")
    frequency: Mapped[Optional[str]] = mapped_column(String(64), comment="발표 주기")
    units: Mapped[Optional[str]] = mapped_column(String(128), comment="단위")
    seasonal_adjustment: Mapped[Optional[str]] = mapped_column(String(64), comment="계절조정 여부")
    notes: Mapped[Optional[str]] = mapped_column(Text, comment="설명")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="사용유무")

    observation_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="관측 시작일")
    observation_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="관측 종료일")
    last_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="최종 업데이트일")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, comment="생성일시")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, comment="수정일시")
