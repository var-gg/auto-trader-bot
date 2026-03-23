# app/features/fred/models/macro_data_series_value.py
from datetime import datetime, date
from typing import Optional
from sqlalchemy import Integer, Date, DateTime, Numeric, Boolean, UniqueConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from app.core.db import Base

class MacroDataSeriesValue(Base):
    __tablename__ = "macro_data_series_value"
    __table_args__ = (
        UniqueConstraint("series_id", "obs_date", "vintage_start", name="uq_macro_value_vintage"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, comment="매크로 값 ID")
    series_id: Mapped[int] = mapped_column(ForeignKey("trading.macro_data_series.id", ondelete="CASCADE"), nullable=False, comment="시리즈 ID")
    obs_date: Mapped[date] = mapped_column(Date, nullable=False, comment="관측일")
    vintage_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, comment="빈티지 시작일")
    value: Mapped[Optional[float]] = mapped_column(Numeric(20, 6), comment="관측값")
    is_missing: Mapped[bool] = mapped_column(Boolean, default=False, comment="결측값 여부")
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, comment="수집일시")
