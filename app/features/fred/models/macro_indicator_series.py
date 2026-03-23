# app/features/fred/models/macro_indicator_series.py
from sqlalchemy import Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import mapped_column, Mapped
from app.core.db import Base

class MacroIndicatorSeries(Base):
    __tablename__ = "macro_indicator_series"
    __table_args__ = (
        UniqueConstraint("indicator_id", "fred_series_id", name="uq_indicator_series"),
        {"schema":"trading"},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    indicator_id: Mapped[int] = mapped_column(ForeignKey("trading.macro_indicator.id", ondelete="CASCADE"), nullable=False)
    fred_series_id: Mapped[str] = mapped_column(String(64), nullable=False)
