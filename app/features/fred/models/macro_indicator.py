# app/features/fred/models/macro_indicator.py
from sqlalchemy import String, Integer, UniqueConstraint
from sqlalchemy.orm import mapped_column, Mapped
from app.core.db import Base

class MacroIndicator(Base):
    __tablename__ = "macro_indicator"
    __table_args__ = (UniqueConstraint("code", name="uq_macro_indicator_code"), {"schema":"trading"})
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)    # e.g., INFLATION
    name: Mapped[str] = mapped_column(String(128), nullable=False)
