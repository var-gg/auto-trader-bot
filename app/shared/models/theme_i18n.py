# app/models/theme_i18n.py
from sqlalchemy import Column, Integer, String, ForeignKey
from app.core.db import Base

class ThemeI18n(Base):
    __tablename__ = "theme_i18n"
    __table_args__ = {"schema": "trading"}

    id = Column(Integer, primary_key=True, index=True)
    theme_id = Column(Integer, ForeignKey("trading.theme.id"), nullable=False)
    lang_code = Column(String, nullable=False)   # "en", "ko", "ja", ...
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
