# app/models/theme.py
from sqlalchemy import Column, Integer, String
from app.core.db import Base

class Theme(Base):
    __tablename__ = "theme"
    __table_args__ = {"schema": "trading"}

    id = Column(Integer, primary_key=True, index=True, comment="테마 ID")
    code = Column(String, unique=True, nullable=False, comment="테마 코드")   # "AI", "EV"
