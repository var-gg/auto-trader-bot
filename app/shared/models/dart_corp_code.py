# app/shared/models/dart_corp_code.py
from sqlalchemy import Column, String, DateTime, Boolean, Integer, Index, UniqueConstraint
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class DartCorpCode(Base):
    """DART 기업코드 정보"""
    __tablename__ = 'dart_corp_code'
    __table_args__ = (
        UniqueConstraint("corp_code", name="uq_dart_corp_code"),
        {"schema": DB_SCHEMA}
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment='DART 기업코드 ID')
    corp_code = Column(String(8), nullable=False, index=True, comment='기업 고유번호 (8자리) - DART corp_code')
    corp_name = Column(String(100), nullable=False, index=True, comment='정식명칭 - 정식회사명칭')
    corp_eng_name = Column(String(100), nullable=True, comment='영문 정식명칭 - 영문정식회사명칭')
    stock_code = Column(String(6), nullable=True, index=True, comment='종목코드 (6자리) - 상장회사인 경우만 존재')
    modify_date = Column(String(8), nullable=False, index=True, comment='최종변경일자 (YYYYMMDD)')
    
    # 메타 정보
    is_stock_listed = Column(Boolean, default=False, nullable=False, index=True, comment='상장여부 - stock_code 존재 여부')
    is_active = Column(Boolean, default=True, nullable=False, index=True, comment='활성여부 - 현재 사용중인지 여부')
    
    # 시스템 관리 필드
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment='생성시간')
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment='수정시간')
    collected_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True, comment='수집시간 - DART API 호출 시점')
    
    def __repr__(self):
        return f"<DartCorpCode(corp_code='{self.corp_code}', corp_name='{self.corp_name}', stock_code='{self.stock_code}')>"
    
    def __str__(self):
        return f"{self.corp_name}({self.corp_code}) - {self.stock_code or '비상장'}"
    
    @property
    def is_listed(self) -> bool:
        """상장여부 확인"""
        return self.is_stock_listed and self.stock_code is not None
    
    @classmethod
    def get_stock_code(cls, corp_code: str, db_session):
        """고유번호로 종목코드 조회"""
        corp = db_session.query(cls).filter(
            cls.corp_code == corp_code,
            cls.is_active == True
        ).first()
        return corp.stock_code if corp and corp.is_stock_listed else None
    
    @classmethod
    def get_corp_code(cls, stock_code: str, db_session):
        """종목코드로 고유번호 조회"""
        corp = db_session.query(cls).filter(
            cls.stock_code == stock_code,
            cls.is_active == True
        ).first()
        return corp.corp_code if corp else None
    
    def to_dict(self) -> dict:
        """딕셔너리 변환"""
        return {
            'id': self.id,
            'corp_code': self.corp_code,
            'corp_name': self.corp_name,
            'corp_eng_name': self.corp_eng_name,
            'stock_code': self.stock_code,
            'modify_date': self.modify_date,
            'is_stock_listed': self.is_stock_listed,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'collected_at': self.collected_at.isoformat() if self.collected_at else None,
        }
