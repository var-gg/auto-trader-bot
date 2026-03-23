from __future__ import annotations
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, JSON
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

class GptCallLog(Base):
    __tablename__ = "gpt_call_log"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True, comment="GPT 호출 로그 ID")

    # 분류/요약/티커 등 태그
    task = Column(String(50), nullable=False, comment="작업 유형")          # e.g., "classify", "summarize", "ticker_pick"
    model = Column(String(100), nullable=False, comment="사용 모델")        # 모델명
    schema_name = Column(String(100), nullable=True, comment="스키마명")   # JSON schema name

    # 페이로드(길이 제한)
    prompt = Column(Text, nullable=True, comment="프롬프트 텍스트")               # user_text 원본 (절단 저장)
    response_text = Column(Text, nullable=True, comment="응답 텍스트")        # 모델 원문 응답 (절단 저장)

    # 결과 메타
    ok = Column(Integer, nullable=False, default=1, comment="성공 여부")    # 1=성공, 0=실패
    error = Column(Text, nullable=True, comment="오류 메시지")

    # 토큰 및 성능
    tokens_input = Column(Integer, nullable=True, comment="입력 토큰 수")
    tokens_output = Column(Integer, nullable=True, comment="출력 토큰 수")
    latency_ms = Column(Float, nullable=True, comment="응답 시간(ms)")

    # 상관키(선택)
    news_id = Column(Integer, nullable=True, comment="관련 뉴스 ID")
    extra = Column(JSON, nullable=True, comment="추가 메타데이터")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
