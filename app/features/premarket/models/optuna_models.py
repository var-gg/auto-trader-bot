# app/features/premarket/models/optuna_models.py
"""
Optuna 백테스트 최적화 결과 모델
- optuna_snapshots: 데이터 스냅샷 메타정보
- optuna_vector_config: 벡터 설정 (최적 파라미터)
- optuna_target_vectors: 타겟 벡터 (TB 라벨 + IAE + shape/ctx 벡터)
- pm_best_signal: 장전 베스트 신호
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, SmallInteger, Date, CheckConstraint, UniqueConstraint, ForeignKey, BigInteger, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector  # pgvector 확장
from app.core.db import Base
from app.core.config import DB_SCHEMA


class OptunaSnapshots(Base):
    """
    Optuna 스냅샷 테이블
    - 백테스트 데이터의 스냅샷 메타정보
    """
    __tablename__ = "optuna_snapshots"
    __table_args__ = (
        UniqueConstraint("source_tag", "data_sha1", name="optuna_snapshots_source_tag_data_sha1_key"),
        Index("ix_optuna_snapshots_created", "created_at", postgresql_ops={"created_at": "DESC"}),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_tag = Column(Text, nullable=False, comment="데이터 소스 태그")
    data_sha1 = Column(Text, nullable=False, comment="데이터 해시")
    date_from = Column(Date, nullable=True, comment="시작일")
    date_to = Column(Date, nullable=True, comment="종료일")
    symbol_count = Column(Integer, nullable=True, comment="종목 수")
    row_count = Column(BigInteger, nullable=True, comment="행 수")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # 관계
    configs = relationship("OptunaVectorConfig", back_populates="snapshot")
    target_vectors = relationship("OptunaTargetVector", back_populates="snapshot")


class OptunaVectorConfig(Base):
    """
    Optuna 벡터 설정 테이블
    - 백테스트 최적화 결과 저장
    """
    __tablename__ = "optuna_vector_config"
    __table_args__ = (
        CheckConstraint("anchor_mode = 'open_next'", name="optuna_vector_config_anchor_mode_check"),
        CheckConstraint("candle_mode = 'pct'", name="optuna_vector_config_candle_mode_check"),
        CheckConstraint("include_candle_meta IS TRUE", name="optuna_vector_config_include_candle_meta_check"),
        CheckConstraint("status = ANY (ARRAY['draft'::text, 'promoted'::text, 'retired'::text])", name="optuna_vector_config_status_check"),
        UniqueConstraint("cfg_sha1", name="optuna_vector_config_cfg_sha1_key"),
        Index("ix_optuna_vector_config_snapshot", "snapshot_id"),
        Index("ix_optuna_vector_config_status", "status", "created_at", postgresql_ops={"created_at": "DESC"}),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.optuna_snapshots.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True)
    cfg_sha1 = Column(Text, nullable=False, comment="설정 해시")
    
    # 백테스트 설정
    future = Column(Integer, nullable=False, comment="미래 예측 기간 (일)")
    target = Column(Float, nullable=False, comment="목표 수익률")
    max_reverse = Column(Float, nullable=False, comment="최대 역행률")
    anchor_mode = Column(Text, nullable=False, server_default="open_next", comment="앵커 모드")
    lookback = Column(Integer, nullable=False, comment="과거 참조 기간")
    
    # 벡터 파라미터
    m = Column(Integer, nullable=False, comment="벡터 차원")
    w_price = Column(Float, nullable=False, comment="가격 가중치")
    w_volume = Column(Float, nullable=False, comment="거래량 가중치")
    w_candle = Column(Float, nullable=False, comment="캔들 가중치")
    w_meta = Column(Float, nullable=False, comment="메타 가중치")
    candle_mode = Column(Text, nullable=False, server_default="pct", comment="캔들 모드")
    include_candle_meta = Column(Boolean, nullable=False, server_default="true", comment="캔들 메타 포함 여부")
    
    # 매크로 설정
    macro_window = Column(Integer, nullable=True, comment="매크로 윈도우")
    macro_cols = Column(JSONB, nullable=True, comment="매크로 컬럼")
    macro_lag_days = Column(Integer, nullable=True, server_default="0", comment="매크로 지연 일수")
    
    # 최적화 파라미터
    alpha = Column(Float, nullable=False, comment="알파 (클릭률 가중치)")
    beta = Column(Float, nullable=False, comment="베타 (다양성 패널티)")
    tau_softmax = Column(Float, nullable=False, comment="소프트맥스 온도")
    threshold = Column(Float, nullable=False, comment="신뢰도 임계값")
    topn = Column(Integer, nullable=False, comment="상위 N개 선택")
    norm = Column(Text, nullable=False, server_default="l2", comment="정규화 방식")
    cost_bps = Column(Float, nullable=True, server_default="20.0", comment="거래 비용 (bps)")
    
    # 상태
    status = Column(Text, nullable=False, server_default="draft", comment="상태 (draft, promoted, retired)")
    promoted_at = Column(DateTime(timezone=True), nullable=True, comment="프로모션 일시")
    build_version = Column(Text, nullable=True, comment="빌드 버전")
    description = Column(Text, nullable=True, comment="설명")
    
    # 타임스탬프
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # ✅ 최적화된 래더 파라미터 (buy/sell)
    ladder_params = Column(JSONB, nullable=True, comment="최적화된 래더 파라미터 (buy/sell)")
    
    # 관계
    snapshot = relationship("OptunaSnapshots", back_populates="configs")
    target_vectors = relationship("OptunaTargetVector", back_populates="config")


class OptunaTargetVector(Base):
    """
    Optuna 타겟 벡터 테이블
    - 각 종목/앵커일의 과거 패턴 벡터
    - tb_label: 트리플 배리어 메타라벨 (UP_FIRST, DOWN_FIRST, TIMEOUT)
    - iae_1_3: Initial Adverse Excursion (1~3일)
    - shape_vec, ctx_vec: 형태/컨텍스트 벡터 (pgvector)
    """
    __tablename__ = "optuna_target_vectors"
    __table_args__ = (
        CheckConstraint("direction = ANY (ARRAY['UP'::text, 'DOWN'::text])", name="optuna_target_vectors_direction_check"),
        CheckConstraint("pp = ANY (ARRAY[0, 1])", name="optuna_target_vectors_pp_check"),
        CheckConstraint("tb_label = ANY (ARRAY['UP_FIRST'::text, 'DOWN_FIRST'::text, 'TIMEOUT'::text])", name="optuna_target_vectors_tb_label_check"),
        UniqueConstraint("config_id", "ticker_id", "idx", "direction", name="uq_optuna_target_vectors"),
        Index("ix_otv_cfg_date", "config_id", "anchor_date"),
        Index("ix_otv_cfg_ticker_date", "config_id", "ticker_id", "anchor_date"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.optuna_vector_config.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", onupdate="CASCADE"), nullable=False)
    symbol = Column(Text, nullable=False, comment="종목 심볼")
    
    anchor_date = Column(Date, nullable=False, comment="앵커 일자")
    idx = Column(Integer, nullable=False, comment="인덱스")
    direction = Column(Text, nullable=False, comment="방향 (UP, DOWN)")
    pp = Column(SmallInteger, nullable=False, server_default="1", comment="패턴 포인트 (0 or 1)")
    
    # ✅ 트리플 배리어 메타라벨
    tb_label = Column(Text, nullable=False, comment="트리플 배리어 라벨 (UP_FIRST, DOWN_FIRST, TIMEOUT)")
    
    # ✅ Initial Adverse Excursion (초기 역행 폭)
    iae_1_3 = Column(Float, nullable=False, comment="IAE 1~3일 (초기 역행 폭)")
    
    # ✅ 벡터 데이터 (pgvector 타입) - 차원은 동적이므로 Vector()로 지정
    shape_vec = Column(Vector, nullable=False, comment="형태 벡터")
    ctx_vec = Column(Vector, nullable=False, comment="컨텍스트 벡터")
    shape_dim = Column(Integer, nullable=False, comment="형태 벡터 차원")
    ctx_dim = Column(Integer, nullable=False, comment="컨텍스트 벡터 차원")
    
    # 메타
    snapshot_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.optuna_snapshots.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True)
    build_version = Column(Text, nullable=True, comment="빌드 버전")
    
    # 타임스탬프
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # 관계
    config = relationship("OptunaVectorConfig", back_populates="target_vectors")
    snapshot = relationship("OptunaSnapshots", back_populates="target_vectors")
    ticker = relationship("Ticker")


class PMBestSignal(Base):
    """
    장전(Pre-market) 베스트 신호 테이블
    - 각 종목의 1일 방향성 점수 (signal_1d)
    - best_target_id: 가장 유사한 과거 패턴 (optuna_target_vectors.id)
    """
    __tablename__ = "pm_best_signal"
    __table_args__ = {"schema": DB_SCHEMA}

    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True)
    symbol = Column(Text, nullable=False, comment="종목 심볼")
    company_name = Column(Text, nullable=True, comment="회사명")
    
    # ✅ 핵심: 베스트 타겟 ID (optuna_target_vectors.id) - NOT NULL
    best_target_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.optuna_target_vectors.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    
    # ✅ 1일 방향성 점수 (-1.0 ~ +1.0) - float4 (real)
    signal_1d = Column(Float(precision=24), nullable=False, comment="1일 방향성 점수")
    
    # ✅ 역돌파 정보 (프로시저에서 채워짐)
    target_achieved_day = Column(Integer, nullable=True, comment="목표 도달 일자")
    target_achieved_date = Column(Date, nullable=True, comment="목표 도달 날짜")
    reverse_breach_day = Column(Integer, nullable=True, comment="역돌파 일자")
    reverse_breach_date = Column(Date, nullable=True, comment="역돌파 날짜")
    
    # 타임스탬프
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # 관계
    ticker = relationship("Ticker")
    best_target = relationship("OptunaTargetVector", foreign_keys=[best_target_id])

