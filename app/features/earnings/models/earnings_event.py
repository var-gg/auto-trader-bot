# app/features/earnings/models/earnings_event.py

from sqlalchemy import Column, Integer, String, Date, Float, UniqueConstraint
from app.core.db import Base


class EarningsEvent(Base):
    __tablename__ = "earnings_event"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="어닝 이벤트 ID")
    ticker_symbol = Column(String, index=True, nullable=False, comment="티커 심볼")

    fiscal_year = Column(Integer, nullable=False, comment="회계연도")
    fiscal_quarter = Column(Integer, nullable=False, comment="회계분기")

    # 날짜 의미 분리
    period_end_date = Column(Date, nullable=True, comment="회계기간 종료일")   # stock/earnings.period
    report_date = Column(Date, nullable=True, comment="레거시 실적 발표일(호환용)")
    confirmed_report_date = Column(Date, nullable=True, comment="실제 또는 고신뢰 확정 실적 발표일")
    expected_report_date_start = Column(Date, nullable=True, comment="예상 실적 발표 시작일")
    expected_report_date_end = Column(Date, nullable=True, comment="예상 실적 발표 종료일")
    report_date_confidence = Column(Float, nullable=True, comment="발표일 신뢰도(0~1)")
    report_date_kind = Column(String, nullable=True, comment="발표일 의미(confirmed/expected/legacy)")
    report_time = Column(String, nullable=True, comment="발표 시간")     # BMO / AMC

    estimate_eps = Column(Float, nullable=True, comment="EPS 예상치")
    actual_eps = Column(Float, nullable=True, comment="EPS 실제치")
    surprise_eps = Column(Float, nullable=True, comment="EPS 서프라이즈")

    estimate_revenue = Column(Float, nullable=True, comment="매출 예상치")
    actual_revenue = Column(Float, nullable=True, comment="매출 실제치")

    status = Column(String, default="scheduled", comment="실적 상태")
    source = Column(String, default="finnhub", comment="데이터 소스")

    __table_args__ = (
        UniqueConstraint("ticker_symbol", "fiscal_year", "fiscal_quarter",
                         name="uq_ticker_year_quarter"),
    )

    @property
    def preferred_report_date(self):
        """읽기 경로에서 우선 사용할 발표일.

        우선순위:
        1) confirmed_report_date
        2) expected_report_date_start
        3) legacy report_date
        """
        return self.confirmed_report_date or self.expected_report_date_start or self.report_date

    @property
    def preferred_report_date_end(self):
        """예상 윈도우 종료일이 있으면 반환, 아니면 단일 선호 발표일을 반환."""
        return self.expected_report_date_end or self.preferred_report_date
