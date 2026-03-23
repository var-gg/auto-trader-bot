# app\features\earnings\repositories\earnings_repository.py
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func, case, desc
from app.features.earnings.models.earnings_event import EarningsEvent
from app.shared.models.ticker import Ticker

class EarningsRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_event(self, event_data: dict) -> EarningsEvent:
        stmt = insert(EarningsEvent).values(**event_data)

        # 기본적으로 COALESCE 적용 (새 값이 NULL이면 기존 값 유지)
        update_cols = {
            col.name: func.coalesce(getattr(stmt.excluded, col.name), getattr(EarningsEvent, col.name))
            for col in EarningsEvent.__table__.columns
            if col.name not in ("id",)  # PK는 제외
        }

        # status는 특별 규칙 적용 (SQLAlchemy 2.x 스타일)
        update_cols["status"] = case(
            (getattr(stmt.excluded, "status") == "reported", "reported"),
            (getattr(stmt.excluded, "status").isnot(None), getattr(stmt.excluded, "status")),
            else_=EarningsEvent.status
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker_symbol", "fiscal_year", "fiscal_quarter"],
            set_=update_cols
        ).returning(EarningsEvent)

        result = self.db.execute(stmt)
        self.db.commit()
        return result.fetchone()

    def find_pending_events(self, today):
        """발표일이 지났는데 아직 actual 없는 이벤트.

        새 의미 분리 필드가 있으면 confirmed/expected 시작일을 우선 보고,
        없으면 legacy report_date를 fallback으로 사용한다.
        """
        pending = []
        events = (
            self.db.query(EarningsEvent)
            .filter(EarningsEvent.actual_eps.is_(None))
            .all()
        )
        for event in events:
            preferred_date = event.preferred_report_date
            if preferred_date is not None and preferred_date <= today:
                pending.append(event)
        return pending

    def get_earnings_for_analyst(self, ticker_id: int):
        """티커 ID로 어닝 정보 조회 (애널리스트 AI용)"""
        ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            return None

        events = (
            self.db.query(EarningsEvent)
            .filter(EarningsEvent.ticker_symbol == ticker.symbol)
            .order_by(desc(EarningsEvent.fiscal_year), desc(EarningsEvent.fiscal_quarter))
            .all()
        )

        if not events:
            return None

        today = datetime.now().date()

        latest_reported = None
        for event in events:
            if event.actual_eps is not None:
                latest_reported = event
                break

        future_scheduled = [
            event for event in events
            if event.status == "scheduled"
            and event.preferred_report_date is not None
            and event.preferred_report_date >= today
        ]

        future_scheduled.sort(key=lambda e: (e.preferred_report_date, -int(e.fiscal_year or 0), -int(e.fiscal_quarter or 0)))
        upcoming = future_scheduled[0] if future_scheduled else None

        if upcoming is None:
            undated_scheduled = [event for event in events if event.status == "scheduled" and event.preferred_report_date is None]
            undated_scheduled.sort(key=lambda e: (-int(e.fiscal_year or 0), -int(e.fiscal_quarter or 0)))
            upcoming = undated_scheduled[0] if undated_scheduled else None

        return {
            "ticker": ticker.symbol,
            "latest": latest_reported,
            "upcoming": upcoming
        }
