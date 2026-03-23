# app/features/fred/services/fred_sync_service.py
from datetime import date, datetime, timedelta, timezone
from typing import List
from sqlalchemy.orm import Session
from app.features.fred.services.fred_client import FredClient
from app.features.fred.repositories.macro_repository import MacroRepository
from app.core.config import FRED_DEFAULT_LOOKBACK_DAYS

def _parse_dt(dt_str: str | None):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z","+00:00"))
    except Exception:
        return None

class FredSyncService:
    def __init__(self, db: Session, client: FredClient | None = None):
        self.db = db
        self.client = client or FredClient()
        self.repo = MacroRepository(db)

    def sync_series_window(self, fred_series_id: str, lookback_days: int | None = None):
        lookback_days = lookback_days or FRED_DEFAULT_LOOKBACK_DAYS

        meta = self.client.series(fred_series_id)["seriess"][0]
        series = self.repo.upsert_series({
            "fred_series_id": fred_series_id,
            "title": meta.get("title"),
            "frequency": meta.get("frequency"),
            "units": meta.get("units"),
            "seasonal_adjustment": meta.get("seasonal_adjustment"),
            "notes": meta.get("notes"),
            "observation_start": _parse_dt(meta.get("observation_start")),
            "observation_end": _parse_dt(meta.get("observation_end")),
            "last_updated_at": _parse_dt(meta.get("last_updated")),
        })

        today = date.today()
        observations = self.client.observations(
            fred_series_id,
            observation_start=(today - timedelta(days=lookback_days)).isoformat(),
            # realtime_* 파라미터 제거 → 최신 빈티지만
        )["observations"]

        vintage_dt = datetime.combine(today, datetime.min.time()).astimezone()
        rows = []
        for o in observations:
            raw = o.get("value")
            is_missing = (raw is None) or (str(raw).strip() == ".")
            rows.append({
                "obs_date": date.fromisoformat(o["date"]),
                "vintage_start": vintage_dt,
                "value": None if is_missing else float(raw),
                "is_missing": is_missing,
                "ingested_at": datetime.utcnow(),
            })
        upserted = self.repo.upsert_values(series.id, rows)
        return series.id, upserted

    def sync_all_active_series(self):
        """사용유무가 true인 모든 시리즈를 2년치 데이터로 동기화합니다."""
        active_series = self.repo.get_active_series()
        total_series = len(active_series)
        total_rows = 0
        
        for series in active_series:
            _, upserted = self.sync_series_window(series.fred_series_id, lookback_days=730)  # 2년 = 730일
            total_rows += upserted
            
        return total_series, total_rows

    def ensure_series_exists(self, fred_series_id: str):
        """시리즈 메타가 DB에 없으면 /series 호출로 생성"""
        existing = self.repo.get_series_by_fred_id(fred_series_id)
        if existing:
            return existing
        meta = self.client.series(fred_series_id)["seriess"][0]
        return self.repo.upsert_series({
            "fred_series_id": fred_series_id,
            "title": meta.get("title"),
            "frequency": meta.get("frequency"),
            "units": meta.get("units"),
            "seasonal_adjustment": meta.get("seasonal_adjustment"),
            "notes": meta.get("notes"),
            "observation_start": _parse_dt(meta.get("observation_start")),
            "observation_end": _parse_dt(meta.get("observation_end")),
            "last_updated_at": _parse_dt(meta.get("last_updated")),
        })

    def sync_series_since(self, fred_series_id: str, since: date):
        """관측 시작일(since)부터 최신까지 전체 싹 긁기 (부트스트랩 용)"""
        series = self.ensure_series_exists(fred_series_id)

        observations = self.client.observations(
            fred_series_id,
            observation_start=since.isoformat(),   # ← 핵심
            # realtime_* 파라미터 미지정: 최신 빈티지만 사용
        )["observations"]

        # 같은 날 여러 번 돌려도 unique key가 같도록 UTC 자정으로 고정
        vintage_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        rows = []
        for o in observations:
            raw = o.get("value")
            is_missing = (raw is None) or (str(raw).strip() == ".")
            rows.append({
                "obs_date": date.fromisoformat(o["date"]),
                "vintage_start": vintage_dt,
                "value": None if is_missing else float(raw),
                "is_missing": is_missing,
                "ingested_at": datetime.utcnow(),
            })
        upserted = self.repo.upsert_values(series.id, rows)
        return series.id, upserted

    def bulk_sync_since(self, series_ids: List[str], since: date):
        """여러 시리즈를 since부터 일괄 부트스트랩"""
        total_series = 0
        total_rows = 0
        for sid in series_ids:
            total_series += 1
            _, upserted = self.sync_series_since(sid, since)
            total_rows += upserted
        return total_series, total_rows
