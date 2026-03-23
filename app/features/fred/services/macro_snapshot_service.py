# app/features/fred/services/macro_snapshot_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.features.fred.repositories.macro_repository import MacroRepository

class MacroSnapshotService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = MacroRepository(db)

    def _latest_vintage_points(self, fred_series_id: str, n: int):
        q = text("""
        with lv as (
          select s.id as series_id, val.obs_date, max(val.vintage_start) v
          from trading.macro_data_series s
          join trading.macro_data_series_value val on val.series_id = s.id
          where s.fred_series_id = :sid
          group by s.id, val.obs_date
        )
        select val.obs_date, val.value
        from lv
        join trading.macro_data_series_value val
          on val.series_id=lv.series_id and val.obs_date=lv.obs_date and val.vintage_start=lv.v
        order by val.obs_date desc
        limit :n
        """)
        rows = self.db.execute(q, {"sid": fred_series_id, "n": n}).all()
        rows = list(reversed(rows))
        return [{"date": r[0].isoformat(), "value": (None if r[1] is None else float(r[1]))} for r in rows]

    def _get_latest_value_and_mom(self, fred_series_id: str):
        """시리즈의 최신값과 전월 대비 변화율을 계산합니다."""
        from sqlalchemy import func
        from app.features.fred.models.macro_data_series import MacroDataSeries
        from app.features.fred.models.macro_data_series_value import MacroDataSeriesValue
        
        # 시리즈 ID 조회
        series = (
            self.db.query(MacroDataSeries)
            .filter(MacroDataSeries.fred_series_id == fred_series_id)
            .first()
        )
        
        if not series:
            return None, None, None
        
        # 최신 2개 관측값 조회 (최신 빈티지 기준)
        subquery = (
            self.db.query(
                MacroDataSeriesValue.obs_date,
                func.max(MacroDataSeriesValue.vintage_start).label('max_vintage')
            )
            .filter(MacroDataSeriesValue.series_id == series.id)
            .group_by(MacroDataSeriesValue.obs_date)
            .subquery()
        )
        
        latest_values = (
            self.db.query(MacroDataSeriesValue.obs_date, MacroDataSeriesValue.value)
            .join(subquery, 
                  (MacroDataSeriesValue.obs_date == subquery.c.obs_date) &
                  (MacroDataSeriesValue.vintage_start == subquery.c.max_vintage))
            .filter(MacroDataSeriesValue.series_id == series.id)
            .order_by(MacroDataSeriesValue.obs_date.desc())
            .limit(2)
            .all()
        )
        
        if not latest_values:
            return None, None, None
        
        latest_date = latest_values[0][0]
        latest_value = latest_values[0][1]
        
        # 전월 대비 변화율 계산
        mom = None
        if len(latest_values) >= 2 and latest_values[1][1] is not None and latest_value is not None:
            mom = latest_value - latest_values[1][1]
        
        return latest_value, mom, latest_date

    def build_compact_snapshot(self):
        """컴팩트한 형태의 매크로 스냅샷을 생성합니다."""
        groups_data = self.repo.get_groups_with_series()
        
        result = {"macro": {}}
        
        for group in groups_data:
            group_code = group["code"]
            result["macro"][group_code] = {}
            
            for series_id in group["series"]:
                latest_value, mom, release_date = self._get_latest_value_and_mom(series_id)
                
                if latest_value is not None:
                    result["macro"][group_code][series_id] = {
                        "last": round(latest_value, 2),
                        "mom": round(mom, 2) if mom is not None else None,
                        "release_date": release_date.isoformat() if release_date else None
                    }
        
        return result
