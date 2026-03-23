# app/features/fred/repositories/macro_repository.py
from datetime import datetime, date
from typing import Iterable, Optional, List
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from app.features.fred.models.macro_data_series import MacroDataSeries
from app.features.fred.models.macro_data_series_value import MacroDataSeriesValue
from app.features.fred.models.macro_group import MacroGroup
from app.features.fred.models.macro_group_series import MacroGroupSeries

class MacroRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_series(self, meta: dict) -> MacroDataSeries:
        stmt = (
            insert(MacroDataSeries)
            .values(**meta)
            .on_conflict_do_update(
                index_elements=[MacroDataSeries.fred_series_id],
                set_={
                    "title": stmt_excluded("title"),
                    "frequency": stmt_excluded("frequency"),
                    "units": stmt_excluded("units"),
                    "seasonal_adjustment": stmt_excluded("seasonal_adjustment"),
                    "notes": stmt_excluded("notes"),
                    "observation_start": stmt_excluded("observation_start"),
                    "observation_end": stmt_excluded("observation_end"),
                    "last_updated_at": stmt_excluded("last_updated_at"),
                    "updated_at": datetime.utcnow(),
                },
            )
            .returning(MacroDataSeries)
        )
        result = self.db.execute(stmt)
        series = result.scalar_one()
        self.db.commit()
        return series

    def get_series_by_fred_id(self, fred_series_id: str) -> Optional[MacroDataSeries]:
        return (
            self.db.query(MacroDataSeries)
            .filter(MacroDataSeries.fred_series_id == fred_series_id)
            .one_or_none()
        )

    def get_active_series(self) -> list[MacroDataSeries]:
        """사용유무가 true인 시리즈들을 조회합니다."""
        return (
            self.db.query(MacroDataSeries)
            .filter(MacroDataSeries.is_active == True)
            .all()
        )

    def get_groups_with_series(self) -> List[dict]:
        """그룹과 포함된 시리즈들을 함께 조회합니다."""
        groups = (
            self.db.query(MacroGroup)
            .filter(MacroGroup.is_active == True)
            .order_by(MacroGroup.sort_order, MacroGroup.id)
            .all()
        )
        
        result = []
        for group in groups:
            series_list = (
                self.db.query(MacroDataSeries)
                .join(MacroGroupSeries, MacroGroupSeries.series_id == MacroDataSeries.id)
                .filter(
                    MacroGroupSeries.group_id == group.id,
                    MacroGroupSeries.is_active == True,
                    MacroDataSeries.is_active == True
                )
                .order_by(MacroGroupSeries.sort_order, MacroDataSeries.fred_series_id)
                .all()
            )
            
            result.append({
                "code": group.code,
                "name": group.name,
                "description": group.description,
                "series": [s.fred_series_id for s in series_list]
            })
        
        return result

    def upsert_group(self, group_data: dict) -> MacroGroup:
        """그룹을 생성하거나 업데이트합니다."""
        stmt = (
            insert(MacroGroup)
            .values(**group_data)
            .on_conflict_do_update(
                index_elements=[MacroGroup.code],
                set_={
                    "name": stmt_excluded("name"),
                    "description": stmt_excluded("description"),
                    "is_active": stmt_excluded("is_active"),
                    "sort_order": stmt_excluded("sort_order"),
                    "updated_at": datetime.utcnow(),
                },
            )
            .returning(MacroGroup)
        )
        result = self.db.execute(stmt)
        group = result.scalar_one()
        self.db.commit()
        return group

    def upsert_group_series(self, group_id: int, series_ids: List[str]) -> int:
        """그룹에 시리즈들을 연결합니다."""
        # 기존 연결 제거
        self.db.query(MacroGroupSeries).filter(MacroGroupSeries.group_id == group_id).delete()
        
        # 시리즈 ID로 실제 시리즈 조회
        series_list = (
            self.db.query(MacroDataSeries)
            .filter(MacroDataSeries.fred_series_id.in_(series_ids))
            .all()
        )
        
        # 새로운 연결 생성
        rows = []
        for idx, series in enumerate(series_list):
            rows.append({
                "group_id": group_id,
                "series_id": series.id,
                "sort_order": idx,
                "is_active": True
            })
        
        if rows:
            stmt = insert(MacroGroupSeries).values(rows)
            self.db.execute(stmt)
            self.db.commit()
        
        return len(rows)

    def upsert_values(self, series_id: int, rows: Iterable[dict]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        
        # 중복 제거: 동일한 series_id, obs_date, value 조합이 이미 존재하는지 확인
        filtered_rows = []
        for r in rows:
            r["series_id"] = series_id
            obs_date = r["obs_date"]
            value = r["value"]
            
            # 동일한 3개 값 조합이 이미 존재하는지 확인
            if not self._exists_same_values(series_id, obs_date, value):
                filtered_rows.append(r)
        
        if not filtered_rows:
            return 0
            
        stmt = insert(MacroDataSeriesValue).values(filtered_rows).on_conflict_do_update(
            constraint="uq_macro_value_vintage",
            set_={
                "value": stmt_excluded("value"),
                "is_missing": stmt_excluded("is_missing"),
                "ingested_at": stmt_excluded("ingested_at"),
            },
        )
        self.db.execute(stmt)
        self.db.commit()
        return len(filtered_rows)

    def _exists_same_values(self, series_id: int, obs_date: date, value: float) -> bool:
        """동일한 series_id, obs_date, value 조합이 이미 존재하는지 확인"""
        result = (
            self.db.query(MacroDataSeriesValue)
            .filter(
                MacroDataSeriesValue.series_id == series_id,
                MacroDataSeriesValue.obs_date == obs_date,
                MacroDataSeriesValue.value == value
            )
            .first()
        )
        return result is not None

    def list_series_ids_by_group_codes(self, group_codes: list[str]) -> list[str]:
        """그룹 코드 배열로부터 연결된 모든 시리즈 ID 조회"""
        rows = (
            self.db.query(MacroDataSeries.fred_series_id)
            .join(MacroGroupSeries, MacroGroupSeries.series_id == MacroDataSeries.id)
            .join(MacroGroup, MacroGroup.id == MacroGroupSeries.group_id)
            .filter(
                MacroGroup.code.in_(group_codes),
                MacroGroupSeries.is_active == True,
                MacroDataSeries.is_active == True
            )
            .all()
        )
        return [r[0] for r in rows]


# 안전한 excluded helper (불리언 캐스트 금지)
def stmt_excluded(col: str):
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    
    # MacroGroup excluded 확인
    if hasattr(pg_insert(MacroGroup).excluded, col):
        return getattr(pg_insert(MacroGroup).excluded, col)
    # MacroGroupSeries excluded 확인
    if hasattr(pg_insert(MacroGroupSeries).excluded, col):
        return getattr(pg_insert(MacroGroupSeries).excluded, col)
    # MacroDataSeries excluded 확인
    if hasattr(pg_insert(MacroDataSeries).excluded, col):
        return getattr(pg_insert(MacroDataSeries).excluded, col)
    # MacroDataSeriesValue excluded 확인
    if hasattr(pg_insert(MacroDataSeriesValue).excluded, col):
        return getattr(pg_insert(MacroDataSeriesValue).excluded, col)
    
    # 모든 테이블에서 찾지 못한 경우, 일반적인 excluded 사용
    return text(f"EXCLUDED.{col}")
