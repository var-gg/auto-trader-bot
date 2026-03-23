# app/features/fred/controllers/macro_ingest_controller.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.fred.repositories.macro_repository import MacroRepository
from app.features.fred.services.fred_sync_service import FredSyncService
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from sqlalchemy import text


ingest_router = APIRouter(prefix="/macro/ingest", tags=["macro"])

@ingest_router.post(
    "/fred",
    summary="FRED 경제지표 수집 ★★★",
    description="FRED API를 통해 사용유무가 활성화된 모든 경제지표의 데이터를 수집하여 데이터베이스에 저장합니다. 2년치 데이터를 자동으로 수집합니다.",
    response_description="수집된 시리즈 수와 데이터 건수를 반환합니다."
)
def ingest_fred(db: Session = Depends(get_db)):
    svc = FredSyncService(db)
    total_series, total_rows = svc.sync_all_active_series()
    return {"ok": True, "series_count": total_series, "rows_upserted": total_rows}


class FredBootstrapReq(BaseModel):
    since: Optional[date] = Field(None, description="이 날짜(포함)부터 관측치 적재. 미지정 시 ohlcv 최소 trade_date 또는 5년 전")
    series_ids: Optional[List[str]] = Field(None, description="직접 시리즈 ID 배열. 미지정 시 group_codes로 확장")
    group_codes: Optional[List[str]] = Field(None, description="매크로 그룹 코드 배열. 해당 그룹에 연결된 시리즈 전부 대상")

@ingest_router.post(
    "/fred/bootstrap",
    summary="FRED 시리즈 대량 부트스트랩 (since부터 전량)",
    description="series_ids 또는 group_codes 기준으로, since 날짜부터 최신까지 모든 관측값을 일괄 적재합니다.",
)
def bootstrap_fred(req: FredBootstrapReq, db: Session = Depends(get_db)):
    svc = FredSyncService(db)
    repo = MacroRepository(db)

    # 1) 대상 시리즈 확정
    series_ids = req.series_ids or []
    if not series_ids and req.group_codes:
        # 그룹에 연결된 모든 시리즈 ID 가져오기
        series_ids = repo.list_series_ids_by_group_codes(req.group_codes)
    if not series_ids:
        # 안전장치: 활성 시리즈 전체
        series_ids = [s.fred_series_id for s in repo.get_active_series()]

    # 2) since 결정: 명시 > ohlcv 최소 trade_date > 5년전
    since = req.since
    if not since:
        # ohlcv 최소일자 조회 (테이블/스키마명은 환경에 맞춰 조정)
        min_trade_date = db.execute(text(
            "SELECT MIN(trade_date) FROM trading.ohlcv_daily"
        )).scalar()
        if min_trade_date:
            since = min_trade_date
        else:
            since = date.today().replace(year=date.today().year - 5)

    total_series, total_rows = svc.bulk_sync_since(series_ids, since)
    return {
        "ok": True,
        "since": since.isoformat(),
        "series_count": total_series,
        "rows_upserted": total_rows,
        "series_ids": series_ids,
    }
