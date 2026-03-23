# app/features/fred/controllers/macro_prompt_controller.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.fred.services.macro_snapshot_service import MacroSnapshotService

prompt_router = APIRouter(prefix="/macro/prompt", tags=["macro"])

@prompt_router.get(
    "/snapshot",
    summary="매크로 스냅샷 생성",
    description="수집된 경제지표 데이터를 바탕으로 컴팩트한 형태의 매크로 스냅샷을 생성합니다. 각 지표의 최신값, 전월 대비 변화율, 발표일을 포함합니다.",
    response_description="컴팩트한 형태의 매크로 스냅샷 데이터를 반환합니다."
)
def macro_prompt_snapshot(db: Session = Depends(get_db)):
    svc = MacroSnapshotService(db)
    return svc.build_compact_snapshot()

