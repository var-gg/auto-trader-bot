from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import logging

from app.core.db import get_db
from live_app.api.responses import wrap_trading_result
from live_app.application.context import RunContext
from live_app.application.trading_commands import RunTradingHybridCommand

router = APIRouter(
    prefix="/api/trading-hybrid",
    tags=["Trading Hybrid"]
)

logger = logging.getLogger(__name__)


async def _run_command(*, market: str, phase: str, test_mode: bool, db: Session):
    route = f"/api/trading-hybrid/{market.lower()}/{phase}"
    ctx = RunContext(
        actor="http",
        channel="live_app.api",
        metadata={
            "route": route,
            "slot": f"{market}_{'OPEN' if phase == 'open' else 'INTRADAY'}",
            "strategy_version": "pm-core-v2",
        },
    )
    command = RunTradingHybridCommand(db)
    if phase == "open":
        result = await command.run_open(market=market, test_mode=test_mode, ctx=ctx)
        success_message = f"{market} open greedy executed"
    else:
        result = await command.run_intraday(market=market, test_mode=test_mode, ctx=ctx)
        success_message = f"{market} intraday cycle executed"
    return wrap_trading_result(result, test_mode=test_mode, success_message=success_message)


@router.post("/kr/open")
async def run_kr_open(test_mode: bool = False, db: Session = Depends(get_db)):
    try:
        return await _run_command(market="KR", phase="open", test_mode=test_mode, db=db)
    except Exception as e:
        logger.error(f"❌ KR Open Greedy 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/us/open")
async def run_us_open(test_mode: bool = False, db: Session = Depends(get_db)):
    try:
        return await _run_command(market="US", phase="open", test_mode=test_mode, db=db)
    except Exception as e:
        logger.error(f"❌ US Open Greedy 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/kr/intraday")
async def run_kr_intraday(test_mode: bool = False, db: Session = Depends(get_db)):
    try:
        return await _run_command(market="KR", phase="intraday", test_mode=test_mode, db=db)
    except Exception as e:
        logger.error(f"❌ KR Intraday Cycle 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/us/intraday")
async def run_us_intraday(test_mode: bool = False, db: Session = Depends(get_db)):
    try:
        return await _run_command(market="US", phase="intraday", test_mode=test_mode, db=db)
    except Exception as e:
        logger.error(f"❌ US Intraday Cycle 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
