# app/features/trading_hybrid/controllers/trading_hybrid_controller.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.trading_hybrid.engines import runbooks
import logging

router = APIRouter(
    prefix="/api/trading-hybrid",
    tags=["Trading Hybrid"]
)

logger = logging.getLogger(__name__)


@router.post("/kr/open")
async def run_kr_open(test_mode: bool = False, db: Session = Depends(get_db)):
    """
    한국 시장 장초 탐욕 레그 실행
    
    - 손익&계좌 동기화 (자동)
    - 스윙 추천 기반으로 장초 다층 분할 매수 주문 생성
    - 현재가 아래 깊은 할인 LIMIT만 배치
    
    Args:
        test_mode: True일 경우 KIS 주문 API 호출 없이 dry-run (기본값: False)
    
    Returns:
        생성된 주문 플랜 정보 (buy_plans, sell_plans, skipped, summary 포함)
    """
    try:
        logger.info(f"🚀 KR Open Greedy 시작 (TEST_MODE={test_mode})")
        result = await runbooks.run_kr_open(db, test_mode=test_mode)
        
        # ✅ 실제 작업 검증: 시장 휴장이 아닌 경우 최소한의 작업이 수행되어야 함
        if not result:
            logger.error("❌ No result returned from runbook")
            raise RuntimeError("KR open greedy returned empty result")
        
        # 시장 휴장인 경우는 정상 처리
        if result.get("message") == "시장 휴장":
            logger.info("⚠️ KR 시장 휴장 - 스킵")
            return {
                "status": "skipped",
                "message": "Market closed - no trading executed",
                "test_mode": test_mode,
                "data": result
            }
        
        # 실제 거래 수행 여부 확인
        summary = result.get("summary", {})
        buy_count = summary.get("buy_count", 0)
        sell_count = summary.get("sell_count", 0)
        
        # test_mode가 아닌 경우, 시장이 열렸는데 아무 작업도 안 했으면 문제
        if not test_mode and buy_count == 0 and sell_count == 0:
            logger.warning("⚠️ 시장 개장 중이지만 주문 생성 없음 - 추천 종목 부족 또는 조건 미달")
            # 이 경우는 정상일 수 있으므로 경고만 (예외 발생 안 함)
        
        logger.info(f"✅ KR Open Greedy 완료 (매수: {buy_count}, 매도: {sell_count})")
        return {
            "status": "success",
            "message": "KR open greedy executed",
            "test_mode": test_mode,
            "data": result
        }
    except Exception as e:
        logger.error(f"❌ KR Open Greedy 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/us/open")
async def run_us_open(test_mode: bool = False, db: Session = Depends(get_db)):
    """
    미국 시장 장초 탐욕 레그 실행
    
    - 손익&계좌 동기화 (자동)
    - 스윙 추천 기반으로 장초 다층 분할 매수 주문 생성
    - 현재가 아래 깊은 할인 LIMIT만 배치
    
    Args:
        test_mode: True일 경우 KIS 주문 API 호출 없이 dry-run (기본값: False)
    
    Returns:
        생성된 주문 플랜 정보 (buy_plans, sell_plans, skipped, summary 포함)
    """
    try:
        logger.info(f"🚀 US Open Greedy 시작 (TEST_MODE={test_mode})")
        result = await runbooks.run_us_open(db, test_mode=test_mode)
        
        # ✅ 실제 작업 검증
        if not result:
            logger.error("❌ No result returned from runbook")
            raise RuntimeError("US open greedy returned empty result")
        
        # 시장 휴장인 경우는 정상 처리
        if result.get("message") == "시장 휴장":
            logger.info("⚠️ US 시장 휴장 - 스킵")
            return {
                "status": "skipped",
                "message": "Market closed - no trading executed",
                "test_mode": test_mode,
                "data": result
            }
        
        # 실제 거래 수행 여부 확인
        summary = result.get("summary", {})
        buy_count = summary.get("buy_count", 0)
        sell_count = summary.get("sell_count", 0)
        
        if not test_mode and buy_count == 0 and sell_count == 0:
            logger.warning("⚠️ 시장 개장 중이지만 주문 생성 없음 - 추천 종목 부족 또는 조건 미달")
        
        logger.info(f"✅ US Open Greedy 완료 (매수: {buy_count}, 매도: {sell_count})")
        return {
            "status": "success",
            "message": "US open greedy executed",
            "test_mode": test_mode,
            "data": result
        }
    except Exception as e:
        logger.error(f"❌ US Open Greedy 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/kr/intraday")
async def run_kr_intraday(test_mode: bool = False, db: Session = Depends(get_db)):
    """
    한국 시장 장중 사이클 실행
    
    - 손익&계좌 동기화 (자동)
    - 5/10분 주기로 단타·리밸런싱·손절 수행
    - 유동성 상위면 5분, 아니면 10분 주기
    
    Args:
        test_mode: True일 경우 KIS 주문 API 호출 없이 dry-run (기본값: False)
    
    Returns:
        생성된 주문 플랜 정보 (buy_plans, sell_plans, skipped, summary 포함)
    """
    try:
        logger.info(f"🚀 KR Intraday Cycle 시작 (TEST_MODE={test_mode})")
        result = await runbooks.run_kr_intraday(db, test_mode=test_mode)
        
        # ✅ 실제 작업 검증
        if not result:
            logger.error("❌ No result returned from runbook")
            raise RuntimeError("KR intraday cycle returned empty result")
        
        # 시장 휴장인 경우는 정상 처리
        if result.get("message") == "시장 휴장":
            logger.info("⚠️ KR 시장 휴장 - 스킵")
            return {
                "status": "skipped",
                "message": "Market closed - no trading executed",
                "test_mode": test_mode,
                "data": result
            }
        
        summary = result.get("summary", {})
        buy_count = summary.get("buy_count", 0)
        sell_count = summary.get("sell_count", 0)
        
        logger.info(f"✅ KR Intraday Cycle 완료 (매수: {buy_count}, 매도: {sell_count})")
        return {
            "status": "success",
            "message": "KR intraday cycle executed",
            "test_mode": test_mode,
            "data": result
        }
    except Exception as e:
        logger.error(f"❌ KR Intraday Cycle 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/us/intraday")
async def run_us_intraday(test_mode: bool = False, db: Session = Depends(get_db)):
    """
    미국 시장 장중 사이클 실행
    
    - 손익&계좌 동기화 (자동)
    - 5/10분 주기로 단타·리밸런싱·손절 수행
    - 유동성 상위면 5분, 아니면 10분 주기
    
    Args:
        test_mode: True일 경우 KIS 주문 API 호출 없이 dry-run (기본값: False)
    
    Returns:
        생성된 주문 플랜 정보 (buy_plans, sell_plans, skipped, summary 포함)
    """
    try:
        logger.info(f"🚀 US Intraday Cycle 시작 (TEST_MODE={test_mode})")
        result = await runbooks.run_us_intraday(db, test_mode=test_mode)
        
        # ✅ 실제 작업 검증
        if not result:
            logger.error("❌ No result returned from runbook")
            raise RuntimeError("US intraday cycle returned empty result")
        
        # 시장 휴장인 경우는 정상 처리
        if result.get("message") == "시장 휴장":
            logger.info("⚠️ US 시장 휴장 - 스킵")
            return {
                "status": "skipped",
                "message": "Market closed - no trading executed",
                "test_mode": test_mode,
                "data": result
            }
        
        summary = result.get("summary", {})
        buy_count = summary.get("buy_count", 0)
        sell_count = summary.get("sell_count", 0)
        
        logger.info(f"✅ US Intraday Cycle 완료 (매수: {buy_count}, 매도: {sell_count})")
        return {
            "status": "success",
            "message": "US intraday cycle executed",
            "test_mode": test_mode,
            "data": result
        }
    except Exception as e:
        logger.error(f"❌ US Intraday Cycle 실패: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

