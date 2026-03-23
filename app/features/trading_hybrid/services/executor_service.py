# app/features/trading_hybrid/services/executor_service.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime
from app.features.trading_hybrid.repositories.order_repository import (
    create_order_batch, create_plan_with_legs
)


def _extract_symbol_hint(plan: Dict[str, Any]) -> str:
    return str(
        plan.get("symbol")
        or plan.get("reference", {}).get("symbol")
        or plan.get("reference", {}).get("ticker_symbol")
        or "UNKNOWN"
    )


def _guard_invalid_plan(plans: List[Dict[str, Any]], skipped: List[Dict[str, Any]], action: str, logger) -> List[Dict[str, Any]]:
    """ticker_id 누락 플랜은 DB insert까지 가지 않도록 명시적 skip 처리."""
    valid = []
    for plan in plans:
        ticker_id = plan.get("ticker_id")
        if ticker_id:
            valid.append(plan)
            continue

        symbol_hint = _extract_symbol_hint(plan)
        note = f"[{symbol_hint}] ticker_id 없음 -> {action} plan skip (upstream mapping issue)"
        logger.error(f"⚠️ {note}")
        skipped.append({
            "ticker_id": 0,
            "symbol": symbol_hint,
            "code": "MISSING_TICKER_ID",
            "note": note,
            "action": action,
        })
    return valid


def persist_batch_and_execute(
    db, now_kst: datetime, currency: str,
    buy_plans: List[Dict[str, Any]],
    sell_plans: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
    batch_meta: Dict[str, Any],
    test_mode: bool = False
) -> Dict[str, Any]:
    """
    플랜을 배치로 묶어서 DB 저장 및 브로커 제출
    
    로직:
    1. BUY 플랜이 있으면 BUY 배치 생성
       - create_order_batch → order_batch 테이블에 INSERT
       - 각 플랜마다 create_plan_with_legs → order_plan + order_leg INSERT
       - 각 레그는 즉시 브로커에 제출
    2. SELL 플랜도 동일하게 처리
    3. SKIPPED는 별도 저장 안함 (필요시 decision='SKIP' plan으로 저장 가능)
    
    Args:
        db: DB 세션
        now_kst: 현재 시각 (KST)
        currency: "KRW" 또는 "USD"
        buy_plans: 매수 플랜 리스트
        sell_plans: 매도 플랜 리스트
        skipped: 스킵된 플랜 리스트
        batch_meta: 배치 메타 정보 (phase, market, caps 등)
        test_mode: 테스트 모드 여부
    
    Returns:
        생성된 플랜 요약 정보 (buy_plans, sell_plans, skipped 포함)
    
    주의:
        create_plan_with_legs 내부에서 leg 단위로 즉시 브로커 제출한다.
        (중복 제출 방지 위해 여기서는 submit_to_broker를 호출하지 않음)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    buy_result = []
    sell_result = []

    buy_plans = _guard_invalid_plan(buy_plans, skipped, action="BUY", logger=logger)
    sell_plans = _guard_invalid_plan(sell_plans, skipped, action="SELL", logger=logger)
    
    if buy_plans:
        logger.info(f"💰 BUY 배치 생성: {len(buy_plans)}개 플랜")
        b_id = create_order_batch(db, now_kst, "BUY", currency, batch_meta)
        logger.info(f"  📦 BUY batch_id={b_id}")
        
        for i, p in enumerate(buy_plans, 1):
            logger.info(f"  {i}. ticker_id={p['ticker_id']}, legs={len(p['legs'])}개")
            plan_id = create_plan_with_legs(db, b_id, p, action="BUY", test_mode=test_mode)
            logger.info(f"    ✅ plan_id={plan_id} 생성 및 제출 완료")
            
            buy_result.append({
                "batch_id": b_id,
                "plan_id": plan_id,
                "ticker_id": p["ticker_id"],
                "symbol": p.get("symbol"),
                "legs": p["legs"],
                "note": p.get("note", "")
            })

    if sell_plans:
        logger.info(f"💸 SELL 배치 생성: {len(sell_plans)}개 플랜")
        s_id = create_order_batch(db, now_kst, "SELL", currency, batch_meta)
        logger.info(f"  📦 SELL batch_id={s_id}")
        
        for i, p in enumerate(sell_plans, 1):
            logger.info(f"  {i}. ticker_id={p['ticker_id']}, legs={len(p['legs'])}개")
            plan_id = create_plan_with_legs(db, s_id, p, action="SELL", test_mode=test_mode)
            logger.info(f"    ✅ plan_id={plan_id} 생성 및 제출 완료")
            
            sell_result.append({
                "batch_id": s_id,
                "plan_id": plan_id,
                "ticker_id": p["ticker_id"],
                "symbol": p.get("symbol"),
                "legs": p["legs"],
                "note": p.get("note", "")
            })
    
    # 🆕 래칫 내역 추가 (batch_meta에서 추출)
    ratchet_summary = batch_meta.get("ratchet_summary", {}) or {}
    negative_signal_closed = batch_meta.get("negative_signal_closed") or []
    
    response = {
        "buy_plans": buy_result,
        "sell_plans": sell_result,
        "skipped": skipped,
        "negative_signal_closed": negative_signal_closed,  # 🆕 음수 청산 내역
        "summary": {
            "buy_count": len(buy_result),
            "sell_count": len(sell_result),
            "skip_count": len(skipped),
            "negative_signal_closed_count": len(negative_signal_closed)  # 🆕
        }
    }
    
    # 래칫 내역이 있으면 추가 (장중만)
    if ratchet_summary and any(ratchet_summary.values()):
        response["ratcheted"] = {
            "buy": ratchet_summary.get("buy_ratcheted", []),
            "sell": ratchet_summary.get("sell_ratcheted", []),
            "cancelled": ratchet_summary.get("cancelled", [])
        }
        response["summary"]["ratchet_count"] = (
            len(ratchet_summary.get("buy_ratcheted", [])) +
            len(ratchet_summary.get("sell_ratcheted", [])) +
            len(ratchet_summary.get("cancelled", []))
        )
    
    return response
