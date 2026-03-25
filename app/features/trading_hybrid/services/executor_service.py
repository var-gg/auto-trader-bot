from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from app.features.trading_hybrid.repositories.order_repository import (
    create_order_batch,
    create_plan_with_legs,
    get_plan_execution_correlation,
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


def _correlation_summary(plan_results: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    batch_ids = sorted({str(p["batch_id"]) for p in plan_results if p.get("batch_id") is not None})
    plan_ids = sorted({str(p["plan_id"]) for p in plan_results if p.get("plan_id") is not None})
    broker_request_ids = sorted({str(leg["broker_order_id"]) for p in plan_results for leg in p.get("execution_correlation", []) if leg.get("broker_order_id") is not None})
    broker_response_ids = sorted({str(leg.get("broker_order_no") or leg.get("broker_order_id")) for p in plan_results for leg in p.get("execution_correlation", []) if (leg.get("broker_order_no") or leg.get("broker_order_id")) is not None})
    return {
        "order_batch_ids": batch_ids,
        "order_plan_ids": plan_ids,
        "broker_request_ids": broker_request_ids,
        "broker_response_ids": broker_response_ids,
    }


def persist_batch_and_execute(
    db,
    now_kst: datetime,
    currency: str,
    buy_plans: List[Dict[str, Any]],
    sell_plans: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
    batch_meta: Dict[str, Any],
    test_mode: bool = False,
) -> Dict[str, Any]:
    """플랜을 배치로 묶어서 DB 저장 및 브로커 제출."""
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
            execution_correlation = get_plan_execution_correlation(db, plan_id)

            buy_result.append({
                "batch_id": b_id,
                "plan_id": plan_id,
                "ticker_id": p["ticker_id"],
                "symbol": p.get("symbol"),
                "legs": p["legs"],
                "note": p.get("note", ""),
                "execution_correlation": execution_correlation,
            })

    if sell_plans:
        logger.info(f"💸 SELL 배치 생성: {len(sell_plans)}개 플랜")
        s_id = create_order_batch(db, now_kst, "SELL", currency, batch_meta)
        logger.info(f"  📦 SELL batch_id={s_id}")

        for i, p in enumerate(sell_plans, 1):
            logger.info(f"  {i}. ticker_id={p['ticker_id']}, legs={len(p['legs'])}개")
            plan_id = create_plan_with_legs(db, s_id, p, action="SELL", test_mode=test_mode)
            logger.info(f"    ✅ plan_id={plan_id} 생성 및 제출 완료")
            execution_correlation = get_plan_execution_correlation(db, plan_id)

            sell_result.append({
                "batch_id": s_id,
                "plan_id": plan_id,
                "ticker_id": p["ticker_id"],
                "symbol": p.get("symbol"),
                "legs": p["legs"],
                "note": p.get("note", ""),
                "execution_correlation": execution_correlation,
            })

    ratchet_summary = batch_meta.get("ratchet_summary", {}) or {}
    negative_signal_closed = batch_meta.get("negative_signal_closed") or []
    all_plan_results = [*buy_result, *sell_result]

    response = {
        "buy_plans": buy_result,
        "sell_plans": sell_result,
        "skipped": skipped,
        "negative_signal_closed": negative_signal_closed,
        "correlation": _correlation_summary(all_plan_results),
        "summary": {
            "buy_count": len(buy_result),
            "sell_count": len(sell_result),
            "skip_count": len(skipped),
            "negative_signal_closed_count": len(negative_signal_closed),
        },
    }

    if ratchet_summary and any(ratchet_summary.values()):
        response["ratcheted"] = {
            "buy": ratchet_summary.get("buy_ratcheted", []),
            "sell": ratchet_summary.get("sell_ratcheted", []),
            "cancelled": ratchet_summary.get("cancelled", []),
        }
        response["summary"]["ratchet_count"] = (
            len(ratchet_summary.get("buy_ratcheted", []))
            + len(ratchet_summary.get("sell_ratcheted", []))
            + len(ratchet_summary.get("cancelled", []))
        )

    return response
