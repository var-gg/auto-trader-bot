# app/features/trading_hybrid/repositories/order_repository.py
from __future__ import annotations
from typing import Dict, Any, List, Set, Optional, Tuple
from sqlalchemy import text
import json
import logging

logger = logging.getLogger(__name__)

# =========================
# Caps / Batch / Plan / Submit
# =========================
def compute_bucket_caps(buying_power: float, total_equity: float,
                        swing_ratio: float, intraday_ratio: float, cash_buffer_ratio: float) -> Dict[str, float]:
    """
    버킷 캡 계산 (매수력 기준)
    """
    swing_cap_cash = float(buying_power) * float(swing_ratio)
    intraday_cap_cash = float(buying_power) * float(intraday_ratio)
    
    return {
        "swing_ratio": float(swing_ratio),
        "intraday_ratio": float(intraday_ratio),
        "cash_buffer_ratio": float(cash_buffer_ratio),
        "bp": float(buying_power),
        "equity": float(total_equity),
        "swing_cap_cash": swing_cap_cash,  # 🆕 시간외 주문용
        "intraday_cap_cash": intraday_cap_cash,  # 🆕 장중용
    }


def create_order_batch(db, asof_kst, mode: str, currency: str, meta: Dict[str, Any]) -> int:
    """
    주문 배치 생성 (trading.order_batch INSERT)
    """
    notes = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
    sql = """
    INSERT INTO trading.order_batch(asof_kst, mode, currency, available_cash, notes)
    VALUES (:asof_kst, :mode, :currency, 0, :notes)
    RETURNING id
    """
    return db.execute(text(sql), {"asof_kst": asof_kst, "mode": mode, "currency": currency, "notes": notes}).scalar()


def create_plan_with_legs(db, batch_id: int, plan: Dict[str, Any], action: str, test_mode: bool = False) -> int:
    """
    주문 플랜 및 레그 생성 (plan + legs) 및 레그 즉시 전송
    """
    ticker_id = plan.get("ticker_id")
    if not ticker_id:
        symbol_hint = plan.get("symbol") or plan.get("reference", {}).get("symbol") or "UNKNOWN"
        raise ValueError(f"ticker_id required for order_plan insert (symbol={symbol_hint}, action={action})")

    sql_p = """
    INSERT INTO trading.order_plan(batch_id, ticker_id, symbol, action, recommendation_id, note, reverse_breach_day, decision)
    VALUES (:batch_id, :ticker_id, (SELECT symbol FROM trading.ticker WHERE id=:ticker_id),
            :action, :rid, :note, :rbd, 'EXECUTE') RETURNING id
    """
    rid = plan.get("reference", {}).get("recommendation_id")
    rbd = plan.get("reverse_breach_day")  # ✅ 역돌파 일자 (pm_best_signal 기반)
    pid = db.execute(text(sql_p), {
        "batch_id": batch_id, "ticker_id": plan["ticker_id"], "action": action,
        "rid": rid, "note": plan.get("note", ""), "rbd": rbd
    }).scalar()

    for i, leg in enumerate(plan["legs"], 1):
        sql_l = """
        INSERT INTO trading.order_leg(plan_id, "type", side, quantity, limit_price)
        VALUES (:pid, :type, :side, :qty, :price)
        RETURNING id
        """
        # ⚠️ MARKET 주문은 limit_price가 None
        limit_price = float(leg["limit_price"]) if leg.get("limit_price") is not None else None
        
        leg_id = db.execute(text(sql_l), {
            "pid": pid, "type": leg["type"], "side": leg["side"],
            "qty": int(leg["quantity"]), "price": limit_price
        }).scalar()
        
        # 로그 출력
        if limit_price is not None:
            logger.info(f"      📍 레그{i}: leg_id={leg_id}, {leg['side']} {leg['quantity']}주 @ {limit_price:.2f}")
        else:
            logger.info(f"      📍 레그{i}: leg_id={leg_id}, {leg['side']} {leg['quantity']}주 @ 시장가")
        
        _submit_leg_to_broker(db, leg_id, test_mode=test_mode)

    return pid


def submit_to_broker(db, plan_id: int, test_mode: bool = False) -> None:
    """
    (호환 유지) plan_id의 모든 legs를 브로커에 제출.
    """
    legs = db.execute(text("""
        SELECT id FROM trading.order_leg WHERE plan_id = :pid ORDER BY id
    """), {"pid": plan_id}).fetchall()
    for r in legs:
        _submit_leg_to_broker(db, r._mapping["id"], test_mode=test_mode)


# =========================
# 브로커 연동 — 공통 유틸
# =========================
def _load_leg_context(db, leg_id: int) -> Optional[Dict[str, Any]]:
    """
    레그 컨텍스트 조회 (레그/플랜/티커/브로커주문)
    """
    row = db.execute(text("""
        WITH bo_last AS (
            SELECT DISTINCT ON (leg_id) id, leg_id, order_number, status, submitted_at
              FROM trading.broker_order
             WHERE leg_id = :leg_id
             ORDER BY leg_id, submitted_at DESC, id DESC
        )
        SELECT
            ol.id            AS leg_id,
            ol.type          AS leg_type,
            ol.side          AS side,
            ol.quantity      AS quantity,
            ol.limit_price   AS limit_price,
            op.id            AS plan_id,
            op.ticker_id     AS ticker_id,
            op.action        AS plan_action,
            op.symbol        AS symbol,
            t.exchange       AS exchange,
            t.country        AS country,
            bo.id            AS broker_order_id,
            bo.order_number  AS broker_order_no,
            bo.status        AS broker_status
        FROM trading.order_leg ol
        JOIN trading.order_plan op ON ol.plan_id = op.id
        JOIN trading.ticker     t  ON op.ticker_id = t.id
        LEFT JOIN bo_last bo       ON bo.leg_id = ol.id
        WHERE ol.id = :leg_id
    """), {"leg_id": leg_id}).fetchone()

    return dict(row._mapping) if row else None


def _kis_client_or_none(db):
    try:
        from app.core.kis_client import KISClient
        return KISClient(db)
    except Exception as e:
        logger.warning(f"KISClient unavailable: {e}")
        return None


def extract_reject_reason(response: Optional[Dict[str, Any]], error: Any = None) -> Tuple[Optional[str], Optional[str]]:
    """브로커 거부 사유 코드/메시지 추출 (응답 포맷 편차 대응)."""
    res = response or {}

    if res.get("rt_cd") == "0" and not res.get("test_mode"):
        return None, None

    output = res.get("output") if isinstance(res.get("output"), dict) else {}

    code = (
        res.get("msg_cd")
        or res.get("error_code")
        or output.get("msg_cd")
        or output.get("error_code")
        or ("TEST_MODE" if res.get("test_mode") else None)
        or ("EXCEPTION" if error is not None else None)
        or (res.get("rt_cd") if res.get("rt_cd") not in (None, "", "0") else None)
        or "UNKNOWN_REJECT"
    )

    msg = (
        res.get("msg1")
        or res.get("error")
        or res.get("message")
        or res.get("detail")
        or output.get("msg1")
        or output.get("error")
        or (str(error) if error is not None else None)
        or "Broker rejected order (no reason provided)"
    )

    return str(code), str(msg)


def _resolve_pm_run_id(db, ticker_id: Optional[int]) -> Optional[int]:
    """PM 실행 run_id를 ticker 기준으로 최근 히스토리에서 안전하게 추정한다."""
    if not ticker_id:
        return None

    row = db.execute(text("""
        SELECT run_id
          FROM trading.pm_candidate_decision_history
         WHERE ticker_id = :ticker_id
         ORDER BY created_at DESC, run_id DESC
         LIMIT 1
    """), {"ticker_id": ticker_id}).fetchone()
    return int(row._mapping["run_id"]) if row else None


def _insert_pm_order_execution_history(
    db,
    *,
    run_id: Optional[int],
    ctx: Dict[str, Any],
    status: str,
    broker_no: Optional[str],
    response: Optional[Dict[str, Any]],
    reject_code: Optional[str],
    reject_message: Optional[str],
) -> None:
    """pm_order_execution_history append (run_id 미해결 시 skip)."""
    if run_id is None:
        return

    side = str(ctx.get("side") or "").upper()
    action_code = "BUY" if side == "BUY" else "REDUCE"

    intended_limit_price = float(ctx["limit_price"]) if ctx.get("limit_price") is not None else None
    submitted_price = intended_limit_price

    unfilled_reason_code = None
    unfilled_reason_text = None
    if status in ("REJECTED", "UNFILLED"):
        unfilled_reason_code = reject_code
        unfilled_reason_text = reject_message

    db.execute(text("""
        INSERT INTO trading.pm_order_execution_history
        (
            run_id, ticker_id, symbol, action_code, order_outcome_code,
            order_id, order_type, intent_qty, intent_price,
            filled_qty, avg_fill_price, slippage_bps,
            error_code, error_message,
            intended_limit_price, submitted_price,
            unfilled_reason_code, unfilled_reason_text,
            executed_at
        )
        VALUES
        (
            :run_id, :ticker_id, :symbol, :action_code, :order_outcome_code,
            :order_id, :order_type, :intent_qty, :intent_price,
            NULL, NULL, NULL,
            :error_code, :error_message,
            :intended_limit_price, :submitted_price,
            :unfilled_reason_code, :unfilled_reason_text,
            NOW()
        )
    """), {
        "run_id": run_id,
        "ticker_id": int(ctx["ticker_id"]),
        "symbol": ctx.get("symbol"),
        "action_code": action_code,
        "order_outcome_code": status,
        "order_id": broker_no,
        "order_type": str(ctx.get("leg_type") or "").upper() or None,
        "intent_qty": float(ctx.get("quantity") or 0),
        "intent_price": intended_limit_price,
        "error_code": reject_code if status in ("REJECTED", "UNFILLED") else None,
        "error_message": reject_message if status in ("REJECTED", "UNFILLED") else None,
        "intended_limit_price": intended_limit_price,
        "submitted_price": submitted_price,
        "unfilled_reason_code": unfilled_reason_code,
        "unfilled_reason_text": unfilled_reason_text,
    })


def _submit_leg_to_broker(db, leg_id: int, test_mode: bool = False) -> None:
    """
    단일 레그를 브로커에 제출하고 결과 저장
    
    Args:
        db: DB 세션
        leg_id: 레그 ID
        test_mode: True일 경우 KIS API 호출 skip (dry-run)
    """
    ctx = _load_leg_context(db, leg_id)
    if not ctx:
        logger.error(f"❌ 레그 컨텍스트 조회 실패: leg_id={leg_id}")
        return
    
    symbol = ctx.get("symbol", "UNKNOWN")
    logger.info(f"        📤 [{symbol}] 브로커 제출: leg_id={leg_id}, {ctx['side']} {ctx['quantity']}주 @ {ctx.get('limit_price', 0):.2f} (TEST_MODE={test_mode})")

    kis = _kis_client_or_none(db)
    response = None
    status = "REJECTED"
    broker_no = None
    caught_error = None

    try:
        # TEST_MODE일 경우 KIS API 호출 skip
        if test_mode:
            logger.info(f"        🧪 [{symbol}] TEST_MODE: KIS API 호출 스킵 (dry-run)")
            status = "REJECTED"  # DB 제약 조건에 맞춤
            broker_no = f"TEST_{leg_id}"
            response = {"test_mode": True, "message": "KIS API call skipped in test mode"}
        else:
            if not kis:
                raise RuntimeError("KIS client not initialized")

            from app.core import config as settings
            cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
            acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD

            if ctx["country"] == "KR":
                # 주문 타입 매핑
                leg_type = ctx["leg_type"]
                if leg_type in ["AFTER_HOURS_06", "AFTER_HOURS_07"]:
                    # 시간외 주문: 대체거래소(NXT)에 지정가(00)로 전송
                    ord_dvsn = "00"  # ✅ 지정가
                    ord_unpr = str(int(ctx["limit_price"])) if ctx.get("limit_price") else "0"
                    excg_id = "NXT"  # ✅ 시간외는 대체거래소
                elif leg_type == "LIMIT":
                    ord_dvsn = "00"  # 지정가
                    ord_unpr = str(int(ctx["limit_price"]))
                    excg_id = "KRX"  # 정규장은 한국거래소
                else:  # MARKET
                    ord_dvsn = "01"  # 시장가
                    ord_unpr = "0"
                    excg_id = "KRX"  # 정규장은 한국거래소
                
                if ctx["side"] == "BUY":
                    response = kis.order_cash_buy(
                        CANO=cano, ACNT_PRDT_CD=acnt_prdt_cd, PDNO=ctx["symbol"],
                        ORD_DVSN=ord_dvsn, ORD_QTY=str(ctx["quantity"]),
                        ORD_UNPR=ord_unpr, EXCG_ID_DVSN_CD=excg_id
                    )
                else:
                    response = kis.order_cash_sell(
                        CANO=cano, ACNT_PRDT_CD=acnt_prdt_cd, PDNO=ctx["symbol"],
                        ORD_DVSN=ord_dvsn, ORD_QTY=str(ctx["quantity"]),
                        ORD_UNPR=ord_unpr, EXCG_ID_DVSN_CD=excg_id
                    )
            else:
                from app.core.config import KIS_OVERSEAS_EXCHANGE_MAP
                exch = KIS_OVERSEAS_EXCHANGE_MAP.get(ctx["exchange"], "NAS")
                response = kis.order_stock(
                    order_type="buy" if ctx["side"] == "BUY" else "sell",
                    symbol=ctx["symbol"],
                    quantity=str(ctx["quantity"]),
                    price=str(ctx["limit_price"]) if ctx["leg_type"] == "LIMIT" else None,
                    order_method="LIMIT" if ctx["leg_type"] == "LIMIT" else "MARKET",
                    exchange=exch
                )

            if response:
                broker_no = (response.get("output", {}) or {}).get("ODNO") or \
                            (response.get("output", {}) or {}).get("KRX_FWDG_ORD_ORGNO")
                status = "SUBMITTED" if response.get("rt_cd") == "0" else "REJECTED"
                
                if status == "SUBMITTED":
                    logger.info(f"        ✅ [{symbol}] 주문 제출 성공: broker_order_no={broker_no}")
                else:
                    logger.error(f"        ❌ [{symbol}] 주문 거부: {response.get('msg1', 'unknown')}")

    except Exception as e:
        caught_error = e
        response = response or {"error": str(e), "msg_cd": "EXCEPTION", "msg1": str(e)}
        status = "REJECTED"
        logger.error(f"        ❌ [{symbol}] 주문 제출 실패: {str(e)}")

    reject_code, reject_message = (None, None)
    if status == "REJECTED":
        reject_code, reject_message = extract_reject_reason(response, caught_error)

    broker_no_effective = broker_no or f"TEMP_{leg_id}"

    db.execute(text("""
        INSERT INTO trading.broker_order(leg_id, payload, status, submitted_at, order_number, reject_code, reject_message)
        VALUES (:leg_id, :payload, :status, NOW(), :ord_no, :reject_code, :reject_message)
    """), {
        "leg_id": leg_id,
        "payload": json.dumps(response or {}, ensure_ascii=False),
        "status": status,
        "ord_no": broker_no_effective,
        "reject_code": reject_code,
        "reject_message": reject_message,
    })

    pm_run_id = _resolve_pm_run_id(db, ctx.get("ticker_id"))
    _insert_pm_order_execution_history(
        db,
        run_id=pm_run_id,
        ctx=ctx,
        status=status,
        broker_no=broker_no_effective,
        response=response,
        reject_code=reject_code,
        reject_message=reject_message,
    )

    db.commit()

    logger.debug(f"        💾 broker_order 저장: leg_id={leg_id}, status={status}")


# =========================
# 리밸런싱/취소/교체
# =========================
def get_pending_buy_legs_by_symbol(db, symbol: str | None, market: str):
    """
    미체결 BUY 레그 조회 (특정 심볼 또는 전체)
    레그별 최신 SUBMITTED 주문 1건만 조회 (중복 정정 방지)
    ⚠️ 24시간 이내 제출된 주문만
    ⚠️ UNFILLED 상태도 포함 (부분 체결)
    ⚠️ country 필터링 적용
    
    Args:
        db: DB 세션
        symbol: 티커 심볼 (None이면 전체 조회)
        market: "KR" 또는 "US"
    """
    country = "KR" if market == "KR" else "US"
    
    symbol_filter = "AND op.symbol = :symbol" if symbol else ""
    sql = f"""
    WITH bo_last AS (
        SELECT DISTINCT ON (leg_id)
               id, leg_id, order_number, status, submitted_at
          FROM trading.broker_order
         WHERE status = 'SUBMITTED'
           AND submitted_at >= NOW() - INTERVAL '24 hours'
         ORDER BY leg_id, submitted_at DESC, id DESC
    )
    SELECT bl.id AS broker_order_id, ol.id AS leg_id, ol.plan_id,
           op.ticker_id, op.symbol, ol.side, ol.quantity, ol.limit_price,
           t.exchange, t.country, bl.order_number AS broker_order_no
      FROM bo_last bl
      JOIN trading.order_leg  ol ON bl.leg_id = ol.id
      JOIN trading.order_plan op ON ol.plan_id = op.id
      JOIN trading.ticker     t  ON op.ticker_id = t.id
 LEFT JOIN trading.order_fill of ON of.broker_order_id = bl.id
     WHERE (of.id IS NULL OR of.fill_status = 'UNFILLED')
       AND ol.side = 'BUY'
       AND t.country = :country
       {symbol_filter}
    """
    params = {"country": country}
    if symbol:
        params["symbol"] = symbol
    return [r._mapping for r in db.execute(text(sql), params).fetchall()]


def get_pending_sell_legs_by_symbol(db, symbol: str | None, market: str):
    """
    미체결 SELL 레그 조회 (특정 심볼 또는 전체)
    레그별 최신 SUBMITTED 주문 1건만 조회 (중복 정정 방지)
    ⚠️ 24시간 이내 제출된 주문만
    ⚠️ UNFILLED 상태도 포함 (부분 체결)
    ⚠️ country 필터링 적용
    
    Args:
        db: DB 세션
        symbol: 티커 심볼 (None이면 전체 조회)
        market: "KR" 또는 "US"
    """
    country = "KR" if market == "KR" else "US"
    
    symbol_filter = "AND op.symbol = :symbol" if symbol else ""
    sql = f"""
    WITH bo_last AS (
        SELECT DISTINCT ON (leg_id)
               id, leg_id, order_number, status, submitted_at
          FROM trading.broker_order
         WHERE status = 'SUBMITTED'
           AND submitted_at >= NOW() - INTERVAL '24 hours'
         ORDER BY leg_id, submitted_at DESC, id DESC
    )
    SELECT bl.id AS broker_order_id, ol.id AS leg_id, ol.plan_id,
           op.ticker_id, op.symbol, ol.side, ol.quantity, ol.limit_price,
           t.exchange, t.country, bl.order_number AS broker_order_no
      FROM bo_last bl
      JOIN trading.order_leg  ol ON bl.leg_id = ol.id
      JOIN trading.order_plan op ON ol.plan_id = op.id
      JOIN trading.ticker     t  ON op.ticker_id = t.id
 LEFT JOIN trading.order_fill of ON of.broker_order_id = bl.id
     WHERE (of.id IS NULL OR of.fill_status = 'UNFILLED')
       AND ol.side = 'SELL'
       AND t.country = :country
       {symbol_filter}
    """
    params = {"country": country}
    if symbol:
        params["symbol"] = symbol
    return [r._mapping for r in db.execute(text(sql), params).fetchall()]


def _broker_cancel(db, ctx: Dict[str, Any], test_mode: bool = False) -> Tuple[bool, str]:
    """
    실제 브로커 취소 (기존 구현된 API 사용)
    
    Args:
        db: DB 세션
        ctx: 레그 컨텍스트
        test_mode: True일 경우 KIS API 호출 skip (dry-run)
    """
    from app.core import config as settings
    
    order_no = ctx.get("broker_order_no")
    symbol = ctx.get("symbol")
    exch = ctx.get("exchange")
    
    # TEST_MODE일 경우 KIS API 호출 skip
    if test_mode:
        logger.info(f"🧪 [{symbol}] TEST_MODE: KIS 취소 API 호출 스킵 (dry-run)")
        return True, "test_mode_skipped"
    
    if not order_no or order_no.startswith("TEMP_") or order_no.startswith("TEST_"):
        return False, "temp_or_test_order_number"
    
    kis = _kis_client_or_none(db)
    if not kis:
        return False, "kis_client_unavailable"

    try:
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD
        
        if ctx["country"] == "KR":
            res = kis.domestic_order_revise_test(
                CANO=cano,
                ACNT_PRDT_CD=acnt_prdt_cd,
                KRX_FWDG_ORD_ORGNO="",
                ORGN_ODNO=order_no,
                ORD_DVSN="00",
                RVSE_CNCL_DVSN_CD="02",
                ORD_QTY="0",
                ORD_UNPR="0",
                QTY_ALL_ORD_YN="Y"
            )
        else:
            from app.core.config import KIS_OVERSEAS_EXCHANGE_MAP
            exch_code = KIS_OVERSEAS_EXCHANGE_MAP.get(exch, "NAS")
            res = kis.overseas_order_revise_test(
                CANO=cano,
                ACNT_PRDT_CD=acnt_prdt_cd,
                OVRS_EXCG_CD=exch_code,
                PDNO=symbol,
                ORGN_ODNO=order_no,
                RVSE_CNCL_DVSN_CD="02",
                ORD_QTY="0",
                OVRS_ORD_UNPR="0"
            )

        ok = bool(res) and (res.get("rt_cd") == "0")
        msg = (res.get("msg1") if res else "no_response") if not ok else "ok"
        return ok, msg
        
    except Exception as e:
        logger.error(f"브로커 취소 실패: {str(e)}")
        return False, f"exception:{e}"


def _broker_revise_price_with_response(db, ctx: Dict[str, Any], new_limit_price: float, test_mode: bool = False) -> Tuple[bool, str, Dict[str, Any]]:
    """
    브로커 주문 정정 (가격만 수정, 수량/주문번호 유지) + 응답 반환
    
    Args:
        db: DB 세션
        ctx: 레그 컨텍스트
        new_limit_price: 새로운 지정가
        test_mode: True일 경우 KIS API 호출 skip (dry-run)
    
    Returns:
        (성공여부, 메시지, 브로커 응답)
    """
    from app.core import config as settings
    
    order_no = ctx.get("broker_order_no")
    symbol = ctx.get("symbol")
    exch = ctx.get("exchange")
    qty = ctx.get("quantity")
    
    # TEST_MODE일 경우 KIS API 호출 skip
    if test_mode:
        logger.info(f"🧪 [{symbol}] TEST_MODE: KIS 정정 API 호출 스킵 (dry-run, new_price={new_limit_price:.4f})")
        return True, "test_mode_skipped", {"test_mode": True, "message": "KIS revise API call skipped in test mode"}
    
    if not order_no or order_no.startswith("TEMP_") or order_no.startswith("TEST_"):
        return False, "temp_or_test_order_number", {}
    
    kis = _kis_client_or_none(db)
    if not kis:
        return False, "kis_client_unavailable", {}

    try:
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD
        
        if ctx["country"] == "KR":
            # 국내주식 정정 (RVSE_CNCL_DVSN_CD="01")
            res = kis.domestic_order_revise_test(
                CANO=cano,
                ACNT_PRDT_CD=acnt_prdt_cd,
                KRX_FWDG_ORD_ORGNO="",
                ORGN_ODNO=order_no,
                ORD_DVSN="00",  # 지정가
                RVSE_CNCL_DVSN_CD="01",  # 정정
                ORD_QTY=str(qty),  # 수량 유지
                ORD_UNPR=str(int(new_limit_price)),
                QTY_ALL_ORD_YN="N"
            )
        else:
            # 해외주식 정정 (RVSE_CNCL_DVSN_CD="01")
            from app.core.config import KIS_OVERSEAS_EXCHANGE_MAP
            exch_code = KIS_OVERSEAS_EXCHANGE_MAP.get(exch, "NAS")
            res = kis.overseas_order_revise_test(
                CANO=cano,
                ACNT_PRDT_CD=acnt_prdt_cd,
                OVRS_EXCG_CD=exch_code,
                PDNO=symbol,
                ORGN_ODNO=order_no,
                RVSE_CNCL_DVSN_CD="01",  # 정정
                ORD_QTY=str(qty),  # 수량 유지
                OVRS_ORD_UNPR=str(new_limit_price)
            )

        ok = bool(res) and (res.get("rt_cd") == "0")
        msg = (res.get("msg1") if res else "no_response") if not ok else "ok"
        
        if ok:
            logger.info(f"        ✅ [{symbol}] 주문 정정 성공: {ctx.get('limit_price', 0):.4f} → {new_limit_price:.4f}")
        else:
            logger.error(f"        ❌ [{symbol}] 주문 정정 실패: {msg}")
        
        return ok, msg, res or {}
        
    except Exception as e:
        logger.error(f"브로커 정정 실패: {str(e)}")
        return False, f"exception:{e}", {}


def cancel_leg_and_log(db, leg_id: int, reason: str, test_mode: bool = False):
    """
    레그 취소 및 로그 기록
    
    Args:
        db: DB 세션
        leg_id: 레그 ID
        reason: 취소 사유
        test_mode: True일 경우 KIS API 호출 skip (dry-run)
    """
    ctx = _load_leg_context(db, leg_id)
    if not ctx:
        logger.error(f"cancel_leg_and_log: leg not found id={leg_id}")
        return

    ok, msg = _broker_cancel(db, ctx, test_mode=test_mode)

    if ctx.get("broker_order_id"):
        db.execute(text("""
            UPDATE trading.broker_order
               SET status = 'CANCELLED', completed_at = NOW(), reject_message = :msg
             WHERE id = :bo_id
        """), {"bo_id": ctx["broker_order_id"], "msg": msg})
    create_leg_action_log(db, leg_id, ctx.get("symbol"), "CANCEL", f"{reason} broker={msg}")
    db.commit()


def replace_leg_price(db, leg_id: int, new_limit_price: float, test_mode: bool = False):
    """
    레그 가격 수정 (브로커 정정 API 사용, race condition 방지)
    
    기존 "취소→신규" 방식의 문제점:
    - 취소 요청 후 브로커 처리 완료 전에 신규 주문이 제출되면 "주문가능수량 부족" 에러 발생
    - 특히 SELL 주문의 경우 보유수량 제약으로 인해 동일 수량의 중복 주문 불가
    
    개선된 "브로커 정정" 방식:
    - 기존 주문번호를 유지한 채 가격만 수정
    - 브로커 시스템 내부에서 잔고/수량 검증 없이 즉시 처리
    - race condition 발생 가능성 제거
    
    Args:
        db: DB 세션
        leg_id: 레그 ID
        new_limit_price: 새로운 가격
        test_mode: True일 경우 KIS API 호출 skip (dry-run)
    """
    ctx = _load_leg_context(db, leg_id)
    if not ctx:
        logger.error(f"replace_leg_price: leg not found id={leg_id}")
        return
    
    symbol = ctx.get("symbol", "UNKNOWN")
    old_price = ctx.get("limit_price", 0)
    broker_order_no = ctx.get("broker_order_no", "UNKNOWN")
    
    logger.info(f"        🔄 [{symbol}] 가격 정정 시도: {old_price:.4f} → {new_limit_price:.4f} (leg_id={leg_id}, ord_no={broker_order_no})")

    # 브로커 정정 API 호출 (내부에서 response 반환)
    ok, msg, response = _broker_revise_price_with_response(db, ctx, new_limit_price, test_mode=test_mode)
    
    # 🟢 정정 성공 시 응답에서 새 주문번호 추출 (없으면 기존 번호 유지)
    out = (response or {}).get("output", {}) if response else {}
    new_order_no = out.get("ODNO") or out.get("KRX_FWDG_ORD_ORGNO") or broker_order_no
    
    # 브로커 정정 이력을 broker_order 테이블에 저장
    status = "SUBMITTED" if ok else "REJECTED"
    reject_code, reject_message = (None, None)
    if status == "REJECTED":
        reject_code, reject_message = extract_reject_reason(response)

    row = db.execute(text("""
        INSERT INTO trading.broker_order(leg_id, payload, status, submitted_at, order_number, reject_code, reject_message)
        VALUES (:leg_id, :payload, :status, NOW(), :ord_no, :reject_code, :reject_message)
        RETURNING id
    """), {
        "leg_id": leg_id,
        "payload": json.dumps(response or {"revise_msg": msg}, ensure_ascii=False),
        "status": status,
        "ord_no": new_order_no,
        "reject_code": reject_code,
        "reject_message": reject_message,
    })
    new_bo_id = row.fetchone()[0]
    
    logger.debug(f"        💾 broker_order 저장: leg_id={leg_id}, status={status}, new_order_no={new_order_no}")
    
    if ok:
        # 정정 성공 → DB의 limit_price 업데이트
        db.execute(text("""
            UPDATE trading.order_leg
               SET limit_price = :new_price
             WHERE id = :leg_id
        """), {"leg_id": leg_id, "new_price": float(new_limit_price)})
        
        # 정정 이력 로그
        create_leg_action_log(
            db, leg_id, symbol, "REVISE_PRICE",
            f"old={old_price:.4f} new={new_limit_price:.4f} broker_msg={msg}"
        )
        
        logger.info(f"        ✅ [{symbol}] 가격 정정 완료 및 DB 업데이트")
    else:
        # 정정 실패 → 로그만 기록 (DB 변경 없음)
        create_leg_action_log(
            db, leg_id, symbol, "REVISE_PRICE_FAILED",
            f"old={old_price:.4f} new={new_limit_price:.4f} broker_msg={msg}"
        )
        
        logger.warning(f"        ⚠️ [{symbol}] 가격 정정 실패: {msg}")
    
    db.commit()


def create_leg_action_log(db, leg_id: int, symbol: str | None, action: str, note: str):
    """
    레그 액션 로그 적재.
    """
    db.execute(text("""
        INSERT INTO trading.leg_action_log(leg_id, symbol, action, note, created_at)
        VALUES (:leg_id, :symbol, :action, :note, NOW())
    """), {"leg_id": leg_id, "symbol": symbol, "action": action, "note": note})
    db.commit()


# =========================
# 리스크/조회 보조
# =========================
def get_positions_with_unrealized_loss(db, threshold_pct: float) -> List[Dict[str, Any]]:
    rows = db.execute(text("""
        WITH last_snap AS (
            SELECT snapshot_id
              FROM trading.account_snapshot
             ORDER BY asof_kst DESC
             LIMIT 1
        )
        SELECT symbol, ticker_id, qty, last_price_ccy, avg_cost_ccy,
               unrealized_pnl_ccy, pnl_rate
          FROM trading.position_snapshot
         WHERE snapshot_id = (SELECT snapshot_id FROM last_snap)
           AND pnl_rate <= :th_pct
    """), {"th_pct": threshold_pct * 100}).fetchall()
    return [r._mapping for r in rows]


def upsert_block_symbol(db, symbol: str):
    db.execute(text("""
    INSERT INTO trading.daily_symbol_block(symbol, block_date)
    VALUES (:s, CURRENT_DATE)
    ON CONFLICT (symbol, block_date) DO NOTHING
    """), {"s": symbol})
    db.commit()


def get_blocked_symbols_today(db) -> Set[str]:
    try:
        rows = db.execute(text("""
            SELECT symbol FROM trading.daily_symbol_block
             WHERE block_date = CURRENT_DATE
        """)).fetchall()
        return {r._mapping["symbol"] for r in rows}
    except Exception:
        return set()


def log_cycle_note(db, now_kst, market: str, note: str):
    db.execute(text("SELECT 1"))
