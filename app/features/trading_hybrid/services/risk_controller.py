# app/features/trading_hybrid/services/risk_controller.py
from __future__ import annotations
from typing import Dict, Any, List, Set
from datetime import datetime, timedelta, time, timezone, date
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
import json

from app.features.trading_hybrid.policy.tuning import Tuning
from app.features.trading_hybrid.utils.ticks import round_to_tick
from app.features.trading_hybrid.utils.timebars import is_near_close, get_session_times_kst
from app.features.trading_hybrid.repositories.order_repository import (
    get_positions_with_unrealized_loss, create_order_batch,
    create_plan_with_legs, submit_to_broker, get_pending_buy_legs_by_symbol,
    cancel_leg_and_log, get_blocked_symbols_today, upsert_block_symbol,
    get_pending_sell_legs_by_symbol, create_leg_action_log, extract_reject_reason
)
from app.core import config as settings
from app.core.config import KIS_OVERSEAS_EXCHANGE_MAP
from app.core.kis_client import KISClient

logger = logging.getLogger(__name__)

# === 공통 헬퍼 ===
def safe_db_exec(db, func_name: str, func, *args, **kwargs):
    """
    디버깅용 안전 실행 래퍼.
    - 모든 예외에 대해 traceback + rollback.
    - 세션 오염 방지.
    """
    try:
        return func(*args, **kwargs)
    except SQLAlchemyError as e:
        logger.exception(f"💥 SQLAlchemyError in {func_name}: {e}")
        db.rollback()
        raise
    except Exception as e:
        logger.exception(f"💥 Exception in {func_name}: {e}")
        db.rollback()
        raise


# === 세션 시간 계산 ===
def _session_times_kst(market: str, ref: datetime | None = None) -> tuple[datetime, datetime]:
    ref = ref or datetime.now(timezone(timedelta(hours=9)))
    d = ref.date()

    if market == "KR":
        start = datetime.combine(d, time(9, 0), tzinfo=ref.tzinfo)
        end = datetime.combine(d, time(15, 30), tzinfo=ref.tzinfo)
    else:
        is_dst = _is_us_dst(d)
        if is_dst:
            start = datetime.combine(d, time(22, 30), tzinfo=ref.tzinfo)
            end = datetime.combine(d + timedelta(days=1), time(5, 0), tzinfo=ref.tzinfo)
        else:
            start = datetime.combine(d, time(23, 30), tzinfo=ref.tzinfo)
            end = datetime.combine(d + timedelta(days=1), time(6, 0), tzinfo=ref.tzinfo)
    return start, end


def _is_us_dst(check_date: date) -> bool:
    year = check_date.year
    # 3월 두 번째 일요일
    march_second_sunday = None
    sunday_count = 0
    for day in range(1, 32):
        try:
            d = date(year, 3, day)
            if d.weekday() == 6:
                sunday_count += 1
                if sunday_count == 2:
                    march_second_sunday = d
                    break
        except ValueError:
            break

    # 11월 첫 번째 일요일
    november_first_sunday = None
    for day in range(1, 32):
        try:
            d = date(year, 11, day)
            if d.weekday() == 6:
                november_first_sunday = d
                break
        except ValueError:
            break

    if march_second_sunday and november_first_sunday:
        return march_second_sunday <= check_date < november_first_sunday
    return 3 <= check_date.month <= 10


# === 리스크 컷 ===
def enforce_intraday_stops(db, market: str, positions: List[Dict[str, Any]], tuning: Tuning, test_mode: bool = False):
    if not positions:
        logger.debug("📭 포지션 없음 → 리스크 컷 스킵")
        return

    session_start, _ = _session_times_kst(market)
    risky: List[Dict[str, Any]] = []

    try:
        for p in positions:
            qty = float(p.get("qty") or 0)
            if qty <= 0:
                continue

            pnl_rate = float(p.get("pnl_rate") or 0.0)
            over_time = (datetime.now(timezone(timedelta(hours=9))) - session_start) >= timedelta(minutes=tuning.TIME_STOP_MINUTES)

            if pnl_rate <= tuning.HARD_STOP_MIN:
                risky.append((p, 0.50))
            elif pnl_rate <= tuning.HARD_STOP_MAX and over_time:
                risky.append((p, 0.30))

        if not risky:
            logger.debug("✅ 리스크 컷 대상 없음")
            return

        batch_id = safe_db_exec(db, "create_order_batch",
            create_order_batch, db, datetime.now(timezone(timedelta(hours=9))), "SELL",
            "KRW" if market == "KR" else "USD",
            {"phase": "RISK_CUT", "market": market})

        for p, frac in risky:
            sym = p["symbol"]
            ticker_id = p.get("ticker_id")
            cur = float(p.get("last_price_ccy") or 0.0)
            if cur <= 0:
                continue

            if not ticker_id:
                logger.error(f"⚠️ 리스크 컷 스킵: ticker_id 없음 symbol={sym} market={market} pnl_rate={p.get('pnl_rate')}")
                continue

            qty = max(1, int(float(p["qty"]) * frac))
            limit = round_to_tick(cur * (1.0 - tuning.RISK_CUT_SLIPPAGE_PCT), market)
            plan = {
                "ticker_id": ticker_id,
                "action": "SELL",
                "reference": {"recommendation_id": None, "breach": "STOP"},
                "note": f"리스크 컷 {qty}주 @ {limit}",
                "legs": [{"type": "LIMIT", "side": "SELL", "quantity": qty, "limit_price": float(limit)}]
            }
            plan_id = safe_db_exec(db, "create_plan_with_legs",
                create_plan_with_legs, db, batch_id, plan, "SELL", test_mode)
            safe_db_exec(db, "submit_to_broker", submit_to_broker, db, plan_id, test_mode)

    except Exception as e:
        logger.exception(f"💥 리스크 컷 처리 중 오류: {e}")
        db.rollback()


# === 마감 전 정리 ===
def near_close_cleanup(db, now_kst: datetime, market: str, cleanup_enabled: bool = True, test_mode: bool = False):
    if not cleanup_enabled:
        return

    try:
        # market 필터링: 현재 market에 해당하는 레그만 처리
        country = "KR" if market == "KR" else "US"
        
        pending_by_sym = {}
        for row in safe_db_exec(db, "get_pending_buy_legs_by_symbol",
                                get_pending_buy_legs_by_symbol, db, None, market):
            # country 필터링
            if row.get("country") == country:
                pending_by_sym.setdefault(row["symbol"], []).append(row)

        for sym, legs in pending_by_sym.items():
            legs_sorted = sorted(legs, key=lambda r: r["leg_id"], reverse=True)
            for leg in legs_sorted[1:]:
                safe_db_exec(db, "cancel_leg_and_log", cancel_leg_and_log, db, leg["leg_id"], reason="NEAR_CLOSE_TRIM", test_mode=test_mode)

        safe_db_exec(db, "_cleanup_today_entries", _cleanup_today_entries, db, market, now_kst, test_mode)

    except Exception as e:
        logger.exception(f"💥 near_close_cleanup 실패: {e}")
        db.rollback()


def _cleanup_today_entries(db, market: str, now_kst: datetime, test_mode: bool = False):
    try:
        sql = """
        SELECT DISTINCT 
            op.symbol,
            op.ticker_id,
            ps.qty,
            ps.last_price_ccy,
            t.exchange,
            t.country
        FROM trading.order_fill of
        JOIN trading.broker_order bo ON of.broker_order_id = bo.id
        JOIN trading.order_leg ol ON bo.leg_id = ol.id
        JOIN trading.order_plan op ON ol.plan_id = op.id
        JOIN trading.ticker t ON op.ticker_id = t.id
        LEFT JOIN (
            SELECT ps.*
            FROM trading.position_snapshot ps
            WHERE ps.snapshot_id = (
                SELECT snapshot_id FROM trading.account_snapshot
                ORDER BY asof_kst DESC LIMIT 1
            )
        ) ps ON ps.symbol = op.symbol
        WHERE ol.side = 'BUY'
          AND DATE(of.filled_at AT TIME ZONE 'Asia/Seoul') = CURRENT_DATE
          AND t.country = :country
          AND ps.qty IS NOT NULL AND ps.qty > 0
        """
        country = "KR" if market == "KR" else "US"
        rows = safe_db_exec(db, "cleanup_today_entries_sql", db.execute, text(sql), {"country": country}).fetchall()

        if not rows:
            logger.info("금일 신규 진입 포지션 없음 (청산 스킵)")
            return

        batch_id = safe_db_exec(db, "create_order_batch",
            create_order_batch, db, now_kst, "SELL",
            "KRW" if market == "KR" else "USD",
            {"phase": "NEAR_CLOSE_CLEANUP", "market": market})

        for row in rows:
            qty = int(float(row.qty) * 0.3)
            if qty <= 0:
                continue
            cur = float(row.last_price_ccy or 0)
            if cur <= 0:
                continue
            limit = round_to_tick(cur * 1.005, market)

            plan = {
                "ticker_id": row.ticker_id,
                "action": "SELL",
                "reference": {"recommendation_id": None, "breach": None},
                "note": f"금일진입 청산 {qty}주 @ {limit}",
                "legs": [{"type": "LIMIT", "side": "SELL", "quantity": qty, "limit_price": float(limit)}]
            }

            safe_db_exec(db, "create_plan_with_legs", create_plan_with_legs, db, batch_id, plan, "SELL", test_mode)
            logger.info(f"금일 진입 청산: {row.symbol} {qty}주")

    except Exception as e:
        logger.exception(f"💥 금일 진입 포지션 청산 중 오류: {e}")
        db.rollback()


# === 시간외 거래 전 펜딩 오더 정리 ===
def cancel_negative_signal_pending_orders(db, market: str) -> Dict[str, Any]:
    """
    시간외 거래 시작 전 역추세 펜딩 오더 취소
    
    취소 조건:
    1. signal_1d < 0 (음수, 하락 예상)
    2. 최신 추천이 SHORT
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
    
    Returns:
        {
            "cancelled_count": int,
            "cancelled_orders": [
                {"symbol": str, "leg_id": int, "reason": str, "note": str},
                ...
            ]
        }
    """
    logger.info(f"🔍 시간외 거래 전 펜딩 오더 정리 시작 (market={market})")
    
    # 1) 펜딩 오더 + signal_1d + 최신 추천 조회 (한 방 쿼리)
    sql = text("""
        WITH latest_recommendations AS (
            SELECT ticker_id, position_type
            FROM trading.analyst_recommendation
            WHERE is_latest = true
        ),
        filled_quantities AS (
            SELECT 
                bo.leg_id,
                COALESCE(SUM(of.fill_qty), 0) AS total_filled
            FROM trading.broker_order bo
            LEFT JOIN trading.order_fill of ON of.broker_order_id = bo.id 
                AND of.fill_status IN ('PARTIAL', 'FULL')
            GROUP BY bo.leg_id
        )
        SELECT DISTINCT
            ol.id AS leg_id,
            ol.plan_id,
            t.symbol,
            t.id AS ticker_id,
            COALESCE(pbs.signal_1d, 0) AS signal_1d,
            lr.position_type,
            bo.order_number AS broker_order_no,
            t.exchange,
            t.country
        FROM trading.order_leg ol
        INNER JOIN trading.order_plan op ON op.id = ol.plan_id
        INNER JOIN trading.ticker t ON t.id = op.ticker_id
        LEFT JOIN trading.pm_best_signal pbs ON pbs.ticker_id = t.id
        LEFT JOIN latest_recommendations lr ON lr.ticker_id = t.id
        INNER JOIN trading.broker_order bo ON bo.leg_id = ol.id
        LEFT JOIN filled_quantities fq ON fq.leg_id = ol.id
        WHERE bo.order_number IS NOT NULL
          AND ol.quantity > COALESCE(fq.total_filled, 0)
          AND ol.created_at >= NOW() - INTERVAL '24 hours'
          AND t.country = :country
          AND (pbs.signal_1d < 0 OR lr.position_type = 'SHORT')
        ORDER BY t.symbol
    """)
    
    country = "KR" if market == "KR" else "US"
    rows = db.execute(sql, {"country": country}).fetchall()
    
    if not rows:
        logger.info(f"✅ 취소 대상 펜딩 오더 없음 (시간외 거래 진행 가능)")
        return {"cancelled_count": 0, "cancelled_orders": []}
    
    logger.info(f"🎯 취소 대상 펜딩 오더: {len(rows)}건")
    
    # 2) KIS 클라이언트 초기화
    try:
        kis = KISClient(db)
        cano = settings.KIS_CANO
        acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD
    except Exception as e:
        logger.error(f"❌ KIS 클라이언트 초기화 실패: {e}")
        return {"cancelled_count": 0, "cancelled_orders": []}
    
    # 3) 취소 실행
    cancelled_orders = []
    cancelled_count = 0
    
    for row in rows:
        leg_id = row.leg_id
        symbol = row.symbol
        signal_1d = float(row.signal_1d)
        position_type = row.position_type
        order_number = row.broker_order_no
        exchange = row.exchange
        
        # 취소 사유 판단
        reasons = []
        if signal_1d < 0:
            reasons.append(f"역추세(sig={signal_1d:+.2f})")
        if position_type and position_type == "SHORT":
            reasons.append("SHORT추천")
        
        reason_str = "+".join(reasons)
        
        if not order_number:
            logger.warning(f"⚠️ [{symbol}] 브로커 주문번호 없음 (leg_id={leg_id}) → DB 취소만")
            cancel_leg_and_log(db, leg_id, symbol, action="CANCEL_NO_BROKER", note=f"시간외정리({reason_str}):브로커번호없음")
            continue
        
        # KIS API 취소 요청
        response = {"error": "initialization"}
        try:
            if market == "KR":
                response = kis.domestic_order_cancel_test(
                    CANO=cano,
                    ACNT_PRDT_CD=acnt_prdt_cd,
                    ORGN_ODNO=order_number,
                    ORD_QTY="0"  # 전량 취소
                )
            else:  # US
                exch_code = KIS_OVERSEAS_EXCHANGE_MAP.get(exchange, "NAS")
                response = kis.overseas_order_cancel_test(
                    CANO=cano,
                    ACNT_PRDT_CD=acnt_prdt_cd,
                    OVRS_EXCG_CD=exch_code,
                    ORGN_ODNO=order_number
                )
        except Exception as e:
            response = {"error": str(e)}
        
        ok = bool(response) and (response.get("rt_cd") == "0" or response.get("test_mode"))
        
        if ok:
            cancelled_count += 1
            note = f"시간외정리({reason_str})"
            cancel_leg_and_log(db, leg_id, symbol, action="AFTER_HOURS_CLEANUP", note=note)
            
            cancelled_orders.append({
                "symbol": symbol,
                "leg_id": leg_id,
                "reason": reason_str,
                "note": note
            })
            
            logger.info(f"✅ [{symbol}] 펜딩 취소 성공: {reason_str} (leg={leg_id})")
        else:
            error_msg = response.get("msg1", response.get("error", "unknown"))
            logger.error(f"❌ [{symbol}] 펜딩 취소 실패: {error_msg} (leg={leg_id})")
    
    db.commit()
    logger.info(f"✅ 펜딩 오더 정리 완료: {cancelled_count}/{len(rows)}건 취소")
    
    return {
        "cancelled_count": cancelled_count,
        "cancelled_orders": cancelled_orders
    }


# === 장마감 직전 음수 신호 포지션 정리 ===
def close_negative_signal_positions(db, now_kst: datetime, market: str, positions: List[Dict[str, Any]], test_mode: bool = False) -> List[Dict[str, Any]]:
    """
    장마감 직전(20분~0분) signal_1d < 0인 포지션의 미체결 매도주문을 체결 유도 주문으로 정정
    
    로직:
    - 장마감 20분~0분 사이인지 체크 (국장/미장 각각 현지시간 기준)
    - signal_1d < 0인 포지션만 필터링
    - 해당 포지션의 미체결 매도 레그를 찾아서:
      * 국장(KR): "02" 조건부지정가로 현재가에 정정 (즉시 체결 유도)
      * 미장(US): 시장가(00)로 정정 (LOC 미지원, 즉시 체결)
    
    Args:
        db: DB 세션
        now_kst: 현재 시각 (KST)
        market: "KR" 또는 "US"
        positions: 포지션 리스트 (signal_1d 포함)
        test_mode: True일 경우 KIS API 호출 skip
    
    Returns:
        정정된 레그 정보 리스트 (응답용)
    """
    # 1. 장마감 20분~0분 체크 (test_mode일 때는 스킵)
    if not test_mode and not is_near_close(market, minutes=20, now_kst=now_kst):
        logger.debug("⏰ 장마감 20분 전이 아님 → 음수 신호 정리 스킵")
        return []
    
    if test_mode:
        logger.info(f"🧪 TEST_MODE: 시간 체크 우회 - signal_1d < 0 포지션 정리 시작 (market={market})")
    else:
        logger.info(f"🔔 장마감 직전 감지: signal_1d < 0 포지션 정리 시작 (market={market})")
    
    # 2. signal_1d < 0인 포지션 필터링
    negative_positions = [
        p for p in positions
        if p.get("signal_1d") is not None and float(p.get("signal_1d")) < 0 and float(p.get("qty", 0)) > 0
    ]
    
    if not negative_positions:
        logger.info("✅ signal_1d < 0인 포지션 없음")
        return []
    
    logger.info(f"📊 signal_1d < 0 포지션: {len(negative_positions)}개")
    
    revised_legs = []  # 응답용 정정 레그 리스트
    
    try:
        # KIS 클라이언트 초기화
        kis = None
        if not test_mode:
            try:
                kis = KISClient(db)
            except Exception as e:
                logger.error(f"KIS 클라이언트 초기화 실패: {e}")
                return []  # 빈 리스트 반환
        
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD
        
        # 3. 각 포지션의 미체결 매도 레그 정리
        revised_count = 0
        skipped_no_legs = 0
        for pos in negative_positions:
            symbol = pos["symbol"]
            signal_1d = pos["signal_1d"]
            cur_price = float(pos.get("last_price_ccy", 0))
            
            if cur_price <= 0:
                logger.warning(f"⚠️ [{symbol}] 현재가 없음 → 스킵")
                continue
            
            # 미체결 매도 레그 조회 (이미 broker_order_no, exchange, country 포함)
            sell_legs = get_pending_sell_legs_by_symbol(db, symbol, market) or []
            
            if not sell_legs:
                logger.info(f"📭 [{symbol}] 미체결 매도 레그 없음 (signal_1d={signal_1d:.3f}) → 스킵")
                skipped_no_legs += 1
                continue
            
            logger.info(f"🎯 [{symbol}] signal_1d={signal_1d:.3f}, 미체결 매도 레그 {len(sell_legs)}개 → 장마감 주문 정정")
            
            # 각 레그별 정정
            for leg in sell_legs:
                leg_id = leg["leg_id"]
                old_price = float(leg.get("limit_price", 0))
                quantity = int(leg.get("quantity", 0))
                order_number = leg.get("broker_order_no")
                exchange = leg.get("exchange")
                
                if not order_number:
                    logger.warning(f"⚠️ [{symbol}] leg_id={leg_id} 주문번호 없음 → 스킵")
                    continue
                
                # 국장/미장별 정정 처리
                new_price_display = None  # 응답용 가격 표시
                
                if market == "KR":
                    # 국장: "02" 조건부지정가로 현재가에 정정
                    new_price_int = int(round(cur_price))
                    new_price_display = new_price_int
                    note = f"역추세청산 sig={signal_1d:+.2f} 02조건부 {quantity}주 {old_price:.0f}→{new_price_int}"
                    
                    if test_mode:
                        logger.info(f"        🧪 [{symbol}] TEST_MODE: {note}")
                        response = {"test_mode": True, "message": "KIS API call skipped"}
                    else:
                        response = {"error": "initialization"}
                        try:
                            response = kis.domestic_order_revise_test(
                                CANO=cano,
                                ACNT_PRDT_CD=acnt_prdt_cd,
                                ORGN_ODNO=order_number,
                                ORD_DVSN="02",  # 조건부지정가
                                RVSE_CNCL_DVSN_CD="01",  # 정정
                                ORD_QTY=str(quantity),
                                ORD_UNPR=str(new_price_int)
                            )
                            logger.info(f"        ✅ [{symbol}] {note}")
                        except Exception as e:
                            logger.error(f"        ❌ [{symbol}] 정정 실패: {e}")
                            response = {"error": str(e)}
                
                else:  # market == "US"
                    # 미장: 현재가로 정정 (즉시 체결 유도)
                    exch_code = KIS_OVERSEAS_EXCHANGE_MAP.get(exchange, "NAS")
                    new_price_display = cur_price
                    note = f"역추세청산 sig={signal_1d:+.2f} LIMIT {quantity}주 {old_price:.2f}→{cur_price:.2f}"
                    
                    if test_mode:
                        logger.info(f"        🧪 [{symbol}] TEST_MODE: {note}")
                        response = {"test_mode": True, "message": "KIS API call skipped"}
                    else:
                        response = {"error": "initialization"}
                        try:
                            response = kis.overseas_order_revise_test(
                                CANO=cano,
                                ACNT_PRDT_CD=acnt_prdt_cd,
                                OVRS_EXCG_CD=exch_code,
                                PDNO=symbol,
                                ORGN_ODNO=order_number,
                                RVSE_CNCL_DVSN_CD="01",  # 정정
                                ORD_QTY=str(quantity),
                                OVRS_ORD_UNPR=str(cur_price),  # 현재가로 정정
                                ORD_SVR_DVSN_CD="0"
                            )
                            logger.info(f"        ✅ [{symbol}] {note}")
                        except Exception as e:
                            logger.error(f"        ❌ [{symbol}] 정정 실패: {e}")
                            response = {"error": str(e)}
                
                # broker_order 이력 저장
                ok = bool(response) and (response.get("rt_cd") == "0" or response.get("test_mode"))
                status = "SUBMITTED" if ok else "REJECTED"
                reject_code, reject_message = (None, None)
                if status == "REJECTED":
                    reject_code, reject_message = extract_reject_reason(response)

                db.execute(text("""
                    INSERT INTO trading.broker_order(leg_id, payload, status, submitted_at, order_number, reject_code, reject_message)
                    VALUES (:leg_id, :payload, :status, NOW(), :ord_no, :reject_code, :reject_message)
                """), {
                    "leg_id": leg_id,
                    "payload": json.dumps(response or {}, ensure_ascii=False),
                    "status": status,
                    "ord_no": order_number,
                    "reject_code": reject_code,
                    "reject_message": reject_message,
                })
                
                # 로그 저장
                create_leg_action_log(db, leg_id, symbol, "CLOSE_NEGATIVE_SIGNAL", note)
                
                if ok:
                    revised_count += 1
                    # 응답용 정보 수집
                    revised_legs.append({
                        "leg_id": leg_id,
                        "symbol": symbol,
                        "signal_1d": signal_1d,
                        "old_price": old_price,
                        "new_price": new_price_display,
                        "quantity": quantity,
                        "order_type": "02조건부" if market == "KR" else "LIMIT현재가",
                        "note": note
                    })
        
        # 모든 처리 완료 후 한 번에 커밋
        db.commit()
        
        # 요약 로그
        logger.info(f"✅ 음수 신호 포지션 정리 완료 - 대상:{len(negative_positions)}개, 정정:{revised_count}건, 매도레그없음:{skipped_no_legs}개")
        
        return revised_legs
        
    except Exception as e:
        logger.exception(f"💥 음수 신호 포지션 정리 중 오류: {e}")
        db.rollback()
        return []


# === 일일 손실 종목 차단 ===
def block_daily_loss_symbols(db, market: str, limit_pct: float = -0.015) -> Set[str]:
    """
    일일 손실이 limit_pct 이하인 종목을 차단
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US" (base_ccy로 필터링)
        limit_pct: 차단 기준 손실률 (기본값: -1.5%)
    
    Returns:
        차단된 심볼 집합
    """
    blocked = set()
    base_ccy = "KRW" if market == "KR" else "USD"

    try:
        # 💡 혹시 이전 aborted 세션 초기화
        db.rollback()

        sql = text("""
            WITH today_snaps AS (
                SELECT ps.ticker_id, ps.symbol, ps.pnl_rate, a.asof_kst,
                       ROW_NUMBER() OVER (PARTITION BY ps.ticker_id ORDER BY a.asof_kst ASC)  AS rn_first,
                       ROW_NUMBER() OVER (PARTITION BY ps.ticker_id ORDER BY a.asof_kst DESC) AS rn_last
                  FROM trading.position_snapshot ps
                  JOIN trading.account_snapshot a ON a.snapshot_id = ps.snapshot_id
                 WHERE DATE(a.asof_kst AT TIME ZONE 'Asia/Seoul') = CURRENT_DATE
                   AND a.base_ccy = :base_ccy
            ),
            firsts AS (SELECT ticker_id, symbol, pnl_rate AS pnl_first FROM today_snaps WHERE rn_first = 1),
            lasts  AS (SELECT ticker_id, symbol, pnl_rate AS pnl_last  FROM today_snaps WHERE rn_last = 1)
            SELECT f.symbol, ((l.pnl_last - f.pnl_first) / 100.0) AS delta_pnl
              FROM firsts f JOIN lasts l ON l.ticker_id = f.ticker_id
        """)
        
        rows = safe_db_exec(
            db, "block_daily_loss_snapshot_query", 
            db.execute, sql, {"base_ccy": base_ccy}
        ).fetchall()

        for r in rows:
            sym = r._mapping["symbol"]
            delta = float(r._mapping["delta_pnl"] or 0.0)
            if delta <= limit_pct:
                safe_db_exec(db, "upsert_block_symbol", upsert_block_symbol, db, sym)
                blocked.add(sym)
    except Exception as e:
        logger.warning(f"⚠️ 스냅샷 기반 차단 실패: {e}")
        # 🔹 rollback 제거 (이미 safe_db_exec가 해줌)
        pass

    try:
        blocked |= safe_db_exec(db, "get_blocked_symbols_today", get_blocked_symbols_today, db)
    except Exception as e:
        logger.warning(f"⚠️ 차단 심볼 병합 실패: {e}")
        # 🔹 rollback 제거 (이미 safe_db_exec가 해줌)
        pass

    return blocked
