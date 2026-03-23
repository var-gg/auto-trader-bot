# app/features/trading_hybrid/services/intraday_session_service.py
from __future__ import annotations
from typing import Dict, Any, List
import logging, numpy as np
import math
from datetime import date
from sqlalchemy import text

from app.features.trading_hybrid.utils.ladder_generator import _qty_from_budget, generate_unified_adaptive_ladder
from app.features.trading_hybrid.utils.ticks import round_to_tick
from app.features.trading_hybrid.policy.tuning import Tuning
from app.features.trading_hybrid.repositories.order_repository import (
    get_pending_buy_legs_by_symbol, get_pending_sell_legs_by_symbol,
    replace_leg_price, cancel_leg_and_log, create_leg_action_log
)

logger = logging.getLogger(__name__)

EARNINGS_DAY_ORDER_MULTIPLIER = 2.0


def _is_earnings_day(db, symbol: str, asof_date: date | None) -> bool:
    if not symbol or asof_date is None:
        return False
    row = db.execute(text("""
        SELECT 1
        FROM trading.earnings_event e
        WHERE e.ticker_symbol = :symbol
          AND :asof_date BETWEEN
              COALESCE(e.confirmed_report_date, e.expected_report_date_start, e.report_date)
              AND COALESCE(e.expected_report_date_end, e.confirmed_report_date, e.expected_report_date_start, e.report_date)
        LIMIT 1
    """), {"symbol": symbol, "asof_date": asof_date}).fetchone()
    return row is not None


def _apply_buy_multiplier_to_legs(legs: List[Dict[str, Any]], current_price: float, market: str, multiplier: float, tag: str) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs
    adjusted = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue
        base_disc = max(0.0, min(0.95, 1.0 - (lp / current_price)))
        new_disc = max(0.0, min(0.95, base_disc * multiplier))
        copied = dict(leg)
        copied["limit_price"] = round_to_tick(current_price * (1.0 - new_disc), market)
        copied["earnings_day"] = True
        copied["earnings_day_buy_multiplier"] = round(multiplier, 4)
        copied[f"{tag}_base_discount"] = round(base_disc, 6)
        copied[f"{tag}_adjusted_discount"] = round(new_disc, 6)
        adjusted.append(copied)
    return adjusted


def _apply_sell_multiplier_to_legs(legs: List[Dict[str, Any]], current_price: float, market: str, multiplier: float, tag: str) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs
    adjusted = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue
        base_markup = max(0.0, min(1.5, (lp / current_price) - 1.0))
        new_markup = max(0.0, min(1.8, base_markup * multiplier))
        copied = dict(leg)
        copied["limit_price"] = round_to_tick(current_price * (1.0 + new_markup), market)
        copied["earnings_day"] = True
        copied["earnings_day_sell_multiplier"] = round(multiplier, 4)
        copied[f"{tag}_base_markup"] = round(base_markup, 6)
        copied[f"{tag}_adjusted_markup"] = round(new_markup, 6)
        adjusted.append(copied)
    return adjusted


def _smart_ratchet_price(old_price: float, cur_price: float, dyn: float, alpha: float = 0.03) -> float:
    """
    Smart Ratchet: 거리 기반 감쇠 적용
    
    - 가까운 레그: 현재가를 더 많이 따라감
    - 먼 레그: 거의 그대로 유지
    
    Args:
        old_price: 기존 지정가
        cur_price: 현재가
        dyn: 최대 조정 비율 (0~1, 예: 0.3 = 30%)
        alpha: 감쇠 계수 (기본 0.03, 작을수록 급격한 감쇠)
    
    Returns:
        조정된 가격
    """
    dist_ratio = abs(cur_price - old_price) / max(cur_price, 1e-6)
    adj_factor = dyn * math.exp(-dist_ratio / alpha)
    new_price = old_price + (cur_price - old_price) * adj_factor
    return float(new_price)

def _pred_to_strength(pred: Dict[str, Any], tuning: Tuning) -> Dict[str, float]:
    """pred(dict) → p_up/p_down/exp_up/exp_down/cur/s 로 통일"""
    cur = float(pred.get("current_price") or 0.0)
    p_up = float(pred.get("p_up", 0.0))
    p_down = float(pred.get("p_down", 0.0))
    exp_up = float(pred.get("exp_up", 0.0))
    exp_down = float(pred.get("exp_down", 0.0))
    # 하위 호환(FALLBACK): dir/prob/exp_move_pct만 있는 경우
    if (p_up == 0.0 and p_down == 0.0) and "prob" in pred and "exp_move_pct" in pred:
        prob = float(pred.get("prob", 0.0))
        mv = float(pred.get("exp_move_pct", 0.0))
        if str(pred.get("dir", "FLAT")).upper() == "UP":
            p_up, p_down, exp_up, exp_down = prob, 1.0 - prob, abs(mv), abs(mv) * 0.7
        elif str(pred.get("dir", "FLAT")).upper() == "DOWN":
            p_up, p_down, exp_up, exp_down = 1.0 - prob, prob, abs(mv) * 0.7, abs(mv)
        else:
            p_up, p_down, exp_up, exp_down = 0.5, 0.5, 0.0, 0.0
    net_strength = (p_up * exp_up) - (p_down * exp_down)
    s = float(np.clip(net_strength / max(tuning.ADAPTIVE_STRENGTH_SCALE, 1e-9), -1.0, 1.0))
    return {"cur": cur, "p_up": p_up, "p_down": p_down, "exp_up": exp_up, "exp_down": exp_down, "s": s}

def plan_intraday_actions(
    now_kst, market: str, currency: str,
    preds: Dict[str, Dict[str, float]],
    account: Dict[str, Any],
    positions: List[Dict[str, Any]],
    pending: List[Dict[str, Any]] | None,
    caps: Dict[str, float],
    tuning: Tuning,
    blocked_symbols=None
):
    """
    장중 단타/리밸런싱 플랜 생성 (원디멘션 s + 통합 사다리)
    - s >= +θ: 상승 우세 → BUY 사다리
    - s <= -θ: 하락 우세 & 보유중 → SELL 사다리(감산)
    """
    buy_plans, sell_plans, skipped = [], [], []
    intraday_cap_cash = float(account.get("buying_power_ccy") or 0.0) * float(caps["intraday_ratio"])
    blocked_symbols = blocked_symbols or set()
    pos_map = {p["symbol"]: p for p in positions}
    asof_date = now_kst.date() if hasattr(now_kst, "date") else None
    earnings_day_cache: Dict[str, bool] = {}

    created = 0
    for sym, p in preds.items():
        meta = _pred_to_strength(p, tuning)
        cur, p_up, p_down, exp_up, exp_down, s = meta["cur"], meta["p_up"], meta["p_down"], meta["exp_up"], meta["exp_down"], meta["s"]
        atr5m = float(p.get("atr5m_pct", 0.0) or 0.0)
        if cur <= 0:
            skipped.append({
                "ticker_id": p.get("ticker_id", 0), 
                "code": "OTHER", 
                "note": f"현재가오류: [{sym}] current_price={cur} (예측 있으나 가격 미상)"
            })
            continue

        abs_s = abs(s)
        if abs_s < tuning.INTRA_MIN_ABS_S:
            direction = "상승" if s > 0 else "하락" if s < 0 else "중립"
            skipped.append({
                "ticker_id": p.get("ticker_id", 0), 
                "code": "HOLD", 
                "note": f"신호약함: s={s:+.2f} (임계값±{tuning.INTRA_MIN_ABS_S:.2f}, {direction}세력, p_up={p_up:.0%}, p_down={p_down:.0%})"
            })
            continue

        # BUY: 상승 우세
        if s > 0:
            if sym in blocked_symbols:
                skipped.append({
                    "ticker_id": p.get("ticker_id", 0), 
                    "code": "RISK", 
                    "note": f"리스크차단: 일중 손실한도 초과로 매수금지 (s={s:+.2f})"
                })
                continue
            # 종목당 예산 → 수량 캡
            per_nameplate = intraday_cap_cash / max(len(preds), 1)
            qty_cap = _qty_from_budget(cur, per_nameplate)
            if qty_cap <= 0:
                skipped.append({
                    "ticker_id": p.get("ticker_id", 0), 
                    "code": "BUDGET", 
                    "note": f"예산부족: 단타예산={intraday_cap_cash:.0f}{currency} / {len(preds)}종목 = {per_nameplate:.0f}{currency}/종목, 현재가={cur:.2f} → 수량0 (s={s:+.2f})"
                })
                continue

            # ATR 힌트를 tuning에 주입(옵션)
            local_tuning = tuning  # 필요 시 dataclasses.replace로 ATR_PCT_HINT 주입 가능

            legs, desc = generate_unified_adaptive_ladder(
                mode="BUY",
                p_up=p_up, p_down=p_down,
                exp_up=exp_up, exp_down=exp_down,
                gain_pct=0.0,
                current_price=cur,
                quantity=int(qty_cap),
                market=market,
                tuning=local_tuning
            )
            
            # ✅ 사다리 생성 함수의 판단을 신뢰 (이중 검증 제거)
            if not legs:
                skipped.append({
                    "ticker_id": p.get("ticker_id", 0), 
                    "code": "NO_LEGS", 
                    "note": f"사다리생성실패: BUY, s={s:+.2f}, p_up={p_up:.0%}×{exp_up:.2%}, 현재가={cur:.2f}, 수량={qty_cap}"
                })
                continue

            earnings_day = earnings_day_cache.get(sym)
            if earnings_day is None:
                earnings_day = _is_earnings_day(db, sym, asof_date)
                earnings_day_cache[sym] = earnings_day
            earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
            if earnings_day:
                legs = _apply_buy_multiplier_to_legs(legs, cur, market, earnings_day_multiplier, "intraday")
                logger.info(f"📆 [{sym}] intraday BUY earnings-day multiplier 적용: x{earnings_day_multiplier:.1f}")

            buy_plans.append({
                "ticker_id": p.get("ticker_id", 0),
                "symbol": sym,
                "action": "BUY",
                "reference": {"recommendation_id": None, "breach": None, "earnings_day": earnings_day, "earnings_day_buy_multiplier": round(earnings_day_multiplier, 4)},
                "note": f"[5m 적응매수] s={s:+.2f}, p_up={p_up:.0%}×{exp_up:.2%}, ATR5m={atr5m:.2%}, earnM={earnings_day_multiplier:.2f} | {desc}",
                "legs": legs
            })
            created += 1
            if created >= tuning.INTRA_MAX_NEW_ORDERS_PER_CYCLE:
                break

        # SELL: 하락 우세 & 보유
        elif s < 0 and sym in pos_map and float(pos_map[sym].get("qty") or 0) > 0:
            cost = float(pos_map[sym].get("avg_cost_ccy") or cur)
            gain_pct = (cur - cost) / max(cost, 1e-6)
            qty_pos = int(float(pos_map[sym].get("orderable_qty") or 0))
            if qty_pos <= 0:
                total_qty = int(float(pos_map[sym].get("qty") or 0))
                skipped.append({
                    "ticker_id": p.get("ticker_id", 0), 
                    "code": "HOLD", 
                    "note": f"주문가능수량없음: 총보유={total_qty}, 주문가능=0, s={s:+.2f} (하락세, 매도불가)"
                })
                continue

            # s 세기에 따라 감산 비율 20~60%
            r_lo, r_hi = 0.20, 0.60
            sell_ratio = float(np.interp(abs_s, [tuning.INTRA_MIN_ABS_S, 1.0], [r_lo, r_hi]))
            sell_qty = max(1, int(qty_pos * sell_ratio))

            legs, desc = generate_unified_adaptive_ladder(
                mode="SELL",
                p_up=p_up, p_down=p_down,
                exp_up=exp_up, exp_down=exp_down,
                gain_pct=gain_pct,
                current_price=cur,
                quantity=int(sell_qty),
                market=market,
                tuning=tuning
            )
            
            # ✅ 사다리 생성 함수의 판단을 신뢰 (이중 검증 제거)
            if not legs:
                skipped.append({
                    "ticker_id": p.get("ticker_id", 0), 
                    "code": "NO_LEGS", 
                    "note": f"사다리생성실패: SELL, s={s:+.2f}, p_down={p_down:.0%}×{exp_down:.2%}, gain={gain_pct:+.1%}, 수량={sell_qty}/{qty_pos}"
                })
                continue

            earnings_day = earnings_day_cache.get(sym)
            if earnings_day is None:
                earnings_day = _is_earnings_day(db, sym, asof_date)
                earnings_day_cache[sym] = earnings_day
            earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
            if earnings_day:
                legs = _apply_sell_multiplier_to_legs(legs, cur, market, earnings_day_multiplier, "intraday")
                logger.info(f"📆 [{sym}] intraday SELL earnings-day multiplier 적용: x{earnings_day_multiplier:.1f}")

            sell_plans.append({
                "ticker_id": p.get("ticker_id", 0),
                "symbol": sym,
                "action": "SELL",
                "reference": {"recommendation_id": None, "breach": None, "earnings_day": earnings_day, "earnings_day_sell_multiplier": round(earnings_day_multiplier, 4)},
                "note": f"[5m 적응매도] s={s:+.2f}, p_down={p_down:.0%}×{exp_down:.2%}, ATR5m={atr5m:.2%}, gain={gain_pct:+.1%}, earnM={earnings_day_multiplier:.2f} | {desc}",
                "legs": legs
            })
            created += 1
            if created >= tuning.INTRA_MAX_NEW_ORDERS_PER_CYCLE:
                break
        
        # 🆕 보유 포지션이 있지만 하락 신호는 아닌 경우 (중립/상승) → 보수적 매도 레그 생성
        elif sym in pos_map and float(pos_map[sym].get("qty") or 0) > 0:
            # 보유 포지션이 있는데 매도 레그가 없으면 생성
            cost = float(pos_map[sym].get("avg_cost_ccy") or cur)
            gain_pct = (cur - cost) / max(cost, 1e-6)
            qty_pos = int(float(pos_map[sym].get("orderable_qty") or 0))
            
            if qty_pos > 0:
                # 보수적 매도: 수익이면 일부 매도, 손실이면 보수
                if gain_pct > 0:
                    sell_ratio = 0.20  # 수익 중: 20%만 매도
                else:
                    sell_ratio = 0.10  # 손실 중: 10%만 매도 (보수)
                
                sell_qty = max(1, int(qty_pos * sell_ratio))
                
                legs, desc = generate_unified_adaptive_ladder(
                    mode="SELL",
                    p_up=p_up, p_down=p_down,
                    exp_up=exp_up, exp_down=exp_down,
                    gain_pct=gain_pct,
                    current_price=cur,
                    quantity=int(sell_qty),
                    market=market,
                    tuning=tuning
                )
                
                if legs:
                    earnings_day = earnings_day_cache.get(sym)
                    if earnings_day is None:
                        earnings_day = _is_earnings_day(db, sym, asof_date)
                        earnings_day_cache[sym] = earnings_day
                    earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
                    if earnings_day:
                        legs = _apply_sell_multiplier_to_legs(legs, cur, market, earnings_day_multiplier, "intraday")
                        logger.info(f"📆 [{sym}] holding SELL earnings-day multiplier 적용: x{earnings_day_multiplier:.1f}")

                    sell_plans.append({
                        "ticker_id": p.get("ticker_id", 0),
                        "symbol": sym,
                        "action": "SELL",
                        "reference": {"recommendation_id": None, "breach": None, "earnings_day": earnings_day, "earnings_day_sell_multiplier": round(earnings_day_multiplier, 4)},
                        "note": f"[보유매도] s={s:+.2f} gain={gain_pct:+.1%}, 보수적 {sell_ratio:.0%}, earnM={earnings_day_multiplier:.2f} | {desc}",
                        "legs": legs
                    })
                    logger.info(f"✅ [{sym}] 보유 {qty_pos}주 → 매도 레그 생성 (수량={sell_qty}, s={s:+.2f})")
                    created += 1
                    if created >= tuning.INTRA_MAX_NEW_ORDERS_PER_CYCLE:
                        break
        else:
            # s < 0 이지만 보유하지 않은 경우 (공매도 불가)
            has_position = sym in pos_map and float(pos_map[sym].get("qty") or 0) > 0
            reason = "보유안함" if not has_position else "기타조건미달"
            skipped.append({
                "ticker_id": p.get("ticker_id", 0), 
                "code": "HOLD", 
                "note": f"매도불가: s={s:+.2f} (하락세이나 {reason}, 공매도불가)"
            })

    return buy_plans, sell_plans, skipped


def apply_rebalancing_rules(
    db, 
    market: str, 
    symbol: str, 
    pred: Dict[str, Any], 
    tuning: Tuning, 
    test_mode: bool = False,
    position_qty: float = 0.0  # 🆕 보유 포지션 수량 추가
) -> Dict[str, List[Dict]]:
    """
    리밸런싱 룰 (예상변화율·s 기반 양방향 래칫)
    - s ≥ +θ: BUY 레그 위로 당김(ratchet up), SELL 레그도 약간 위로
    - s ≤ -θ: SELL 레그 아래로 내림(ratchet down), BUY 레그도 아래로 래칫다운 (깊은 할인 유도)
    - 🆕 보유 포지션 존재 시 매도 레그 자동 생성
    
    Args:
        position_qty: 보유 포지션 수량 (매도 레그 생성을 위해 추가)
    
    Returns:
        {
            "buy_ratcheted": [{leg_id, symbol, old_price, new_price, action}, ...],
            "sell_ratcheted": [{leg_id, symbol, old_price, new_price, action}, ...],
            "cancelled": [{leg_id, symbol, old_price, reason}, ...]
        }
    """
    meta = _pred_to_strength(pred, tuning)
    cur, p_up, p_down, exp_up, exp_down, s = meta["cur"], meta["p_up"], meta["p_down"], meta["exp_up"], meta["exp_down"], meta["s"]
    
    # 조정 내역 추적
    result = {
        "buy_ratcheted": [],
        "sell_ratcheted": [],
        "cancelled": []
    }
    
    if cur <= 0:
        return result

    abs_s = abs(s)
    buy_legs = get_pending_buy_legs_by_symbol(db, symbol, market) or []
    sell_legs = get_pending_sell_legs_by_symbol(db, symbol, market) or []
    
    # 🆕 보유 포지션이 있는데 매도 레그가 없으면 플래그만 설정 (실제 생성은 plan_intraday_actions에서)
    needs_sell_legs = position_qty > 0 and not sell_legs
    if needs_sell_legs:
        logger.info(f"✅ [{symbol}] 보유포지션 {position_qty:.2f}주 존재 & 매도레그 없음 → 매도 레그 생성 필요")

    # 공통: 동적 스텝 (기대변화율 반영)
    dyn_up = min(tuning.REPRICE_STEP_UP_PCT_MAX, tuning.REPRICE_STEP_UP_PCT + tuning.REPRICE_DYNAMIC_MULT * max(0.0, exp_up))
    dyn_dn_sell = min(tuning.REPRICE_SELL_STEP_DOWN_PCT_MAX, tuning.REPRICE_SELL_STEP_DOWN_PCT + tuning.REPRICE_DYNAMIC_MULT * max(0.0, exp_down))
    dyn_dn_buy = min(tuning.REPRICE_STEP_UP_PCT_MAX, tuning.REPRICE_STEP_UP_PCT + tuning.REPRICE_DYNAMIC_MULT * max(0.0, exp_down))  # 🆕 하락 시 BUY 래칫다운용

    # s ≥ +θ: BUY 위로 래칫 (Smart Ratchet)
    if s >= tuning.INTRA_MIN_ABS_S:
        if buy_legs:
            # 🆕 트리거 체크: 가장 가까운 레그 기준
            closest = min(buy_legs, key=lambda x: abs(cur - float(x.get('limit_price', 0))))
            closest_discount = (cur - float(closest.get('limit_price', 0))) / cur
            
            if closest_discount >= tuning.REPRICE_TRIGGER_DISCOUNT_PCT:  # 1% 이상 벌어졌을 때만
                # 🆕 Smart Ratchet: 각 레그별 거리 기반 감쇠 적용
                dyn = min(dyn_up / 0.01, 0.7)  # 0.003/0.01 = 0.3 (정규화)
                alpha = 0.03
                
                for leg in buy_legs:
                    old = float(leg.get("limit_price", 0))
                    if old <= 0:
                        continue
                    
                    # Smart Ratchet 적용
                    new_price = _smart_ratchet_price(old, cur, dyn, alpha)
                    new_price = round_to_tick(new_price, market)
                    
                    # 안전 체크: 상승만 & 현재가 미만
                    if new_price > old and new_price < cur:
                        replace_leg_price(db, leg_id=leg["leg_id"], new_limit_price=new_price, test_mode=test_mode)
                        adj_factor = dyn * math.exp(-abs(cur - old) / cur / alpha)
                        create_leg_action_log(db, leg["leg_id"], symbol, action="SMART_RATCHET_UP_BUY",
                                              note=f"{old:.2f}->{new_price:.2f} cur={cur:.2f} adj={adj_factor:.3f}")
                        result["buy_ratcheted"].append({
                            "leg_id": leg["leg_id"],
                            "symbol": symbol,
                            "old_price": old,
                            "new_price": new_price,
                            "action": "RATCHET_UP",
                            "adj_factor": adj_factor
                        })

        if sell_legs:
            # 🆕 트리거 체크: 가장 가까운 레그 기준
            closest = min(sell_legs, key=lambda x: abs(float(x.get('limit_price', 0)) - cur))
            closest_premium = (float(closest.get('limit_price', 0)) - cur) / cur
            
            if closest_premium <= tuning.REPRICE_SELL_TRIGGER_PREMIUM_PCT:  # 프리미엄 1% 이하
                # 🆕 Smart Ratchet: SELL 래칫업
                dyn = min((tuning.REPRICE_SELL_STEP_UP_PCT + tuning.REPRICE_DYNAMIC_MULT * max(0.0, exp_up)) / 0.01, 0.7)
                alpha = 0.03
                
                for leg in sell_legs:
                    old = float(leg.get("limit_price", 0))
                    if old <= 0:
                        continue
                    
                    # Smart Ratchet 적용
                    new_price = _smart_ratchet_price(old, cur, dyn, alpha)
                    new_price = round_to_tick(new_price, market)
                    
                    # 안전 체크: 상승만 & 현재가 초과
                    if new_price > old and new_price > cur:
                        replace_leg_price(db, leg_id=leg["leg_id"], new_limit_price=new_price, test_mode=test_mode)
                        adj_factor = dyn * math.exp(-abs(old - cur) / cur / alpha)
                        create_leg_action_log(db, leg["leg_id"], symbol, action="SMART_RATCHET_UP_SELL",
                                              note=f"{old:.2f}->{new_price:.2f} cur={cur:.2f} adj={adj_factor:.3f}")
                        result["sell_ratcheted"].append({
                            "leg_id": leg["leg_id"],
                            "symbol": symbol,
                            "old_price": old,
                            "new_price": new_price,
                            "action": "RATCHET_UP",
                            "adj_factor": adj_factor
                        })

    # s ≤ -θ: 하락 모멘텀 → BUY 래칫다운 (깊은 할인)
    if s <= -tuning.INTRA_MIN_ABS_S:
        if buy_legs:
            # BUY 레그 아래로 래칫다운 (Smart Ratchet - 역방향)
            # 현재가보다 낮은 레그만 대상
            legs_below = [leg for leg in buy_legs if float(leg.get("limit_price", 0)) < cur]
            
            if legs_below:
                # 🆕 Smart Ratchet 다운: 음수 dyn으로 하락 유도
                dyn = -min(dyn_dn_buy / 0.01, 0.5)  # 음수 (하락)
                alpha = 0.03
                
                for leg in legs_below:
                    old = float(leg.get("limit_price", 0))
                    if old <= 0:
                        continue
                    
                    # Smart Ratchet 적용 (하락)
                    new_price = _smart_ratchet_price(old, cur, dyn, alpha)
                    new_price = round_to_tick(new_price, market)
                    
                    # 안전 체크: 하락만 & 현재가 미만 유지
                    if new_price < old and new_price < cur:
                        replace_leg_price(db, leg_id=leg["leg_id"], new_limit_price=new_price, test_mode=test_mode)
                        adj_factor = abs(dyn) * math.exp(-abs(cur - old) / cur / alpha)
                        create_leg_action_log(db, leg["leg_id"], symbol, action="SMART_RATCHET_DOWN_BUY",
                                              note=f"{old:.2f}->{new_price:.2f} cur={cur:.2f} adj={adj_factor:.3f} (하락→깊은할인)")
                        result["buy_ratcheted"].append({
                            "leg_id": leg["leg_id"],
                            "symbol": symbol,
                            "old_price": old,
                            "new_price": new_price,
                            "action": "RATCHET_DOWN",
                            "adj_factor": adj_factor
                        })
        
        if sell_legs:
            # SELL 레그 아래로 래칫 (Smart Ratchet - 체결 유도)
            # 현재가보다 높은 레그만 대상
            legs_above = [leg for leg in sell_legs if float(leg.get("limit_price", 0)) > cur]
            
            if legs_above:
                # 트리거 체크: 가장 가까운 레그의 프리미엄
                closest = min(legs_above, key=lambda x: abs(float(x.get('limit_price', 0)) - cur))
                closest_premium = (float(closest.get('limit_price', 0)) - cur) / cur
                
                if closest_premium >= max(tuning.INTRA_SELL_PREMIUM_BASE, 0.001):  # 프리미엄 있을 때만
                    # 🆕 Smart Ratchet 다운: 음수 dyn으로 하락 유도
                    dyn = -min(dyn_dn_sell / 0.01, 0.5)  # 음수 (하락)
                    alpha = 0.03
                    
                    for leg in legs_above:
                        old = float(leg.get("limit_price", 0))
                        if old <= 0:
                            continue
                        
                        # Smart Ratchet 적용 (하락)
                        new_price = _smart_ratchet_price(old, cur, dyn, alpha)
                        new_price = round_to_tick(new_price, market)
                        
                        # 최소 프리미엄 보장 (현재가보다 아래로 안 내려가게)
                        min_price = round_to_tick(cur * 1.0001, market)
                        new_price = max(new_price, min_price)
                        
                        # 안전 체크: 하락만
                        if new_price < old and new_price > cur:
                            replace_leg_price(db, leg_id=leg["leg_id"], new_limit_price=new_price, test_mode=test_mode)
                            adj_factor = abs(dyn) * math.exp(-abs(old - cur) / cur / alpha)
                            create_leg_action_log(db, leg["leg_id"], symbol, action="SMART_RATCHET_DOWN_SELL",
                                                  note=f"{old:.2f}->{new_price:.2f} cur={cur:.2f} adj={adj_factor:.3f} (체결유도)")
                            result["sell_ratcheted"].append({
                                "leg_id": leg["leg_id"],
                                "symbol": symbol,
                                "old_price": old,
                                "new_price": new_price,
                                "action": "RATCHET_DOWN",
                                "adj_factor": adj_factor
                            })
    
    return result


# =============================================================================
# 시간외 거래 함수들
# =============================================================================

def plan_kr_after_hours_orders(
    db,
    now_kst,
    market: str,
    currency: str,
    positions: List[Dict[str, Any]],
    country: str,
    swing_cap_cash: float,
    order_type: str,  # "06" (15:30~16:00) or "07" (16:00~18:00)
    pending: List[Dict[str, Any]] | None = None  # 🆕 pending 주문 (정정용)
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """
    국장 시간외 거래 주문 (LONG 추천 + PM 신호 + 포지션 0주)
    
    🆕 로직:
    - pending 주문이 있으면 가격만 정정 (revise)
    - pending 주문이 없으면 신규 주문 생성
    
    Args:
        order_type: "06" (장후 시간외) or "07" (시간외 단일가)
        pending: 미체결 주문 리스트 (None이면 조회)
        
    Returns:
        (buy_plans, skipped, revised_count)
    """
    from app.features.premarket.services.pm_active_set_service import PMActiveSetService
    from app.features.premarket.utils.pm_ladder_generator import generate_pm_adaptive_ladder
    from app.features.trading_hybrid.repositories.order_repository import get_pending_buy_legs_by_symbol
    from app.core.kis_client import KISClient
    import json
    
    logger.info(f"🌙 국장 시간외 주문({order_type}) 시작: 예산={swing_cap_cash:,.2f}{currency}")
    asof_date = now_kst.date() if hasattr(now_kst, "date") else None
    earnings_day_cache: Dict[str, bool] = {}
    
    # 0) Pending 주문 조회 및 심볼별 맵 생성
    if pending is None:
        pending = get_pending_buy_legs_by_symbol(db, None, market)
    
    # 심볼별 pending BUY 주문 맵 (symbol → [legs])
    pending_map = {}
    for leg in pending:
        symbol = leg.get("symbol")
        if symbol:
            if symbol not in pending_map:
                pending_map[symbol] = []
            pending_map[symbol].append(leg)
    
    logger.info(f"📋 Pending BUY 주문: {len(pending)}개 (종목 {len(pending_map)}개)")
    
    # 1) PM 액티브 후보 조회
    pm_service = PMActiveSetService(db)
    candidates, ladder_params = pm_service.get_pm_active_candidates(
        country=country,
        min_signal=0.5,
        limit=10,
        exclude_short=True,
        mode="BUY"
    )
    
    if not candidates:
        logger.info("⚠️ PM 액티브 후보 없음 → 스킵")
        return [], [], 0
    
    # 2) LONG 추천 필터링
    long_candidates = [c for c in candidates if c.get("has_long_recommendation", False)]
    
    if not long_candidates:
        logger.info(f"⚠️ LONG 추천 후보 없음 (전체 {len(candidates)}개 중) → 스킵")
        return [], [], 0
    
    logger.info(f"✅ LONG 추천 후보: {len(long_candidates)}개")
    
    # 3) 포지션 0주 필터링
    pos_map = {p["symbol"]: float(p.get("qty") or 0) for p in positions}
    zero_pos = [c for c in long_candidates if pos_map.get(c["symbol"], 0) == 0]
    
    if not zero_pos:
        logger.info(f"⚠️ 포지션 0주 종목 없음 (LONG {len(long_candidates)}개 중) → 스킵")
        return [], [], 0
    
    logger.info(f"✅ 포지션 0주 종목: {len(zero_pos)}개")
    
    # 3-1) KIS API로 실시간 현재가 조회 (DB 종가 대신)
    # KISClient는 이미 import됨
    kis = KISClient(db)
    
    for cand in zero_pos:
        symbol = cand["symbol"]
        try:
            response = kis.kr_current_price(symbol)
            if response and response.get("output"):
                output = response["output"]
                # 현재가 (stck_prpr)
                current_price = float(output.get("stck_prpr", 0))
                if current_price > 0:
                    cand["current_price"] = current_price
                    logger.debug(f"✅ [{symbol}] 실시간 현재가: {current_price:,.0f}원")
                else:
                    logger.warning(f"⚠️ [{symbol}] 현재가 0원 (DB 종가 유지: {cand.get('current_price', 0):,.0f}원)")
            else:
                logger.warning(f"⚠️ [{symbol}] KIS API 응답 없음 (DB 종가 유지)")
        except Exception as e:
            logger.error(f"❌ [{symbol}] 현재가 조회 실패: {e} (DB 종가 유지)")
    
    logger.info(f"🔄 실시간 현재가 조회 완료: {len(zero_pos)}개")
    
    # 4) 심볼별 예산 분배 (장초 방식과 동일)
    from app.features.premarket.services.pm_open_session_service import allocate_symbol_budgets_pm
    
    zero_pos_selected, budget_map, skipped_budget = allocate_symbol_budgets_pm(
        zero_pos, swing_cap_cash, market, logger
    )
    
    if not zero_pos_selected or not budget_map:
        logger.warning("❌ 시간외 예산 분배 실패 (2주를 살 수 있는 종목 없음)")
        return [], skipped_budget, 0
    
    logger.info(f"🧮 시간외 예산 분배: {len(zero_pos_selected)}개 종목 선정")
    
    # 5) 주문 생성 또는 정정
    buy_plans = []
    skipped = skipped_budget  # 예산 분배에서 스킵된 항목
    revised_count = 0  # 정정된 주문 수
    
    # 튜닝 파라미터 (BUY)
    tuning_buy = ladder_params.get("buy", {}) if ladder_params else {}
    
    # KIS 클라이언트 초기화 (정정용)
    from app.core.config import settings
    kis = KISClient(db)
    
    for cand in zero_pos_selected:
        ticker_id = cand["ticker_id"]
        symbol = cand["symbol"]
        cur = float(cand.get("current_price") or 0)
        signal_1d = cand.get("signal_1d", 0)
        signal_strength = abs(signal_1d)
        tb_label = cand.get("tb_label")
        iae_1_3 = cand.get("iae_1_3")
        has_long = cand.get("has_long_recommendation", False)
        
        if cur <= 0:
            skipped.append({
                "ticker_id": ticker_id,
                "code": "INVALID_PRICE",
                "note": f"[{symbol}] 현재가 오류: {cur}"
            })
            continue
        
        # 종목별 예산 조회
        symbol_budget = budget_map.get(ticker_id, 0.0)

        earnings_day = earnings_day_cache.get(symbol)
        if earnings_day is None:
            earnings_day = _is_earnings_day(db, symbol, asof_date)
            earnings_day_cache[symbol] = earnings_day
        earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
        
        # 🆕 Pending 주문 체크
        pending_legs = pending_map.get(symbol, [])
        
        if order_type == "06":
            # 06: 장후 시간외 (15:30~16:00) - 1주 0.3% 할인
            limit_price = round_to_tick(cur * 0.997, market)
            if earnings_day:
                limit_price = round_to_tick(cur * (1.0 - min(0.95, (1.0 - (limit_price / cur)) * earnings_day_multiplier)), market)
            qty = min(1, int(symbol_budget // limit_price))  # 예산 내 최대 1주
            
            if qty <= 0:
                skipped.append({
                    "ticker_id": ticker_id,
                    "code": "BUDGET",
                    "note": f"[{symbol}] 예산 부족: 배정={symbol_budget:,.2f} < 필요≥{limit_price:.2f}"
                })
                continue
            
            # 🆕 Pending 주문이 있으면 가격만 정정, 없으면 신규 주문 생성
            if pending_legs:
                # 가격 정정 (첫 번째 pending leg만)
                leg = pending_legs[0]
                old_price = float(leg.get("limit_price", 0))
                broker_order_no = leg.get("broker_order_no")
                
                if old_price == limit_price:
                    logger.info(f"✅ [{symbol}] 가격 동일 → 정정 불필요: {limit_price:.2f}")
                    continue
                
                try:
                    response = kis.domestic_order_revise_test(
                        PDNO=symbol,
                        ORGN_ODNO=broker_order_no,
                        ORD_DVSN="00",  # 지정가
                        ORD_QTY=str(qty),
                        ORD_UNPR=str(int(limit_price)),
                        EXCG_ID_DVSN_CD="NXT"  # 시간외는 NXT
                    )
                    
                    if response and response.get("rt_cd") == "0":
                        revised_count += 1
                        logger.info(f"✅ [{symbol}] 주문 정정 성공: {old_price:.2f} → {limit_price:.2f} (주문번호: {broker_order_no})")
                    else:
                        err_msg = response.get("msg1", "정정 실패") if response else "응답 없음"
                        logger.warning(f"⚠️ [{symbol}] 주문 정정 실패: {err_msg}")
                        skipped.append({
                            "ticker_id": ticker_id,
                            "code": "REVISE_FAILED",
                            "note": f"[{symbol}] 정정 실패: {err_msg}"
                        })
                except Exception as e:
                    logger.error(f"❌ [{symbol}] 주문 정정 오류: {e}", exc_info=True)
                    skipped.append({
                        "ticker_id": ticker_id,
                        "code": "REVISE_ERROR",
                        "note": f"[{symbol}] 정정 오류: {str(e)}"
                    })
            else:
                # 신규 주문 생성
                buy_plans.append({
                    "ticker_id": ticker_id,
                    "symbol": symbol,
                    "action": "BUY",
                    "reference": {
                        "type": "KR_AFTER_HOURS_06",
                        "has_long_recommendation": has_long,
                        "pm_signal": signal_1d,
                        "earnings_day": earnings_day,
                        "earnings_day_buy_multiplier": round(earnings_day_multiplier, 4)
                    },
                    "note": f"국장06장후시간외:cur={cur:.2f}, limit={limit_price:.2f}(0.3%할인, earnM={earnings_day_multiplier:.2f}), LONG추천+PM신호={signal_1d:.3f}, 예산={symbol_budget:,.0f}",
                    "legs": [{
                        "type": "AFTER_HOURS_06",  # ✅ 시간외 주문 타입 (명시적)
                        "side": "BUY",
                        "quantity": qty,
                        "limit_price": limit_price
                    }]
                })
            
        else:  # order_type == "07"
            # 07: 시간외 단일가 (16:00~18:00) - 단주 1주 + 레그 방식
            # 1) 단주 1주도 current-price 추격을 피하도록 최소 할인 floor 적용
            # 2) 나머지 예산으로 레그 생성
            
            atr_pct = float(cand.get("atr_pct", 0.05) or 0.05)
            instant_required = max(0.005, 0.15 * atr_pct)
            if earnings_day:
                instant_required = min(0.95, instant_required * earnings_day_multiplier)
            instant_share_cost = round_to_tick(cur * (1.0 - instant_required), market)
            
            # 단주 추가 가능 여부 체크
            if instant_share_cost > symbol_budget:
                skipped.append({
                    "ticker_id": ticker_id,
                    "code": "BUDGET",
                    "note": f"[{symbol}] 단주 1주 예산 부족: 배정={symbol_budget:,.2f} < 현재가={instant_share_cost:.2f}"
                })
                continue
            
            # 🆕 Pending 주문이 있으면 가격만 정정 (단주 1주)
            if pending_legs:
                # 가격 정정 (첫 번째 pending leg만, 단주 1주로 가정)
                leg = pending_legs[0]
                old_price = float(leg.get("limit_price", 0))
                broker_order_no = leg.get("broker_order_no")
                
                if old_price == instant_share_cost:
                    logger.info(f"✅ [{symbol}] 가격 동일 → 정정 불필요: {instant_share_cost:.2f}")
                    continue
                
                try:
                    response = kis.domestic_order_revise_test(
                        PDNO=symbol,
                        ORGN_ODNO=broker_order_no,
                        ORD_DVSN="00",  # 지정가
                        ORD_QTY="1",  # 단주 1주
                        ORD_UNPR=str(int(instant_share_cost)),
                        EXCG_ID_DVSN_CD="NXT"  # 시간외는 NXT
                    )
                    
                    if response and response.get("rt_cd") == "0":
                        revised_count += 1
                        logger.info(f"✅ [{symbol}] 주문 정정 성공: {old_price:.2f} → {instant_share_cost:.2f} (주문번호: {broker_order_no})")
                    else:
                        err_msg = response.get("msg1", "정정 실패") if response else "응답 없음"
                        logger.warning(f"⚠️ [{symbol}] 주문 정정 실패: {err_msg}")
                        skipped.append({
                            "ticker_id": ticker_id,
                            "code": "REVISE_FAILED",
                            "note": f"[{symbol}] 정정 실패: {err_msg}"
                        })
                except Exception as e:
                    logger.error(f"❌ [{symbol}] 주문 정정 오류: {e}", exc_info=True)
                    skipped.append({
                        "ticker_id": ticker_id,
                        "code": "REVISE_ERROR",
                        "note": f"[{symbol}] 정정 오류: {str(e)}"
                    })
                continue  # 정정 처리 완료, 다음 종목으로
            
            # 나머지 예산 계산
            remaining_budget = symbol_budget - instant_share_cost
            
            # 레그 생성용 예산이 있으면 레그 생성
            legs = []
            desc = "단주만"  # 기본값
            
            if remaining_budget > instant_share_cost:  # 최소 1주 더 살 수 있을 때만 레그 생성
                atr_pct = cand.get("atr_pct", 0.05)
                required = max(instant_required, 0.2 * atr_pct)  # 첫레그 할인 추정 (단주 floor 이상 유지)
                first_limit_est = round_to_tick(cur * (1.0 - required), market)
                
                # 나머지 예산 기반 수량 계산
                qty_cap = int(remaining_budget // first_limit_est)
                
                if qty_cap > 0:
                    # PM 래더 생성 (장초 방식)
                    legs, desc = generate_pm_adaptive_ladder(
                        mode="BUY",
                        s=signal_strength,
                        gain_pct=0.0,
                        current_price=cur,
                        quantity=qty_cap,
                        market=market,
                        tuning=tuning_buy,
                        tb_label=tb_label,
                        iae_1_3=iae_1_3,
                        has_long_recommendation=has_long
                    )
                    
                    if legs:
                        if earnings_day:
                            legs = _apply_buy_multiplier_to_legs(legs, cur, market, earnings_day_multiplier, "after_hours")
                        # 🆕 시간외 주문 타입으로 변환 (LIMIT → AFTER_HOURS_07)
                        for leg in legs:
                            leg["type"] = "AFTER_HOURS_07"
            
            # 단주 1주 추가 (최소 할인 floor 적용)
            legs.insert(0, {
                "type": "AFTER_HOURS_07",
                "side": "BUY",
                "quantity": 1,
                "limit_price": instant_share_cost,
                "entry_anchor": round(cur, 4),
                "entry_discount_floor": round(instant_required, 6)
            })
            
            total_cost = sum(l["quantity"] * l["limit_price"] for l in legs)
            
            # 배정 예산 초과 체크
            if total_cost > symbol_budget * 1.05:  # 5% 여유
                skipped.append({
                    "ticker_id": ticker_id,
                    "code": "BUDGET",
                    "note": f"[{symbol}] 총 비용 초과: {total_cost:,.2f} > 배정={symbol_budget:,.2f}"
                })
                continue
            
            buy_plans.append({
                "ticker_id": ticker_id,
                "symbol": symbol,
                "action": "BUY",
                "reference": {
                    "type": "KR_AFTER_HOURS_07",
                    "has_long_recommendation": has_long,
                    "pm_signal": signal_1d,
                    "tb_label": tb_label,
                    "iae_1_3": iae_1_3,
                    "earnings_day": earnings_day,
                    "earnings_day_buy_multiplier": round(earnings_day_multiplier, 4)
                },
                "note": f"국장07시간외단일가:cur={cur:.2f}, instDisc={instant_required:.3%}, earnM={earnings_day_multiplier:.2f}, LONG추천+PM신호={signal_1d:.3f}, TB={tb_label}, IAE={iae_1_3:.2%}, 단주=1@{instant_share_cost:.0f}, 예산={symbol_budget:,.0f}, {desc}",
                "legs": legs
            })
        
        logger.info(f"✅ [{symbol}] 시간외({order_type}): 현재가={cur:.2f}, 예산={symbol_budget:,.0f}")
    
    total_cost = sum(sum(l["quantity"] * l["limit_price"] for l in p["legs"]) for p in buy_plans)
    logger.info(f"📊 국장 시간외({order_type}) 완료: 신규={len(buy_plans)}건, 정정={revised_count}건, 스킵={len(skipped)}건, 예산사용={total_cost:,.2f}/{swing_cap_cash:,.2f}")
    
    return buy_plans, skipped, revised_count


def plan_us_after_market_orders(
    db,
    now_kst,
    market: str,
    currency: str,
    positions: List[Dict[str, Any]],
    country: str,
    swing_cap_cash: float
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    미장 애프터마켓 단주 주문 (LONG 추천 + PM 신호 + 포지션 0주)
    
    현재가 00 : 지정가로 주문
    
    Returns:
        (buy_plans, skipped)
    """
    from app.features.premarket.services.pm_active_set_service import PMActiveSetService
    
    logger.info(f"🌙 미장 애프터마켓 단주 주문 시작: 예산={swing_cap_cash:,.2f}{currency}")
    asof_date = now_kst.date() if hasattr(now_kst, "date") else None
    earnings_day_cache: Dict[str, bool] = {}
    
    # 1) PM 액티브 후보 조회
    pm_service = PMActiveSetService(db)
    candidates, _ = pm_service.get_pm_active_candidates(
        country=country,
        min_signal=0.5,
        limit=10,
        exclude_short=True,
        mode="BUY"
    )
    
    if not candidates:
        logger.info("⚠️ PM 액티브 후보 없음 → 스킵")
        return [], []
    
    # 2) LONG 추천 필터링
    long_candidates = [c for c in candidates if c.get("has_long_recommendation", False)]
    
    if not long_candidates:
        logger.info(f"⚠️ LONG 추천 후보 없음 (전체 {len(candidates)}개 중) → 스킵")
        return [], []
    
    logger.info(f"✅ LONG 추천 후보: {len(long_candidates)}개")
    
    # 3) 포지션 0주 필터링
    pos_map = {p["symbol"]: float(p.get("qty") or 0) for p in positions}
    zero_pos = [c for c in long_candidates if pos_map.get(c["symbol"], 0) == 0]
    
    if not zero_pos:
        logger.info(f"⚠️ 포지션 0주 종목 없음 (LONG {len(long_candidates)}개 중) → 스킵")
        return [], []
    
    logger.info(f"✅ 포지션 0주 종목: {len(zero_pos)}개")
    
    # 3-1) KIS API로 실시간 현재가 조회 (DB 종가 대신)
    from app.core.kis_client import KISClient
    from app.shared.models.ticker import Ticker
    
    kis = KISClient(db)
    
    # 티커 정보 조회 (거래소 정보 필요)
    ticker_ids = [c["ticker_id"] for c in zero_pos]
    tickers = db.query(Ticker).filter(Ticker.id.in_(ticker_ids)).all()
    ticker_map = {t.id: t for t in tickers}
    
    for cand in zero_pos:
        symbol = cand["symbol"]
        ticker_id = cand["ticker_id"]
        ticker = ticker_map.get(ticker_id)
        
        if not ticker:
            logger.warning(f"⚠️ [{symbol}] 티커 정보 없음 (DB 종가 유지)")
            continue
        
        try:
            # KIS API용 거래소 코드 변환
            exchange_map = {"NASDAQ": "NAS", "NYSE": "NYS", "AMEX": "AMS"}
            excd = exchange_map.get(ticker.exchange, "NAS")
            
            response = kis.overseas_current_price_test(AUTH="", EXCD=excd, SYMB=symbol)
            if response and response.get("output"):
                output = response["output"]
                # 현재가 (last)
                current_price = float(output.get("last", 0))
                if current_price > 0:
                    cand["current_price"] = current_price
                    logger.debug(f"✅ [{symbol}] 실시간 현재가: ${current_price:.2f}")
                else:
                    logger.warning(f"⚠️ [{symbol}] 현재가 $0 (DB 종가 유지: ${cand.get('current_price', 0):.2f})")
            else:
                logger.warning(f"⚠️ [{symbol}] KIS API 응답 없음 (DB 종가 유지)")
        except Exception as e:
            logger.error(f"❌ [{symbol}] 현재가 조회 실패: {e} (DB 종가 유지)")
    
    logger.info(f"🔄 실시간 현재가 조회 완료: {len(zero_pos)}개")
    
    # 4) 가격 싼순 정렬
    zero_pos.sort(key=lambda x: float(x.get("current_price") or 0))
    
    # 5) 1주씩 현재가 지정가 주문
    buy_plans = []
    skipped = []
    used_budget = 0.0
    
    for cand in zero_pos:
        ticker_id = cand["ticker_id"]
        symbol = cand["symbol"]
        cur = float(cand.get("current_price") or 0)
        signal_1d = cand.get("signal_1d", 0)
        has_long = cand.get("has_long_recommendation", False)
        
        if cur <= 0:
            skipped.append({
                "ticker_id": ticker_id,
                "code": "INVALID_PRICE",
                "note": f"[{symbol}] 현재가 오류: {cur}"
            })
            continue
        
        earnings_day = earnings_day_cache.get(symbol)
        if earnings_day is None:
            earnings_day = _is_earnings_day(db, symbol, asof_date)
            earnings_day_cache[symbol] = earnings_day
        earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0

        # 현재가 00 추격 대신 최소 할인 floor 적용
        atr_pct = float(cand.get("atr_pct", 0.05) or 0.05)
        instant_required = max(0.005, 0.15 * atr_pct)
        if earnings_day:
            instant_required = min(0.95, instant_required * earnings_day_multiplier)
        limit_price = round_to_tick(cur * (1.0 - instant_required), market)
        qty = 1
        cost = limit_price * qty
        
        if used_budget + cost > swing_cap_cash:
            skipped.append({
                "ticker_id": ticker_id,
                "code": "BUDGET",
                "note": f"[{symbol}] 예산 초과: {used_budget:,.2f}+{cost:,.2f}>{swing_cap_cash:,.2f}"
            })
            continue
        
        buy_plans.append({
            "ticker_id": ticker_id,
            "symbol": symbol,
            "action": "BUY",
            "reference": {
                "type": "US_AFTER_MARKET",
                "has_long_recommendation": has_long,
                "pm_signal": signal_1d,
                "earnings_day": earnings_day,
                "earnings_day_buy_multiplier": round(earnings_day_multiplier, 4)
            },
            "note": f"미장애프터마켓할인지정가:cur={cur:.2f}, limit={limit_price:.2f}(instDisc={instant_required:.3%}, earnM={earnings_day_multiplier:.2f}), LONG추천+PM신호={signal_1d:.3f}",
            "legs": [{
                "type": "LIMIT",
                "side": "BUY",
                "quantity": qty,
                "limit_price": limit_price,
                "entry_anchor": round(cur, 4),
                "entry_discount_floor": round(instant_required, 6)
            }]
        })
        used_budget += cost
        
        logger.info(f"✅ [{symbol}] 애프터마켓: 현재가={cur:.2f}, 지정가={limit_price:.2f}, 예산={used_budget:,.2f}/{swing_cap_cash:,.2f}")
    
    logger.info(f"📊 미장 애프터마켓 완료: {len(buy_plans)}건, 스킵={len(skipped)}건, 예산={used_budget:,.2f}/{swing_cap_cash:,.2f}")
    
    return buy_plans, skipped