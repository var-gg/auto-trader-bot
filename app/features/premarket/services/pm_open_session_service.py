# app/features/premarket/services/pm_open_session_service.py
"""
PM 신호 기반 장초(Open Session) 서비스
- pm_best_signal 기반 액티브 셋 선정
- TB 메타라벨 + IAE 반영 래더 생성
- 기존 open_session_service를 대체하는 완전히 새로운 로직
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
import logging
from datetime import date
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.features.premarket.utils.pm_ladder_generator import generate_pm_adaptive_ladder, qty_from_budget
from app.features.premarket.services.pm_active_set_service import PMActiveSetService
from app.features.premarket.repositories.position_maturity_repository import PositionMaturityRepository
from app.features.trading_hybrid.utils.ticks import round_to_tick
from app.features.trading_hybrid.policy.tuning import Tuning
from app.features.premarket.services.headline_risk_service import HeadlineRiskService
from app.core.config import (
    NEWS_RISK_MAX_MULTIPLIER,
    ENABLE_NEWS_BULL_MULTIPLIER,
    NEWS_BULL_MAX_MULTIPLIER,
)

logger = logging.getLogger(__name__)

EARNINGS_DAY_ORDER_MULTIPLIER = 2.0


def _summarize_risk_reason(reason: Any) -> str:
    text_value = str(reason or "core-policy")
    compact = " ".join(text_value.replace("\n", " ").split())
    return compact[:48] if compact else "core-policy"


def _format_pm_buy_risk_note(*, risk_multiplier: float, risk_snapshot_id: Optional[int], risk_meta: Dict[str, Any]) -> str:
    policy = str(risk_meta.get("policy") or "core")
    status = str(risk_meta.get("status") or "unknown")
    freshness = str(risk_meta.get("freshness") or "unknown")
    reason = _summarize_risk_reason(risk_meta.get("reason"))
    snapshot_txt = str(risk_snapshot_id) if risk_snapshot_id is not None else "none"
    return (
        f"riskM={risk_multiplier:.2f}, riskSnap={snapshot_txt}, "
        f"riskState={policy}/{status}/{freshness}, riskWhy={reason}"
    )


def _is_earnings_day(db: Session, symbol: str, asof_date: date) -> bool:
    """해당 종목이 당일 실적발표일(확정/예상/legacy 포함)인지 판정."""
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
    """), {
        "symbol": symbol,
        "asof_date": asof_date,
    }).fetchone()
    return row is not None


def _apply_earnings_day_buy_multiplier_to_legs(
    *,
    legs: List[Dict[str, Any]],
    current_price: float,
    market: str,
    multiplier: float,
) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs

    adjusted: List[Dict[str, Any]] = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue

        base_disc = max(0.0, min(0.95, 1.0 - (lp / current_price)))
        new_disc = max(0.0, min(0.95, base_disc * multiplier))
        new_lp = round_to_tick(current_price * (1.0 - new_disc), market)

        copied = dict(leg)
        copied["limit_price"] = new_lp
        copied["earnings_day"] = True
        copied["earnings_day_buy_multiplier"] = round(multiplier, 4)
        copied["base_discount"] = round(base_disc, 6)
        copied["adjusted_discount"] = round(new_disc, 6)
        adjusted.append(copied)
    return adjusted


def _apply_earnings_day_sell_multiplier_to_legs(
    *,
    legs: List[Dict[str, Any]],
    current_price: float,
    market: str,
    multiplier: float,
) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs

    adjusted: List[Dict[str, Any]] = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue

        base_markup = max(0.0, min(1.5, (lp / current_price) - 1.0))
        new_markup = max(0.0, min(1.8, base_markup * multiplier))
        new_lp = round_to_tick(current_price * (1.0 + new_markup), market)

        copied = dict(leg)
        copied["limit_price"] = new_lp
        copied["earnings_day"] = True
        copied["earnings_day_sell_multiplier"] = round(multiplier, 4)
        copied["base_markup"] = round(base_markup, 6)
        copied["adjusted_markup"] = round(new_markup, 6)
        adjusted.append(copied)
    return adjusted


def _required_discount(atr_pct: float) -> float:
    """유효 할인율 계산"""
    return max(0.012, 0.4 * float(atr_pct or 0.05))


def _sigmoid(x: float) -> float:
    return float(1.0 / (1.0 + np.exp(-x)))


def _compute_pm_sell_intensity(
    *,
    gain_pct: float,
    signal_raw: float,
    tuning: Tuning,
) -> float:
    """PM 신호 + 손익 기반 연속형 매도 강도 (0~1)."""
    gain_component = np.tanh(gain_pct / max(tuning.SELL_INTENSITY_GAIN_SCALE, 1e-6))
    # signal_raw > 0(상승우위)면 매도강도 완화, <0(하락우위)면 강화
    trend_component = float(-signal_raw)
    drawdown_component = max(0.0, -gain_pct) / max(tuning.SELL_INTENSITY_DRAWDOWN_SCALE, 1e-6)

    raw = (
        tuning.SELL_INTENSITY_BASE
        + tuning.SELL_INTENSITY_GAIN_WEIGHT * gain_component
        + tuning.SELL_INTENSITY_TREND_WEIGHT * trend_component
        - tuning.SELL_INTENSITY_DRAWDOWN_WEIGHT * drawdown_component
    )

    intensity = _sigmoid(raw)
    return float(np.clip(intensity, tuning.SELL_INTENSITY_MIN, tuning.SELL_INTENSITY_MAX))


def _rescale_legs_by_intensity(
    legs: List[Dict[str, Any]],
    intensity: float,
    min_qty: int = 1,
) -> tuple[List[Dict[str, Any]], int, int]:
    if not legs:
        return legs, 0, 0

    total_qty = int(sum(int(l.get("quantity", 0) or 0) for l in legs))
    if total_qty <= 0:
        return legs, 0, 0

    target_qty = int(round(total_qty * float(intensity)))
    target_qty = max(0, min(total_qty, target_qty))
    if target_qty == total_qty:
        return legs, total_qty, total_qty
    if target_qty <= 0:
        return [], total_qty, 0

    target_qty = max(min_qty, target_qty)

    weights = np.array([max(0, int(l.get("quantity", 0) or 0)) for l in legs], dtype=float)
    wsum = float(np.sum(weights))
    if wsum <= 0:
        return [], total_qty, 0

    alloc_float = (weights / wsum) * target_qty
    alloc_int = np.floor(alloc_float).astype(int)
    rem = int(target_qty - int(np.sum(alloc_int)))
    if rem > 0:
        remainders = alloc_float - alloc_int
        for idx in np.argsort(-remainders)[:rem]:
            alloc_int[idx] += 1

    scaled: List[Dict[str, Any]] = []
    for leg, q in zip(legs, alloc_int.tolist()):
        if q <= 0:
            continue
        copied = dict(leg)
        copied["quantity"] = int(q)
        scaled.append(copied)

    final_qty = int(sum(l["quantity"] for l in scaled))
    return scaled, total_qty, final_qty


def _apply_news_risk_multiplier_to_legs(
    *,
    legs: List[Dict[str, Any]],
    current_price: float,
    market: str,
    multiplier: float,
) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs

    adjusted: List[Dict[str, Any]] = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue
        base_disc = max(0.0, min(0.95, 1.0 - (lp / current_price)))
        new_disc = max(0.0, min(0.95, base_disc * multiplier))
        new_lp = round_to_tick(current_price * (1.0 - new_disc), market)
        copied = dict(leg)
        copied["limit_price"] = new_lp
        copied["base_discount"] = round(base_disc, 6)
        copied["adjusted_discount"] = round(new_disc, 6)
        copied["news_risk_multiplier"] = round(multiplier, 4)
        adjusted.append(copied)
    return adjusted


def _apply_news_bull_multiplier_to_sell_legs(
    *,
    legs: List[Dict[str, Any]],
    current_price: float,
    market: str,
    multiplier: float,
) -> List[Dict[str, Any]]:
    if multiplier <= 1.0 or current_price <= 0:
        return legs

    adjusted: List[Dict[str, Any]] = []
    for leg in legs:
        lp = float(leg.get("limit_price") or 0.0)
        if lp <= 0:
            adjusted.append(leg)
            continue

        base_markup = max(0.0, min(1.5, (lp / current_price) - 1.0))
        new_markup = max(0.0, min(1.8, base_markup * multiplier))
        new_lp = round_to_tick(current_price * (1.0 + new_markup), market)

        copied = dict(leg)
        copied["limit_price"] = new_lp
        copied["base_markup"] = round(base_markup, 6)
        copied["adjusted_markup"] = round(new_markup, 6)
        copied["news_bull_multiplier"] = round(multiplier, 4)
        adjusted.append(copied)
    return adjusted


def allocate_symbol_budgets_pm(
    candidates: List[Dict[str, Any]],
    swing_cap_cash: float,
    market: str,
    logger: logging.Logger,
    tuning: Optional[Tuning] = None,
) -> tuple[List[Dict[str, Any]], Dict[int, float], List[Dict[str, Any]]]:
    """
    PM 신호 기반 GARP (Granularity-Aware Risk Parity) 예산 분배
    
    핵심 아이디어:
    - 비싼 종목은 "그레뉼러리티(최소 단위)" 문제로 포트를 왜곡 → 자동 스킵/축소
    - PM 신호(signal_strength)와 리스크(price×ATR%)를 함께 고려
    
    전략:
    (1) 하드캡 계산: hard_cap = min(soft_cap, MAX_SYMBOL_WEIGHT × S)
    (2) 최소 사다리 비용: required = MIN_LADDER_LEGS × price
    (3) Affordability ratio: g = hard_cap / required
    (4) 우선순위: prior = (signal^β / unit_risk^α) × min(1, g)^γ
    (5) g < 1이면 사다리 불가 → 스킵
    (6) Pass 1: 최소 사다리 비용부터 배정
    (7) Pass 2: 남은 예산은 prior 비율로 배분 (hard_cap 한도)
    
    Args:
        candidates: PM 후보 종목 리스트
        swing_cap_cash: 스윙 버킷 총 예산 (S)
        market: 시장 (US/KR)
        logger: Logger
        tuning: Tuning 파라미터 (None이면 기본값)
        
    Returns:
        (selected, budget_map{ticker_id: budget}, skipped_pool)
    """
    if tuning is None:
        tuning = Tuning.default_for_market(market)
    
    S = swing_cap_cash
    N = len(candidates)
    
    if N == 0 or S <= 0:
        logger.warning("❌ [PM GARP] 후보 없음 또는 예산 없음")
        return [], {}, []
    
    # 캡 계산
    soft_cap = tuning.SOFT_CAP_MULT * S / N
    hard_cap_global = tuning.MAX_SYMBOL_WEIGHT * S
    
    logger.info(
        f"🧮 [PM GARP] 총예산={S:,.2f}, 종목수={N}, "
        f"soft_cap={soft_cap:,.2f}, hard_cap_global={hard_cap_global:,.2f}"
    )
    
    # 각 종목 분석: 첫 레그 추정가 기반 최소비용 계산
    scored = []
    for cand in candidates:
        tid = cand.get("ticker_id")
        sym = cand.get("symbol", "UNKNOWN")
        price = float(cand.get("current_price") or 0.0)
        atr_pct = float(cand.get("atr_pct") or 0.05)
        signal = float(cand.get("signal_strength", 0.0))  # abs(signal_1d)
        
        if not tid or price <= 0 or signal <= 0:
            continue
        
        # 종목별 하드캡
        hard_cap = min(soft_cap, hard_cap_global)
        
        # 첫 레그 추정가: 현재가 × (1 - 유효 할인율)
        required_disc = _required_discount(atr_pct)
        first_limit_est = round_to_tick(price * (1.0 - required_disc), market)
        
        # 최소 필요비용: MIN_LADDER_LEGS × 첫 레그 추정가
        required = tuning.MIN_LADDER_LEGS * first_limit_est
        
        # Affordability ratio (예산 / 필요금액)
        g = hard_cap / max(required, 1e-9)
        
        # 단위 위험
        unit_risk = max(price * atr_pct, 1e-9)
        
        # 기본 우선순위 (PM 신호 강도 + 리스크 패리티)
        base_priority = (signal ** tuning.RP_BETA) / (unit_risk ** tuning.RP_ALPHA)
        
        # 그레뉼러리티 패널티 (g < 1이면 패널티 증가)
        priority = base_priority * (min(1.0, g) ** tuning.GRANULARITY_PENALTY_POW)
        
        scored.append({
            "cand": cand,
            "tid": tid,
            "sym": sym,
            "price": price,
            "atr_pct": atr_pct,
            "first_limit_est": first_limit_est,
            "priority": priority,
            "hard_cap": hard_cap,
            "required": required,
            "signal": signal,
            "g": g
        })
    
    if not scored:
        logger.warning("❌ [PM GARP] 분석 대상이 없음")
        return [], {}, [
            {"ticker_id": cand.get("ticker_id"), "code": "INVALID", 
             "note": f"[{cand.get('symbol')}] 데이터 오류"}
            for cand in candidates if cand.get("ticker_id")
        ]
    
    # 상세 로깅
    logger.info(f"  📋 [PM GARP 종목별 분석] 총 {len(scored)}개 종목")
    for item in scored[:10]:  # 상위 10개
        logger.info(
            f"    [{item['sym']}] prior={item['priority']:.2f}, price={item['price']:.2f}, "
            f"first_est={item['first_limit_est']:.2f}, required={item['required']:.2f}, hard_cap={item['hard_cap']:.2f}, g={item['g']:.3f}"
        )
    
    # 고가부터 1개씩 제외 루프: 최소 필요비용 합이 총 예산 S를 넘으면 가장 비싼 종목부터 제외
    cands = sorted(scored, key=lambda x: x["price"], reverse=True)
    while cands:
        min_sum = sum(min(it["required"], it["hard_cap"]) for it in cands)
        if min_sum <= S:
            break
        dropped = cands.pop(0)  # 고가 1개 제외
        logger.info(
            f"  ⏭️ [PM GARP] 고가제외: [{dropped['sym']}] price={dropped['price']:.2f}, "
            f"required={dropped['required']:.2f}, hard_cap={dropped['hard_cap']:.2f}"
        )
    
    if not cands:
        logger.warning("❌ [PM GARP] 고가 제외 후에도 배정 불가")
        return [], {}, [
            {"ticker_id": it["tid"], "code": "GRANULARITY",
             "note": f"[{it['sym']}] 최소 레그 비용 충족 불가"} for it in scored
        ]
    
    # 우선순위 정렬 (우선순위↓, 동일시 저가↑)
    cands.sort(key=lambda x: (-x["priority"], x["price"]))
    
    # Pass1: 각 종목에 '최소 필요비용'을 우선 배정 (하드캡 이내, S 이내)
    budget_map = {}
    S_remaining = S
    
    for item in cands:
        need = min(item["required"], item["hard_cap"])
        if S_remaining >= need:
            budget_map[item["tid"]] = need
            S_remaining -= need
            logger.debug(
                f"  ✅ [PM Pass1] [{item['sym']}] 최소비용={need:,.2f}, "
                f"잔액={S_remaining:,.2f}"
            )
    
    if not budget_map:
        logger.warning("❌ [PM GARP] Pass1 배정 실패")
        return [], {}, [
            {"ticker_id": it["tid"], "code": "BUDGET_INSUFFICIENT",
             "note": f"[{it['sym']}] need={min(it['required'], it['hard_cap']):.2f} > 잔액"}
            for it in cands
        ]
    
    logger.info(
        f"  🔹 [PM GARP Pass1] {len(budget_map)}개 종목, "
        f"총배정={sum(budget_map.values()):,.2f}, 잔액={S_remaining:,.2f}"
    )
    
    # Pass2: 잔액은 우선순위 비율로 하드캡까지 추가 배정
    if S_remaining > 0:
        alloc_items = [it for it in cands if it["tid"] in budget_map]
        psum = sum(it["priority"] for it in alloc_items) or 1.0
        
        for it in alloc_items:
            room = it["hard_cap"] - budget_map[it["tid"]]
            if room <= 0:
                continue
            add = min(room, S_remaining * (it["priority"] / psum))
            if add > 0:
                budget_map[it["tid"]] += add
                S_remaining -= add
        
        logger.info(
            f"  🔹 [PM GARP Pass2] 우선순위 기반 추가 배정, "
            f"최종잔액={S_remaining:,.2f}"
        )
    
    # 최종 결과
    selected_cands = [it["cand"] for it in cands if it["tid"] in budget_map]
    skipped = [
        {"ticker_id": it["tid"], "code": "BUDGET_ALLOCATION",
         "note": f"[{it['sym']}] 예산 부족으로 제외"}
        for it in scored if it["tid"] not in budget_map
    ]
    
    # 할당된 종목별 예산 로깅
    logger.info(f"  💰 [PM GARP 배정 내역] ({len(budget_map)}개 종목)")
    for tid, budg in sorted(budget_map.items(), key=lambda x: x[1], reverse=True):
        item = next((it for it in cands if it["tid"] == tid), None)
        if item:
            max_qty = int(budg // item['price'])
            logger.info(
                f"    [{item['sym']}] {budg:,.2f} → 최대 {max_qty}주 "
                f"(price={item['price']:.2f}, prior={item['priority']:.2f})"
            )
    
    logger.info(
        f"✅ [PM GARP 완료] 선택={len(selected_cands)}개, 스킵={len(skipped)}개, "
        f"총배정={sum(budget_map.values()):,.2f}/{S:,.2f}"
    )
    
    return selected_cands, budget_map, skipped


def plan_pm_open_buy_orders(
    db: Session,
    now_kst,
    market: str,
    currency: str,
    account: Dict[str, Any],
    positions: List[Dict[str, Any]],
    caps: Dict[str, float],
    country: str,  # 'KR' or 'US'
    min_signal: float = 0.5,
    limit: int = 10,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    PM 신호 기반 장초 매수 플랜 생성
    
    Args:
        db: DB 세션
        now_kst: 현재 시각 (KST)
        market: 시장 (US/KR)
        currency: 통화 (USD/KRW)
        account: 계좌 정보
        positions: 보유 포지션
        caps: 예산 비율
        country: 국가 (KR/US)
        min_signal: 최소 신호 점수 (기본 0.5)
        limit: 최대 종목 수 (기본 10)
    
    Returns:
        (buy_plans, sell_plans(empty), skipped)
    """
    buy_plans, sell_plans, skipped = [], [], []

    # 1) 예산 편성
    bp = float(account.get("buying_power_ccy") or 0.0)
    swing_ratio = float(caps.get("swing_ratio", 0.0))
    swing_cap_cash = bp * swing_ratio

    logger.info(f"💰 [PM 예산편성] BP={bp:,.2f} {currency}")
    logger.info(f"💰 [PM 예산편성] swing_ratio={swing_ratio:.2%} → swing_cap={swing_cap_cash:,.2f} {currency}")

    # 2) PM 액티브 후보 조회 (BUY: 양수 신호)
    pm_service = PMActiveSetService(db)
    candidates, ladder_params = pm_service.get_pm_active_candidates(
        country=country,
        min_signal=min_signal,
        limit=limit,
        exclude_short=True,
        mode="BUY"  # ⚠️ BUY는 signal_1d > 0 (양수)
    )

    if not candidates:
        logger.warning(f"❌ PM 매수 후보 없음: country={country}, |signal|≥{min_signal}")
        return [], [], []

    logger.info(f"🎯 [PM 매수 후보] {len(candidates)}개 조회 완료")

    # 3) 래더 파라미터 검증 (이미 get_pm_active_candidates에서 기본값 제공됨)
    if not ladder_params:
        logger.error("❌ 래더 파라미터 없음 (예상치 못한 오류)")
        return [], [], []
    
    tuning_buy = ladder_params.get("buy", {})
    if not tuning_buy:
        logger.error("❌ BUY 래더 파라미터 없음")
        return [], [], []

    logger.info(f"✅ BUY 래더 파라미터 로드: MIN_TICK_GAP={tuning_buy.get('MIN_TICK_GAP')}, "
                f"ADAPTIVE_BASE_LEGS={tuning_buy.get('ADAPTIVE_BASE_LEGS')}")

    # 🆕 Tuning 객체 생성 (GARP 파라미터용)
    tuning = Tuning.default_for_market(market)

    # 4) 심볼별 예산 분배 (GARP)
    candidates_sel, budget_map, skipped_pool = allocate_symbol_budgets_pm(
        candidates, swing_cap_cash, market, logger, tuning
    )
    skipped.extend(skipped_pool)

    if not candidates_sel or not budget_map:
        logger.warning("❌ [예산분배] 2주를 살 수 있는 종목이 없음")
        return [], [], skipped

    logger.info(f"🔧 [최종선별] 활성종목={len(candidates_sel)}개, 심볼별 예산 배정 완료")

    # 5) 보유 맵 (손익률 반영용)
    pos_map = {p.get("symbol"): p for p in positions}

    # 5-1) 뉴스 리스크 멀티플라이어 (기본 정책)
    # 주의: PM BUY는 더 이상 feature flag로 비활성화하지 않는다.
    # 신선한 스냅샷이 없더라도 리스크 정책 메타데이터를 남겨
    # 운영자가 fallback/stale 상태를 즉시 식별할 수 있어야 한다.
    risk_multiplier = 1.0
    risk_snapshot_id = None
    scope = "KR" if country.upper() == "KR" else "US"
    risk_meta: Dict[str, Any] = {
        "enabled": True,
        "policy": "core",
        "scope": scope,
        "status": "fallback",
        "freshness": "no_snapshot",
        "reason": "core-policy:no-snapshot",
        "multiplier": risk_multiplier,
    }
    try:
        risk_svc = HeadlineRiskService(db)
        m, snap_id, snap = risk_svc.get_discount_multiplier(scope=scope)
        risk_multiplier = max(1.0, min(float(NEWS_RISK_MAX_MULTIPLIER), float(m)))
        risk_snapshot_id = snap_id
        snap = snap or {}
        snap_reason = snap.get("reason") or snap.get("reason_short") or "core-policy:active"
        status = "applied" if risk_multiplier > 1.0 else "neutral"
        freshness = str(snap.get("reason") or "active")
        if freshness == "auto_refreshed_snapshot":
            status = "refreshed"
        elif freshness == "recently_expired_snapshot":
            status = "stale-grace"
        elif freshness == "stale_snapshot_fallback":
            status = "fallback"
        risk_meta = {
            "enabled": True,
            "policy": "core",
            "scope": scope,
            "risk_snapshot_id": snap_id,
            "risk_score": snap.get("risk_score"),
            "confidence": snap.get("confidence"),
            "regime_score": snap.get("regime_score"),
            "reason": snap_reason,
            "freshness": freshness,
            "status": status,
            "multiplier": risk_multiplier,
        }
        logger.info(
            f"📰 [PM Risk] scope={scope}, snapshot={snap_id}, multiplier={risk_multiplier:.3f}, "
            f"status={status}, freshness={freshness}"
        )
    except Exception as e:
        risk_meta = {
            **risk_meta,
            "status": "fallback-error",
            "freshness": "error",
            "reason": f"core-policy:error:{type(e).__name__}",
            "error": str(e),
        }
        logger.warning(f"[PM Risk] core policy fetch failed, fallback 1.0: {e}")

    # 6) 매수 래더 생성
    asof_date = now_kst.date() if hasattr(now_kst, "date") else None
    earnings_day_cache: Dict[str, bool] = {}

    for cand in candidates_sel:
        try:
            symbol = cand.get("symbol", "UNKNOWN")
            ticker_id = cand.get("ticker_id")
            cur = float(cand.get("current_price") or 0.0)
            atr_pct = float(cand.get("atr_pct", 0.05))
            signal_1d = float(cand.get("signal_1d", 0.0))  # 원본 (BUY는 양수)
            signal_strength = float(cand.get("signal_strength", 0.0))  # abs(signal_1d)
            tb_label = cand.get("tb_label")
            iae_1_3 = cand.get("iae_1_3")
            direction = cand.get("direction")
            budget_i = float(budget_map.get(ticker_id, 0.0))

            if not ticker_id or cur <= 0 or budget_i <= 0:
                skipped.append({
                    "ticker_id": ticker_id or 0,
                    "code": "INVALID",
                    "note": f"[{symbol}] 데이터 오류: cur={cur}, budget={budget_i:.2f}"
                })
                continue

            # 보유 손익률
            pos = pos_map.get(symbol)
            if pos and float(pos.get("qty") or 0) > 0 and float(pos.get("avg_cost_ccy") or 0) > 0:
                cost = float(pos["avg_cost_ccy"])
                gain_pct = (cur - cost) / cost
            else:
                gain_pct = 0.0

            # 첫 레그 추정가로 qty_cap 계산
            required = max(0.012, 0.4 * atr_pct)
            first_limit_est = round_to_tick(cur * (1.0 - required), market)
            qty_cap = qty_from_budget(first_limit_est, budget_i)
            
            logger.debug(
                f"💵 [{symbol}] PM: signal={signal_1d:.3f} (|{signal_strength:.3f}|), TB={tb_label}, IAE={iae_1_3:.2%}, "
                f"cur={cur:.2f}, 첫레그추정={first_limit_est:.2f}, 배정예산={budget_i:,.2f} → qty_cap={qty_cap}"
            )
            
            if qty_cap <= 0:
                skipped.append({
                    "ticker_id": ticker_id,
                    "code": "BUDGET",
                    "note": f"[{symbol}] 예산부족: 필요≥{first_limit_est:.2f} vs 배정={budget_i:.2f}"
                })
                continue
            
            # 🆕 GARP: 최소 레그 개수 체크 (마이크로 레그 허용 옵션)
            min_ladder_legs = tuning.MIN_LADDER_LEGS
            allow_micro = getattr(tuning, "ALLOW_MICRO_LADDER_OPEN", True)
            
            if qty_cap < min_ladder_legs:
                if allow_micro and qty_cap >= 1:
                    logger.info(
                        f"ℹ️ [{symbol}] 마이크로 레그 허용: qty_cap={qty_cap} < MIN_LADDER_LEGS={min_ladder_legs}"
                    )
                    # 그냥 진행 (1레그라도 허용)
                else:
                    skipped.append({
                        "ticker_id": ticker_id,
                        "code": "GRANULARITY",
                        "note": f"[{symbol}] 최소 {min_ladder_legs}레그 불가 (qty_cap={qty_cap}): "
                               f"price={cur:.2f}, 배정예산={budget_i:.2f}"
                    })
                    logger.warning(
                        f"❌ [{symbol}] PM 그레뉼러리티 제약으로 스킵: "
                        f"qty_cap={qty_cap} < 최소레그={min_ladder_legs}"
                    )
                    continue

            # ✅ PM 적응형 래더 생성 (TB 메타라벨 + IAE 반영)
            # ⚠️ BUY는 signal_1d > 0 (양수) → s는 절댓값 사용
            s = signal_strength  # abs(signal_1d)
            has_long = cand.get("has_long_recommendation", False)
            
            legs, desc = generate_pm_adaptive_ladder(
                mode="BUY",
                s=s,
                gain_pct=gain_pct,
                current_price=cur,
                quantity=int(max(1, qty_cap)),  # ✅ 최소 1주 보장
                market=market,
                tuning=tuning_buy,
                tb_label=tb_label,
                iae_1_3=iae_1_3,
                has_long_recommendation=has_long
            )

            if not legs:
                skipped.append({
                    "ticker_id": ticker_id,
                    "code": "NO_LEGS",
                    "note": f"[{symbol}] PM 래더 생성 실패"
                })
                continue

            # 뉴스 리스크 배수 적용 (feature flag on + active snapshot)
            legs = _apply_news_risk_multiplier_to_legs(
                legs=legs,
                current_price=cur,
                market=market,
                multiplier=risk_multiplier,
            )

            earnings_day = earnings_day_cache.get(symbol)
            if earnings_day is None:
                earnings_day = _is_earnings_day(db, symbol, asof_date)
                earnings_day_cache[symbol] = earnings_day

            earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
            if earnings_day:
                legs = _apply_earnings_day_buy_multiplier_to_legs(
                    legs=legs,
                    current_price=cur,
                    market=market,
                    multiplier=earnings_day_multiplier,
                )
                logger.info(
                    f"📆 [{symbol}] earnings day BUY multiplier 적용: x{earnings_day_multiplier:.1f}"
                )

            # 예산 사용 분석
            est_cost = sum(l["quantity"] * float(l["limit_price"]) for l in legs)
            total_qty = sum(l["quantity"] for l in legs)
            budget_usage_pct = (est_cost / budget_i * 100) if budget_i > 0 else 0
            
            logger.info(
                f"✅ [{symbol}] PM매수 완료: "
                f"signal={signal_1d:.3f} (강도={signal_strength:.3f}), TB={tb_label}, IAE={iae_1_3:.2%}, "
                f"레그={len(legs)}개, 총수량={total_qty}, "
                f"예산={est_cost:,.2f}/{budget_i:,.2f} ({budget_usage_pct:.1f}% 사용)"
            )

            buy_plans.append({
                "ticker_id": ticker_id,
                "symbol": symbol,
                "action": "BUY",
                "reference": {
                    "pm_signal": signal_1d,
                    "signal_strength": signal_strength,
                    "best_target_id": cand.get("best_target_id"),
                    "risk_snapshot_id": risk_snapshot_id,
                    "news_risk_multiplier": round(risk_multiplier, 4),
                    "risk_meta": risk_meta,
                    "earnings_day": earnings_day,
                    "earnings_day_buy_multiplier": round(earnings_day_multiplier, 4),
                },
                "note": (
                    f"PM매수:cur={cur:.2f}, signal={signal_1d:.3f}(s={signal_strength:.3f}), TB={tb_label}, "
                    f"{_format_pm_buy_risk_note(risk_multiplier=risk_multiplier, risk_snapshot_id=risk_snapshot_id, risk_meta=risk_meta)}, "
                    f"earnM={earnings_day_multiplier:.2f}, {desc}"
                ),
                "reverse_breach_day": cand.get("reverse_breach_day"),  # ✅ 역돌파 일자 전달
                "legs": legs
            })

        except Exception as e:
            logger.warning(f"❌ [{cand.get('symbol','?')}] PM 매수 실패: {e}", exc_info=True)
            skipped.append({"ticker_id": cand.get("ticker_id", 0), "code": "ERROR", "note": str(e)})
            continue

    # 최종 집계
    total_budget_allocated = sum(
        budget_map.get(plan["ticker_id"], 0.0) for plan in buy_plans
    )
    total_budget_used = sum(
        sum(l["quantity"] * float(l["limit_price"]) for l in plan["legs"])
        for plan in buy_plans
    )
    efficiency = (total_budget_used / total_budget_allocated * 100) if total_budget_allocated > 0 else 0
    
    # 스킵 사유별 집계
    skip_by_code = {}
    for sk in skipped:
        code = sk.get("code", "UNKNOWN")
        skip_by_code[code] = skip_by_code.get(code, 0) + 1
    
    logger.info(f"📊 PM 장초 매수 완료: BUY={len(buy_plans)}, SKIP={len(skipped)}")
    logger.info(
        f"💰 [예산사용] 총배정={total_budget_allocated:,.2f} → "
        f"실사용≈{total_budget_used:,.2f} {currency} (효율={efficiency:.1f}%)"
    )
    
    if skip_by_code:
        skip_summary = ", ".join([f"{code}:{count}건" for code, count in sorted(skip_by_code.items())])
        logger.info(f"⏭️ [스킵사유] {skip_summary}")
    
    return buy_plans, sell_plans, skipped


def plan_pm_take_profit_orders(
    db: Session,
    now_kst,
    market: str,
    currency: str,
    positions: List[Dict[str, Any]],
    pending: List[Dict[str, Any]] | None,
    country: str  # 'KR' or 'US'
):
    """
    PM 기반 매도 플랜 생성 (익절/손절)
    
    ⚠️ 전략: 보유 포지션은 **무조건 매도 래더 설치**
    - signal_1d > 0 (양수, 상승 예상) → 높은 가격 익절 (희망 매도, s 크게)
    - signal_1d < 0 (음수, 하락 예상) → 낮은 가격 손절 (빠른 매도, s 작게)
    - signal_1d ≈ 0 or 없음 → 중립 익절 (s=0.5 기본)
    
    Args:
        db: DB 세션
        now_kst: 현재 시각 (KST)
        market: 시장 (US/KR)
        currency: 통화 (USD/KRW)
        positions: 보유 포지션
        pending: 미체결 주문
        country: 국가 (KR/US)
    
    Returns:
        (sell_plans, skipped)
    """
    sell_plans, skipped = [], []
    pending = pending or []

    # 래더 파라미터 조회
    pm_service = PMActiveSetService(db)
    optuna_repo = pm_service.optuna_repo
    ladder_params = optuna_repo.get_ladder_params()
    
    # 기본값 fallback
    if not ladder_params or "sell" not in ladder_params:
        logger.warning("⚠️ SELL 래더 파라미터 없음 → 기본값 사용")
        ladder_params = {
            "sell": {
                "MIN_TICK_GAP": 2,
                "ADAPTIVE_BASE_LEGS": 3,
                "ADAPTIVE_LEG_BOOST": 1.0,
                "MIN_TOTAL_SPREAD_PCT": 0.012,
                "ADAPTIVE_STRENGTH_SCALE": 0.19,
                "FIRST_LEG_BASE_PCT": 0.015,
                "FIRST_LEG_MIN_PCT": 0.01,
                "FIRST_LEG_MAX_PCT": 0.060,
                "FIRST_LEG_GAIN_WEIGHT": 0.6,
                "FIRST_LEG_ATR_WEIGHT": 0.5,
                "FIRST_LEG_REQ_FLOOR_PCT": 0.0,
                "ADAPTIVE_MAX_STEP_PCT": 0.060,
                "ADAPTIVE_FRAC_ALPHA": 1.25,
                "ADAPTIVE_GAIN_SCALE": 0.10,
                "MIN_LOT_QTY": 1
            }
        }
    
    tuning_sell = ladder_params["sell"]
    tuning_common = Tuning.default_for_market(market)
    logger.info(f"✅ SELL 래더 파라미터 로드: MIN_TICK_GAP={tuning_sell.get('MIN_TICK_GAP')}, "
                f"ADAPTIVE_BASE_LEGS={tuning_sell.get('ADAPTIVE_BASE_LEGS')}")

    # 기존 SELL 미체결 존재 여부
    pen_sell = {}
    for po in pending:
        try:
            if str(po.get("side", "")).upper() == "SELL":
                tid = po.get("ticker_id")
                if tid:
                    pen_sell[tid] = pen_sell.get(tid, 0) + int(po.get("quantity", 1))
        except Exception:
            continue

    logger.info(f"🎯 PM 기반 매도 플랜 생성 시작 (country={country})")

    # 뉴스 호황 멀티플라이어 (옵션)
    bull_multiplier = 1.0
    regime_snapshot_id = None
    regime_meta: Dict[str, Any] = {"enabled": False}
    if ENABLE_NEWS_BULL_MULTIPLIER:
        try:
            scope = "KR" if country.upper() == "KR" else "US"
            risk_svc = HeadlineRiskService(db)
            m, snap_id, snap = risk_svc.get_sell_markup_multiplier(scope=scope)
            bull_multiplier = max(1.0, min(float(NEWS_BULL_MAX_MULTIPLIER), float(m)))
            regime_snapshot_id = snap_id
            regime_meta = {
                "enabled": True,
                "scope": scope,
                "risk_snapshot_id": snap_id,
                "regime_score": (snap or {}).get("regime_score"),
                "confidence": (snap or {}).get("confidence"),
                "multiplier": bull_multiplier,
            }
            logger.info(f"🟢 [PM Bull] scope={scope}, snapshot={snap_id}, multiplier={bull_multiplier:.3f}")
        except Exception as e:
            logger.warning(f"[PM Bull] multiplier fetch failed, fallback 1.0: {e}")
    
    # 만기 체크 Repository
    maturity_repo = PositionMaturityRepository(db)
    asof_date = now_kst.date() if hasattr(now_kst, "date") else None
    earnings_day_cache: Dict[str, bool] = {}
    
    for p in positions:
        tid = p.get("ticker_id")
        sym = p.get("symbol")
        qty = int(float(p.get("orderable_qty") or 0))
        
        if qty <= 0:
            continue
        
        # if pen_sell.get(tid):
        #     skipped.append({"ticker_id": tid, "code": "PENDING_SELL_EXISTS", "note": f"[{sym}] 기존 매도 주문 존재"})
        #     continue
        
        cur = float(p.get("last_price_ccy") or 0.0)
        cost = float(p.get("avg_cost_ccy") or 0.0)
        if cur <= 0 or cost <= 0:
            skipped.append({
                "ticker_id": tid,
                "code": "INVALID_PRICE",
                "note": f"[{sym}] 가격정보오류: 현재가={cur:.2f}, 평단가={cost:.2f}"
            })
            continue
        
        gain_pct = (cur - cost) / cost
        
        # ⚠️ 만기 체크 (future 값 기반)
        maturity_info = maturity_repo.check_position_maturity(tid, sym)
        
        if maturity_info["is_matured"]:
            # 만기 도달 → 현재가 지정가 매도 (KIS 시장가 미지원)
            maturity_source = maturity_info.get("maturity_source", "unknown")

            # ✅ 만기청산도 연속형 강도 적용 (전량 강제 대신 데이터 기반 분할)
            signal_raw_for_maturity = 0.0
            try:
                from app.features.premarket.models.optuna_models import PMBestSignal
                pm_sig = db.query(PMBestSignal.signal_1d).filter(PMBestSignal.ticker_id == tid).first()
                if pm_sig and pm_sig[0] is not None:
                    signal_raw_for_maturity = float(pm_sig[0])
            except Exception:
                signal_raw_for_maturity = 0.0

            intensity_maturity = 1.0
            matured_leg_qty = qty
            if getattr(tuning_common, "DATA_DRIVEN_SELL_ENABLE", True):
                intensity_maturity = _compute_pm_sell_intensity(
                    gain_pct=gain_pct,
                    signal_raw=signal_raw_for_maturity,
                    tuning=tuning_common,
                )
                matured_leg_qty = int(round(qty * intensity_maturity))
                matured_leg_qty = max(1, min(qty, matured_leg_qty))

            logger.info(
                f"⏰ [{sym}] 만기 도달: 보유 {maturity_info['business_days_held']}영업일 ≥ "
                f"만기 {maturity_info['future_days']}일 (출처: {maturity_source}, 최초매수일: {maturity_info['last_buy_date']}) "
                f"→ 현재가 {cur:.2f} 분할청산 qty={matured_leg_qty}/{qty}, intensity={intensity_maturity:.2f}, signal={signal_raw_for_maturity:+.3f}"
            )
            
            note = (
                f"PM만기청산:cur={cur:.2f}, "
                f"보유={maturity_info['business_days_held']}일≥만기={maturity_info['future_days']}일, "
                f"출처={maturity_source}, "
                f"최초매수={maturity_info['last_buy_date']}, "
                f"gain={gain_pct:+.2%}, signal={signal_raw_for_maturity:+.3f}, "
                f"intensity={intensity_maturity:.2f}, qty={matured_leg_qty}/{qty} | "
                f"⏰ 만기 도달 → 현재가 지정가 분할 청산"
            )
            
            sell_plans.append({
                "ticker_id": tid,
                "symbol": sym,
                "action": "SELL",
                "reference": {
                    "maturity": "EXPIRED", 
                    "business_days_held": maturity_info['business_days_held'],
                    "future_days": maturity_info['future_days'],
                    "maturity_source": maturity_info.get('maturity_source', 'unknown'),
                    "last_buy_date": str(maturity_info['last_buy_date']),
                    "pm_signal": signal_raw_for_maturity,
                    "sell_intensity": round(float(intensity_maturity), 4),
                    "sell_qty_ratio": round(float(matured_leg_qty / max(qty, 1)), 4),
                },
                "note": note,
                "legs": [{
                    "type": "LIMIT",  # ⚠️ KIS는 시장가 미지원 → 현재가 지정가
                    "side": "SELL",
                    "quantity": matured_leg_qty,
                    "limit_price": cur  # 현재가로 지정가 (즉시 체결 유도)
                }]
            })
            
            logger.info(
                f"✅ [{sym}] 만기 분할청산: 보유={maturity_info['business_days_held']}일≥만기={maturity_info['future_days']}일 "
                f"(출처: {maturity_source}), 현재가={cur:.2f}, gain={gain_pct:+.2%}, qty={matured_leg_qty}/{qty}, intensity={intensity_maturity:.2f}"
            )
            continue  # 정상 래더 생성 스킵
        
        # PM 신호 재조회 (해당 종목만)
        try:
            from app.features.premarket.models.optuna_models import PMBestSignal, OptunaTargetVector
            
            pm_row = db.query(
                PMBestSignal.signal_1d,
                OptunaTargetVector.tb_label,
                OptunaTargetVector.iae_1_3
            ).join(
                OptunaTargetVector,
                OptunaTargetVector.id == PMBestSignal.best_target_id
            ).filter(
                PMBestSignal.ticker_id == tid
            ).first()
            
            # PM 신호 조회 (없으면 중립 기본값)
            if not pm_row:
                logger.debug(f"[{sym}] PM 신호 없음 → 중립 익절 (s=0.5)")
                signal_raw = 0.0
                tb_label = None
                iae_1_3 = None
            else:
                signal_1d, tb_label, iae_1_3 = pm_row
                signal_raw = float(signal_1d)
            
            # ⚠️ 신호 해석 (보유 포지션 익절 전략)
            # signal_1d > 0 (양수) → 상승 예상 → 높은 가격 익절 (s 크게)
            # signal_1d < 0 (음수) → 하락 예상 → 낮은 가격 손절 (s 작게)
            # signal_1d ≈ 0 or 없음 → 중립 (s=0.5)
            
            if signal_raw > 0:
                # 상승 예상 → 희망 매도 (높은 가격)
                s = abs(signal_raw)
                strategy = "희망익절"
            elif signal_raw < 0:
                # 하락 예상 → 빠른 손절/익절 (낮은 가격)
                s = abs(signal_raw)
                strategy = "빠른손절" if cur < cost else "빠른익절"
            else:
                # 중립 → 기본 익절
                s = 0.5
                strategy = "중립익절"
            
            logger.debug(
                f"📊 [{sym}] PM 재조회: signal={signal_raw:.3f} (s={s:.3f}, {strategy}), "
                f"TB={tb_label}, IAE={iae_1_3:.2%}, gain={gain_pct:+.2%}"
            )
            
            # PM 적응형 매도 래더 생성
            legs, desc = generate_pm_adaptive_ladder(
                mode="SELL",
                s=s,
                gain_pct=gain_pct,
                current_price=cur,
                quantity=qty,
                market=market,
                tuning=tuning_sell,
                tb_label=tb_label,
                iae_1_3=iae_1_3
            )
            
            if not legs:
                skipped.append({
                    "ticker_id": tid,
                    "code": "NO_LEGS",
                    "note": f"[{sym}] PM 매도 래더 생성 실패"
                })
                continue

            legs = _apply_news_bull_multiplier_to_sell_legs(
                legs=legs,
                current_price=cur,
                market=market,
                multiplier=bull_multiplier,
            )

            earnings_day = earnings_day_cache.get(sym)
            if earnings_day is None:
                earnings_day = _is_earnings_day(db, sym, asof_date)
                earnings_day_cache[sym] = earnings_day

            earnings_day_multiplier = EARNINGS_DAY_ORDER_MULTIPLIER if earnings_day else 1.0
            if earnings_day:
                legs = _apply_earnings_day_sell_multiplier_to_legs(
                    legs=legs,
                    current_price=cur,
                    market=market,
                    multiplier=earnings_day_multiplier,
                )
                logger.info(
                    f"📆 [{sym}] earnings day SELL multiplier 적용: x{earnings_day_multiplier:.1f}"
                )

            intensity = 1.0
            # ✅ 데이터 기반 연속형 매도 강도 적용 (수동 레짐 스위치 없음)
            if getattr(tuning_common, "DATA_DRIVEN_SELL_ENABLE", True):
                intensity = _compute_pm_sell_intensity(
                    gain_pct=gain_pct,
                    signal_raw=signal_raw,
                    tuning=tuning_common,
                )
                legs, before_qty, after_qty = _rescale_legs_by_intensity(
                    legs,
                    intensity=intensity,
                    min_qty=max(1, int(getattr(tuning_common, "SELL_INTENSITY_MIN_QTY", 1) or 1)),
                )

                if after_qty <= 0:
                    skipped.append({
                        "ticker_id": tid,
                        "code": "SELL_INTENSITY_ZERO",
                        "note": f"[{sym}] 강도={intensity:.2f}로 매도보류 (signal={signal_raw:+.3f}, gain={gain_pct:+.2%})",
                    })
                    continue

                logger.info(
                    f"🎛️ [{sym}] PM sell_intensity={intensity:.2f}, qty {before_qty}->{after_qty} "
                    f"(signal={signal_raw:+.3f}, gain={gain_pct:+.2%})"
                )

            # 노트: 전략 명시
            pnl_label = "손절" if cur < cost else "익절"
            note = f"PM {strategy}({pnl_label}):cur={cur:.2f}, signal={signal_raw:.3f}(s={s:.3f}), TB={tb_label}, bullM={bull_multiplier:.2f}, earnM={earnings_day_multiplier:.2f}, intensity={intensity:.2f}, {desc}"
            
            sell_plans.append({
                "ticker_id": tid,
                "symbol": sym,
                "action": "SELL",
                "reference": {
                    "pm_signal": signal_raw,
                    "signal_strength": s,
                    "strategy": strategy,
                    "risk_snapshot_id": regime_snapshot_id,
                    "news_bull_multiplier": round(bull_multiplier, 4),
                    "regime_meta": regime_meta,
                    "earnings_day": earnings_day,
                    "earnings_day_sell_multiplier": round(earnings_day_multiplier, 4),
                },
                "note": note,
                "legs": legs
            })
            
            logger.info(
                f"✅ [{sym}] PM 매도 완료 ({strategy}): signal={signal_raw:.3f} (s={s:.3f}), "
                f"TB={tb_label}, IAE={iae_1_3:.2%}, 레그={len(legs)}개, gain={gain_pct:+.2%}"
            )
            
        except Exception as e:
            logger.warning(f"❌ [{sym}] PM 매도 실패: {e}", exc_info=True)
            skipped.append({"ticker_id": tid, "code": "ERROR", "note": f"[{sym}] {str(e)}"})
            continue
    
    logger.info(f"📊 PM 매도 플랜 완료: 생성={len(sell_plans)}건, 스킵={len(skipped)}건")
    return sell_plans, skipped

