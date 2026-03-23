# app/features/trading_hybrid/services/open_session_service.py
"""
장초(Open Session) 전용 서비스
- 탐욕 레그 생성
- 패턴 기반 매수 플랜
- 패턴 기반 익절 플랜
"""
from __future__ import annotations
from typing import Dict, Any, List
import logging
import numpy as np
from sqlalchemy.orm import Session

from app.features.trading_hybrid.utils.ladder_generator import (
    generate_unified_adaptive_ladder,
    _qty_from_budget
)
from app.features.trading_hybrid.utils.ticks import round_to_tick
from app.features.trading_hybrid.policy.tuning import Tuning
from app.features.signals.services.signal_detection_service import SignalDetectionService
from app.features.signals.models.similarity_models import SimilaritySearchRequest
from app.features.signals.models.signal_models import AlgorithmVersion

logger = logging.getLogger(__name__)


# === 심볼별 예산 맵 분배 (B안: 권장) ===
def _required_discount(atr_pct: float) -> float:
    """유효 할인율 계산 (첫 레그 앵커용: ATR의 40% 할인, 최소 1.2%)"""
    return max(0.012, 0.4 * float(atr_pct or 0.05))


def _sigmoid(x: float) -> float:
    return float(1.0 / (1.0 + np.exp(-x)))


def _compute_sell_intensity(
    gain_pct: float,
    p_up: float,
    p_down: float,
    exp_up: float,
    exp_down: float,
    tuning: Tuning,
) -> float:
    """
    데이터 기반 연속형 매도 강도(0~1) 계산.

    해석:
    - 수익이 클수록(이익실현 여지 ↑) 강도 ↑
    - 하락 위험(down_strength) > 상승 우위(up_strength)면 강도 ↑
    - 급락/과매도 맥락(음수 gain)일수록 강도 ↓
    """
    up_strength = float(max(0.0, p_up * exp_up))
    down_strength = float(max(0.0, p_down * abs(exp_down)))
    trend_component = (down_strength - up_strength) / max(tuning.SELL_INTENSITY_STRENGTH_SCALE, 1e-6)

    gain_component = np.tanh(gain_pct / max(tuning.SELL_INTENSITY_GAIN_SCALE, 1e-6))
    drawdown_component = max(0.0, -gain_pct) / max(tuning.SELL_INTENSITY_DRAWDOWN_SCALE, 1e-6)

    raw = (
        tuning.SELL_INTENSITY_BASE
        + tuning.SELL_INTENSITY_GAIN_WEIGHT * gain_component
        + tuning.SELL_INTENSITY_TREND_WEIGHT * trend_component
        - tuning.SELL_INTENSITY_DRAWDOWN_WEIGHT * drawdown_component
    )

    intensity = _sigmoid(raw)
    return float(np.clip(intensity, tuning.SELL_INTENSITY_MIN, tuning.SELL_INTENSITY_MAX))


def _rescale_sell_legs_by_intensity(
    legs: List[Dict[str, Any]],
    intensity: float,
    min_qty: int = 1,
) -> tuple[List[Dict[str, Any]], int, int]:
    """레그 수량 총합을 intensity 비율로 축소(분할 구조 유지)."""
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

    # 비율 기반 정수 배분
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

    scaled = []
    for leg, q in zip(legs, alloc_int.tolist()):
        if q <= 0:
            continue
        leg2 = dict(leg)
        leg2["quantity"] = int(q)
        scaled.append(leg2)

    final_qty = int(sum(l["quantity"] for l in scaled))
    return scaled, total_qty, final_qty


def allocate_symbol_budgets(
    active: List[Dict[str, Any]],
    swing_cap_cash: float,
    market: str,
    tuning,
    logger: logging.Logger,
    prioritizer=None,
) -> tuple[List[Dict[str, Any]], Dict[int, float], List[Dict[str, Any]]]:
    """
    GARP (Granularity-Aware Risk Parity) 예산 분배
    
    핵심 아이디어:
    - 비싼 종목은 "그레뉼러리티(최소 단위)" 문제로 포트를 왜곡 → 자동 스킵/축소
    - 싸고 변동성 낮은 종목은 여러 레그로 분산 → 진입·청산 용이
    - 리스크 패리티: 단위 위험(price×ATR%) 역수로 가중
    
    전략:
    (1) 하드캡 계산: hard_cap = min(soft_cap, MAX_SYMBOL_WEIGHT × S)
    (2) 최소 사다리 비용: required = MIN_LADDER_LEGS × price
    (3) Affordability ratio: g = hard_cap / required
    (4) 우선순위: prior = (score^β / unit_risk^α) × min(1, g)^γ
    (5) g < 1이면 사다리 불가 → 스킵
    (6) Pass 1: 최소 사다리 비용부터 배정
    (7) Pass 2: 남은 예산은 prior 비율로 배분 (hard_cap 한도)
    
    Args:
        active: 후보 종목 리스트
        swing_cap_cash: 스윙 버킷 총 예산 (S)
        market: 시장 (US/KR)
        tuning: Tuning 파라미터 (GARP 설정 포함)
        logger: Logger
        prioritizer: 우선순위 함수 (rec -> float, None이면 score 사용)
        
    Returns:
        (selected, budget_map{ticker_id: budget}, skipped_pool)
    """
    S = swing_cap_cash
    N = len(active)
    
    if N == 0 or S <= 0:
        logger.warning("❌ [GARP] 활성 종목 없음 또는 예산 없음")
        return [], {}, []
    
    # 캡 계산
    soft_cap = tuning.SOFT_CAP_MULT * S / N
    hard_cap_global = tuning.MAX_SYMBOL_WEIGHT * S
    
    logger.info(
        f"🧮 [GARP] 총예산={S:,.2f}, 종목수={N}, "
        f"soft_cap={soft_cap:,.2f}, hard_cap_global={hard_cap_global:,.2f}"
    )
    
    # 각 종목 분석: 첫 레그 추정가 기반 최소비용 계산
    scored = []
    for rec in active:
        tid = rec.get("ticker_id")
        sym = rec.get("symbol", "UNKNOWN")
        price = float(rec.get("current_price") or 0.0)
        atr_pct = float(rec.get("atr_pct") or 0.05)
        score = float(rec.get("score") or 1.0)
        
        if not tid or price <= 0:
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
        
        # 우선순위 계산 (prioritizer 사용)
        base_score = prioritizer(rec) if callable(prioritizer) else score
        base_priority = (float(base_score) ** tuning.RP_BETA) / (unit_risk ** tuning.RP_ALPHA)
        
        # 그레뉼러리티 패널티 (g < 1이면 패널티 증가)
        priority = base_priority * (min(1.0, g) ** tuning.GRANULARITY_PENALTY_POW)
        
        scored.append({
            "rec": rec,
            "tid": tid,
            "sym": sym,
            "price": price,
            "atr_pct": atr_pct,
            "first_limit_est": first_limit_est,
            "priority": priority,
            "hard_cap": hard_cap,
            "required": required,
            "g": g
        })
    
    if not scored:
        logger.warning("❌ [GARP] 분석 대상이 없음")
        return [], {}, [
            {"ticker_id": rec.get("ticker_id"), "code": "INVALID", 
             "note": f"[{rec.get('symbol')}] 데이터 오류"}
            for rec in active if rec.get("ticker_id")
        ]
    
    # 상세 로깅
    logger.info(f"  📋 [GARP 종목별 분석] 총 {len(scored)}개 종목")
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
            f"  ⏭️ [GARP] 고가제외: [{dropped['sym']}] price={dropped['price']:.2f}, "
            f"required={dropped['required']:.2f}, hard_cap={dropped['hard_cap']:.2f}"
        )
    
    if not cands:
        logger.warning("❌ [GARP] 고가 제외 후에도 배정 불가")
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
                f"  ✅ [Pass1] [{item['sym']}] 최소비용={need:,.2f}, "
                f"잔액={S_remaining:,.2f}"
            )
    
    if not budget_map:
        logger.warning("❌ [GARP] Pass1 배정 실패")
        return [], {}, [
            {"ticker_id": it["tid"], "code": "BUDGET_INSUFFICIENT",
             "note": f"[{it['sym']}] need={min(it['required'], it['hard_cap']):.2f} > 잔액"}
            for it in cands
        ]
    
    logger.info(
        f"  🔹 [GARP Pass1] {len(budget_map)}개 종목, "
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
            f"  🔹 [GARP Pass2] 우선순위 기반 추가 배정, "
            f"최종잔액={S_remaining:,.2f}"
        )
    
    # 남은 예산이 있으면 알림만
    if S_remaining > 100:  # 100 이상일 때만 알림
        logger.info(
            f"  💰 [GARP 남은예산] {S_remaining:,.2f} (미배정)"
        )
    
    # 최종 결과
    selected_recs = [it["rec"] for it in cands if it["tid"] in budget_map]
    skipped = [
        {"ticker_id": it["tid"], "code": "BUDGET_ALLOCATION",
         "note": f"[{it['sym']}] 예산 부족으로 제외"}
        for it in scored if it["tid"] not in budget_map
    ]
    
    # 할당된 종목별 예산 로깅
    logger.info(f"  💰 [GARP 배정 내역] ({len(budget_map)}개 종목)")
    for tid, budg in sorted(budget_map.items(), key=lambda x: x[1], reverse=True):
        item = next((it for it in cands if it["tid"] == tid), None)
        if item:
            max_qty = int(budg // item['price'])
            logger.info(
                f"    [{item['sym']}] {budg:,.2f} → 최대 {max_qty}주 "
                f"(price={item['price']:.2f}, prior={item['priority']:.2f})"
            )
    
    logger.info(
        f"✅ [GARP 완료] 선택={len(selected_recs)}개, 스킵={len(skipped)}개, "
        f"총배정={sum(budget_map.values()):,.2f}/{S:,.2f}"
    )
    
    return selected_recs, budget_map, skipped


def plan_pattern_open_buy_orders(
    db: Session,
    now_kst,
    market: str,
    currency: str,
    active: List[Dict[str, Any]],        # 추천/액티브 후보 (symbol, ticker_id, current_price, entry_price, atr_pct 등)
    account: Dict[str, Any],
    positions: List[Dict[str, Any]],
    caps: Dict[str, float],
    tuning: Tuning,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    장초 '패턴 기반 매수' 플랜 생성 (통합형 적응 사다리 사용)

    반환:
        (buy_plans, sell_plans(empty), skipped)
    """
    buy_plans, sell_plans, skipped = [], [], []

    # 예산 편성
    bp = float(account.get("buying_power_ccy") or 0.0)
    swing_ratio = float(caps.get("swing_ratio", 0.0))
    swing_cap_cash = bp * swing_ratio
    active_count = len(active)

    logger.info(f"💰 [예산편성] BP={bp:,.2f} {currency}")
    logger.info(f"💰 [예산편성] swing_ratio={swing_ratio:.2%} → swing_cap={swing_cap_cash:,.2f} {currency}")
    logger.info(f"💰 [예산편성] 초기 활성종목={active_count}개")

    # 🆕 심볼별 예산 맵 분배 (균등분배 제거, 최소비용 보장 + 잔액 우선순위 분배)
    active, budget_map, skipped_pool = allocate_symbol_budgets(
        active, swing_cap_cash, market, tuning, logger,
        prioritizer=lambda r: float(r.get("score") or 1.0)  # score 없으면 1.0
    )
    skipped.extend(skipped_pool)

    if not active or not budget_map:
        logger.warning("❌ [예산분배] 2주를 살 수 있는 종목이 없음")
        return [], [], skipped

    logger.info(f"🔧 [최종선별] 활성종목={len(active)}개, 심볼별 예산 배정 완료")

    # 보유 맵 (손익률 반영용)
    pos_map = {p.get("symbol"): p for p in positions}

    # 패턴 서비스
    signal_service = SignalDetectionService(db)

    for rec in active:
        try:
            symbol = rec.get("symbol", "UNKNOWN")
            ticker_id = rec.get("ticker_id")
            cur = float(rec.get("current_price") or 0.0)
            entry = float(rec.get("entry_price") or cur)
            atr_pct = float(rec.get("atr_pct", 0.05))
            budget_i = float(budget_map.get(ticker_id, 0.0))  # 🆕 심볼별 예산

            if not ticker_id or cur <= 0 or budget_i <= 0:
                reason_parts = []
                if not ticker_id:
                    reason_parts.append("ticker_id없음")
                if cur <= 0:
                    reason_parts.append(f"현재가={cur}")
                if budget_i <= 0:
                    reason_parts.append(f"예산={budget_i:.2f}{currency}")
                
                skipped.append({
                    "ticker_id": ticker_id or 0, 
                    "code": "INVALID", 
                    "note": f"[{symbol}] 데이터 오류: {', '.join(reason_parts)}"
                })
                continue

            # 보유 손익률 → 매수 사다리 확대/축소 신호로 활용
            pos = pos_map.get(symbol)
            if pos and float(pos.get("qty") or 0) > 0 and float(pos.get("avg_cost_ccy") or 0) > 0:
                cost = float(pos["avg_cost_ccy"])
                gain_pct = (cur - cost) / cost
            else:
                gain_pct = 0.0

            # 🆕 첫 레그 추정가로 qty_cap 계산 (심볼별 예산 budget_i 기반)
            required = max(0.012, 0.4 * atr_pct)
            first_limit_est = round_to_tick(cur * (1.0 - required), market)
            qty_cap = _qty_from_budget(first_limit_est, budget_i)
            
            logger.debug(
                f"💵 [{symbol}] 예산분석: cur={cur:.2f}, "
                f"첫레그추정={first_limit_est:.2f}, 배정예산={budget_i:,.2f} → qty_cap={qty_cap}"
            )
            
            if qty_cap <= 0:
                skipped.append({
                    "ticker_id": ticker_id, 
                    "code": "BUDGET", 
                    "note": f"[{symbol}] 예산부족: 필요≥{first_limit_est:.2f} vs 배정={budget_i:.2f}"
                })
                logger.warning(
                    f"❌ [{symbol}] 예산 부족으로 스킵: "
                    f"첫레그추정={first_limit_est:.2f}, 배정예산={budget_i:,.2f} → qty_cap={qty_cap}"
                )
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
                        f"❌ [{symbol}] 그레뉼러리티 제약으로 스킵: "
                        f"qty_cap={qty_cap} < 최소레그={min_ladder_legs}"
                    )
                    continue

            # === 패턴 분석 ===
            req = SimilaritySearchRequest(
                ticker_id=ticker_id,
                reference_date=None,
                lookback=tuning.PATTERN_SIMILARITY_LOOKBACK,
                top_k=tuning.PATTERN_SIMILARITY_TOP_K,
                direction_filter=None,
                version=AlgorithmVersion.V3,
            )
            resp = signal_service.search_similar_signals(req)

            if len(resp.similar_signals) < tuning.PATTERN_MIN_SIMILARITY_COUNT:
                skipped.append({
                    "ticker_id": ticker_id, 
                    "code": "PATTERN_FEW", 
                    "note": f"[{symbol}] 유사패턴부족: {len(resp.similar_signals)}개 < 임계값{tuning.PATTERN_MIN_SIMILARITY_COUNT}개 (cur={cur:.2f})"
                })
                continue

            sims = np.array([s.similarity for s in resp.similar_signals], dtype=float)
            chg = np.array([s.change_7_24d if s.direction == "UP" else -s.change_7_24d for s in resp.similar_signals], dtype=float)
            w = sims / max(np.sum(sims), 1e-12)

            up_mask = chg > 0
            down_mask = chg < 0
            p_up = float(np.sum(w[up_mask]))
            p_down = float(np.sum(w[down_mask]))

            exp_up = float(np.sum(w[up_mask] * chg[up_mask]) / max(p_up, 1e-9)) if p_up > 0 else 0.0
            exp_down = float(np.sum(w[down_mask] * chg[down_mask]) / max(p_down, 1e-9)) if p_down > 0 else 0.0

            logger.debug(f"📊 [{symbol}] 패턴: p_up={p_up:.2%}×{exp_up:.2%}, p_down={p_down:.2%}×{abs(exp_down):.2%}, gain={gain_pct:.2%}")

            # === 통합형 적응 사다리로 BUY 생성 ===
            legs, desc = generate_unified_adaptive_ladder(
                mode="BUY",
                p_up=p_up, p_down=p_down,
                exp_up=exp_up, exp_down=exp_down,
                gain_pct=gain_pct,
                current_price=cur,
                quantity=int(max(1, qty_cap)),  # ✅ 최소 1주 보장
                market=market,
                tuning=tuning
            )

            # ✅ 사다리 생성 함수의 판단을 신뢰 (이중 검증 제거)
            # 이유: generate_unified_adaptive_ladder가 이미 패턴 기반으로 적절한 가격 설정
            #       메인에서 일괄 "1.2% 할인" 강제는 적응형 로직 파괴
            if not legs:
                skipped.append({
                    "ticker_id": ticker_id, 
                    "code": "NO_LEGS", 
                    "note": f"[{symbol}] 사다리 생성 실패 (패턴/조건 불충분)"
                })
                continue

            # 예산 사용 분석 (심볼별 예산 budget_i 기준)
            est_cost = sum(l["quantity"] * float(l["limit_price"]) for l in legs)
            total_qty = sum(l["quantity"] for l in legs)
            budget_usage_pct = (est_cost / budget_i * 100) if budget_i > 0 else 0
            
            logger.info(
                f"✅ [{symbol}] 패턴매수 완료: "
                f"레그={len(legs)}개, 총수량={total_qty}, "
                f"예산={est_cost:,.2f}/{budget_i:,.2f} ({budget_usage_pct:.1f}% 사용) | {desc}"
            )

            buy_plans.append({
                "ticker_id": ticker_id,
                "symbol": symbol,
                "action": "BUY",
                "reference": {"recommendation_id": rec.get("recommendation_id"), "breach": None},
                "note": f"적응매수: cur={cur:.2f} {desc}",
                "legs": legs
            })

        except Exception as e:
            logger.warning(f"❌ [{rec.get('symbol','?')}] 패턴매수 실패: {e}")
            skipped.append({"ticker_id": rec.get("ticker_id", 0), "code": "ERROR", "note": str(e)})
            continue

    # 최종 예산 집계 (심볼별 예산 맵 기반)
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
    
    logger.info(f"📊 장초 패턴매수 완료: BUY={len(buy_plans)}, SKIP={len(skipped)}")
    logger.info(
        f"💰 [예산사용] 총배정={total_budget_allocated:,.2f} → "
        f"실사용≈{total_budget_used:,.2f} {currency} (효율={efficiency:.1f}%)"
    )
    
    if skip_by_code:
        skip_summary = ", ".join([f"{code}:{count}건" for code, count in sorted(skip_by_code.items())])
        logger.info(f"⏭️ [스킵사유] {skip_summary}")
        
        if skip_by_code.get("BUDGET", 0) > 0:
            logger.warning(
                f"⚠️ 예산부족으로 {skip_by_code['BUDGET']}개 종목 스킵됨 "
                f"(심볼별 예산 맵 분배 후 발생 → 드물어야 정상)"
            )
    
    return buy_plans, sell_plans, skipped


def plan_take_profit_orders(
    db: Session,
    now_kst, 
    market: str, 
    currency: str,
    positions: List[Dict[str, Any]],
    pending: List[Dict[str, Any]] | None,
    tuning: Tuning
):
    """
    장초 매도 플랜 생성: 통합형 적응적 사다리 (패턴 기반)
    
    전략:
    - 모든 보유 종목에 대해 패턴 분석 실행
    - net_strength = (p_up × exp_up) - (p_down × |exp_down|)
    - s ∈ [-1, 1] 정규화
    
    연속적 스펙트럼:
    - s ≈ -1 (강한 하락): 빠른 익절 (현재가 +0.5~2%)
    - s ≈ 0 (중립): 일반 익절 (현재가 +1~3%)
    - s ≈ +1 (강한 상승): 희망매도 (현재가 +3~8%)
    
    ✅ 기존 "패턴익절 vs 희망매도" 이분법 제거
    ✅ 단일 연속함수로 통합 (더 정교하고 부드러운 전환)
    
    최소 조건:
    - gain ≥ 1% 수익
    - 유사 패턴 ≥ 3개
    - 기존 SELL 미체결 없음

    Returns:
        (sell_plans, skipped)
    """
    sell_plans, skipped = [], []
    pending = pending or []

    if not tuning.PATTERN_TAKE_PROFIT_ENABLE:
        logger.info("📊 패턴 기반 익절 비활성화됨")
        return sell_plans, skipped

    # 기존 SELL 미체결 존재 여부
    pen_sell = {}
    for po in pending:
        try:
            if str(po.get("side", "")).upper() == "SELL":
                tid = po.get("ticker_id")
                if tid: pen_sell[tid] = pen_sell.get(tid, 0) + int(po.get("quantity", 1))
        except Exception:
            continue

    signal_service = SignalDetectionService(db)
    
    logger.info("🎯 통합형 적응적 매도 사다리 생성 시작 (연속함수 모델)")
    
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
                "note": f"[{sym}] 가격정보오류: 현재가={cur:.2f}, 평단가={cost:.2f}, 수량={qty}"
            })
            continue
        
        gain_pct = (cur - cost) / cost
        # if gain_pct < tuning.TAKE_PROFIT_MIN_GAIN_PCT:
        #     skipped.append({"ticker_id": tid, "code": "LOW_GAIN", "note": f"[{sym}] 수익 부족 ({gain_pct:.2%})"})
        #     continue
        
        # === 패턴 분석 실행 ===
        try:
            request = SimilaritySearchRequest(
                ticker_id=tid,
                reference_date=None,
                lookback=tuning.PATTERN_SIMILARITY_LOOKBACK,
                top_k=tuning.PATTERN_SIMILARITY_TOP_K,
                direction_filter=None,
                version=AlgorithmVersion.V3
            )
            response = signal_service.search_similar_signals(request)
            
            if len(response.similar_signals) < tuning.PATTERN_MIN_SIMILARITY_COUNT:
                skipped.append({
                    "ticker_id": tid, 
                    "code": "INSUFFICIENT_PATTERNS", 
                    "note": f"[{sym}] 익절패턴부족: {len(response.similar_signals)}개 < 임계값{tuning.PATTERN_MIN_SIMILARITY_COUNT}개 (cur={cur:.2f}, gain={gain_pct:+.2%})"
                })
                continue
            
            # 상승/하락 분석 (양방향)
            sims = np.array([s.similarity for s in response.similar_signals])
            chgs = np.array([
                s.change_7_24d if s.direction == "UP" else -s.change_7_24d
                for s in response.similar_signals
            ])
            weights = sims / np.sum(sims)
            
            up_mask = chgs > 0
            down_mask = chgs < 0
            
            p_up = np.sum(weights[up_mask])
            p_down = np.sum(weights[down_mask])
            
            exp_up = np.sum(weights[up_mask] * chgs[up_mask]) / max(p_up, 1e-9) if p_up > 0 else 0.0
            exp_down = np.sum(weights[down_mask] * chgs[down_mask]) / max(p_down, 1e-9) if p_down > 0 else 0.0
            
            logger.debug(
                f"📊 [{sym}] 패턴: p_up={p_up:.2%}×{exp_up:.2%}, p_down={p_down:.2%}×{abs(exp_down):.2%}"
            )
            
            # 🆕 통합형 적응적 사다리 생성
            # net_strength 기반으로 자동 판단 (하락→익절, 상승→희망매도)
            legs, level = generate_unified_adaptive_ladder(
                mode="SELL",
                p_up=p_up,
                p_down=p_down,
                exp_up=exp_up,
                exp_down=exp_down,
                gain_pct=gain_pct,
                current_price=cur,
                quantity=qty,
                market=market,
                tuning=tuning
            )

            if not legs:
                skipped.append({
                    "ticker_id": tid,
                    "code": "EMPTY_SELL_LEGS",
                    "note": f"[{sym}] 매도 사다리 생성 실패 (qty={qty}, cur={cur:.2f})"
                })
                continue

            # ✅ 데이터 기반 연속형 매도강도 적용 (수동 모드 전환 없음)
            if getattr(tuning, "DATA_DRIVEN_SELL_ENABLE", True):
                intensity = _compute_sell_intensity(
                    gain_pct=gain_pct,
                    p_up=p_up,
                    p_down=p_down,
                    exp_up=exp_up,
                    exp_down=exp_down,
                    tuning=tuning,
                )
                scaled_legs, before_qty, after_qty = _rescale_sell_legs_by_intensity(
                    legs,
                    intensity=intensity,
                    min_qty=max(1, int(getattr(tuning, "SELL_INTENSITY_MIN_QTY", 1) or 1)),
                )

                if after_qty <= 0:
                    skipped.append({
                        "ticker_id": tid,
                        "code": "SELL_INTENSITY_ZERO",
                        "note": f"[{sym}] 강도={intensity:.2f}로 매도보류 (gain={gain_pct:+.2%}, p_up={p_up:.2%}, p_down={p_down:.2%})"
                    })
                    continue

                logger.info(
                    f"🎛️ [{sym}] sell_intensity={intensity:.2f}, qty {before_qty} -> {after_qty} "
                    f"(gain={gain_pct:+.2%}, up={p_up*exp_up:+.4f}, down={p_down*abs(exp_down):+.4f})"
                )
                legs = scaled_legs
                level = f"{level} | intensity={intensity:.2f} qty={after_qty}/{before_qty}"

            note = f"적응{'손절' if cur < cost else '익절'}:cur={cur:.2f} {level}"

            sell_plans.append({
                "ticker_id": tid,
                "symbol": sym,
                "action": "SELL",
                "reference": {"recommendation_id": None, "breach": None},
                "note": note,
                "legs": legs
            })
            
            # ✅ 통합 함수가 이미 상세 로그 출력함 (중복 제거)
            
        except Exception as e:
            logger.warning(f"  ❌ [{sym}] 패턴 분석 실패: {str(e)}")
            skipped.append({"ticker_id": tid, "code": "PATTERN_ERROR", "note": f"[{sym}] {str(e)}"})
            continue
    
    logger.info(f"📊 통합형 매도 플랜 완료: 생성={len(sell_plans)}건, 스킵={len(skipped)}건")
    return sell_plans, skipped

