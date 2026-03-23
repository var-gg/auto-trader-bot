# app/features/premarket/utils/pm_ladder_generator.py
"""
PM 신호 기반 적응형 래더 생성기
- ladder_exec.py 스타일 차용
- TB 메타라벨(UP_FIRST, DOWN_FIRST, TIMEOUT) + IAE 활용
- 틱 단위 보정, req 바닥선, MIN_TICK_GAP, MIN_TOTAL_SPREAD_PCT 보장
"""
from __future__ import annotations
import logging
import numpy as np
from typing import Dict, List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP

logger = logging.getLogger(__name__)


# ============================================================================
# 예산 유틸리티
# ============================================================================

def qty_from_budget(price: float, budget: float, cap_qty: Optional[int] = None) -> int:
    """
    예산에서 수량 계산
    
    Args:
        price: 가격
        budget: 예산
        cap_qty: 최대 수량 제한 (옵션)
    
    Returns:
        계산된 수량
    """
    q = int(budget // max(price, 1e-6))
    return min(q, cap_qty) if cap_qty else q


# ============================================================================
# 틱 유틸리티 (ticks.py와 독립적으로 작동)
# ============================================================================

def _kr_tick_size(price: float) -> float:
    """한국거래소 호가 단위"""
    if price < 1000:    return 1.0
    if price < 5000:    return 5.0
    if price < 10000:   return 10.0
    if price < 50000:   return 50.0
    if price < 100000:  return 100.0
    if price < 500000:  return 500.0
    return 1000.0


def get_tick_size(price: float, market: str) -> float:
    """시장별 호가 단위 조회"""
    if market == "KR":
        return _kr_tick_size(price)
    else:
        return 0.01


def round_to_tick(
    price: float,
    market: str,
    side: Optional[str] = None
) -> float:
    """
    시장별 호가 단위 반올림
    
    Args:
        price: 원가격
        market: 시장 (KR, US 등)
        side: 거래 방향 (BUY: 내림, SELL: 올림, None: 반올림)
    
    Returns:
        틱 단위로 라운딩된 가격
    """
    if market == "KR":
        step = _kr_tick_size(price)
    else:
        step = 0.01
    
    step_dec = Decimal(str(step))
    price_dec = Decimal(str(price))
    
    # 거래 방향별 라운딩
    if side == "BUY":
        # 매수: 내림 (유리하게)
        rounded = (price_dec / step_dec).quantize(Decimal('1'), rounding=ROUND_DOWN)
    elif side == "SELL":
        # 매도: 올림 (유리하게)
        rounded = (price_dec / step_dec).quantize(Decimal('1'), rounding=ROUND_UP)
    else:
        # 기본: 반올림
        rounded = (price_dec / step_dec).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    
    return float(rounded * step_dec)


# ============================================================================
# 첫 레그 % 결정 (TB 메타라벨 + IAE 반영)
# ============================================================================

def _first_leg_pct(
    mode: str,
    s: float,
    gain_pct: float,
    tuning: Dict,
    current_price: float,
    market: str,
    tb_label: Optional[str] = None,
    iae_1_3: Optional[float] = None,
    has_long_recommendation: bool = False
) -> float:
    """
    첫 레그 % 결정 (실전 로직 + TB 메타라벨 반영)
    
    Args:
        mode: 'BUY' 또는 'SELL'
        s: 방향성 점수 (-1 ~ +1)
        gain_pct: 현재 PnL (백테스트에서는 0)
        tuning: 튜닝 파라미터 딕셔너리
        current_price: 현재 가격
        market: 시장 ('KR', 'US' 등)
        tb_label: 트리플 배리어 라벨 (UP_FIRST, DOWN_FIRST, TIMEOUT)
        iae_1_3: 초기 역행 폭 (Initial Adverse Excursion, 1~3일)
        has_long_recommendation: LONG 애널리스트 추천 존재 여부
    
    Returns:
        첫 레그 비율
    """
    base = tuning.get('FIRST_LEG_BASE_PCT', tuning.get('ADAPTIVE_BASE_STEP_PCT', 0.010))
    min_pct = tuning.get('FIRST_LEG_MIN_PCT', 0.005)
    max_pct = tuning.get('FIRST_LEG_MAX_PCT', tuning.get('ADAPTIVE_MAX_STEP_PCT', 0.060))
    
    # s 영향 (대칭)
    if mode == 'SELL':
        s_mult = np.interp(s, [-1.0, 1.0], [0.8, 2.4])
        gain_eff = gain_pct  # 수익↑ → 멀리
    else:
        # BUY: s 영향 반전 (상승 시 할인 ↓ = 가까이)
        s_mult = np.interp(s, [-1.0, 1.0], [2.4, 0.8])
        gain_eff = -gain_pct  # 손실(음수)↑ → 깊게
    
    # PnL 영향
    gain_scale = max(tuning.get('ADAPTIVE_GAIN_SCALE', 0.10), 1e-6)
    gain_unit = np.clip(gain_eff / gain_scale, -1.0, 1.0)
    gain_mult = 1.0 + tuning.get('FIRST_LEG_GAIN_WEIGHT', 0.6) * gain_unit
    
    # ATR 바닥선(옵션)
    atr_hint = float(tuning.get('ATR_PCT_HINT', 0.0) or 0.0)
    atr_floor = atr_hint * tuning.get('FIRST_LEG_ATR_WEIGHT', 0.5)
    
    # 초기
    pct = base * s_mult * gain_mult
    pct = max(pct, atr_floor, min_pct)
    pct = min(pct, max_pct)
    
    # 강bias 완화 (대칭)
    if mode == 'BUY' and s > 0.7:
        pct *= np.interp(s, [0.7, 1.0], [1.0, 0.7])
    elif mode == 'SELL' and s < -0.7:
        pct *= np.interp(abs(s), [0.7, 1.0], [1.0, 0.7])
    
    # ✅ TB 메타라벨 + IAE 반영 (BUY 관점)
    if mode == 'BUY' and tb_label and iae_1_3 is not None:
        # DOWN_FIRST + IAE 음수 크면 → 진입 즉시 흔들림 큼 → 첫 레그 할인 ↑ (더 깊게)
        if tb_label == 'DOWN_FIRST' and iae_1_3 < -0.01:
            # IAE 증폭: 음수 IAE를 할인 증가로 변환
            iae_penalty = min(abs(iae_1_3) * 0.5, 0.08)  # 최대 8%p 증폭
            pct = pct * (1.0 + iae_penalty)
            logger.debug(f"  🔽 [{mode}] TB=DOWN_FIRST, IAE={iae_1_3:.2%} → 첫레그 할인 증폭 {iae_penalty:.1%}")
        
        # UP_FIRST + IAE 작으면 → 흔들림 적음 → 가중치 유지/소폭 감소
        elif tb_label == 'UP_FIRST' and iae_1_3 > -0.005:
            # IAE가 -0.5% 이내면 가중치 유지 (신뢰도 높음)
            logger.debug(f"  🔼 [{mode}] TB=UP_FIRST, IAE={iae_1_3:.2%} → 안정적 패턴")
    
    # SELL 관점도 대칭 적용 가능 (옵션)
    elif mode == 'SELL' and tb_label and iae_1_3 is not None:
        # SELL은 역으로 UP_FIRST가 불리 (상승 후 하락)
        if tb_label == 'UP_FIRST' and iae_1_3 > 0.01:
            # 양수 IAE가 크면 → 초기 반등 있었음 → 보수적 프리미엄 (가까이)
            pct = pct * 0.85  # 15% 감소
            logger.debug(f"  🔼 [{mode}] TB=UP_FIRST, IAE={iae_1_3:.2%} → 보수적 프리미엄")
    
    # req(유효할인) 바닥선
    req_default = 0.010 if market == 'KR' else 0.0
    req_floor = float(tuning.get('FIRST_LEG_REQ_FLOOR_PCT', req_default) or 0.0)
    if req_floor > 0:
        if mode == 'BUY' and s > 0.7:
            pct = max(pct, req_floor)
        if mode == 'SELL' and s < -0.7:
            pct = max(pct, req_floor)
    
    # ✅ LONG 애널리스트 추천은 후보 우선순위엔 도움을 줄 수 있지만,
    #    첫 레그 할인을 current-price에 과도하게 붙이는 방향으로 약화시키면
    #    비싼 진입을 반복할 수 있으므로 완화폭을 제한한다.
    if mode == 'BUY' and has_long_recommendation:
        old_pct = pct
        pct = max(pct * 0.85, min_pct)
        logger.debug(f"  📊 [{mode}] LONG 추천 존재 → 첫레그 할인 완만 축소: {old_pct:.2%} → {pct:.2%}")
    
    # 틱단위 보정
    try:
        tick_size = get_tick_size(current_price, market)
        tick_pct = tick_size / current_price
        pct = max(round(pct / tick_pct) * tick_pct, tick_pct)
    except Exception:
        pass
    
    # 최종 가격 테스트 (거래 방향별 유리한 라운딩)
    direction_sign = +1 if mode == 'SELL' else -1
    test_price = round_to_tick(current_price * (1.0 + direction_sign * pct), market, side=mode)
    if mode == 'SELL' and test_price <= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market, side=mode)
    if mode == 'BUY' and test_price >= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market, side=mode)
    
    pct_adjusted = abs(test_price / current_price - 1.0)
    return float(np.clip(pct_adjusted, min_pct, max_pct))


# ============================================================================
# 통합 적응형 래더 생성 (PM 버전)
# ============================================================================

def generate_pm_adaptive_ladder(
    mode: str,
    s: float,
    gain_pct: float,
    current_price: float,
    quantity: int,
    market: str,
    tuning: Dict,
    tb_label: Optional[str] = None,
    iae_1_3: Optional[float] = None,
    has_long_recommendation: bool = False
) -> Tuple[List[Dict], str]:
    """
    PM 신호 기반 적응형 래더 생성
    
    Args:
        mode: 'BUY' 또는 'SELL'
        s: 방향성 점수 (-1 ~ +1)
        gain_pct: 현재 PnL (백테스트에서는 보통 0)
        current_price: 현재 가격
        quantity: 총 수량
        market: 시장 ('KR', 'US' 등)
        tuning: 튜닝 파라미터 딕셔너리
            - ADAPTIVE_BASE_LEGS: 기본 레그 수
            - ADAPTIVE_LEG_BOOST: 레그 수 부스트
            - ADAPTIVE_BASE_STEP_PCT: 기본 스텝 비율
            - ADAPTIVE_MAX_STEP_PCT: 최대 스텝 비율
            - ADAPTIVE_STRENGTH_SCALE: 강도 스케일
            - ADAPTIVE_FRAC_ALPHA: 분할 감쇠 계수
            - MIN_TICK_GAP: 최소 틱 간격
            - MIN_TOTAL_SPREAD_PCT: 최소 전체 스프레드 비율
            - MIN_LOT_QTY: 최소 로트 수량
            - MIN_FIRST_LEG_GAP_PCT: 첫 레그와 어떤 레그 간 최소 갭 비율 (기본 0.03 = 3%)
            - STRICT_MIN_FIRST_GAP: True면 ADAPTIVE_MAX_STEP_PCT를 넘어도 위 갭을 강제 (기본 True)
        tb_label: 트리플 배리어 라벨 (UP_FIRST, DOWN_FIRST, TIMEOUT)
        iae_1_3: 초기 역행 폭 (Initial Adverse Excursion)
        has_long_recommendation: LONG 애널리스트 추천 존재 여부
    
    Returns:
        (legs, description)
        legs: [{"type": "LIMIT", "side": mode, "quantity": qty, "limit_price": price}, ...]
        description: 설명 문자열
    """
    mode = mode.upper()
    
    # 0) 가드
    min_lot = tuning.get('MIN_LOT_QTY', 1) or 1
    if quantity < min_lot:
        return [], f"❌ {mode} 수량부족: qty={quantity}, min_lot={min_lot}"
    
    # 1) 레그 개수
    n_legs_target = int(np.clip(
        tuning.get('ADAPTIVE_BASE_LEGS', 3) + abs(s) * tuning.get('ADAPTIVE_LEG_BOOST', 1.0),
        2, 6
    ))
    max_legs_by_qty = max(1, quantity // min_lot)
    n_legs = max(1, min(n_legs_target, max_legs_by_qty))
    
    # ✅ 2주인 경우 무조건 2개 LEG로 분할 (가격 차별화 보장)
    if quantity == 2 * min_lot:
        n_legs = 2
        logger.debug(f"✅ [{mode}] 정확히 2주 → 무조건 2개 LEG 강제 (qty={quantity}, min_lot={min_lot})")
    
    # 2) 방향 부호
    direction_sign = +1 if mode == 'SELL' else -1
    
    # 3) 첫 레그 + 확장
    first_pct = _first_leg_pct(mode, s, gain_pct, tuning, current_price, market, tb_label, iae_1_3, has_long_recommendation)
    
    # 4) 틱/스프레드 하한
    tick = get_tick_size(current_price, market)
    tick_pct = tick / current_price
    min_tick_gap = max(int(tuning.get('MIN_TICK_GAP', 1)), 1)
    min_total_spread_pct_cfg = float(tuning.get('MIN_TOTAL_SPREAD_PCT', 0.0) or 0.0)
    
    min_total_spread_pct_ticks = (max(1, n_legs - 1)) * min_tick_gap * tick_pct
    min_total_spread_pct = max(min_total_spread_pct_cfg, min_total_spread_pct_ticks)
    
    # 5) pct_steps 생성
    # ⬇️ 새로운 설정 변수 초기화 (전체 함수에서 사용 가능하도록)
    req_gap = float(tuning.get('MIN_FIRST_LEG_GAP_PCT', 0.03) or 0.0)
    strict_gap = bool(tuning.get('STRICT_MIN_FIRST_GAP', True))
    max_step_cap_cfg = float(tuning.get('ADAPTIVE_MAX_STEP_PCT', 0.060) or 0.060)
    
    if n_legs == 1:
        pct_steps = np.array([first_pct], dtype=float)
        last_pct = first_pct
        effective_max_cap = max_step_cap_cfg
    else:
        # 강bias 구간은 과도한 확장 방지
        if (mode == 'BUY' and s > 0.7) or (mode == 'SELL' and s < -0.7):
            widen_lo, widen_hi = 1.2, 2.0
        else:
            widen_lo, widen_hi = 1.5, 3.0
        
        widen_mult = np.interp(abs(s), [0.0, 1.0], [widen_lo, widen_hi])
        
        # 기본 후보: 가변 확장
        last_pct_candidate = first_pct * widen_mult
        
        # 스프레드/틱 하한
        last_pct_candidate = max(last_pct_candidate, first_pct + min_total_spread_pct)
        
        # ✅ 첫레그+요구갭(예: +3%) 하한
        last_pct_candidate = max(last_pct_candidate, first_pct + req_gap)
        
        # 엄격 모드면 max cap을 넘어도 허용
        effective_max_cap = max_step_cap_cfg if not strict_gap else max(max_step_cap_cfg, last_pct_candidate)
        
        last_pct = min(last_pct_candidate, effective_max_cap)
        
        pct_steps = np.geomspace(first_pct, last_pct, n_legs).astype(float)
    
    # 6) 가중 분할
    decay_indices = np.linspace(0, tuning.get('ADAPTIVE_FRAC_ALPHA', 1.25), n_legs)
    fracs = np.exp(-decay_indices)
    if mode == 'SELL' and s > 0.3:
        fracs = fracs[::-1]
    fracs = fracs / np.sum(fracs)
    
    # 7) 정수 수량화
    base_alloc = np.full(n_legs, min_lot, dtype=int)
    remaining = quantity - int(np.sum(base_alloc))
    if remaining < 0:
        base_alloc = np.array([quantity], dtype=int)
        remaining = 0
    if remaining > 0:
        extra_float = fracs * remaining
        extra_int = np.floor(extra_float).astype(int)
        base_alloc += extra_int
        rem2 = remaining - int(np.sum(extra_int))
        if rem2 > 0:
            remainders = extra_float - extra_int
            add_order = np.argsort(-remainders)
            for idx in add_order[:rem2]:
                base_alloc[idx] += 1
    
    # 8) 레그 생성(라운딩) + 같은 가격 병합 (거래 방향별 유리한 라운딩)
    raw_legs = []
    for qty_i, pct in zip(base_alloc.tolist(), pct_steps.tolist()):
        price_raw = current_price * (1.0 + direction_sign * pct)
        price = round_to_tick(price_raw, market, side=mode)
        raw_legs.append((float(price), int(qty_i)))
    
    # 동일가격 합산
    merged = {}
    for price, q in raw_legs:
        merged[price] = merged.get(price, 0) + q
    
    # 9) 라운딩 후 중복 방지 리트라이
    if n_legs >= 2 and len(merged) < n_legs:
        attempts = 0
        need_gap_ticks = max(1, tuning.get('MIN_TICK_GAP', 1))
        
        while attempts < 3 and len(merged) < n_legs and first_pct < effective_max_cap:
            bump = (n_legs - len(merged)) * need_gap_ticks * (tick_pct * 1.2)
            last_pct_try = min(first_pct + (last_pct - first_pct) + bump, effective_max_cap)
            
            pct_steps = np.geomspace(first_pct, last_pct_try, n_legs).astype(float)
            raw_legs = []
            for qty_i, pct in zip(base_alloc.tolist(), pct_steps.tolist()):
                price_raw = current_price * (1.0 + direction_sign * pct)
                price = round_to_tick(price_raw, market, side=mode)
                raw_legs.append((float(price), int(qty_i)))
            
            merged = {}
            for price, q in raw_legs:
                merged[price] = merged.get(price, 0) + q
            
            attempts += 1
        
        # 그래도 부족하면 한 틱 차 보조 복구
        if len(merged) < n_legs:
            ordered_tmp = sorted(list(merged.items()), key=lambda x: x[0], reverse=(mode == 'BUY'))
            uniq_prices = []
            uniq = {}
            for price, q in ordered_tmp:
                if uniq_prices and abs(price - uniq_prices[-1]) < need_gap_ticks * tick:
                    shifted = round_to_tick(price + (tick if mode == 'SELL' else -tick), market, side=mode)
                    price = shifted
                uniq_prices.append(price)
                uniq[price] = uniq.get(price, 0) + q
            merged = uniq
            
            # ✅ 2주인 경우 반드시 2개 가격 확보 (가격 차별화 필수)
            if quantity == 2 * min_lot and len(merged) < 2:
                logger.warning(f"⚠️ [{mode}] 2주인데 유니크 가격 1개만 생성됨 → 강제 분리")
                prices_list = sorted(list(merged.keys()), reverse=(mode == 'BUY'))
                if len(prices_list) == 1:
                    base_price = prices_list[0]
                    # 한 틱씩 벌림
                    if mode == 'BUY':
                        price1 = base_price
                        price2 = round_to_tick(base_price - tick, market, side=mode)
                    else:
                        price1 = base_price
                        price2 = round_to_tick(base_price + tick, market, side=mode)
                    
                    # 2주를 1:1로 분할
                    merged = {float(price1): min_lot, float(price2): min_lot}
                    logger.info(f"✅ [{mode}] 2주 강제 분리 완료: {price1:.2f}, {price2:.2f}")
            
                # 여전히 유니크가 모자라면 레그 수를 축소
                elif len(merged) < 2:
                    logger.warning("⚠️ 틱 격자 한계로 유니크 가격 확보 실패 → 레그 수 축소")
    
    # 9.5) ✅ 라운딩/병합 이후에도 '첫레그와 최소 3% 갭' 보장
    if n_legs >= 2:
        # 첫 레그(현재가에 가장 가까운 쪽)와 가장 먼 레그 찾기
        if mode == 'BUY':
            p_first = max(merged.keys())  # BUY는 높은 가격일수록 현재가에 가까움
            p_far   = min(merged.keys())  # 가장 낮은 가격이 가장 멂
        else:
            p_first = min(merged.keys())  # SELL은 낮은 가격이 현재가에 가까움
            p_far   = max(merged.keys())  # 가장 높은 가격이 가장 멂
        
        achieved_gap = abs(p_far - p_first) / current_price
        # 틱 라운딩 여유분(반 틱) 고려
        required_gap = max(req_gap - (tick_pct * 0.5), 0.0)
        
        if achieved_gap + 1e-12 < required_gap:
            # 목표: '첫레그의 실제 pct + req_gap'만큼 떨어진 가격으로 far 레그 재배치
            first_pct_real = abs(p_first / current_price - 1.0)
            target_pct = first_pct_real + req_gap
            
            # STRICT가 아니면 상한을 지킴
            if not strict_gap:
                target_pct = min(target_pct, max_step_cap_cfg)
            
            # 목표 가격 계산 및 유리한 라운딩
            target_price_raw = current_price * (1.0 + direction_sign * target_pct)
            target_price = round_to_tick(target_price_raw, market, side=mode)
            
            # 방향상 '더 멀리'가 맞는지, 충돌/최소틱간격 위반 없도록 한 틱씩 보정
            step = tick * max(1, tuning.get('MIN_TICK_GAP', 1))
            def violates(price: float) -> bool:
                # 기존 다른 가격들과 최소틱간격 위반 여부
                for qprice in merged.keys():
                    if qprice == p_far:
                        continue
                    if abs(price - qprice) < step:
                        return True
                # 방향상 더 멀어졌는가?
                if mode == 'BUY' and price >= p_far:
                    return True
                if mode == 'SELL' and price <= p_far:
                    return True
                return False
            
            # 필요시 한 틱씩 추가 밀어내기
            n_guard = 0
            while violates(target_price) and n_guard < 50:
                target_price = round_to_tick(
                    target_price + (-tick if mode == 'BUY' else +tick),
                    market, side=mode
                )
                n_guard += 1
            
            # 마지막으로 실제 갭이 여전히 부족한데 STRICT가 False면 포기(로그만)
            final_gap = abs(target_price - p_first) / current_price
            if final_gap + 1e-12 < required_gap and not strict_gap:
                logger.warning(f"⚠️ [{mode}] STRICT=False로 {req_gap:.1%} 갭 미충족(final={final_gap:.2%})")
            else:
                # far 레그 가격 교체
                merged[target_price] = merged.pop(p_far)
    
    # 정렬: BUY는 높은 가격이 앞, SELL은 낮은 가격이 앞
    ordered_prices = sorted(merged.keys(), reverse=(mode == 'BUY'))
    legs = [{"type": "LIMIT", "side": mode, "quantity": merged[p], "limit_price": float(p)} for p in ordered_prices]
    
    # 10) 설명 (TB 라벨 포함)
    if legs:
        first_leg_price = legs[0]["limit_price"]
        first_pct_out = abs(first_leg_price / current_price - 1.0)
    else:
        first_pct_out = first_pct
    
    mode_emoji = "📈" if mode == "SELL" else "📉"
    bias = "상승" if s > 0 else "하락" if s < 0 else "중립"
    
    tb_info = ""
    if tb_label:
        tb_emoji = {"UP_FIRST": "🔼", "DOWN_FIRST": "🔽", "TIMEOUT": "⏱️"}.get(tb_label, "")
        tb_info = f", TB={tb_emoji}{tb_label}"
        if iae_1_3 is not None:
            tb_info += f", IAE={iae_1_3:+.1%}"
    
    header = (
        f"{mode_emoji} {mode}({bias}:s={s:+.2f}, legs={len(legs)}, "
        f"1st={first_pct_out:.1%}, gain={gain_pct:+.1%}, qty={quantity}{tb_info})"
    )
    
    reason = (
        f"PM 신호 기반 적응형 래더. "
        f"방향성={bias}(s={s:+.2f}), 첫레그={first_pct_out:.1%} (틱/req/TB 반영), "
        f"분할={[l['quantity'] for l in legs]}"
    )
    
    description = f"{header}\n↳ {reason}"
    
    prices_str = ", ".join(f"{l['limit_price']:.2f}" for l in legs)
    logger.info(f"✅ [{mode}] {header}")
    logger.info(f"   prices=[{prices_str}]")
    
    return legs, description

