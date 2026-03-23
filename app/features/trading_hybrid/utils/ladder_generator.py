# app/features/trading_hybrid/utils/ladder_generator.py
from __future__ import annotations
import logging
import numpy as np
from app.features.trading_hybrid.utils.ticks import round_to_tick, get_tick_size
from app.features.trading_hybrid.policy.tuning import Tuning

logger = logging.getLogger(__name__)


def _qty_from_budget(price: float, budget: float, cap_qty: int | None = None) -> int:
    q = int(budget // max(price, 1e-6))
    return min(q, cap_qty) if cap_qty else q


def _first_leg_pct(mode: str, s: float, gain_pct: float, tuning: Tuning, current_price: float, market: str) -> float:
    """
    첫 레그 % 결정 (v2)
    - 틱단위 보정, 강bias 완화, req(유효할인) 바닥선 반영
    """
    base = getattr(tuning, "FIRST_LEG_BASE_PCT", getattr(tuning, "ADAPTIVE_BASE_STEP_PCT", 0.015))
    min_pct = getattr(tuning, "FIRST_LEG_MIN_PCT", 0.01)
    max_pct = getattr(tuning, "FIRST_LEG_MAX_PCT", getattr(tuning, "ADAPTIVE_MAX_STEP_PCT", 0.060))

    # s 영향 (대칭)
    if mode == "SELL":
        s_mult = np.interp(s, [-1.0, 1.0], [0.8, 2.4])
        gain_eff = gain_pct  # 수익↑ → 멀리
    else:
        s_mult = np.interp(s, [-1.0, 1.0], [2.4, 0.8])
        gain_eff = -gain_pct  # 손실(음수)↑ → 깊게

    # PnL 영향
    gain_scale = max(getattr(tuning, "ADAPTIVE_GAIN_SCALE", 0.10), 1e-6)
    gain_unit = np.clip(gain_eff / gain_scale, -1.0, 1.0)
    gain_mult = 1.0 + getattr(tuning, "FIRST_LEG_GAIN_WEIGHT", 0.6) * gain_unit

    # ATR 바닥선(옵션)
    atr_hint = float(getattr(tuning, "ATR_PCT_HINT", 0.0) or 0.0)
    atr_floor = atr_hint * getattr(tuning, "FIRST_LEG_ATR_WEIGHT", 0.5)

    # 초기
    pct = base * s_mult * gain_mult
    pct = max(pct, atr_floor, min_pct)
    pct = min(pct, max_pct)

    # 강bias 완화 (대칭)
    if mode == "BUY" and s > 0.7:
        pct *= np.interp(s, [0.7, 1.0], [1.0, 0.7])
    elif mode == "SELL" and s < -0.7:
        pct *= np.interp(abs(s), [0.7, 1.0], [1.0, 0.7])

    # req(유효할인) 바닥선: tuning 없으면 KR은 0.6% 기본, 그외 0
    req_default = 0.006 if market == "KR" else 0.0
    req_floor = float(getattr(tuning, "FIRST_LEG_REQ_FLOOR_PCT", req_default) or 0.0)
    if req_floor > 0:
        # BUY 강상승일 때 near-leg가 req 미달로 잘리는 문제 방지용: 최소 req 이상으로 올림
        if mode == "BUY" and s > 0.7:
            pct = max(pct, req_floor)
        # SELL 강하락일 때도 대칭 적용(원치 않으면 주석 처리 가능)
        if mode == "SELL" and s < -0.7:
            pct = max(pct, req_floor)

    # 틱단위 보정
    try:
        tick_size = get_tick_size(current_price, market)
        tick_pct = tick_size / current_price
        pct = max(round(pct / tick_pct) * tick_pct, tick_pct)
    except Exception:
        pass

    # 최종 가격 테스트(한 틱 밀림 방지)
    direction_sign = +1 if mode == "SELL" else -1
    test_price = round_to_tick(current_price * (1.0 + direction_sign * pct), market)
    if mode == "SELL" and test_price <= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market)
    if mode == "BUY" and test_price >= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market)

    pct_adjusted = abs(test_price / current_price - 1.0)
    return float(np.clip(pct_adjusted, min_pct, max_pct))


def generate_unified_adaptive_ladder(
    mode: str,
    p_up: float,
    p_down: float,
    exp_up: float,
    exp_down: float,
    gain_pct: float,
    current_price: float,
    quantity: int,
    market: str,
    tuning: Tuning
) -> tuple[list[dict], str]:

    mode = mode.upper()

    # 0) 가드
    min_lot = getattr(tuning, "MIN_LOT_QTY", 1) or 1
    if quantity < min_lot:
        return [], f"❌ {mode} 수량부족: qty={quantity}, min_lot={min_lot}"

    # 1) 방향성 점수 → s
    net_strength = (p_up * exp_up) - (p_down * abs(exp_down))
    s = np.clip(net_strength / tuning.ADAPTIVE_STRENGTH_SCALE, -1.0, 1.0)
    logger.debug(f"🧮 [{mode}] net_strength={net_strength:.4f}, s={s:.3f} "
                 f"(p_up={p_up:.2%}×{exp_up:.2%} - p_down={p_down:.2%}×{abs(exp_down):.2%})")

    # 2) 레그 개수
    n_legs_target = int(np.clip(
        tuning.ADAPTIVE_BASE_LEGS + abs(s) * tuning.ADAPTIVE_LEG_BOOST, 2, 6
    ))
    max_legs_by_qty = max(1, quantity // min_lot)
    n_legs = max(1, min(n_legs_target, max_legs_by_qty))
    
    # ✅ 2주인 경우 무조건 2개 LEG로 분할 (가격 차별화 보장)
    if quantity == 2 * min_lot:
        n_legs = 2
        logger.debug(f"✅ [{mode}] 정확히 2주 → 무조건 2개 LEG 강제 (qty={quantity}, min_lot={min_lot})")

    # 3) 방향 부호
    direction_sign = +1 if mode == "SELL" else -1

    # 4) 첫 레그 + 확장
    first_pct = _first_leg_pct(mode, s, gain_pct, tuning, current_price, market)

    # --- NEW: 틱/스프레드 하한 ---
    tick = get_tick_size(current_price, market)
    tick_pct = tick / current_price
    min_tick_gap = max(int(getattr(tuning, "MIN_TICK_GAP", 1)), 1)  # 레그 간 최소 k틱
    min_total_spread_pct_cfg = float(getattr(tuning, "MIN_TOTAL_SPREAD_PCT", 0.0) or 0.0)

    # 레그가 n개면 간격은 n-1개 → 최소 스프레드 = (n-1) * k틱
    min_total_spread_pct_ticks = (max(1, n_legs - 1)) * min_tick_gap * tick_pct
    # 사용자가 강제한 하한과 틱기반 하한 중 큰 것
    min_total_spread_pct = max(min_total_spread_pct_cfg, min_total_spread_pct_ticks)

    # 기존 방식으로 last_pct 생성
    if n_legs == 1:
        pct_steps = np.array([first_pct], dtype=float)
    else:
        # 강bias 구간은 과도한 확장 방지(1.2~2.0), 평시 1.5~3.0
        if (mode == "BUY" and s > 0.7) or (mode == "SELL" and s < -0.7):
            widen_lo, widen_hi = 1.2, 2.0
        else:
            widen_lo, widen_hi = 1.5, 3.0

        widen_mult = np.interp(abs(s), [0.0, 1.0], [widen_lo, widen_hi])
        last_pct = min(first_pct * widen_mult, getattr(tuning, "ADAPTIVE_MAX_STEP_PCT", 0.060))

        # --- NEW: 스프레드 하한 강제 ---
        # last_pct ≥ first_pct + 최소스프레드
        max_step_cap = getattr(tuning, "ADAPTIVE_MAX_STEP_PCT", 0.060)
        last_pct = max(last_pct, first_pct + min_total_spread_pct)
        last_pct = min(last_pct, max_step_cap)

        pct_steps = np.geomspace(first_pct, last_pct, n_legs).astype(float)

    # 5) 가중 분할
    decay_indices = np.linspace(0, getattr(tuning, "ADAPTIVE_FRAC_ALPHA", 1.25), n_legs)
    fracs = np.exp(-decay_indices)
    if mode == "SELL" and s > 0.3:
        fracs = fracs[::-1]
    fracs = fracs / np.sum(fracs)

    # 6) 정수 수량화
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

    assert int(np.sum(base_alloc)) == int(quantity), f"분할 합계 불일치: {np.sum(base_alloc)} != {quantity}"

    # 7) 레그 생성(라운딩) + 같은 가격 병합
    raw_legs = []
    for qty_i, pct in zip(base_alloc.tolist(), pct_steps.tolist()):
        price_raw = current_price * (1.0 + direction_sign * pct)
        price = round_to_tick(price_raw, market)
        raw_legs.append((float(price), int(qty_i)))

    # 동일가격 합산(틱 라운딩으로 겹치는 경우)
    merged = {}
    for price, q in raw_legs:
        merged[price] = merged.get(price, 0) + q

    # --- NEW: 라운딩 후 중복 방지 리트라이 ---
    # 라운딩으로 유니크 가격 수가 줄었으면 last_pct를 조금씩 키워 다시 생성
    if n_legs >= 2 and len(merged) < n_legs:
        attempts = 0
        max_step_cap = getattr(tuning, "ADAPTIVE_MAX_STEP_PCT", 0.060)
        need_gap_ticks = max(1, getattr(tuning, "MIN_TICK_GAP", 1))

        while attempts < 3 and len(merged) < n_legs and first_pct < max_step_cap:
            # last_pct를 '부족한 레그 수 × k틱'만큼 더 벌림
            bump = (n_legs - len(merged)) * need_gap_ticks * (tick_pct * 1.2)  # 여유 20%
            last_pct_try = min(first_pct + (last_pct - first_pct) + bump, max_step_cap)

            # 재계산
            pct_steps = np.geomspace(first_pct, last_pct_try, n_legs).astype(float)
            raw_legs = []
            for qty_i, pct in zip(base_alloc.tolist(), pct_steps.tolist()):
                price_raw = current_price * (1.0 + direction_sign * pct)
                price = round_to_tick(price_raw, market)
                raw_legs.append((float(price), int(qty_i)))

            merged = {}
            for price, q in raw_legs:
                merged[price] = merged.get(price, 0) + q

            attempts += 1

        # 그래도 부족하면 한 틱 차 보조 복구/축소
        if len(merged) < n_legs:
            # 가능하면 1틱씩 더 멀리 밀어 유니크 확보
            ordered_tmp = sorted(list(merged.items()), key=lambda x: x[0], reverse=(mode == "BUY"))
            uniq_prices = []
            uniq = {}
            for price, q in ordered_tmp:
                if uniq_prices and abs(price - uniq_prices[-1]) < need_gap_ticks * tick:
                    # 더 깊게(SELL은 위로, BUY는 아래로) 한 틱 밀기
                    shifted = round_to_tick(price + (tick if mode == "SELL" else -tick), market)
                    price = shifted
                uniq_prices.append(price)
                uniq[price] = uniq.get(price, 0) + q
            merged = uniq

            # ✅ 2주인 경우 반드시 2개 가격 확보 (가격 차별화 필수)
            if quantity == 2 * min_lot and len(merged) < 2:
                logger.warning(f"⚠️ [{mode}] 2주인데 유니크 가격 1개만 생성됨 → 강제 분리")
                prices_list = sorted(list(merged.keys()), reverse=(mode == "BUY"))
                if len(prices_list) == 1:
                    base_price = prices_list[0]
                    # 한 틱씩 벌림
                    if mode == "BUY":
                        price1 = base_price
                        price2 = round_to_tick(base_price - tick, market)
                    else:
                        price1 = base_price
                        price2 = round_to_tick(base_price + tick, market)
                    
                    # 2주를 1:1로 분할
                    merged = {float(price1): min_lot, float(price2): min_lot}
                    logger.info(f"✅ [{mode}] 2주 강제 분리 완료: {price1:.2f}, {price2:.2f}")
            
            # 여전히 유니크가 모자라면 레그 수를 축소(틱 격자 한계)
            elif len(merged) < 2:
                logger.warning("⚠️ 틱 격자 한계로 유니크 가격 확보 실패 → 레그 수 축소")

    # 정렬: BUY는 높은 가격이 앞(근처→깊게), SELL은 낮은 가격이 앞(근처→멀리)
    ordered_prices = sorted(merged.keys(), reverse=(mode == "BUY"))
    legs = [{"type": "LIMIT", "side": mode, "quantity": merged[p], "limit_price": float(p)} for p in ordered_prices]

    # 8) 설명(WHY) - 실제 첫 레그 %로 표기 동기화
    if legs:
        first_leg_price = legs[0]["limit_price"]
        first_pct_out = abs(first_leg_price / current_price - 1.0)
    else:
        first_pct_out = first_pct  # fallback

    mode_emoji = "📈" if mode == "SELL" else "📉"
    bias = "상승" if s > 0 else "하락" if s < 0 else "중립"
    style = (
        "탐욕형 매도" if (mode == "SELL" and s > 0)
        else "보수형 매도" if (mode == "SELL" and s <= 0)
        else "공격형 매수" if (mode == "BUY" and s < 0)
        else "보수형 매수"
    )

    header = (
        f"{mode_emoji} {mode}({bias}:s={s:+.2f}, legs={len(legs)}, "
        f"1st={first_pct_out:.1%}, gain={gain_pct:+.1%}, qty={quantity})"
    )
    reason = (
        f"p_up={p_up:.0%}(+{exp_up*100:.1f}%) vs "
        f"p_down={p_down:.0%}({exp_down*100:.1f}%) → {bias} 우세. "
        f"{style}: 첫레그={first_pct_out:.1%} (틱/req 반영), "
        f"{'할인폭 확대' if mode=='BUY' and s<0 else '목표가 간격 확대' if mode=='SELL' and s>0 else '보수적 분할'}."
        f" 분할={ [l['quantity'] for l in legs] }"
    )
    description = f"{header}\n↳ 이유: {reason}"

    prices_str = ", ".join(f"{l['limit_price']:.2f}" for l in legs)
    logger.info(f"✅ [{mode}] {header}")
    logger.info(f"   prices=[{prices_str}]")

    return legs, description
