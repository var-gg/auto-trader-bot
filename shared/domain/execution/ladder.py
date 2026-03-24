from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Dict, List, Sequence, Tuple

from shared.domain.models import LadderLeg, MarketCode, OrderPlan, OrderType, Side, SignalCandidate


def qty_from_budget(price: float, budget: float, cap_qty: int | None = None) -> int:
    q = int(float(budget) // max(float(price), 1e-6))
    return min(q, cap_qty) if cap_qty else q


def _interp(x: float, xp0: float, xp1: float, fp0: float, fp1: float) -> float:
    if xp1 == xp0:
        return fp0
    ratio = (x - xp0) / (xp1 - xp0)
    return fp0 + ratio * (fp1 - fp0)


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _exp(v: float) -> float:
    import math
    return math.exp(v)


def _kr_tick_size(price: float) -> float:
    if price < 1000:
        return 1.0
    if price < 5000:
        return 5.0
    if price < 10000:
        return 10.0
    if price < 50000:
        return 50.0
    if price < 100000:
        return 100.0
    if price < 500000:
        return 500.0
    return 1000.0


def get_tick_size(price: float, market: MarketCode | str) -> float:
    return _kr_tick_size(price) if str(market) == "MarketCode.KR" or str(market) == "KR" else 0.01


def round_to_tick(price: float, market: MarketCode | str, side: str | None = None) -> float:
    step = _kr_tick_size(price) if str(market) == "MarketCode.KR" or str(market) == "KR" else 0.01
    step_dec = Decimal(str(step))
    price_dec = Decimal(str(price))
    if side == "BUY":
        rounded = (price_dec / step_dec).quantize(Decimal("1"), rounding=ROUND_DOWN)
    elif side == "SELL":
        rounded = (price_dec / step_dec).quantize(Decimal("1"), rounding=ROUND_UP)
    else:
        rounded = (price_dec / step_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(rounded * step_dec)


def _first_leg_pct(mode: str, s: float, gain_pct: float, tuning: Dict, current_price: float, market: str, tb_label: str | None = None, iae_1_3: float | None = None, has_long_recommendation: bool = False) -> float:
    base = tuning.get("FIRST_LEG_BASE_PCT", tuning.get("ADAPTIVE_BASE_STEP_PCT", 0.010))
    min_pct = tuning.get("FIRST_LEG_MIN_PCT", 0.005)
    max_pct = tuning.get("FIRST_LEG_MAX_PCT", tuning.get("ADAPTIVE_MAX_STEP_PCT", 0.060))
    if mode == "SELL":
        s_mult = _interp(s, -1.0, 1.0, 0.8, 2.4)
        gain_eff = gain_pct
    else:
        s_mult = _interp(s, -1.0, 1.0, 2.4, 0.8)
        gain_eff = -gain_pct
    gain_scale = max(tuning.get("ADAPTIVE_GAIN_SCALE", 0.10), 1e-6)
    gain_unit = _clip(gain_eff / gain_scale, -1.0, 1.0)
    gain_mult = 1.0 + tuning.get("FIRST_LEG_GAIN_WEIGHT", 0.6) * gain_unit
    atr_hint = float(tuning.get("ATR_PCT_HINT", 0.0) or 0.0)
    atr_floor = atr_hint * tuning.get("FIRST_LEG_ATR_WEIGHT", 0.5)
    pct = min(max(base * s_mult * gain_mult, atr_floor, min_pct), max_pct)
    if mode == "BUY" and s > 0.7:
        pct *= _interp(s, 0.7, 1.0, 1.0, 0.7)
    elif mode == "SELL" and s < -0.7:
        pct *= _interp(abs(s), 0.7, 1.0, 1.0, 0.7)
    if mode == "BUY" and tb_label and iae_1_3 is not None:
        if tb_label == "DOWN_FIRST" and iae_1_3 < -0.01:
            iae_penalty = min(abs(iae_1_3) * 0.5, 0.08)
            pct = pct * (1.0 + iae_penalty)
    elif mode == "SELL" and tb_label and iae_1_3 is not None:
        if tb_label == "UP_FIRST" and iae_1_3 > 0.01:
            pct = pct * 0.85
    req_default = 0.010 if market == "KR" else 0.0
    req_floor = float(tuning.get("FIRST_LEG_REQ_FLOOR_PCT", req_default) or 0.0)
    if req_floor > 0:
        if mode == "BUY" and s > 0.7:
            pct = max(pct, req_floor)
        if mode == "SELL" and s < -0.7:
            pct = max(pct, req_floor)
    if mode == "BUY" and has_long_recommendation:
        pct = max(pct * 0.85, min_pct)
    tick_size = get_tick_size(current_price, market)
    tick_pct = tick_size / current_price
    pct = max(round(pct / tick_pct) * tick_pct, tick_pct)
    direction_sign = +1 if mode == "SELL" else -1
    test_price = round_to_tick(current_price * (1.0 + direction_sign * pct), market, side=mode)
    if mode == "SELL" and test_price <= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market, side=mode)
    if mode == "BUY" and test_price >= current_price:
        test_price = round_to_tick(current_price * (1.0 + direction_sign * (pct + 1e-4)), market, side=mode)
    pct_adjusted = abs(test_price / current_price - 1.0)
    return _clip(pct_adjusted, min_pct, max_pct)


def _geomspace(start: float, stop: float, num: int) -> List[float]:
    import math
    if num == 1:
        return [float(start)]
    ratio = (stop / start) ** (1.0 / (num - 1)) if start > 0 and stop > 0 else 1.0
    return [float(start * (ratio ** i)) for i in range(num)]


def generate_pm_ladder(candidate: SignalCandidate, quantity: int, market: str, tuning: Dict, *, gain_pct: float = 0.0, side: Side = Side.BUY) -> Tuple[List[LadderLeg], str]:
    mode = side.value
    min_lot = tuning.get("MIN_LOT_QTY", 1) or 1
    if quantity < min_lot:
        return [], f"{mode} qty too small"
    s = float(abs(candidate.signal_strength) if mode == "BUY" else candidate.signal_strength)
    n_legs_target = int(_clip(tuning.get("ADAPTIVE_BASE_LEGS", 3) + abs(s) * tuning.get("ADAPTIVE_LEG_BOOST", 1.0), 2, 6))
    max_legs_by_qty = max(1, quantity // min_lot)
    n_legs = max(1, min(n_legs_target, max_legs_by_qty))
    if quantity == 2 * min_lot:
        n_legs = 2
    current_price = float(candidate.current_price or 0.0)
    if current_price <= 0:
        return [], f"{mode} invalid current_price"
    first_pct = _first_leg_pct(
        mode,
        s,
        gain_pct,
        tuning,
        current_price,
        market,
        candidate.outcome_label.value if candidate.outcome_label else None,
        candidate.diagnostics.get("iae_1_3") if candidate.diagnostics else None,
        bool(candidate.provenance.get("has_long_recommendation", False)),
    )
    tick = get_tick_size(current_price, market)
    tick_pct = tick / current_price
    min_tick_gap = max(int(tuning.get("MIN_TICK_GAP", 1)), 1)
    min_total_spread_pct = max(float(tuning.get("MIN_TOTAL_SPREAD_PCT", 0.0) or 0.0), max(1, n_legs - 1) * min_tick_gap * tick_pct)
    req_gap = float(tuning.get("MIN_FIRST_LEG_GAP_PCT", 0.03) or 0.0)
    strict_gap = bool(tuning.get("STRICT_MIN_FIRST_GAP", True))
    max_step_cap_cfg = float(tuning.get("ADAPTIVE_MAX_STEP_PCT", 0.060) or 0.060)
    if n_legs == 1:
        pct_steps = [first_pct]
        effective_max_cap = max_step_cap_cfg
    else:
        if (mode == "BUY" and s > 0.7) or (mode == "SELL" and s < -0.7):
            widen_lo, widen_hi = 1.2, 2.0
        else:
            widen_lo, widen_hi = 1.5, 3.0
        widen_mult = _interp(abs(s), 0.0, 1.0, widen_lo, widen_hi)
        last_pct_candidate = first_pct * widen_mult
        last_pct_candidate = max(last_pct_candidate, first_pct + min_total_spread_pct)
        last_pct_candidate = max(last_pct_candidate, first_pct + req_gap)
        effective_max_cap = max_step_cap_cfg if not strict_gap else max(max_step_cap_cfg, last_pct_candidate)
        last_pct = min(last_pct_candidate, effective_max_cap)
        pct_steps = _geomspace(first_pct, last_pct, n_legs)
    alpha = float(tuning.get("ADAPTIVE_FRAC_ALPHA", 1.25))
    decay_indices = [alpha * i / max(1, n_legs - 1) for i in range(n_legs)]
    fracs = [_exp(-idx) for idx in decay_indices]
    total_frac = sum(fracs) or 1.0
    fracs = [f / total_frac for f in fracs]
    base_alloc = [min_lot for _ in range(n_legs)]
    remaining = quantity - sum(base_alloc)
    if remaining < 0:
        base_alloc = [quantity]
        remaining = 0
    if remaining > 0:
        extra_float = [f * remaining for f in fracs]
        extra_int = [int(v) for v in extra_float]
        base_alloc = [base_alloc[i] + extra_int[i] for i in range(len(base_alloc))]
        rem2 = remaining - sum(extra_int)
        if rem2 > 0:
            remainders = sorted(range(len(extra_float)), key=lambda i: -(extra_float[i] - extra_int[i]))
            for idx in remainders[:rem2]:
                base_alloc[idx] += 1
    direction_sign = +1 if mode == "SELL" else -1
    merged: Dict[float, int] = {}
    for qty_i, pct in zip(base_alloc, pct_steps):
        price = round_to_tick(current_price * (1.0 + direction_sign * pct), market, side=mode)
        merged[float(price)] = merged.get(float(price), 0) + int(qty_i)
    if n_legs >= 2 and merged:
        if mode == "BUY":
            p_first = max(merged.keys())
            p_far = min(merged.keys())
        else:
            p_first = min(merged.keys())
            p_far = max(merged.keys())
        achieved_gap = abs(p_far - p_first) / current_price
        required_gap = max(req_gap - (tick_pct * 0.5), 0.0)
        if achieved_gap + 1e-12 < required_gap:
            first_pct_real = abs(p_first / current_price - 1.0)
            target_pct = first_pct_real + req_gap
            if not strict_gap:
                target_pct = min(target_pct, max_step_cap_cfg)
            direction_sign2 = +1 if mode == "SELL" else -1
            target_price = round_to_tick(current_price * (1.0 + direction_sign2 * target_pct), market, side=mode)
            step = tick * max(1, int(tuning.get("MIN_TICK_GAP", 1)))
            def violates(price: float) -> bool:
                for qprice in merged.keys():
                    if qprice == p_far:
                        continue
                    if abs(price - qprice) < step:
                        return True
                if mode == "BUY" and price >= p_far:
                    return True
                if mode == "SELL" and price <= p_far:
                    return True
                return False
            guard = 0
            while violates(target_price) and guard < 50:
                target_price = round_to_tick(target_price + (-tick if mode == "BUY" else +tick), market, side=mode)
                guard += 1
            final_gap = abs(target_price - p_first) / current_price
            if final_gap + 1e-12 >= required_gap or strict_gap:
                merged[target_price] = merged.pop(p_far)

    ordered_prices = sorted(merged.keys(), reverse=(mode == "BUY"))
    legs = [
        LadderLeg(
            leg_id=f"{candidate.symbol.lower()}-{mode.lower()}-{i+1}",
            side=side,
            order_type=OrderType.LIMIT,
            quantity=merged[p],
            limit_price=float(p),
        )
        for i, p in enumerate(ordered_prices)
    ]
    desc = f"{mode} ladder legs={len(legs)}"
    return legs, desc


def allocate_symbol_budgets(candidates: Sequence[SignalCandidate], swing_cap_cash: float, market: str, tuning: Dict) -> Tuple[List[SignalCandidate], Dict[int, float], List[Dict[str, str]]]:
    s_total = float(swing_cap_cash)
    n = len(candidates)
    if n == 0 or s_total <= 0:
        return [], {}, []
    soft_cap = float(tuning["SOFT_CAP_MULT"]) * s_total / n
    hard_cap_global = float(tuning["MAX_SYMBOL_WEIGHT"]) * s_total
    scored = []
    for cand in candidates:
        tid = cand.ticker_id
        price = float(cand.current_price or 0.0)
        atr_pct = float(cand.atr_pct or 0.05)
        signal = float(cand.signal_strength or 0.0)
        if not tid or price <= 0 or signal <= 0:
            continue
        hard_cap = min(soft_cap, hard_cap_global)
        required_disc = max(0.012, 0.4 * atr_pct)
        first_limit_est = round_to_tick(price * (1.0 - required_disc), market)
        required = int(tuning["MIN_LADDER_LEGS"]) * first_limit_est
        g = hard_cap / max(required, 1e-9)
        unit_risk = max(price * atr_pct, 1e-9)
        base_priority = (signal ** float(tuning["RP_BETA"])) / (unit_risk ** float(tuning["RP_ALPHA"]))
        priority = base_priority * (min(1.0, g) ** float(tuning["GRANULARITY_PENALTY_POW"]))
        scored.append({"cand": cand, "tid": tid, "price": price, "priority": priority, "hard_cap": hard_cap, "required": required})
    cands = sorted(scored, key=lambda x: x["price"], reverse=True)
    while cands:
        min_sum = sum(min(it["required"], it["hard_cap"]) for it in cands)
        if min_sum <= s_total:
            break
        cands.pop(0)
    cands.sort(key=lambda x: (-x["priority"], x["price"]))
    budget_map: Dict[int, float] = {}
    remaining = s_total
    for item in cands:
        need = min(item["required"], item["hard_cap"])
        if remaining >= need:
            budget_map[item["tid"]] = need
            remaining -= need
    if remaining > 0:
        alloc_items = [it for it in cands if it["tid"] in budget_map]
        psum = sum(it["priority"] for it in alloc_items) or 1.0
        for it in alloc_items:
            room = it["hard_cap"] - budget_map[it["tid"]]
            if room <= 0:
                continue
            add = min(room, remaining * (it["priority"] / psum))
            if add > 0:
                budget_map[it["tid"]] += add
                remaining -= add
    selected = [it["cand"] for it in cands if it["tid"] in budget_map]
    skipped = [{"ticker_id": str(it["tid"]), "code": "BUDGET_ALLOCATION"} for it in scored if it["tid"] not in budget_map]
    return selected, budget_map, skipped
