from decimal import Decimal, ROUND_HALF_UP

def _kr_tick_size(price: float) -> float:
    """한국거래소 호가 단위 (KOSPI/KOSDAQ 공통, ETF 미포함)"""
    if price < 1000:    return 1
    if price < 5000:    return 5
    if price < 10000:   return 10
    if price < 50000:   return 50
    if price < 100000:  return 100
    if price < 500000:  return 500
    return 1000

def round_to_tick(price: float, market: str) -> float:
    """시장별 호가 단위 반올림"""
    if market == "KR":
        step = _kr_tick_size(price)
        return float((Decimal(price) / Decimal(step)).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * Decimal(step))
    else:
        # 기본 0.01단위
        return float(Decimal(price).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

# ✅ 추가
def get_tick_size(price: float, market: str) -> float:
    """시장별 호가 단위 조회 (BUY/SELL 공용)"""
    if market == "KR":
        return _kr_tick_size(price)
    else:
        return 0.01
