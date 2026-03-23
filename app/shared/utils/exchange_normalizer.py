# 야후파이낸스 거래소코드 우선으로 정규화
# - 입력이 표준명(NASDAQ/NYSE 등)이어도 야후코드로 변환 시도
# - 입력이 이미 야후코드(NMS/NYQ/ARCA/ASE/BATS/…/KOE/TSE/JPX/TYO)면 그대로 둠

STANDARD_TO_YF = {
    "NASDAQ": "NMS",
    "NYSE": "NYQ",
    "NYSEARCA": "ARCA",
    "AMEX": "ASE",
    "CBOE": "BATS",
    "TSE": "TSE",   # 일본 도쿄 증권거래소
    "JPX": "JPX",
    "TYO": "TYO",
    "KRX": "KOE",
}

def normalize_exchange_to_yf(ex: str | None) -> str | None:
    if not ex:
        return None
    e = ex.strip().upper()
    # 이미 야후코드면 그대로
    if e in {"NMS","NYQ","ARCA","ASE","BATS","TSE","JPX","TYO","KOE"}:
        return e
    # 표준명을 야후코드로
    return STANDARD_TO_YF.get(e, e)
