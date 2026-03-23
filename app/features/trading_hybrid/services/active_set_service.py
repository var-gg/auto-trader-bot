# app/features/trading_hybrid/services/active_set_service.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session

def select_active_set(
    db: Session,
    market: str,
    recommendations: List[Dict[str, Any]],
    positions: List[Dict[str, Any]],
    pending: List[Dict[str, Any]],
    active_set_max: int = 12
) -> List[Dict[str, Any]]:
    """
    액티브셋 선정 (연산량/과매매 방지, 최대 12개)
    
    로직 (우선순위):
    1. 필수 포함: 보유중 종목 + 미체결 존재 종목
    2. 선택 포함: 스코어 상위 종목
       - 스코어 = 0.30 × pattern_p_up + 0.30 × swing_conf + 0.15 × liquidity
                 + 0.10 × dist_to_entry + 0.10 × near_leg + 0.05 × news_heat
    3. 최대 N개로 제한
    
    Note:
    - recommendations는 이미 패턴 유사도 필터(top_sim>0.7, p_up>0.8)를 통과한 종목만 포함
    - 각 추천에는 pattern_p_up, pattern_top_sim 등 패턴 정보가 포함됨
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
        recommendations: 추천 종목 리스트 (패턴 정보 포함)
        positions: 포지션 리스트
        pending: 미체결 주문 리스트
        active_set_max: 최대 종목 수 (기본값: 12)
    
    Returns:
        액티브셋 종목 리스트 (스코어 포함)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    pos_map = {p["ticker_id"]: p for p in positions}
    pen_map = {}
    for po in pending:
        pen_map.setdefault(po["ticker_id"], 0)
        pen_map[po["ticker_id"]] += 1

    def score(rec):
        """
        종목 스코어 계산 (0~1 범위)
        
        가중치:
        - 30%: 패턴 상승 확률 (p_up)
        - 30%: swing 추천 신뢰도
        - 15%: 유동성 순위
        - 10%: 진입가 근접도
        - 10%: 미체결 레그 존재 여부
        - 5%: 뉴스 히트 점수
        """
        pattern_p_up = float(rec.get("pattern_p_up", 0.8))  # 패턴 상승 확률
        swing_conf = float(rec.get("confidence_score", 0.6))
        liquidity_pct = float(rec.get("liquidity_rank_pct", 50)) / 100.0
        cur = float(rec.get("current_price", 0) or 0.0)
        ent = float(rec.get("entry_price", cur) or cur)
        dist_to_entry = abs((cur - ent) / cur) if cur else 0.0
        near_leg = 1.0 if pen_map.get(rec["ticker_id"]) else 0.0
        news_heat = min(float(rec.get("news_heat", 0.0)) / 10.0, 1.0)  # 0~10 정규화
        
        return (0.30*pattern_p_up + 0.30*swing_conf + 0.15*liquidity_pct + 
                0.10*dist_to_entry + 0.10*near_leg + 0.05*news_heat)
    
    # ===== 1) 필수 종목 (보유+미체결) vs 일반 종목 분류 =====
    must, normal = [], []
    for r in recommendations:
        r1 = dict(r)
        r1["score"] = score(r1)
        if r["ticker_id"] in pos_map or pen_map.get(r["ticker_id"]):
            must.append(r1)
        else:
            normal.append(r1)
    
    logger.info(f"🎯 액티브셋 분류: 필수={len(must)}개 (보유+미체결), 일반={len(normal)}개")

    # ===== 2) 일반 종목 스코어 정렬 =====
    normal.sort(key=lambda x: x["score"], reverse=True)
    
    # ===== 3) 결합: 필수 → 일반 (스코어 상위) =====
    seen = set()
    merged = []
    
    # 필수 종목 먼저 추가
    for r in must:
        if r["ticker_id"] not in seen:
            merged.append(r)
            seen.add(r["ticker_id"])
    
    # 일반 종목 스코어 상위부터 추가
    for r in normal:
        if r["ticker_id"] not in seen:
            merged.append(r)
            seen.add(r["ticker_id"])
    
    result = merged[:active_set_max]
    
    pattern_count = len([r for r in result if r.get("pattern_p_up")])
    logger.info(f"✅ 최종 액티브셋: {len(result)}개 선정 (패턴정보={pattern_count}개)")
    return result
