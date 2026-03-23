# app/features/trading_hybrid/policy/tuning.py
"""
튜닝 파라미터 중앙 관리
시장별 미세 조정이 필요한 파라미터들을 관리합니다.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple

@dataclass
class Tuning:
    """
    하이브리드 트레이딩 튜닝 파라미터
    """
    # ============================================================
    # 1. 예산 관리
    # ============================================================
    CASH_BUFFER_RATIO: float = 0.12
    # 사용처: compute_bucket_caps (hybrid_trader_engine.py)
    # 기능: 총 buying_power에서 버퍼로 남겨둘 비율 (12%)
    #      swing/intraday 예산 계산 시 실제 사용 가능한 현금 = buying_power × (1 - buffer_ratio)

    # ============================================================
    # 1-1. GARP (Granularity-Aware Risk Parity) 예산 분배
    # ============================================================
    SOFT_CAP_MULT: float = 1.5
    # 사용처: allocate_symbol_budgets (open_session_service.py)
    # 기능: 균등 몫 대비 상한 배수 (1.5×)
    #      한 종목의 기본 예산 상한 = (total_budget / N) × SOFT_CAP_MULT

    MAX_SYMBOL_WEIGHT: float = 0.30
    # 사용처: allocate_symbol_budgets (open_session_service.py)
    # 기능: 종목별 최대 비중 (스윙 예산 대비 30%)
    #      한 종목이 차지할 수 있는 최대 금액 = total_budget × MAX_SYMBOL_WEIGHT
    #      ⬆ 0.12 → 0.30으로 상향: 고가 제외 로직이 효과적으로 작동하도록

    MIN_LADDER_LEGS: int = 3
    # 사용처: allocate_symbol_budgets, plan_pattern_open_buy_orders (open_session_service.py)
    # 기능: 최소 사다리 레그 개수 (3개)
    #      매수·분할·청산 용이성 확보를 위한 최소선
    #      비싼 종목은 이 조건을 만족 못하면 자동 스킵

    ALLOW_MICRO_LADDER_OPEN: bool = True
    # 사용처: plan_pattern_open_buy_orders (open_session_service.py:355)
    # 기능: 장초 오프닝에서 1레그(마이크로 레그) 허용 여부 (True)
    #      False면 qty_cap < MIN_LADDER_LEGS인 종목은 스킵
    #      True면 1레그라도 허용해 0건 생성 방지

    GRANULARITY_PENALTY_POW: float = 2.0
    # 사용처: allocate_symbol_budgets (open_session_service.py)
    # 기능: 그레뉼러리티 패널티 지수 (2.0)
    #      affordability_ratio(g) < 1이면 우선순위에 g^γ 패널티 적용
    #      높을수록 비싼 종목에 강한 패널티

    RP_ALPHA: float = 1.0
    # 사용처: allocate_symbol_budgets (open_session_service.py)
    # 기능: 리스크 패리티 - 단위 위험(price×ATR%) 역수 지수
    #      우선순위 계산 시 unit_risk^(-α)로 반영

    RP_BETA: float = 1.0
    # 사용처: allocate_symbol_budgets (open_session_service.py)
    # 기능: 리스크 패리티 - 시그널 스코어 지수
    #      우선순위 계산 시 score^β로 반영

    # ============================================================
    # 2. [미사용] Open greedy 관련 (레거시)
    # ============================================================
    OPEN_LEG_COUNT_MIN: int = 2        # ❌ 사용되지 않음 - 삭제 고려
    OPEN_LEG_COUNT_MAX: int = 4        # ❌ 사용되지 않음 - 삭제 고려
    OPEN_MIN_DISCOUNT: float = 0.01    # ❌ 사용되지 않음 - 삭제 고려
    OPEN_LEG_MIN_GAP: float = 0.02     # ❌ 사용되지 않음 - 삭제 고려

    # ============================================================
    # 3. [미사용] Intraday signals 레거시
    # ============================================================
    VALID_SIGNAL_THRESHOLD: float = 0.015    # ❌ 사용되지 않음 - 삭제 고려 (prob*expected_move%)
    SUSPEND_PROB_DOWN: float = 0.60          # ❌ 사용되지 않음 - 삭제 고려

    # ============================================================
    # 4. 리프라이싱 규칙 - BUY 레그 (상승장에서 매수호가 올림)
    # ============================================================
    REPRICE_TRIGGER_DISCOUNT_PCT: float = 0.010
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:225)
    # 기능: 현재가 대비 1% 이상 할인(gap)이 벌어져 있으면 BUY 레그를 위로 올림 (래칫업)

    REPRICE_STEP_UP_PCT: float = 0.003
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:214, 216)
    # 기능: BUY 레그 올릴 때 기본 스텝 (+0.3%p), 동적 보정: base + REPRICE_DYNAMIC_MULT × exp_up/down

    REPRICE_STEP_UP_PCT_MAX: float = 0.006
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:214, 229, 259, 261)
    # 기능: BUY 레그 올림 상한 (+0.6%p) - 현재가에 너무 가까워지지 않도록 제한

    # ============================================================
    # 5. 리프라이싱 규칙 - SELL 레그 래칫 업 (상승장에서 익절호가 올림)
    # ============================================================
    REPRICE_SELL_TRIGGER_PREMIUM_PCT: float = 0.010
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:242)
    # 기능: 현재가 대비 프리미엄이 +1.0% 이하로 붙어있으면 SELL 호가를 위로 올림 (익절가 상향)

    REPRICE_SELL_STEP_UP_PCT: float = 0.003
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:243)
    # 기능: SELL 레그 위로 올릴 때 기본 스텝 (+0.3%p)

    REPRICE_SELL_STEP_UP_PCT_MAX: float = 0.009
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:243)
    # 기능: SELL 레그 올림 상한 (+0.9%p)

    # ============================================================
    # 6. 리프라이싱 규칙 - SELL 레그 래칫 다운 (하락장에서 익절호가 내림)
    # ============================================================
    REPRICE_SELL_STEP_DOWN_PCT: float = 0.003
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:215)
    # 기능: 하락 모멘텀 시 SELL 호가를 낮춰서 체결 유도 (-0.3%p)

    REPRICE_SELL_STEP_DOWN_PCT_MAX: float = 0.012
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:215)
    # 기능: SELL 레그 내림 상한 (-1.2%p)

    REPRICE_DYNAMIC_MULT: float = 0.5
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:214-216, 243)
    # 기능: 동적 스텝 계산 시 기대변화율 반영 계수: dyn_step = base + (exp_move × mult)

    # ============================================================
    # 7. 익절 전략 (패턴 기반)
    # ============================================================
    TAKE_PROFIT_ENABLE: bool = True
    # ❌ 사용되지 않음 - 삭제 고려

    TAKE_PROFIT_MIN_GAIN_PCT: float = 0.01
    # ⚠️ 주석 처리됨 (open_session_service.py:422) - 현재 미사용

    PATTERN_TAKE_PROFIT_ENABLE: bool = True
    # 사용처: plan_pattern_take_profit_orders (open_session_service.py:381)
    # 기능: 패턴 기반 익절 활성화 여부 (True면 유사패턴 분석 후 매도 사다리 생성)

    PATTERN_SIMILARITY_LOOKBACK: int = 10
    # 사용처: plan_pattern_open_buy_orders, plan_pattern_take_profit_orders (open_session_service.py:233, 431)
    # 기능: 유사 패턴 검색 시 과거 며칠 데이터를 볼지 (10일)

    PATTERN_SIMILARITY_TOP_K: int = 10
    # 사용처: plan_pattern_open_buy_orders, plan_pattern_take_profit_orders (open_session_service.py:234, 432)
    # 기능: 유사도 높은 상위 K개 패턴 선택 (10개)

    PATTERN_MIN_SIMILARITY_COUNT: int = 3
    # 사용처: plan_pattern_open_buy_orders, plan_pattern_take_profit_orders (open_session_service.py:240, 438)
    # 기능: 최소 유사 패턴 수 - 이보다 적으면 신뢰도 부족으로 스킵 (3개)

    PATTERN_MIN_STRENGTH: float = 0.005
    # ❌ 사용되지 않음 - 삭제 고려

    # ============================================================
    # 7-1. 데이터 기반 연속형 매도 강도 (수동 모드 스위치 없음)
    # ============================================================
    DATA_DRIVEN_SELL_ENABLE: bool = True
    # 사용처: plan_pattern_take_profit_orders (open_session_service.py)
    # 기능: 패턴/손익 기반 연속형 sell intensity 적용 여부

    SELL_INTENSITY_BASE: float = 0.0
    # 기능: sell intensity 로짓 기본값 (sigmoid 입력)

    SELL_INTENSITY_GAIN_WEIGHT: float = 2.2
    # 기능: 수익률(gain_pct) 영향 가중치 (이익실현 압력)

    SELL_INTENSITY_TREND_WEIGHT: float = 3.0
    # 기능: (down_strength - up_strength) 영향 가중치

    SELL_INTENSITY_DRAWDOWN_WEIGHT: float = 2.0
    # 기능: 음수 손익(급락/과매도 맥락)에서 매도 강도 완화 가중치

    SELL_INTENSITY_GAIN_SCALE: float = 0.03
    # 기능: gain_pct tanh 스케일 (±3% 수준에서 민감)

    SELL_INTENSITY_STRENGTH_SCALE: float = 0.015
    # 기능: up/down strength 차이 정규화 스케일

    SELL_INTENSITY_DRAWDOWN_SCALE: float = 0.05
    # 기능: drawdown 완화 정규화 스케일 (5%)

    SELL_INTENSITY_MIN: float = 0.15
    # 기능: 최소 매도 강도 (15%는 항상 분할청산 가능)

    SELL_INTENSITY_MAX: float = 1.0
    # 기능: 최대 매도 강도 (전량 허용 상한)

    SELL_INTENSITY_MIN_QTY: int = 1
    # 기능: 강도 축소 후에도 최소 주문 수량

    # ============================================================
    # 8. 통합형 적응적 사다리 생성 (매수/매도 공용)
    # ============================================================
    ADAPTIVE_STRENGTH_SCALE: float = 0.030
    # 사용처: _pred_to_strength (intraday_session_service.py:34), generate_unified_adaptive_ladder (ladder_generator.py:106)
    # 기능: net_strength를 s(-1~+1)로 정규화하는 스케일 (3%)
    #      s = net_strength / ADAPTIVE_STRENGTH_SCALE

    ADAPTIVE_BASE_STEP_PCT: float = 0.015
    # 사용처: _first_leg_pct (ladder_generator.py:21) - FIRST_LEG_BASE_PCT 없을 때 대체값으로 사용
    # 기능: 사다리 레그 간격 기본값 (1.5%)

    ADAPTIVE_MAX_STEP_PCT: float = 0.060
    # 사용처: ladder_generator.py (23, 133) - FIRST_LEG_MAX_PCT 없을 때 대체값
    # 기능: 사다리 최대 간격 (6%)

    ADAPTIVE_BASE_LEGS: int = 3
    # 사용처: generate_unified_adaptive_ladder (ladder_generator.py:112)
    # 기능: 기본 레그 수, 신호 강도에 따라 증가: n_legs = BASE + |s| × LEG_BOOST

    ADAPTIVE_LEG_BOOST: int = 3
    # 사용처: generate_unified_adaptive_ladder (ladder_generator.py:112)
    # 기능: 신호 강도당 추가 레그 수 (최대 3개 추가)

    ADAPTIVE_FRAC_ALPHA: float = 3.0
    # 사용처: generate_unified_adaptive_ladder (ladder_generator.py:137)
    # 기능: 수량 분배 decay 곡률 - 높을수록 첫 레그에 수량 집중

    ADAPTIVE_GAIN_SCALE: float = 0.10
    # 사용처: _first_leg_pct (ladder_generator.py:34)
    # 기능: 수익/손실 반영 스케일 (10%) - gain_unit = gain_pct / ADAPTIVE_GAIN_SCALE

    ADAPTIVE_STEP_SCALE_RANGE: Tuple[float, float] = (0.3, 2.0)
    # ⚠️ 현재 ladder_generator.py에서 직접 참조되지 않음 - 미래 확장용 또는 미사용

    # ============================================================
    # 9. [미사용] 5분봉 패턴 단타 전용 (레거시)
    # ============================================================
    INTRA_PATTERN_ENABLE: bool = True          # ❌ 사용되지 않음
    INTRA_LOOKBACK_DAYS: int = 30              # ❌ 사용되지 않음
    INTRA_TOP_K: int = 60                      # ❌ 사용되지 않음
    INTRA_MIN_SIM_COUNT: int = 6               # ❌ 사용되지 않음
    INTRA_HORIZON_BARS: Tuple[int, int] = (3, 6)  # ❌ 사용되지 않음

    # ============================================================
    # 10. 장중 신호 강도 임계값
    # ============================================================
    INTRA_MIN_ABS_S: float = 0.25
    # 사용처: plan_intraday_actions, apply_rebalancing_rules (intraday_session_service.py:71, 151, 219, 251)
    # 기능: 신호 강도 |s| 최소 임계값 (0.25) - 이보다 약하면 "신호약함"으로 스킵/리밸런싱 제외

    # ============================================================
    # 11. [대부분 미사용] 장중 필수 할인/프리미엄 (레거시)
    # ============================================================
    INTRA_REQ_DISCOUNT_BASE: float = 0.004      # ❌ 사용되지 않음
    INTRA_REQ_DISCOUNT_ATR_MULT: float = 0.35   # ❌ 사용되지 않음
    
    INTRA_SELL_PREMIUM_BASE: float = 0.002
    # 사용처: apply_rebalancing_rules (intraday_session_service.py:276)
    # 기능: SELL 래칫다운 시 최소 프리미엄 바닥값 (0.2%)
    
    INTRA_SELL_PREMIUM_ATR_MULT: float = 0.25   # ❌ 사용되지 않음

    # ============================================================
    # 12. 장중 주문 제한
    # ============================================================
    INTRA_MAX_NEW_ORDERS_PER_CYCLE: int = 6
    # 사용처: plan_intraday_actions (intraday_session_service.py:132, 183)
    # 기능: 한 사이클당 생성할 수 있는 최대 신규 주문 수 (BUY + SELL 합계)

    # ============================================================
    # 13. 첫 레그 제어 (사다리의 첫 번째 호가 미세 조정)
    # ============================================================
    FIRST_LEG_BASE_PCT: float = 0.010
    # 사용처: _first_leg_pct (ladder_generator.py:21)
    # 기능: 첫 레그 간격 기본값 (1.0%)

    FIRST_LEG_MIN_PCT: float = 0.006
    # 사용처: _first_leg_pct (ladder_generator.py:22)
    # 기능: 첫 레그 최소 간격 (0.6%)

    FIRST_LEG_MAX_PCT: float = 0.050
    # 사용처: _first_leg_pct (ladder_generator.py:23)
    # 기능: 첫 레그 최대 간격 (5%)

    FIRST_LEG_GAIN_WEIGHT: float = 0.6
    # 사용처: _first_leg_pct (ladder_generator.py:36)
    # 기능: 수익/손실 영향 가중치 - 높을수록 손익에 민감하게 간격 조정

    FIRST_LEG_ATR_WEIGHT: float = 0.5
    # 사용처: _first_leg_pct (ladder_generator.py:40)
    # 기능: ATR 기반 바닥선 가중치 - 변동성 큰 종목은 간격 넓게

    FIRST_LEG_REQ_FLOOR_PCT: float = 0.0
    # 사용처: _first_leg_pct (ladder_generator.py:55)
    # 기능: 유효 할인 바닥선 (KR만 0.017 등 설정 가능) - 현재 0.0이면 시장별 기본값 사용

    # ============================================================
    # 14. 리스크 통제 (손절/시간정지)
    # ============================================================
    HARD_STOP_MIN: float = -5.0
    # 사용처: enforce_intraday_stops (risk_controller.py:107)
    # 기능: 포지션 손실률 -5% 이하면 즉시 손절 (hard stop)

    HARD_STOP_MAX: float = -3.5
    # 사용처: enforce_intraday_stops (risk_controller.py:109)
    # 기능: 포지션 손실률 -3.5% 이하 + 시간 초과 시 손절 (time + loss stop)

    TIME_STOP_MINUTES: int = 75
    # 사용처: enforce_intraday_stops (risk_controller.py:105)
    # 기능: 포지션 보유 시간이 75분 초과하면 HARD_STOP_MAX 적용

    RISK_CUT_SLIPPAGE_PCT: float = 0.004
    # 사용처: enforce_intraday_stops (risk_controller.py:128)
    # 기능: 손절 시 슬리피지 여유 (-0.4%) - 현재가보다 약간 낮게 주문해서 빠른 체결 유도

    # ============================================================
    # 15. 마감 청소 & 일일 손실 차단
    # ============================================================
    NEAR_CLOSE_MIN: int = 20
    # 사용처: is_near_close 호출 (hybrid_trader_engine.py:252)
    # 기능: 장 마감 N분 전 (20분) - 이때부터 청소 작업 시작

    NEAR_CLOSE_CLEANUP_ENABLED: bool = True
    # 사용처: near_close_cleanup 호출 (hybrid_trader_engine.py:254)
    # 기능: 마감 청소 기능 활성화 여부 (True면 미체결 주문 정리 등)

    DAILY_LOSS_BLOCK_PCT: float = -0.015
    # 사용처: block_daily_loss_symbols (hybrid_trader_engine.py:206)
    # 기능: 일일 손실률이 -1.5% 이하인 종목은 당일 추가 매수 차단

    @staticmethod
    def default_for_market(market: str) -> "Tuning":
        return Tuning()
