# Experiment Journal

## 실험 목록

| # | 날짜 | 제목 | 상태 | best equity | 비고 |
|---|------|------|------|-------------|------|
| 000 | 2026-04-04 | baseline (고정 오프셋) | done | 0.893 | Optuna 32t, trial#8 유일 feasible |
| 001 | 2026-04-05 | 분포 기반 지정가 v1 (앵커 테이블) | **done** | **1.0796** | 64/64 feasible, FLAG C+D 채택 |
| 002 | 2026-04-05 | KR 적재 + US/KR 비교 | done | 1.0941(통합) | US+8.5%, KR+12.3%, KR only best |
| 003 | 2026-04-05 | 매수 후보 스코어 + 사다리 + US/KR 분리자본 | **done** | **1.0930** | 합산+9.3%, 승률59.2%, 모든 연도 양수 |

## Idea Backlog (TODO)

- [ ] kernel_temperature 스윕 (8/12/16) — 낮추면 유사도 차이에 덜 민감, weight 분산
- [ ] top_k 확대 96->256 — ESS 증가, 분포 안정화 (seed 재생성 필요)
- [ ] IQR 기반 inner_density = (q75-q25)/interval_width — 중심 집중도 측정
- [ ] 매수가에 q25 활용 — q10보다 보수적인 하한
- [ ] 매도가에 q75 활용 — q90보다 보수적인 상한
- [ ] 국장(KR) 데이터 추가 — seed row에 market 컬럼 이미 있음
- [ ] holding period 제한 — d3/d5 미체결 시 시장가 청산
- [ ] 분포의 bimodality 감지 — 양봉 분포에서 중간값 매도 위험

### 속도/아키텍처 개선 (seed 재생성 96분 해소)
- [ ] **FAISS ANN 인덱스**: 사전 정규화된 벡터 + approximate nearest neighbor → brute-force O(N) → O(1)
- [ ] **rolling 정규화 캐시**: decision_date별 재정규화 대신 rolling window 기준 고정 → 정규화된 벡터를 미리 저장
- [ ] **event 정규화 사전 계산**: scaler 안 바뀌는 구간에서 이전 결과 재활용 (현재는 매번 재계산)
- [ ] **미래 결과에 시가대비 고가/저가 비율 추가**: 현재 after_cost_return만 있는데, 앵커시가 대비 d1~d5 low_ratio/high_ratio를 저장하면 매수/매도 지정가에 직접 매핑 가능
- [ ] **feature window 10일 실험**: 현재 66feature 중 캔들직접shape은 당일1봉, 나머지는 사전집계 통계. 10일 raw candle 직접 벡터화(40~50dim) vs 현재 66dim 성능 비교

---

## EXP-000: Baseline (고정 오프셋)

### 설정
- seed rows: 831,196 (optuna_eligible: 479)
- Optuna: 32 trials, frozen_seed_v1
- 매수가: `t1_open * (1 - buy_entry_offset_pct)` — 고정 오프셋
- 매도가: `sell_markup_pct` clipped by q90

### 결과
- feasible: 1/32 (trial #8)
- final_equity_ratio: 0.893 (-10.7%)
- params: execution_mode=ladder_v1, min_buy_score=-0.01, min_lower_bound=-0.01, min_member_ess=6.0, buy_entry_offset_pct=0.021, sell_markup_pct=0.004

### 문제 진단
- eligibility 기준(lower_bound > 0)이 831k seed 중 479개만 통과시킴
- 매수/매도 지정가가 분포와 무관한 고정값이라 체결 품질이 낮음

---

## EXP-001: 분포 기반 지정가 v1

### 가설
매수가/매도가를 유사앵커 미래가격 분포의 하단/상단에서 직접 도출하면,
고정 오프셋 대비 체결 품질이 향상되어 수익률이 개선된다.
분포의 비대칭성(skew)과 ESS 신뢰도를 플래그로 추가 검증한다.

### 변경 사항
1. seed row에 q25/q75 추가
2. eligibility: lower_bound > 0.0 -> > -0.03
3. 매수가 = `t1_open * (1 + blend(q10, lower_bound))` (분포 하단 기반)
4. 매도가 = `t1_open * (1 + blend(q90, q50))` (분포 상단 기반)
5. FLAG A: 매수 비대칭 보정 (하방 넓으면 더 깊게)
6. FLAG B: ESS 신뢰도 보정 (ESS 높으면 시가 가깝게)
7. FLAG C: 매도 비대칭 보정 (상방 좁으면 보수적)
8. FLAG D: uncertainty 감산 (불확실하면 일찍 익절)

### 파라미터 (Optuna 탐색 공간)

| 파라미터 | 타입 | 범위 | 목적 |
|---|---|---|---|
| buy_dist_blend | float | 0.0~1.0 | q10 vs lower_bound 블렌딩 |
| sell_dist_blend | float | 0.0~1.0 | q90 vs q50 블렌딩 |
| use_skew_adjust | bool | — | FLAG A: 매수 비대칭 보정 |
| skew_dampener | float | 0.0~0.5 | FLAG A 강도 |
| use_ess_tightening | bool | — | FLAG B: ESS 신뢰도 보정 |
| ess_cap | float | 5~50 | FLAG B ESS 정규화 상한 |
| tighten_ratio | float | 0.0~0.5 | FLAG B 최대 조임률 |
| use_sell_skew_adjust | bool | — | FLAG C: 매도 비대칭 보정 |
| sell_skew_floor | float | 0.3~0.8 | FLAG C skew 하한 |
| sell_skew_ceil | float | 1.2~2.0 | FLAG C skew 상한 |
| use_uncertainty_discount | bool | — | FLAG D: uncertainty 감산 |
| sell_unc_weight | float | 0.0~2.0 | FLAG D 강도 |

### 결과
- baseline (EXP-000): final_equity=0.893 (-10.7%), 1/32 feasible
- **EXP-001 best: equity=1.0796 (+8.0%), 64/64 feasible**
- 앵커 테이블: 428,799행 (US 502종목, 2019-10~2026-03)
- test 구간: 2024-01 이후, 20% 샘플 (53k test anchors)
- Optuna: 64 trials, 275초 (precompute 107초 + trials 168초)
- best trial #14: 215 trades, top_k=40, min_sim=0.3, temp=20, hold=8일

### Best 파라미터
```json
{
  "top_k": 40, "min_similarity": 0.3, "temperature": 20.0,
  "max_holding_days": 8, "max_new_buys": 2, "per_name_cap_fraction": 0.35,
  "buy_dist_blend": 0.4, "sell_dist_blend": 0.3,
  "fallback_min_sell_markup": 0.008,
  "use_skew_adjust": false, "use_ess_tightening": false,
  "use_sell_skew_adjust": true, "sell_skew_floor": 0.7, "sell_skew_ceil": 1.7,
  "use_uncertainty_discount": true, "sell_unc_weight": 1.0
}
```

### 플래그별 분석

| flag | ON 평균 equity | OFF 평균 equity | 채택 |
|------|---------------|----------------|------|
| A: skew_adjust | 0.913 (11) | **1.017 (53)** | **OFF** — 해로움 |
| B: ess_tightening | 0.905 (11) | **1.019 (53)** | **OFF** — 해로움 |
| C: sell_skew_adjust | **1.012 (53)** | 0.936 (11) | **ON** — 유효 |
| D: uncertainty_discount | **1.006 (53)** | 0.968 (11) | **ON** — 유효 |

### 결론 & 다음 실험 제안

**흡수**: FLAG C(매도 비대칭 보정) + FLAG D(불확실성 감산) 채택. 매도쪽 분포 보정이 수익에 기여.
**버림**: FLAG A(매수 비대칭) + FLAG B(ESS 보정) 제거. 매수쪽은 단순할수록 좋음.
**해석**: 높은 temperature(20)와 낮은 min_similarity(0.3)가 best → 유사 앵커를 넓게 보되 가중은 날카롭게.
         소수 집중 투자(max_buys=2, cap=35%)가 분산 투자보다 나음.

**다음 실험 후보**:
- FLAG A,B 제거 + C,D 고정 후 나머지 파라미터만 128 trials 추가 탐색
- 전체 test set(100%)으로 best params 검증 (현재는 20% 샘플)
- 아키텍처: candle_shape 벡터 구성 변경 실험 (현재 4feature×10일 = 40dim)

---

## EXP-003: 매수 후보 스코어 + 사다리 + US/KR 분리자본

### 변경 사항
1. FLAG A,B 제거(하드코딩 OFF), C,D 고정(하드코딩 ON) — EXP-001 결론 흡수
2. US/KR 별도 자본 (US $6,667 ≈ ₩1000만, KR ₩1000만)
3. 복합 매수 후보 스코어: w_profit×기대수익 + w_sim×유사도 + w_ess×ESS - w_width×분포폭
4. FLAG E: min_buy_score 게이팅
5. FLAG F: 사다리 매수 (2~3단계, spread 0.005~0.03)
6. kr_search_pool: KR_ONLY vs ALL
7. yearly_returns 버그 수정: 거래평균 → equity 스냅샷 기반 포트폴리오 연간 수익률

### 결과 (64t, 62/64 feasible)

```
=== Best (t4, combined +9.3%, 승률 59.2%, 667건) ===

US  $6,667 → ~$7,117  +6.8%
    연도별: 2024 +5.1%  2025 +2.4%  2026 +5.0%

KR  ₩10,000,000 → ~₩11,184,000  +11.8%
    연도별: 2024 +2.8%  2025 +2.6%  2026 +23.4%

합산 ₩20,000,000 → ~₩21,860,000  +9.3%
모든 시장, 모든 연도 양수 수익.
```

### Best 파라미터
```json
{
  "top_k": 45, "min_similarity": 0.9, "temperature": 14.0,
  "max_holding_days": 5, "max_new_buys": 6, "per_name_cap_fraction": 0.3,
  "buy_dist_blend": 0.2, "sell_dist_blend": 0.2, "fallback_min_sell_markup": 0.008,
  "sell_skew_floor": 0.5, "sell_skew_ceil": 2.0, "sell_unc_weight": 1.9,
  "w_profit": 1.0, "w_sim": 2.0, "w_ess_score": 0.5, "w_width_penalty": 0.5,
  "min_buy_score": 0.0,
  "use_ladder_buy": true, "ladder_leg_count": 2, "ladder_spread": 0.01,
  "kr_search_pool": "ALL"
}
```

### FLAG/옵션 분석

| FLAG | ON 평균 | OFF 평균 | 판정 |
|------|---------|---------|------|
| F: 사다리 매수 | **1.048 (44)** | 1.040 (18) | 약간 우위, 추가 검증 필요 |
| KR 풀 | KR_ONLY 1.046 | ALL 1.044 | 거의 동일, best는 ALL |

### 결론

**흡수**: 복합 스코어(w_sim=2.0 유사도 중시), 사다리 매수, kr_search_pool=ALL
**주요 발견**:
- 유사도 가중(w_sim=2.0)이 기대수익보다 중요 → "확실한 유사 패턴"에 집중이 수익적
- 모든 연도 양수는 전략 안정성 확인
- KR 2026 +23.4%는 표본 작아서 과적합 가능성 주의
