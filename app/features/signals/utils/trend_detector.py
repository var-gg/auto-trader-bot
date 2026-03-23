# app/features/signals/utils/trend_detector.py
"""
트렌드 시그널 탐지 알고리즘
- 버전별로 다른 탐지 로직 사용 가능
- 현재 v1: 직전 구간 평탄성 + 이후 구간 변동률 기반 탐지
"""
from __future__ import annotations
from typing import Literal
import pandas as pd
import numpy as np

from app.features.signals.models.signal_models import SignalDirection

EPS = 1e-9  # 0으로 나누기 방지


def detect_trend_starts_v1(
    df: pd.DataFrame,
    direction: SignalDirection,
    lookback: int = 5,
    future_window: int = 15,
    min_change: float = 0.05,
    max_reverse: float = 0.05,
    flatness_k: float = 1.0
) -> pd.DataFrame:
    """
    트렌드 개시점 탐지 알고리즘 v1
    
    직전 구간의 평탄성을 확인하고, 이후 구간에서 충분한 변동이 발생하는 지점을 탐지
    
    Args:
        df: 일봉 DataFrame (columns: ['date', 'close'])
        direction: 시그널 방향 (UP: 상승, DOWN: 하락)
        lookback: 직전 구간 확인 기간
        future_window: 이후 구간 평가 기간
        min_change: 최소 변동률
        max_reverse: 반대 방향 최대 허용폭
        flatness_k: 직전 구간 평탄성 허용치 (ATR 배수)
        
    Returns:
        탐지된 시그널 DataFrame
        columns: ['date', 'close', 'change_7_24d', 'past_slope', 'past_std', 'atr']
    """
    df = df.copy()
    df['atr'] = df['close'].diff().abs().rolling(7).mean()
    
    signals = []
    is_uptrend = (direction == SignalDirection.UP)
    
    for i in range(lookback, len(df) - future_window):
        past = df['close'].iloc[i - lookback:i]
        future = df['close'].iloc[i:i + future_window]
        current = df['close'].iloc[i]
        
        # (1) 직전 구간 조건: 평탄성 확인
        past_slope = (past.iloc[-1] - past.iloc[0]) / lookback
        past_std = past.std()
        atr_value = df['atr'].iloc[i]
        
        if is_uptrend:
            # 상승 시그널: 직전 구간이 하락 또는 평탄
            slope_condition = (past_slope <= 0) and (past_std < atr_value * flatness_k)
        else:
            # 하락 시그널: 직전 구간이 상승 또는 평탄
            slope_condition = (past_slope >= 0) and (past_std < atr_value * flatness_k)
        
        # (2) 이후 구간 조건: 충분한 변동 발생
        # 0 또는 NaN 체크
        if pd.isna(current) or current <= EPS:
            continue
        
        if is_uptrend:
            # 상승 시그널: 충분한 상승 발생
            extreme_future = future.max()
            change = (extreme_future - current) / (current + EPS)
            reverse = (current - past.min()) / (current + EPS)  # 하락폭 제한
        else:
            # 하락 시그널: 충분한 하락 발생
            extreme_future = future.min()
            change = (current - extreme_future) / (current + EPS)
            reverse = (past.max() - current) / (current + EPS)  # 상승폭 제한
        
        # 조건 만족 시 시그널 추가
        if slope_condition and change >= min_change and reverse <= max_reverse:
            signals.append({
                'date': df['date'].iloc[i],
                'close': current,
                'change_7_24d': change,
                'past_slope': past_slope,
                'past_std': past_std,
                'atr': atr_value
            })
    
    return pd.DataFrame(signals)


def detect_trend_starts_v2(
    df: pd.DataFrame,
    direction: SignalDirection,
    lookback: int = 5,
    future_window: int = 15,
    min_change: float = 0.05,
    max_reverse: float = 0.05,
    flatness_k: float = 1.0
) -> pd.DataFrame:
    """
    트렌드 지속형 탐지 알고리즘 v2 (유의미한 강화 포인트만)
    - 직전 구간은 벡터화만 하고, 판정은 '이후 구간'만 본다
    - v1에서 못 잡은 케이스만 대상으로(사전조건 비중복)
    - 가속(momentum) + 로컬피크(local max) + 간격(cooldown) 필터로 희소화
    """
    df = df.copy()
    df['atr'] = df['close'].diff().abs().rolling(7).mean()

    signals = []
    is_uptrend = (direction == SignalDirection.UP)

    # 내부 파생 하이퍼(인자 추가 없이 기존 인자에서 유도)
    recent_win = max(3, min(7, lookback))          # 로컬피크 확인용 과거창
    min_gap = max(2, future_window // 3)           # 시그널 간 최소 간격
    mom_thresh = min_change * 0.35                 # 가속 임계(경험치)
    eps = 1e-12

    # rolling change 저장용
    recent_changes = []   # 길이 recent_win 유지
    prev_change = None
    last_signal_idx = -10_000

    for i in range(lookback, len(df) - future_window):
        current = df['close'].iloc[i]
        future = df['close'].iloc[i:i + future_window]
        atr_value = df['atr'].iloc[i]
        
        # 0 또는 NaN 체크
        if pd.isna(current) or current <= eps:
            continue

        # --- v1 사전조건과 비중복화: v1이 잡는 '평탄/역방향+저변동'이면 스킵
        past = df['close'].iloc[i - lookback:i]
        past_slope = (past.iloc[-1] - past.iloc[0]) / max(1, lookback)
        past_std = past.std()
        v1_slope_cond = (
            ((past_slope <= 0) if is_uptrend else (past_slope >= 0))
            and (past_std < (atr_value + eps) * flatness_k)
        )
        if v1_slope_cond:
            # v1이 담당해야 할 맥락이므로 v2 대상에서 제외
            # (v1 실제 시그널 여부와 무관, '맥락' 중복만 제거)
            # -> v1 못잡은 룩백만 v2가 본다
            # continue 하지 않고 '지속형'이 확실한 경우만 통과시키려면 아래처럼:
            # if (is_uptrend and past_slope > 0) or ((not is_uptrend) and past_slope < 0): pass else: continue
            continue

        # --- 이후 변동률/낙폭 (지속형 판정의 본체)
        if is_uptrend:
            max_future = future.max()
            min_future = future.min()
            change = (max_future - current) / (current + eps)              # 총 상승률
            drawdown_abs = abs((min_future - current) / (current + eps))   # 중간 낙폭
        else:
            min_future = future.min()
            max_future = future.max()
            change = (current - min_future) / (current + eps)              # 총 하락률
            drawdown_abs = abs((max_future - current) / (current + eps))   # 중간 되돌림

        # 1차 필터: 지속조건 충족
        if change < min_change or drawdown_abs > max_reverse:
            # recent_changes는 유지(다음 i에서 과거값 역할)
            recent_changes.append(change)
            if len(recent_changes) > recent_win:
                recent_changes.pop(0)
            prev_change = change if prev_change is None else prev_change
            continue

        # 가속(momentum): change(i) - change(i-1) > mom_thresh
        momentum = 0.0 if prev_change is None else (change - prev_change)
        if momentum <= mom_thresh:
            recent_changes.append(change)
            if len(recent_changes) > recent_win:
                recent_changes.pop(0)
            prev_change = change
            continue

        # 로컬 피크: 최근 recent_win 내 최고치 갱신이어야
        if len(recent_changes) > 0 and change <= max(recent_changes):
            recent_changes.append(change)
            if len(recent_changes) > recent_win:
                recent_changes.pop(0)
            prev_change = change
            continue

        # 쿨다운(간격) 확보: 같은 추세에서 과도한 잦은 신호 방지
        if (i - last_signal_idx) < min_gap:
            recent_changes.append(change)
            if len(recent_changes) > recent_win:
                recent_changes.pop(0)
            prev_change = change
            continue

        # --- 모든 필터 통과: '추세 강화 포인트'로 채택
        signals.append({
            'date': df['date'].iloc[i],
            'close': current,
            'change_7_24d': change,   # 이름 유지 (downstream 호환)
            'past_slope': past_slope,
            'past_std': past_std,
            'atr': atr_value,
            'momentum': momentum,     # 참고지표(옵션)
        })
        last_signal_idx = i

        # 상태 업데이트
        recent_changes.append(change)
        if len(recent_changes) > recent_win:
            recent_changes.pop(0)
        prev_change = change

    return pd.DataFrame(signals)



def detect_trend_starts_v3(
    df: pd.DataFrame,
    direction: SignalDirection,
    lookback: int = 5,
    future_window: int = 15,
    min_change: float = 0.05,
    max_reverse: float = 0.05,
    flatness_k: float = 1.0
) -> pd.DataFrame:
    """
    트렌드 혼합형 탐지 알고리즘 v3
    
    - v1과 v2를 모두 실행하여 결과 병합
    - 중복 제거 (같은 날짜는 하나만 유지)
    
    Args:
        df: 일봉 DataFrame
        direction: 시그널 방향
        lookback: 직전 구간 길이
        future_window: 이후 구간 평가 기간
        min_change: 최소 변동률
        max_reverse: 반대방향 최대 허용폭
        flatness_k: 평탄성 허용치
        
    Returns:
        탐지된 시그널 DataFrame (v1 + v2 병합)
    """
    # v1 탐지
    signals_v1 = detect_trend_starts_v1(
        df, direction, lookback, future_window,
        min_change, max_reverse, flatness_k
    )
    
    # v2 탐지
    signals_v2 = detect_trend_starts_v2(
        df, direction, lookback, future_window,
        min_change, max_reverse, flatness_k
    )
    
    # 병합 및 중복 제거
    if len(signals_v1) == 0 and len(signals_v2) == 0:
        return pd.DataFrame()
    
    combined = pd.concat([signals_v1, signals_v2], ignore_index=True)
    
    # 같은 날짜는 하나만 유지 (먼저 나온 것 우선)
    if len(combined) > 0:
        combined = combined.drop_duplicates(subset=['date'], keep='first')
        combined = combined.sort_values('date').reset_index(drop=True)
    
    return combined


def detect_trend_starts_v4(
    df: pd.DataFrame,
    direction: SignalDirection,
    lookback: int = 5,
    future_window: int = 15,
    min_change: float = 0.05,
    max_reverse: float = 0.05,
    flatness_k: float = 1.0
) -> pd.DataFrame:
    """
    트렌드 개시점 탐지 알고리즘 v5
    - future_window 내에서 목표변동률(min_change)을 달성한 캔들을 찾고
    - 해당 목표달성 캔들 종가 기준으로 이후 구간의 역변동이 max_reverse 이내일 때만 신호 검출
    """
    import numpy as np
    import pandas as pd

    EPS_LOCAL = 1e-9
    df = df.copy()
    df['atr'] = df['close'].diff().abs().rolling(7).mean()

    signals = []
    is_up = (direction == SignalDirection.UP)
    closes = df['close'].values
    n = len(df)

    for i in range(0, n - future_window):
        base = closes[i]
        if base is None or not np.isfinite(base) or base <= 0:
            continue

        fut = closes[i + 1:i + 1 + future_window]
        if len(fut) < future_window or np.all(np.isnan(fut)):
            continue

        atr_val = df['atr'].iloc[i]
        atr_val = float(atr_val) if (atr_val is not None and np.isfinite(atr_val)) else np.nan

        # 상승 트렌드 탐지
        if is_up:
            # 목표상승 달성 지점 찾기
            target_indices = np.where(fut >= base * (1.0 + min_change))[0]
            if len(target_indices) == 0:
                continue
            t_idx = target_indices[0]  # 최초 달성 시점
            target_price = fut[t_idx]

            # 이후 구간에서 목표달성 종가 대비 역변동 확인
            remain = fut[t_idx:]  # 목표달성 포함 이후 구간
            if np.nanmin(remain) >= target_price * (1.0 - max_reverse):
                change = (target_price - base) / (base + EPS_LOCAL)
                signals.append({
                    'date': df['date'].iloc[i],
                    'close': float(base),
                    'change_7_24d': float(change),
                    'past_slope': 0.0,
                    'past_std': 0.0,
                    'atr': atr_val
                })

        # 하락 트렌드 탐지
        else:
            target_indices = np.where(fut <= base * (1.0 - min_change))[0]
            if len(target_indices) == 0:
                continue
            t_idx = target_indices[0]
            target_price = fut[t_idx]

            remain = fut[t_idx:]
            if np.nanmax(remain) <= target_price * (1.0 + max_reverse):
                change = (base - target_price) / (base + EPS_LOCAL)
                signals.append({
                    'date': df['date'].iloc[i],
                    'close': float(base),
                    'change_7_24d': float(change),
                    'past_slope': 0.0,
                    'past_std': 0.0,
                    'atr': atr_val
                })

    if not signals:
        return pd.DataFrame(columns=['date', 'close', 'change_7_24d', 'past_slope', 'past_std', 'atr'])

    result = pd.DataFrame(signals)
    for col in ['close', 'change_7_24d', 'atr']:
        result[col] = pd.to_numeric(result[col], errors='coerce')
    return result


def get_trend_detector(version: str = "v1"):
    """
    버전별 탐지 함수 반환
    
    Args:
        version: 알고리즘 버전 (v1, v2, v3, v4)
        
    Returns:
        탐지 함수
    """
    detectors = {
        "v1": detect_trend_starts_v1,
        "v2": detect_trend_starts_v2,
        "v3": detect_trend_starts_v3,
        "v4": detect_trend_starts_v4,
    }
    
    if version not in detectors:
        raise ValueError(f"지원하지 않는 버전입니다: {version}. 사용 가능: {list(detectors.keys())}")
    
    return detectors[version]

