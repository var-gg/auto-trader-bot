# app/features/signals/utils/shape_vector.py
"""
무차원 형태 벡터 생성 유틸리티
- PAA (Piecewise Aggregate Approximation) 리샘플링
- Z-Score 정규화
- 메타 피처 추출 (realized vol, downside vol, up_ratio 등)
"""
from __future__ import annotations
import numpy as np
from typing import List, Tuple


EPS = 1e-9


def _z_normalize(x: np.ndarray) -> np.ndarray:
    """
    Z-Score 정규화 (평균 0, 표준편차 1)
    - std가 너무 작으면 0으로 처리 (flat 구간 안전 처리)
    - 극값 클리핑으로 폭주 방지
    
    Args:
        x: 입력 배열
        
    Returns:
        정규화된 배열
    """
    x = np.asarray(x, float)
    if len(x) == 0:
        return x
    mean = x.mean()
    std = x.std()
    
    # ✅ std가 너무 작으면 flat 구간으로 간주 → 0 반환
    if std < 1e-8:
        return np.zeros_like(x)
    
    # Z-Score 계산
    z = (x - mean) / std
    
    # ✅ 극값 클리핑 (-5 ~ 5)
    return np.clip(z, -5, 5)


def _paa_resample(x: np.ndarray, m: int) -> np.ndarray:
    """
    PAA (Piecewise Aggregate Approximation) 리샘플링
    선형 보간 기반으로 고정 길이 m으로 리샘플
    
    Args:
        x: 입력 시계열
        m: 출력 길이
        
    Returns:
        리샘플된 배열 (길이 m)
    """
    x = np.asarray(x, float)
    if m <= 0:
        return np.array([], float)
    if len(x) == 0:
        return np.zeros(m, float)
    if len(x) == m:
        return x
    
    # 선형 보간
    grid = np.linspace(0, len(x) - 1, m)
    return np.interp(grid, np.arange(len(x)), x)


def _spearman_correlation(x: np.ndarray, y: np.ndarray) -> float:
    """
    스피어만 순위 상관계수 (scipy 없이 구현)
    
    Args:
        x, y: 입력 배열
        
    Returns:
        순위 상관계수 [-1, 1]
    """
    if len(x) <= 1 or len(y) <= 1 or len(x) != len(y):
        return 0.0
    
    # 순위 계산
    rx = np.argsort(np.argsort(x))
    ry = np.argsort(np.argsort(y))
    
    # 상관계수 계산
    corr_matrix = np.corrcoef(rx, ry)
    return float(corr_matrix[0, 1]) if not np.isnan(corr_matrix[0, 1]) else 0.0


def extract_meta_features(log_returns: np.ndarray, log_volumes: np.ndarray) -> np.ndarray:
    """
    메타 피처 추출 (7개)
    
    Args:
        log_returns: 로그 수익률 배열
        log_volumes: 로그 거래량 변화 배열
        
    Returns:
        메타 피처 배열 [rv, downside_vol, up_ratio, run_up_max, acf1, slope_ts, rho_pv]
    """
    lr = np.asarray(log_returns, float)
    lv = np.asarray(log_volumes, float)
    
    # 1. Realized Volatility (realized vol)
    rv = float(np.sqrt(np.mean(lr**2))) if len(lr) > 0 else 0.0
    
    # 2. Downside Volatility
    downside_vol = float(np.sqrt(np.mean(np.minimum(lr, 0)**2))) if len(lr) > 0 else 0.0
    
    # 3. Up Ratio (상승 비율)
    up_ratio = float(np.mean(lr > 0)) if len(lr) > 0 else 0.0
    
    # 4. Run Up Max (연속 상승 최장 길이)
    run_up_max = 0
    cur = 0
    for is_up in (lr > 0):
        cur = cur + 1 if is_up else 0
        run_up_max = max(run_up_max, cur)
    
    # 5. ACF(1) (1시차 자기상관)
    acf1 = 0.0
    if len(lr) > 1:
        corr_matrix = np.corrcoef(lr[:-1], lr[1:])
        acf1 = float(corr_matrix[0, 1]) if not np.isnan(corr_matrix[0, 1]) else 0.0
    
    # 6. Slope (OLS on log price)
    # log_returns의 누적합 = log price의 변화
    # 간단히 선형 회귀 기울기
    if len(lr) > 1:
        t = np.arange(len(lr))
        slope_ts = float(np.polyfit(t, lr, 1)[0])
    else:
        slope_ts = 0.0
    
    # 7. Spearman correlation (price-volume)
    rho_pv = _spearman_correlation(lr, lv) if len(lr) > 0 and len(lv) > 0 else 0.0
    
    return np.array([rv, downside_vol, up_ratio, run_up_max, acf1, slope_ts, rho_pv], float)


def make_shape_vector(
    prices: List[float],
    volumes: List[float],
    m: int = 5,
    w_seq: float = 1.0,
    w_meta: float = 0.5,
    meta_scaler: str = "tanh"
) -> np.ndarray:
    """
    무차원 형태 벡터 생성
    
    Args:
        prices: 가격 시계열 (최소 3개 이상)
        volumes: 거래량 시계열 (prices와 같은 길이)
        m: PAA 리샘플링 길이 (기본값: 5)
        w_seq: 시퀀스 피처 가중치 (기본값: 1.0)
        w_meta: 메타 피처 가중치 (기본값: 0.5)
        meta_scaler: 메타 피처 스케일링 방법 ("tanh" 또는 "z")
        
    Returns:
        형태 벡터 (길이: 2*m + 7)
        - 첫 m개: 로그수익률 (PAA)
        - 다음 m개: 로그거래량 변화 (PAA)
        - 마지막 7개: 메타 피처
        
    Raises:
        AssertionError: prices 길이가 3 미만이거나, prices와 volumes 길이가 다를 때
    """
    p = np.asarray(prices, float)
    v = np.asarray(volumes, float)
    
    assert len(p) >= 3, f"prices 길이는 최소 3 이상이어야 합니다. (현재: {len(p)})"
    assert len(p) == len(v), f"prices와 volumes의 길이가 같아야 합니다. (prices: {len(p)}, volumes: {len(v)})"
    
    # --- 시퀀스 피처 (무차원화) ---
    # 로그 수익률
    log_returns = np.diff(np.log(p + EPS))
    # 로그 거래량 변화 (안정적)
    log_volumes = np.diff(np.log(v + 1.0))
    
    # Z-Score 정규화
    lr_z = _z_normalize(log_returns)
    lv_z = _z_normalize(log_volumes)
    
    # PAA 리샘플링
    lr_paa = _paa_resample(lr_z, m)
    lv_paa = _paa_resample(lv_z, m)
    
    # --- 메타 피처 ---
    meta = extract_meta_features(log_returns, log_volumes)
    
    # 메타 피처 스케일링
    if meta_scaler == "tanh":
        meta_scaled = np.tanh(meta)  # [-1, 1]로 압축
    elif meta_scaler == "z":
        meta_scaled = _z_normalize(meta)
    else:
        meta_scaled = meta
    
    # --- 결합 & 가중치 적용 ---
    vec = np.concatenate([
        w_seq * lr_paa,      # m개
        w_seq * lv_paa,      # m개
        w_meta * meta_scaled  # 7개
    ])
    
    return vec


def make_shape_vector_from_ohlcv(
    ohlcv_data: List[dict],
    m: int = 5,
    w_seq: float = 1.0,
    w_meta: float = 0.5,
    meta_scaler: str = "tanh"
) -> Tuple[np.ndarray, dict]:
    """
    OHLCV 데이터로부터 형태 벡터 생성 (편의 함수)
    
    Args:
        ohlcv_data: OHLCV 딕셔너리 리스트 [{'close': ..., 'volume': ...}, ...]
        m: PAA 리샘플링 길이
        w_seq: 시퀀스 피처 가중치
        w_meta: 메타 피처 가중치
        meta_scaler: 메타 피처 스케일링 방법
        
    Returns:
        (벡터, 메타데이터) 튜플
        - 벡터: np.ndarray
        - 메타데이터: {'vector_dim': int, 'm': int, 'candles': int}
    """
    prices = [d['close'] for d in ohlcv_data]
    volumes = [d.get('volume', 1.0) for d in ohlcv_data]  # volume 없으면 1.0
    
    vector = make_shape_vector(prices, volumes, m, w_seq, w_meta, meta_scaler)
    
    metadata = {
        'vector_dim': len(vector),
        'm': m,
        'candles': len(ohlcv_data),
        'w_seq': w_seq,
        'w_meta': w_meta,
        'meta_scaler': meta_scaler
    }
    
    return vector, metadata


def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    두 벡터 간 코사인 유사도 계산
    
    Args:
        vec1: 첫 번째 벡터
        vec2: 두 번째 벡터
        
    Returns:
        코사인 유사도 (0~1, 높을수록 유사)
        
    Note:
        - 벡터 길이가 다르면 0.0 반환
        - 0 벡터가 있으면 0.0 반환
    """
    vec1 = np.asarray(vec1, float)
    vec2 = np.asarray(vec2, float)
    
    # 길이 검증
    if len(vec1) != len(vec2) or len(vec1) == 0:
        return 0.0
    
    # 노름 계산
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    # 0 벡터 체크
    if norm1 < EPS or norm2 < EPS:
        return 0.0
    
    # 코사인 유사도 = dot(v1, v2) / (||v1|| * ||v2||)
    similarity = np.dot(vec1, vec2) / (norm1 * norm2)
    
    # [-1, 1] → [0, 1] 변환 (선택적, 음수도 허용하려면 제거)
    # return float((similarity + 1.0) / 2.0)
    
    # 또는 음수는 0으로 클리핑
    return float(max(0.0, similarity))


def make_shape_vector_v1(
    prices: List[float],
    volumes: List[float],
    m: int = 5,
    w_seq: float = 1.0,
    w_meta: float = 0.5,
    meta_scaler: str = "tanh"
) -> np.ndarray:
    """
    형태 벡터 생성 v1 (현재 기본 알고리즘)
    make_shape_vector와 동일 (버전 명시)
    """
    return make_shape_vector(prices, volumes, m, w_seq, w_meta, meta_scaler)


def _spearman_correlation(x: np.ndarray, y: np.ndarray) -> float:
    """스피어만 순위 상관계수 (scipy 없이)"""
    if len(x) <= 1 or len(y) <= 1 or len(x) != len(y):
        return 0.0
    rx = np.argsort(np.argsort(x))
    ry = np.argsort(np.argsort(y))
    corr = np.corrcoef(rx, ry)[0, 1]
    return float(corr) if not np.isnan(corr) else 0.0


def _extract_candle_sequences(ohlcv: List[dict], mode: str = "diff") -> dict:
    """
    캔들 무차원 특성 시퀀스 계산 (v2용)
    body        = (C - O) / O
    shadow_up   = (H - max(O,C)) / O
    shadow_down = (min(O,C) - L) / O
    range_vol   = (H - L) / O
    """
    O = np.array([d.get("open", np.nan) for d in ohlcv], float)
    H = np.array([d.get("high", np.nan) for d in ohlcv], float)
    L = np.array([d.get("low", np.nan) for d in ohlcv], float)
    C = np.array([d.get("close", np.nan) for d in ohlcv], float)
    
    if np.isnan(O).any() or np.isnan(H).any() or np.isnan(L).any() or np.isnan(C).any():
        return {}
    
    body = (C - O) / (O + EPS)
    shadow_up = (H - np.maximum(O, C)) / (O + EPS)
    shadow_down = (np.minimum(O, C) - L) / (O + EPS)
    range_vol = (H - L) / (O + EPS)
    
    if mode == "diff":
        body = np.diff(body)
        shadow_up = np.diff(shadow_up)
        shadow_down = np.diff(shadow_down)
        range_vol = np.diff(range_vol)
    
    return {
        "body": body,
        "shadow_up": shadow_up,
        "shadow_down": shadow_down,
        "range_vol": range_vol,
    }


def _extract_candle_meta(cndl: dict) -> np.ndarray:
    """
    캔들형 메타 피처 (3개)
    - doji_ratio: |body| <= 0.1*range 비율
    - upper_bias: mean(shadow_up - shadow_down)
    - body_range_corr: |body| vs range 상관(Spearman)
    """
    if not cndl:
        return np.zeros(3, float)
    
    body = cndl["body"]
    rng = cndl["range_vol"]
    su = cndl["shadow_up"]
    sd = cndl["shadow_down"]
    
    doji_ratio = float(np.mean(np.abs(body) <= 0.1 * (rng + EPS))) if len(body) else 0.0
    upper_bias = float(np.mean(su - sd)) if len(su) and len(sd) else 0.0
    body_range_corr = _spearman_correlation(np.abs(body), rng) if len(body) and len(rng) else 0.0
    
    return np.array([doji_ratio, upper_bias, body_range_corr], float)


def make_shape_vector_v2(
    ohlcv_data: List[dict],
    m: int = 5,
    w_price: float = 1.0,
    w_volume: float = 1.0,
    w_candle: float = 1.0,
    w_meta: float = 0.5,
    meta_scaler: str = "tanh",
    candle_mode: str = "diff",
    include_candle_meta: bool = True,
) -> np.ndarray:
    """
    완전 통합형 무차원 형태 벡터 v2
    [log_returns(m) + log_volumes(m) + 4*candle_seq(m) + meta(가격+거래량+캔들)]
    
    Args:
        ohlcv_data: OHLCV 딕셔너리 리스트 [{'open': ..., 'high': ..., 'low': ..., 'close': ..., 'volume': ...}, ...]
        m: PAA 리샘플링 길이
        w_price: 가격 시퀀스 가중치
        w_volume: 거래량 시퀀스 가중치
        w_candle: 캔들 시퀀스 가중치
        w_meta: 메타 피처 가중치
        meta_scaler: 메타 피처 스케일링 방법
        candle_mode: 캔들 모드 (diff/raw)
        include_candle_meta: 캔들 메타 피처 포함 여부
        
    Returns:
        형태 벡터 (길이: 2*m + 4*m + meta_count)
    """
    C = np.array([d.get("close", np.nan) for d in ohlcv_data], float)
    V = np.array([d.get("volume", 1.0) for d in ohlcv_data], float)
    assert len(C) >= 3, f"캔들 수가 너무 적습니다. ({len(C)})"
    
    # === 1. 종가/거래량 시퀀스 ===
    lr = np.diff(np.log(C + EPS))
    lv = np.diff(np.log(V + 1.0))
    lr_z = _z_normalize(lr)
    lv_z = _z_normalize(lv)
    lr_paa = _paa_resample(lr_z, m)
    lv_paa = _paa_resample(lv_z, m)
    
    # === 2. 캔들 시퀀스 ===
    cndl = _extract_candle_sequences(ohlcv_data, mode=candle_mode)
    candle_vecs = []
    if cndl:
        for k in ("body", "shadow_up", "shadow_down", "range_vol"):
            seq_z = _z_normalize(cndl[k])
            seq_paa = _paa_resample(seq_z, m)
            candle_vecs.append(seq_paa)
    candle_concat = np.concatenate(candle_vecs) if candle_vecs else np.array([], float)
    
    # === 3. 메타 ===
    meta_core = extract_meta_features(lr, lv)
    meta_candle = _extract_candle_meta(cndl) if include_candle_meta else np.array([], float)
    meta_all = np.concatenate([meta_core, meta_candle])
    if meta_scaler == "tanh":
        meta_scaled = np.tanh(meta_all)
    elif meta_scaler == "z":
        meta_scaled = _z_normalize(meta_all)
    else:
        meta_scaled = meta_all
    
    # === 4. 결합 ===
    vec = np.concatenate([
        w_price * lr_paa,
        w_volume * lv_paa,
        w_candle * candle_concat,
        w_meta * meta_scaled,
    ])
    
    return vec


def get_vector_generator(version: str = "v1"):
    """
    버전별 벡터 생성 함수 반환
    
    Args:
        version: 알고리즘 버전 (v1, v2, v3)
        
    Returns:
        벡터 생성 함수
        
    Note:
        - v1: make_shape_vector_v1 (prices, volumes 필요)
        - v2: make_shape_vector_v2 (ohlcv_data 필요)
        - v3: make_shape_vector_v2 사용 (v2와 동일한 벡터)
    """
    generators = {
        "v1": make_shape_vector_v1,
        "v2": make_shape_vector_v2,
        "v3": make_shape_vector_v2,  # v3는 벡터 생성은 v2 사용
        "v4": make_shape_vector_v2
    }
    
    if version not in generators:
        raise ValueError(f"지원하지 않는 버전입니다: {version}. 사용 가능: {list(generators.keys())}")
    
    return generators[version]


def needs_ohlcv(version: str) -> bool:
    """
    해당 버전이 OHLCV 전체 데이터가 필요한지 여부
    
    Args:
        version: 알고리즘 버전
        
    Returns:
        True if OHLCV needed, False if only prices/volumes
    """
    return version in ["v2", "v3", "v4"]