# app/features/premarket/utils/vector_builder.py
"""
PM Best Signal용 벡터 생성 유틸리티

중요:
- Volume은 로그 변화율 (절대값 아님!)
- 블록별 정규화 금지 (전체 L2 정규화 1회만)
- tail_weight=0.0 (균등 가중)
"""
from __future__ import annotations
import numpy as np
from typing import Dict, List, Tuple
from datetime import date
from sqlalchemy import text
from sqlalchemy.orm import Session


EPS = 1e-9


def _normalize_safe(x: np.ndarray) -> np.ndarray:
    """
    안전한 L2 정규화
    
    ⚠️ 0벡터 또는 NaN이면 그대로 반환
    """
    x = np.asarray(x, dtype=np.float32)
    if x.size == 0:
        return x
    
    n = float(np.linalg.norm(x))
    
    if not np.isfinite(n) or n == 0.0:
        return x
    
    return (x / n).astype(np.float32)


def _fit_dim(x: np.ndarray, dim: int) -> np.ndarray:
    """
    벡터 차원 맞춤 (padding or trimming)
    
    ⚠️ 라이브러리 차원에 맞추기 위한 용도만
    """
    x = np.asarray(x, dtype=np.float32)
    
    if x.size == dim:
        return x
    
    if x.size > dim:
        return x[:dim]  # Trimming
    
    return np.pad(x, (0, dim - x.size), mode='constant')  # Padding


def paa_transform(series: np.ndarray, m: int, tail_weight: float = 0.0) -> np.ndarray:
    """
    PAA (Piecewise Aggregate Approximation) 변환
    
    Args:
        series: 입력 시계열 (N,)
        m: 세그먼트 개수
        tail_weight: 최근 데이터 가중치 (0.0 = 균등 가중, 추후 확장용)
        
    Returns:
        평균값 배열 (m,)
    
    Note:
        tail_weight는 현재 0.0 고정 (균등 가중)
        향후 최근 데이터 강조가 필요하면 > 0.0 사용
    """
    n = len(series)
    if n < m:
        raise ValueError(f"Series length {n} < m {m}")
    
    seg_len = n / m
    result = np.zeros(m, dtype=np.float32)
    
    # tail_weight = 0.0이면 균등 가중 (단순 평균)
    if tail_weight == 0.0:
        for i in range(m):
            start = int(i * seg_len)
            end = int((i + 1) * seg_len)
            result[i] = np.mean(series[start:end])
    else:
        # 향후 확장: 지수 가중 평균 등
        # 현재는 사용 안 함
        for i in range(m):
            start = int(i * seg_len)
            end = int((i + 1) * seg_len)
            result[i] = np.mean(series[start:end])
    
    return result


def compute_log_returns(close: np.ndarray) -> np.ndarray:
    """로그 수익률 계산"""
    close = np.maximum(close, EPS)
    return np.diff(np.log(close))


def compute_log_volume_returns(volume: np.ndarray) -> np.ndarray:
    """
    로그 거래량 변화율 계산
    
    ⚠️ 중요: 절대값이 아닌 변화율!
    """
    volume = np.maximum(volume, 1.0)
    return np.diff(np.log(volume))


def build_shape_vector(ohlcv: np.ndarray, config: dict) -> np.ndarray:
    """
    Shape Vector 생성
    
    Args:
        ohlcv: (N, 5) [open, high, low, close, volume]
        config: 벡터 설정 (m, w_price, w_volume, w_candle, w_meta, tail_weight, shape_dim, etc.)
        
    Returns:
        Shape vector (L2 정규화됨)
    """
    m = config['m']
    w_price = config['w_price']
    w_volume = config['w_volume']
    w_candle = config['w_candle']
    w_meta = config['w_meta']
    tail_weight = config.get('tail_weight', 0.0)  # ⚠️ 기본값 0.0
    shape_dim = config.get('shape_dim')  # 라이브러리 차원 (검증용)
    
    parts = []
    
    # 1) Price (Log Returns)
    if w_price > 0:
        log_ret = compute_log_returns(ohlcv[:, 3])  # close
        log_ret_paa = paa_transform(log_ret, m, tail_weight)  # ← tail_weight 전달
        parts.append(log_ret_paa * w_price)
    
    # 2) Volume (Log Returns) ⚠️ 중요!
    if w_volume > 0:
        log_vol = compute_log_volume_returns(ohlcv[:, 4])  # volume
        log_vol_paa = paa_transform(log_vol, m, tail_weight)  # ← tail_weight 전달
        parts.append(log_vol_paa * w_volume)
    
    # 3) Candle (body, upper, lower)
    if w_candle > 0:
        o, h, l, c = ohlcv[:, 0], ohlcv[:, 1], ohlcv[:, 2], ohlcv[:, 3]
        close_safe = np.maximum(np.abs(c), EPS)
        
        body = (c - o) / close_safe
        upper = (h - np.maximum(o, c)) / close_safe
        lower = (np.minimum(o, c) - l) / close_safe
        
        for series in [body, upper, lower]:
            paa = paa_transform(series, m, tail_weight)  # ← tail_weight 전달
            parts.append(paa * w_candle)
    
    # 4) Meta (range, atr7)
    if w_meta > 0 and config.get('include_candle_meta', True):
        o, h, l, c = ohlcv[:, 0], ohlcv[:, 1], ohlcv[:, 2], ohlcv[:, 3]
        close_safe = np.maximum(np.abs(c), EPS)
        
        # Range
        range_pct = (h - l) / close_safe
        
        # ATR7
        prev_c = np.roll(c, 1)
        prev_c[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
        atr7 = np.array([tr[max(0, i-6):i+1].mean() for i in range(len(tr))])
        atr7 = atr7 / close_safe
        
        for series in [range_pct, atr7]:
            paa = paa_transform(series, m, tail_weight)  # ← tail_weight 전달
            parts.append(paa * w_meta)
    
    # 결합 + NaN 체크
    vector = np.concatenate(parts).astype(np.float32)
    
    if np.isnan(vector).any():
        raise ValueError("Vector contains NaN")
    
    # 차원 검증 (라이브러리와 일치해야 함)
    if shape_dim is not None and len(vector) != shape_dim:
        raise ValueError(
            f"SHAPE_DIM_MISMATCH: built={len(vector)}, lib={shape_dim}"
        )
    
    # L2 정규화 (안전 버전)
    return _normalize_safe(vector)


def build_context_vector(db: Session, anchor_date: date, config: dict) -> np.ndarray:
    """
    Context Vector 생성
    
    macro_cols는 series id 배열 또는 코드명 배열:
    - Yahoo Finance ids (1-3) 또는 코드 (^GSPC, ^KS200, KRW=X)
    - FRED ids (302-307) 또는 코드 (VIXCLS, DTWEXBGS, etc.)
    
    Returns:
        Context vector (L2 정규화됨) - [macro_changes..., breadth]
    """
    macro_cols = config.get('macro_cols', [])
    macro_window = config.get('macro_window', 2)
    macro_lag_days = config.get('macro_lag_days', 1)  # ✅ 배치 기본값: 1 (한국장 고려)
    
    parts = []
    
    # 1) Macro 변화율 (최신 2개 관측치만 사용 - 배치와 동일)
    for series_code in macro_cols:
        # ✅ 코드 기반 분기 (배치와 동일)
        # Yahoo Finance: ^로 시작하거나 = 포함
        # FRED: 그 외
        if isinstance(series_code, str) and (series_code.startswith('^') or '=' in series_code):
            # Yahoo Finance 조회
            result = db.execute(text("""
                SELECT yit.v AS value, yit.d AS obs_date
                FROM trading.yahoo_index_timeseries yit
                JOIN trading.yahoo_index_series yis ON yis.id = yit.series_id
                WHERE yis.code = :code
                  AND yit.d <= :anchor_date - :lag_days
                ORDER BY yit.d DESC
                LIMIT 2
            """), {
                "code": series_code,
                "anchor_date": anchor_date,
                "lag_days": macro_lag_days
            })
        else:
            # FRED 조회 (정수 id 또는 FRED 코드)
            if isinstance(series_code, int):
                # id로 직접 조회
                result = db.execute(text("""
                    SELECT mdsv.value::numeric AS value, mdsv.obs_date
                    FROM trading.macro_data_series_value mdsv
                    WHERE mdsv.series_id = :series_id
                      AND mdsv.obs_date <= :anchor_date - :lag_days
                    ORDER BY mdsv.obs_date DESC
                    LIMIT 2
                """), {
                    "series_id": series_code,
                    "anchor_date": anchor_date,
                    "lag_days": macro_lag_days
                })
            else:
                # FRED 코드로 조회
                result = db.execute(text("""
                    SELECT mdsv.value::numeric AS value, mdsv.obs_date
                    FROM trading.macro_data_series_value mdsv
                    JOIN trading.macro_data_series mds ON mds.id = mdsv.series_id
                    WHERE mds.fred_series_id = :code
                      AND mdsv.obs_date <= :anchor_date - :lag_days
                    ORDER BY mdsv.obs_date DESC
                    LIMIT 2
                """), {
                    "code": series_code,
                    "anchor_date": anchor_date,
                    "lag_days": macro_lag_days
                })
        
        rows = result.fetchall()
        
        if len(rows) >= 2:
            # ✅ 최신 2개 관측치로 변화율 계산 (배치와 동일)
            v_now = float(rows[0][0]) if rows[0][0] is not None else None
            v_prev = float(rows[1][0]) if rows[1][0] is not None else None
            
            if v_now is not None and v_prev is not None and np.isfinite(v_now) and np.isfinite(v_prev):
                # ✅ 로그 변화율 우선 (배치와 동일)
                if v_prev > 0 and v_now > 0:
                    # 로그 변화율
                    change = np.log(v_now + EPS) - np.log(v_prev + EPS)
                else:
                    # 음수/0이 있으면 단순 변화
                    change = v_now - v_prev
                
                parts.append(float(change) if np.isfinite(change) else 0.0)
            else:
                parts.append(0.0)  # 결측
        else:
            parts.append(0.0)  # 결측
    
    # 2) Breadth (시장 폭)
    result = db.execute(text("""
        WITH recent AS (
            SELECT t.id AS ticker_id, o.trade_date, o.close,
                   AVG(o.close) OVER (
                       PARTITION BY t.id 
                       ORDER BY o.trade_date 
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS ma20
            FROM trading.ohlcv_daily o
            JOIN trading.ticker t ON t.id = o.ticker_id
            WHERE o.trade_date >= :anchor_date - 80
              AND o.trade_date < :anchor_date
              AND o.is_final = true
        ),
        lastrows AS (
            SELECT DISTINCT ON (ticker_id) ticker_id, close, ma20
            FROM recent
            ORDER BY ticker_id, trade_date DESC
        )
        SELECT 
            SUM(CASE WHEN close > ma20 THEN 1 ELSE 0 END)::float AS up_cnt,
            COUNT(*)::float AS total_cnt
        FROM lastrows
        WHERE ma20 IS NOT NULL AND close IS NOT NULL
    """), {"anchor_date": anchor_date})
    
    row = result.fetchone()
    if row and row[1] > 0:
        breadth = (row[0] / row[1]) * 2.0 - 1.0  # [-1, 1]
    else:
        breadth = 0.0
    
    parts.append(breadth)
    
    # Context 벡터 생성
    q_ctx = np.array(parts, dtype=np.float32)
    
    # ========================================================================
    # 🔧 breadth 축 약화 (v2 개선)
    # ========================================================================
    # breadth가 컨텍스트 벡터의 방향을 거의 결정하는 문제 완화
    w_breadth = float(config.get('w_breadth', 0.25))  # 기본 0.25 (25% 가중치)
    if q_ctx.size > 0 and len(parts) > 0:
        q_ctx[-1] = q_ctx[-1] * w_breadth
    
    # 차원 맞춤 (라이브러리 ctx_dim에 맞춤)
    ctx_dim = config.get('ctx_dim')
    if ctx_dim is not None:
        q_ctx = _fit_dim(q_ctx, ctx_dim)
    
    # L2 정규화 (안전 버전)
    return _normalize_safe(q_ctx)


def pgvector_to_numpy(vec) -> np.ndarray:
    """
    pgvector → numpy array (배치와 동일)
    
    Args:
        vec: pgvector 객체 (list, tuple, numpy array, str 등)
    
    Returns:
        numpy array (float32)
    """
    if vec is None:
        return np.zeros(1, dtype=np.float32)
    
    # pgvector는 list로 반환될 수 있음
    if isinstance(vec, (list, tuple)):
        return np.array(vec, dtype=np.float32)
    
    # 이미 numpy array인 경우
    if isinstance(vec, np.ndarray):
        return vec.astype(np.float32)
    
    # str인 경우 (예: '[1.0, 2.0, 3.0]')
    if isinstance(vec, str):
        import json
        vec_list = json.loads(vec.replace('(', '[').replace(')', ']'))
        return np.array(vec_list, dtype=np.float32)
    
    raise TypeError(f"Unexpected pgvector type: {type(vec).__name__}")


def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    코사인 유사도 계산
    
    Returns:
        Cosine similarity [-1, 1]
    """
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 < EPS or norm2 < EPS:
        return 0.0
    
    return float(np.dot(vec1, vec2) / (norm1 * norm2))

