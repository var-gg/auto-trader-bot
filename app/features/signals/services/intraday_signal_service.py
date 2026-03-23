# app/features/signals/services/intraday_signal_service.py
"""
5분봉 시그널 탐지 서비스
- 청크 단위 배치 처리 (500개 5분봉씩)
- API 호출 → 시그널 탐지 → 벡터화 → 저장 → gc.collect() 반복
"""
from __future__ import annotations
import logging
import gc
from typing import List, Dict, Any
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_
import pandas as pd
import numpy as np

from app.shared.models.ticker import Ticker
from app.features.signals.models.signal_models import (
    IntradaySignalRequest,
    IntradaySignalResponse,
    IntradaySignalPoint,
    SignalDirection,
    SaveOption
)
from app.features.signals.models.similarity_models import (
    IntradaySimilaritySearchRequest,
    IntradaySimilaritySearchResponse,
    IntradaySimilarSignal
)
from app.features.signals.services.intraday_data_service import IntradayDataService
from app.features.signals.repositories.intraday_signal_repository import IntradaySignalRepository
from app.features.signals.utils.trend_detector import get_trend_detector
from app.features.signals.utils.shape_vector import needs_ohlcv, get_vector_generator


logger = logging.getLogger(__name__)


def _is_valid_vector(vec: np.ndarray) -> tuple[bool, str]:
    """
    벡터 품질 게이트 - 망가진 벡터 차단
    
    Args:
        vec: 벡터 배열
        
    Returns:
        (유효 여부, 실패 사유)
    """
    v = np.asarray(vec, dtype=float).ravel()
    if v.size == 0:
        return False, "empty"
    if not np.all(np.isfinite(v)):
        return False, "nan_or_inf"
    # 과도한 폭주 방지
    if np.max(np.abs(v)) > 10:
        return False, "out_of_range"
    # 지나치게 평평한 벡터
    if np.std(v) < 1e-6:
        return False, "near_constant"
    # 값 다양성 너무 낮으면 (거의 같은 값 반복)
    uniq = np.unique(np.round(v, 6))
    if uniq.size / v.size < 0.15:
        return False, "low_diversity"
    return True, ""


class IntradaySignalService:
    """
    5분봉 시그널 탐지 서비스
    - API에서 500개 5분봉씩 청크로 수집
    - 청크마다 시그널 탐지 → 벡터화 → 저장 → gc.collect()
    """
    
    CHUNK_SIZE_5MIN = 500  # 5분봉 기준 500개씩 처리
    
    def __init__(self, db: Session):
        self.db = db
        self.data_service = IntradayDataService(db)
        self.repository = IntradaySignalRepository(db)
    
    def detect_intraday_signals(self, request: IntradaySignalRequest) -> IntradaySignalResponse:
        """
        5분봉 시그널 탐지 메인 로직
        
        Args:
            request: 5분봉 시그널 탐지 요청
            
        Returns:
            5분봉 시그널 탐지 응답
        """
        # 🔍 ticker_id가 없으면 배치 처리
        if request.ticker_id is None:
            # SAVE 모드가 아니면 에러
            if request.save_option != SaveOption.SAVE:
                raise ValueError("ticker_id가 없는 경우 save_option은 SAVE여야 합니다.")
            
            return self._detect_intraday_signals_batch(request)
        
        # 단일 티커 처리
        return self._detect_intraday_signals_single(request)
    
    def _detect_intraday_signals_batch(self, request: IntradaySignalRequest) -> IntradaySignalResponse:
        """
        전체 active 티커에 대해 배치 처리
        """
        logger.info(f"전체 티커 배치 처리 시작 - {request.candles}개 캔들, 버전: {request.version.value}")
        
        # Active 티커 조회
        active_tickers = self.db.query(Ticker).all()
        
        logger.info(f"처리할 티커: {len(active_tickers)}개")
        
        # 배치 처리 결과
        total_processed = 0
        total_success = 0
        total_failed = 0
        total_skipped = 0
        total_signals = 0
        errors = []
        
        for idx, ticker in enumerate(active_tickers, 1):
            try:
                logger.debug(f"[{idx}/{len(active_tickers)}] 처리 중: {ticker.symbol}:{ticker.exchange} (ID: {ticker.id})")
                
                # 개별 티커 처리
                single_request = IntradaySignalRequest(
                    ticker_id=ticker.id,
                    candles=request.candles,
                    direction=request.direction,
                    version=request.version,
                    save_option=SaveOption.SAVE,
                    lookback=request.lookback,
                    future_window=request.future_window,
                    min_change=request.min_change,
                    max_reverse=request.max_reverse,
                    flatness_k=request.flatness_k
                )
                
                result = self._detect_intraday_signals_single(single_request)
                
                total_processed += 1
                
                if result.skipped:
                    total_skipped += 1
                    logger.debug(f"스킵: {result.skip_reason}")
                else:
                    total_success += 1
                    total_signals += result.total_signals
                    logger.debug(f"완료: {result.total_signals}개 시그널")
                
            except Exception as e:
                total_processed += 1
                total_failed += 1
                error_msg = f"{ticker.symbol}:{ticker.exchange} - {str(e)}"
                errors.append(error_msg)
                logger.error(f"티커 처리 실패 {ticker.id}: {e}", exc_info=True)
        
        # 배치 처리 결과
        batch_summary = {
            "total_tickers": len(active_tickers),
            "processed": total_processed,
            "success": total_success,
            "skipped": total_skipped,
            "failed": total_failed,
            "total_signals": total_signals,
            "errors": errors[:10]
        }
        
        logger.info(f"배치 처리 완료! 전체: {len(active_tickers)}개, 성공: {total_success}개, 스킵: {total_skipped}개, 실패: {total_failed}개, 총 시그널: {total_signals}개")
        
        return IntradaySignalResponse(
            ticker_id=None,
            symbol=None,
            exchange=None,
            requested_direction=request.direction.value,
            total_candles=0,
            total_signals=total_signals,
            up_signals=0,
            down_signals=0,
            version=request.version.value,
            signals=[],
            is_batch=True,
            batch_summary=batch_summary
        )
    
    def _detect_intraday_signals_single(self, request: IntradaySignalRequest) -> IntradaySignalResponse:
        """
        단일 티커 처리 (청크 단위 또는 일괄)
        """
        ticker_id = request.ticker_id
        version = request.version.value
        
        # 티커 정보
        ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        logger.info(f"시그널 탐지 시작 - {ticker.symbol}:{ticker.exchange}, 목표: {request.candles}개 캔들, 버전: {version}, 저장: {request.save_option.value}")
        
        # SAVE 모드면 청크 단위 처리
        if request.save_option == SaveOption.SAVE:
            return self._process_with_chunks_and_save(ticker, request)
        else:
            # NONE 모드는 한 번에 처리
            return self._process_all_at_once(ticker, request)
    
    def _process_with_chunks_and_save(
        self,
        ticker: Ticker,
        request: IntradaySignalRequest
    ) -> IntradaySignalResponse:
        """
        슬라이딩 윈도우 방식으로 처리 + 저장
        - 전체 데이터 한 번에 로드 (메모리 내)
        - 날짜별로 독립적으로 처리
        - 각 날짜별로 코어 + 꼬리로 슬라이딩하며 탐지/저장
        - 코어 범위 시그널만 저장 (중복 방지)
        """
        ticker_id = ticker.id
        version = request.version.value
        total_candles_target = request.candles
        
        # 1) Config 준비
        configs = {}
        if request.direction == SignalDirection.ALL:
            configs['UP'] = self.repository.get_or_create_config(
                direction='UP', lookback=request.lookback, future_window=request.future_window,
                min_change=request.min_change, max_reverse=request.max_reverse,
                flatness_k=request.flatness_k, atr_window=7, version=version
            )
            configs['DOWN'] = self.repository.get_or_create_config(
                direction='DOWN', lookback=request.lookback, future_window=request.future_window,
                min_change=request.min_change, max_reverse=request.max_reverse,
                flatness_k=request.flatness_k, atr_window=7, version=version
            )
            logger.debug(f"Config ID - UP: {configs['UP'].config_id}, DOWN: {configs['DOWN'].config_id}")
        else:
            direction_key = request.direction.value
            configs[direction_key] = self.repository.get_or_create_config(
                direction=direction_key, lookback=request.lookback, future_window=request.future_window,
                min_change=request.min_change, max_reverse=request.max_reverse,
                flatness_k=request.flatness_k, atr_window=7, version=version
            )
            logger.debug(f"Config ID ({direction_key}): {configs[direction_key].config_id}")
        
        # 🔍 최근 데이터 체크 (N시간 전 기준)
        from datetime import timedelta
        cutoff_hours = 6  # 6시간 전 데이터가 있으면 스킵
        cutoff_datetime = datetime.now() - timedelta(hours=cutoff_hours)
        
        config_ids_to_check = [cfg.config_id for cfg in configs.values()]
        has_recent_data = self.repository.check_recent_signals_exist(
            ticker_id=ticker_id,
            config_ids=config_ids_to_check,
            cutoff_datetime=cutoff_datetime
        )
        
        if has_recent_data:
            logger.info(f"티커 {ticker_id} ({ticker.symbol}): {cutoff_hours}시간 이내 최근 데이터 존재 → 스킵")
            return IntradaySignalResponse(
                ticker_id=ticker_id,
                symbol=ticker.symbol,
                exchange=ticker.exchange,
                requested_direction=request.direction.value,
                total_candles=0,
                total_signals=0,
                up_signals=0,
                down_signals=0,
                version=version,
                signals=[],
                skipped=True,
                skip_reason=f"Recent data exists within {cutoff_hours} hours"
            )
        
        # 🧹 기존 시그널 클린징 (cutoff_datetime 이전 데이터만 삭제)
        deleted_count = self.repository.delete_ticker_signals_before(
            ticker_id=ticker_id,
            config_ids=config_ids_to_check,
            cutoff_datetime=cutoff_datetime
        )
        if deleted_count > 0:
            logger.debug(f"기존 시그널 클린징: {deleted_count}개 삭제 (before {cutoff_datetime}, config_ids={config_ids_to_check})")
            self.db.commit()  # 삭제 커밋
        
        # 2) 탐지 함수 (한 번만)
        detect_fn = get_trend_detector(version)
        needs_ohlcv_data = needs_ohlcv(version)
        
        # 3) 한 번에 전체 5분봉 로드
        logger.debug(f"전체 데이터 수집 시작: {total_candles_target}개 캔들")
        all_minute_data = self._fetch_all_data(ticker, total_candles_target)
        if not all_minute_data:
            logger.warning("분봉 데이터 없음")
            return IntradaySignalResponse(
                ticker_id=ticker_id, symbol=ticker.symbol, exchange=ticker.exchange,
                requested_direction=request.direction.value, total_candles=0,
                total_signals=0, up_signals=0, down_signals=0, version=version, signals=[]
            )
        
        N = len(all_minute_data)
        logger.debug(f"전체 5분봉 로드: {N}개")
        
        # 🔥 날짜별로 그룹화
        df_all = self._create_dataframe(all_minute_data)
        if len(df_all) == 0:
            logger.warning("DataFrame 생성 실패")
            return IntradaySignalResponse(
                ticker_id=ticker_id, symbol=ticker.symbol, exchange=ticker.exchange,
                requested_direction=request.direction.value, total_candles=0,
                total_signals=0, up_signals=0, down_signals=0, version=version, signals=[]
            )
        
        df_all['date_only'] = df_all['date'].str[:8]  # YYYYMMDD만 추출
        grouped_by_date = df_all.groupby('date_only', sort=True)
        
        logger.debug(f"날짜별 그룹화: {len(grouped_by_date)}일")
        
        # 4) 오버랩 길이 산정
        atr_need = 7
        required_overlap = max(request.lookback, request.future_window, atr_need)
        logger.debug(f"오버랩 길이: {required_overlap}개 (lookback={request.lookback}, future={request.future_window}, atr={atr_need})")
        
        # 5) 전역 dedup: (config_id, signal_datetime)
        seen_keys = set()
        
        total_signals_found = 0
        total_up_signals = 0
        total_down_signals = 0
        
        # 날짜별로 처리
        for date_str, day_df in grouped_by_date:
            day_df = day_df.reset_index(drop=True)  # 인덱스 리셋 (중요!)
            N_day = len(day_df)
            
            logger.debug(f"{date_str}: {N_day}개 캔들")
            
            # 슬라이딩: 코어=CHUNK_SIZE, 꼬리=required_overlap
            start = 0
            while start < N_day:
                core_end = min(start + self.CHUNK_SIZE_5MIN, N_day)
                # 꼬리를 붙여서 future_window 확보
                end = min(core_end + required_overlap, N_day)
                
                window_df = day_df.iloc[start:end].copy()
                window_df = window_df.reset_index(drop=True)  # 인덱스 리셋
                
                if len(window_df) == 0:
                    logger.debug("빈 윈도우")
                    start = core_end
                    continue
                
                logger.debug(f"청크: [{start}:{core_end}) + tail({end-core_end}) → DF={len(window_df)}행")
                
                # 탐지
                signal_points = self._detect_signals_from_df(window_df, request, detect_fn)
                logger.debug(f"탐지된 시그널(코어+꼬리): {len(signal_points)}개")
                
                # 코어 인덱스까지만 저장 (꼬리 영역 시그널은 다음 슬라이스에서 코어가 됨)
                date_to_idx = {d: i for i, d in enumerate(window_df['date'])}
                core_cutoff_idx = min(core_end - start, len(window_df)) - 1  # window_df 기준 코어 마지막 인덱스
                
                # ① 코어 범위 필터
                core_points = []
                for sp in signal_points:
                    idx = date_to_idx.get(sp.signal_datetime)
                    if idx is not None and idx <= core_cutoff_idx:
                        core_points.append(sp)
                
                tail_points = len(signal_points) - len(core_points)
                if tail_points > 0:
                    logger.debug(f"코어 범위 필터: {len(core_points)}개 저장, {tail_points}개는 tail(다음 코어로)")
                
                # ② 전역 dedup (config_id + signal_datetime)
                if core_points:
                    filtered = []
                    for sp in core_points:
                        cfg_id = configs[sp.direction].config_id
                        key = (cfg_id, sp.signal_datetime)
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        filtered.append(sp)
                    
                    global_dup = len(core_points) - len(filtered)
                    if global_dup > 0:
                        logger.debug(f"전역 중복 제거: {global_dup}개")
                    core_points = filtered
                
                logger.debug(f"저장 대상(코어만, dedup 후): {len(core_points)}개")
                
                # 저장
                if core_points:
                    # ✅ 벡터 함수 재생성 (캐시 문제 방지)
                    vector_fn = get_vector_generator(version)
                    
                    saved_count, chunk_up, chunk_down = self._save_signals_immediate(
                        ticker_id=ticker_id, configs=configs, signal_points=core_points,
                        df=window_df, vector_fn=vector_fn, needs_ohlcv_data=needs_ohlcv_data,
                        lookback=request.lookback
                    )
                    logger.debug(f"저장: {saved_count}개 (UP: {chunk_up}, DOWN: {chunk_down})")
                    total_signals_found += saved_count
                    total_up_signals += chunk_up
                    total_down_signals += chunk_down
                
                # 메모리 정리
                del window_df, signal_points, core_points
                gc.collect()
                
                # 다음 코어로 진행
                start = core_end
        
        logger.info(f"전체 처리 완료! 총 시그널 저장: {total_signals_found}개 (UP: {total_up_signals}, DOWN: {total_down_signals})")
        
        return IntradaySignalResponse(
            ticker_id=ticker_id, symbol=ticker.symbol, exchange=ticker.exchange,
            requested_direction=request.direction.value, total_candles=N,
            total_signals=total_signals_found, up_signals=total_up_signals,
            down_signals=total_down_signals, version=version, signals=[]
        )
    
    def _process_all_at_once(
        self,
        ticker: Ticker,
        request: IntradaySignalRequest
    ) -> IntradaySignalResponse:
        """
        한 번에 전체 처리 (NONE 모드)
        - 날짜별로 독립적으로 시그널 탐지
        """
        ticker_id = ticker.id
        version = request.version.value
        
        # 데이터 수집
        minute_data = self._fetch_all_data(ticker, request.candles)
        
        if not minute_data:
            raise ValueError("분봉 데이터를 수집할 수 없습니다.")
        
        logger.debug(f"수집된 5분봉: {len(minute_data)}개")
        
        # DataFrame 생성
        df = self._create_dataframe(minute_data)
        
        if len(df) == 0:
            return IntradaySignalResponse(
                ticker_id=ticker_id,
                symbol=ticker.symbol,
                exchange=ticker.exchange,
                requested_direction=request.direction.value,
                total_candles=0,
                total_signals=0,
                version=version,
                signals=[]
            )
        
        # 🔥 날짜별로 그룹화 (YYYYMMDD 추출)
        df['date_only'] = df['date'].str[:8]  # YYYYMMDD만 추출
        grouped_by_date = df.groupby('date_only', sort=True)
        
        logger.debug(f"날짜별 그룹화: {len(grouped_by_date)}일")
        
        # 날짜별로 독립적으로 시그널 탐지
        detect_fn = get_trend_detector(version)
        all_signal_points = []
        
        for date_str, day_df in grouped_by_date:
            day_df = day_df.reset_index(drop=True)  # 인덱스 리셋 (중요!)
            candle_count = len(day_df)
            
            logger.debug(f"{date_str}: {candle_count}개 캔들")
            
            # 해당 날짜의 시그널 탐지
            day_signal_points = self._detect_signals_from_df(day_df, request, detect_fn)
            
            logger.debug(f"시그널: {len(day_signal_points)}개")
            
            all_signal_points.extend(day_signal_points)
        
        logger.info(f"전체 시그널 탐지 완료: {len(all_signal_points)}개")
        
        up_count = sum(1 for sp in all_signal_points if sp.direction == "UP")
        down_count = sum(1 for sp in all_signal_points if sp.direction == "DOWN")
        
        return IntradaySignalResponse(
            ticker_id=ticker_id,
            symbol=ticker.symbol,
            exchange=ticker.exchange,
            requested_direction=request.direction.value,
            total_candles=len(df),
            total_signals=len(all_signal_points),
            up_signals=up_count,
            down_signals=down_count,
            version=version,
            signals=all_signal_points
        )
    
    def _fetch_all_data(self, ticker: Ticker, candles: int) -> List[Dict[str, Any]]:
        """
        전체 데이터 한 번에 수집 (5분봉 개수)
        """
        if ticker.country == "KR":
            return self.data_service.fetch_kr_minute_data(ticker, candles)
        else:
            return self.data_service.fetch_us_minute_data(ticker, candles)
    
    def _fetch_for_similarity_search(
        self,
        ticker: Ticker,
        reference_datetime: str | None,
        lookback: int
    ) -> List[Dict[str, Any]]:
        """
        유사도 검색용 최적화된 데이터 수집
        - KIS API 특성에 맞춘 최소 호출 전략
        - 국내: 1분봉 → 5분봉 리샘플링
        - 해외: 5분봉 직접 조회
        
        Args:
            ticker: 티커 정보
            reference_datetime: 기준 일시 (YYYYMMDD HHMMSS) 또는 None
            lookback: 필요한 lookback 기간 (5분봉 기준)
            
        Returns:
            5분봉 데이터 리스트
        """
        if ticker.country == "KR":
            return self.data_service.fetch_for_similarity_kr(ticker, reference_datetime, lookback)
        else:
            return self.data_service.fetch_for_similarity_us(ticker, reference_datetime, lookback)
    
    def _create_dataframe(self, minute_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        분봉 데이터를 DataFrame으로 변환
        """
        if not minute_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(minute_data)
        
        # date+time 합쳐서 datetime 문자열로 (YYYYMMDD HHMMSS 형식 통일)
        df['datetime_str'] = df['date'] + ' ' + df['time']
        
        # ✅ date 컬럼을 "YYYYMMDD HHMMSS" 형식으로 명확하게 설정
        # (signal_datetime과 매칭을 위해)
        df['date'] = df['datetime_str']  # 이미 "YYYYMMDD HHMMSS" 형태
        
        # close가 0 또는 NaN인 행 제거
        df = df[(df['close'].notna()) & (df['close'] > 0)]
        
        # 인덱스 리셋 (중요: date_to_idx 매핑을 위해)
        df = df.reset_index(drop=True)
        
        return df
    
    def _detect_signals_from_df(
        self,
        df: pd.DataFrame,
        request: IntradaySignalRequest,
        detect_fn
    ) -> List[IntradaySignalPoint]:
        """
        DataFrame에서 시그널 탐지
        """
        signal_points = []
        
        if request.direction == SignalDirection.ALL:
            up_signals_df = detect_fn(
                df, SignalDirection.UP, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            up_points = self._create_signal_points(up_signals_df, df, direction="UP")
            
            down_signals_df = detect_fn(
                df, SignalDirection.DOWN, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            down_points = self._create_signal_points(down_signals_df, df, direction="DOWN")
            
            signal_points = up_points + down_points
            signal_points.sort(key=lambda x: x.signal_datetime)
        else:
            signals_df = detect_fn(
                df, request.direction, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            signal_points = self._create_signal_points(signals_df, df, direction=request.direction.value)
        
        return signal_points
    
    def _create_signal_points(
        self,
        signals_df: pd.DataFrame,
        full_df: pd.DataFrame,
        direction: str
    ) -> List[IntradaySignalPoint]:
        """
        탐지된 시그널을 IntradaySignalPoint 객체 리스트로 변환
        """
        if len(signals_df) == 0:
            return []
        
        date_to_idx = {d: i for i, d in enumerate(full_df['date'])}
        signal_points = []
        
        for _, row in signals_df.iterrows():
            signal_datetime = row['date']
            signal_idx = date_to_idx.get(signal_datetime)
            
            if signal_idx is None:
                continue
            
            signal_point = IntradaySignalPoint(
                signal_datetime=signal_datetime,
                direction=direction,
                close=float(row['close']),
                change_7_24d=float(row['change_7_24d']),
                past_slope=float(row['past_slope']),
                past_std=float(row['past_std']),
                atr=float(row['atr']) if pd.notna(row['atr']) else None,
                prior_candles=signal_idx
            )
            signal_points.append(signal_point)
        
        return signal_points
    
    def _save_signals_immediate(
        self,
        ticker_id: int,
        configs: Dict[str, Any],
        signal_points: List[IntradaySignalPoint],
        df: pd.DataFrame,
        vector_fn,
        needs_ohlcv_data: bool,
        lookback: int
    ) -> tuple[int, int, int]:
        """
        시그널을 즉시 DB에 저장
        - 각 시그널마다 lookback 기간만 추출해서 벡터 생성
        - 각 시그널의 direction에 맞는 config_id 사용
        - 모두 저장 후 commit
        
        Returns:
            (총 저장 개수, UP 개수, DOWN 개수)
        """
        if not signal_points:
            return 0, 0, 0
        
        vector_m = 5
        date_to_idx = {d: i for i, d in enumerate(df['date'])}
        results = []
        
        # 🔍 디버깅: date 포맷 확인
        if len(df) > 0:
            logger.debug(f"DataFrame date 샘플: {df['date'].iloc[0]}")
        if len(signal_points) > 0:
            logger.debug(f"signal_datetime 샘플: {signal_points[0].signal_datetime}")
        
        skipped_no_idx = 0
        skipped_short_lookback = 0
        skipped_lookback_len = 0
        skipped_vector_quality = 0
        
        for signal_point in signal_points:
            # 시그널의 direction에 맞는 config 선택
            signal_direction = signal_point.direction
            config = configs.get(signal_direction)
            if config is None:
                logger.warning(f"Config not found for direction: {signal_direction}")
                continue
            
            config_id = config.config_id
            dt_str = signal_point.signal_datetime
            signal_datetime = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
            signal_idx = date_to_idx.get(dt_str)
            
            if signal_idx is None:
                skipped_no_idx += 1
                continue
            
            if signal_idx < lookback:
                skipped_short_lookback += 1
                continue
            
            # 🎯 lookback 기간만 추출
            start_idx = signal_idx - lookback + 1
            end_idx = signal_idx + 1
            lookback_candles = df.iloc[start_idx:end_idx].copy()  # ✅ view 문제 방지
            
            if len(lookback_candles) != lookback:
                skipped_lookback_len += 1
                continue
            
            # 🔢 벡터 생성
            try:
                if needs_ohlcv_data:
                    ohlcv_data = lookback_candles.to_dict('records')
                    shape_vector = vector_fn(
                        ohlcv_data=ohlcv_data,
                        m=vector_m,
                        w_price=1.0,
                        w_volume=1.0,
                        w_candle=1.0,
                        w_meta=0.5,
                        meta_scaler="tanh"
                    )
                else:
                    prices = lookback_candles['close'].tolist()
                    volumes = lookback_candles['volume'].tolist()
                    shape_vector = vector_fn(
                        prices=prices,
                        volumes=volumes,
                        m=vector_m,
                        w_seq=1.0,
                        w_meta=0.5,
                        meta_scaler="tanh"
                    )
                
                if shape_vector is None or len(shape_vector) == 0:
                    continue
                
                shape_vec_list = shape_vector.tolist()
                
                # ✅ 벡터 품질 게이트
                is_valid, fail_reason = _is_valid_vector(shape_vec_list)
                if not is_valid:
                    skipped_vector_quality += 1
                    logger.warning(f"Invalid vector for signal {dt_str}: {fail_reason}")
                    continue
                
                vector_dim = len(shape_vec_list)
                
            except Exception as e:
                logger.error(f"Error generating vector for signal {dt_str}: {e}")
                continue
            
            # DB 레코드 준비 (numeric 오버플로우 방지)
            # ✅ change_7_24d, past_slope, past_std, atr 안전망 (±999999 제한)
            safe_change = np.clip(signal_point.change_7_24d, -999999, 999999) if signal_point.change_7_24d is not None else None
            safe_slope = np.clip(signal_point.past_slope, -999999, 999999)
            safe_std = np.clip(signal_point.past_std, -999999, 999999)
            safe_atr = np.clip(signal_point.atr, -999999, 999999) if signal_point.atr is not None else None
            
            result_record = {
                "ticker_id": ticker_id,
                "config_id": config_id,
                "signal_datetime": signal_datetime,
                "close": Decimal(str(signal_point.close)),
                "change_7_24d": Decimal(str(safe_change)) if safe_change is not None else None,
                "past_slope": Decimal(str(safe_slope)),
                "past_std": Decimal(str(safe_std)),
                "atr": Decimal(str(safe_atr)) if safe_atr is not None else None,
                "shape_vector": shape_vec_list,
                "vector_dim": vector_dim,
                "vector_m": vector_m,
                "prior_candles": signal_point.prior_candles,
                "signal_score": None,
                "_direction": signal_direction  # ✅ 방향 임시 저장 (카운트용)
            }
            results.append(result_record)
        
        # 스킵 통계 출력
        total_skipped = skipped_no_idx + skipped_short_lookback + skipped_lookback_len + skipped_vector_quality
        if total_skipped > 0:
            logger.warning(f"스킵: idx없음={skipped_no_idx}, idx<lookback={skipped_short_lookback}, "
                  f"len불일치={skipped_lookback_len}, 벡터품질={skipped_vector_quality}")
        
        # DB Upsert + Commit
        if results:
            # direction 카운트
            saved_up = sum(1 for r in results if r.get("_direction") == "UP")
            saved_down = sum(1 for r in results if r.get("_direction") == "DOWN")
            
            # _direction 제거 (DB 저장용이 아님)
            for r in results:
                r.pop("_direction", None)
            
            self.repository.upsert_results(results)
            self.db.commit()
            return len(results), saved_up, saved_down
        
        return 0, 0, 0
    
    def search_intraday_similar_signals(
        self,
        request: IntradaySimilaritySearchRequest
    ) -> IntradaySimilaritySearchResponse:
        """
        분봉 유사도 검색 메인 로직
        
        Args:
            request: 분봉 유사도 검색 요청
            
        Returns:
            분봉 유사도 검색 응답
        """
        ticker_id = request.ticker_id
        reference_datetime = request.reference_datetime
        lookback = request.lookback
        top_k = request.top_k
        version = request.version.value
        
        logger.info(f"분봉 유사도 검색 시작 - 티커ID: {ticker_id}, 기준일시: {reference_datetime or '현재'}, 버전: {version}")
        
        # 벡터 생성 함수
        vector_fn = get_vector_generator(version)
        needs_ohlcv_data = needs_ohlcv(version)
        
        # 📊 Step 1: 티커 정보 조회
        ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        logger.debug(f"티커 정보: {ticker.symbol}:{ticker.exchange}")
        
        # 📅 Step 2: 기준일시 결정
        if reference_datetime is None:
            from datetime import datetime as dt
            now = dt.now()
            reference_datetime = now.strftime("%Y%m%d %H%M%S")
        
        logger.debug(f"기준일시: {reference_datetime}")
        
        # 📦 Step 3: 유사도 검색 최적화 - 필요한 만큼만 수집
        minute_data = self._fetch_for_similarity_search(ticker, reference_datetime, lookback)
        
        if not minute_data:
            raise ValueError(f"데이터를 수집할 수 없습니다.")
        
        # DataFrame 생성
        df = self._create_dataframe(minute_data)
        
        logger.debug(f"수집된 데이터: {len(df)}개")
        if len(df) > 0:
            logger.debug(f"기간: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
        
        # 기준일시 찾기
        ref_idx = None
        for idx, row_date in enumerate(df['date']):
            if row_date == reference_datetime:
                ref_idx = idx
                break
        
        if ref_idx is None:
            # 정확히 일치하는 시간이 없으면 기준일시 이하의 가장 가까운 시간 찾기
            df_before = df[df['date'] <= reference_datetime]
            if len(df_before) == 0:
                raise ValueError(f"기준일시({reference_datetime}) 이하의 데이터가 없습니다.")
            ref_idx = len(df_before) - 1
            actual_ref_datetime = df.iloc[ref_idx]['date']
            logger.warning(f"정확한 시간 없음, 가장 가까운 시간 사용: {actual_ref_datetime}")
        
        # 기준일시에서 lookback 확보 확인
        if ref_idx < lookback - 1:
            raise ValueError(f"기준일시({reference_datetime})의 lookback 구간이 부족합니다. (idx={ref_idx}, 필요: {lookback-1} 이상)")
        
        # lookback 기간 추출 (기준일시 포함)
        start_idx = ref_idx - lookback + 1
        end_idx = ref_idx + 1
        query_candles = df.iloc[start_idx:end_idx].copy()
        
        logger.debug(f"쿼리 캔들: {len(query_candles)}개")
        
        # 🔢 Step 4: 쿼리 벡터 생성
        try:
            if needs_ohlcv_data:
                ohlcv_data = query_candles.to_dict('records')
                query_vector = vector_fn(
                    ohlcv_data=ohlcv_data,
                    m=5,
                    w_price=1.0,
                    w_volume=1.0,
                    w_candle=1.0,
                    w_meta=0.5,
                    meta_scaler="tanh"
                )
            else:
                prices = query_candles['close'].tolist()
                volumes = query_candles['volume'].tolist()
                query_vector = vector_fn(
                    prices=prices,
                    volumes=volumes,
                    m=5,
                    w_seq=1.0,
                    w_meta=0.5,
                    meta_scaler="tanh"
                )
            
            query_vec_list = query_vector.tolist()
            vector_dim = len(query_vec_list)
            
            logger.debug(f"쿼리 벡터 생성 완료: {vector_dim}차원")
            
        except Exception as e:
            logger.error(f"Failed to generate query vector: {e}")
            raise ValueError(f"쿼리 벡터 생성 실패: {str(e)}")
        
        # 🔍 Step 5: DB에서 유사도 검색
        direction_filter_str = request.direction_filter.value if request.direction_filter else None
        
        similar_results = self.repository.search_similar_vectors(
            query_vector=query_vec_list,
            top_k=top_k,
            direction_filter=direction_filter_str,
            version_filter=version
        )
        
        logger.debug(f"유사도 검색 완료: {len(similar_results)}건")
        
        # 📊 Step 6: 응답 생성
        similar_signals = []
        for row in similar_results:
            # signal_datetime을 문자열로 변환
            signal_dt = row['signal_datetime']
            if isinstance(signal_dt, datetime):
                signal_dt_str = signal_dt.strftime("%Y%m%d %H%M%S")
            else:
                signal_dt_str = str(signal_dt)
            
            similar_signals.append(IntradaySimilarSignal(
                result_id=row['result_id'],
                ticker_id=row['ticker_id'],
                symbol=row['symbol'],
                exchange=row['exchange'],
                signal_datetime=signal_dt_str,
                direction=row['direction'],
                close=float(row['close']),
                change_7_24d=float(row['change_7_24d']) if row['change_7_24d'] is not None else 0.0,
                similarity=float(row['similarity']),
                config_id=row['config_id']
            ))
        
        response = IntradaySimilaritySearchResponse(
            query_ticker_id=ticker_id,
            query_symbol=ticker.symbol,
            query_exchange=ticker.exchange,
            reference_datetime=reference_datetime,
            lookback=lookback,
            vector_dim=vector_dim,
            total_compared=len(similar_results),
            similar_signals=similar_signals
        )
        
        logger.info("분봉 유사도 검색 완료!")
        
        return response
