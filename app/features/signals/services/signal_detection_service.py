# app/features/signals/services/signal_detection_service.py
from __future__ import annotations
import logging
from datetime import date
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

import pandas as pd
import numpy as np

from app.shared.models.ticker import Ticker
from app.features.signals.repositories.signal_repository import SignalRepository
from app.features.signals.models.signal_models import (
    SignalDetectionRequest,
    SignalDetectionResponse,
    SignalPoint,
    SignalDirection,
    SaveOption
)
from app.features.marketdata.services.kr_daily_ingestor import KRDailyIngestor
from app.features.marketdata.services.us_daily_ingestor import USDailyIngestor
from app.features.signals.repositories.trend_detection_repository import TrendDetectionRepository
from app.features.signals.utils.shape_vector import get_vector_generator
from app.features.signals.utils.trend_detector import get_trend_detector
from app.features.signals.models.similarity_models import (
    SimilaritySearchRequest,
    SimilaritySearchResponse,
    SimilarSignal
)


logger = logging.getLogger(__name__)


class SignalDetectionService:
    """
    🎯 시그널 탐지 서비스
    - 상승 개시점 탐지 알고리즘 실행
    - 데이터 부족 시 자동 적재
    - 버전별 탐지/벡터 생성 알고리즘 사용 가능
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = SignalRepository(db)
        self.trend_repo = TrendDetectionRepository(db)
    
    def detect_signals(self, request: SignalDetectionRequest) -> SignalDetectionResponse:
        """
        시그널 탐지 메인 로직
        
        Args:
            request: 시그널 탐지 요청
            
        Returns:
            시그널 탐지 응답
        """
        # 🔍 ticker_id가 없으면 배치 처리
        if request.ticker_id is None:
            # SAVE 모드가 아니면 에러
            if request.save_option != SaveOption.SAVE:
                raise ValueError("ticker_id가 없는 경우 save_option은 SAVE여야 합니다. (전체 티커 배치 처리는 SAVE 모드에서만 가능)")
            
            return self._detect_signals_batch(request)
        
        # 단일 티커 처리
        ticker_id = request.ticker_id
        days = request.days
        version = request.version.value  # Enum -> str
        
        logger.info(f"시그널 탐지 시작 - 티커ID: {ticker_id}, 기간: {days}일, 버전: {version}, lookback: {request.lookback}")
        
        # 버전별 함수 로드
        detect_fn = get_trend_detector(version)
        vector_fn = get_vector_generator(version)
        
        # 📊 Step 1: 티커 정보 조회
        ticker = self._get_ticker(ticker_id)
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        logger.debug(f"티커 정보: {ticker.symbol}:{ticker.exchange} ({ticker.country})")
        
        # 📈 Step 2: 데이터 확인 및 부족 시 적재
        # lookback + future_window 때문에 더 많은 데이터 필요
        required_days = days + request.lookback + request.future_window
        self._ensure_sufficient_data(ticker, required_days)
        
        # 📦 Step 3: 일봉 데이터 조회 (days 파라미터로 limit 설정)
        daily_data = self.repo.get_daily_data_as_dict(ticker_id, limit=required_days, order_desc=False)
        
        if len(daily_data) < 20:  # 최소한의 데이터 필요
            raise ValueError(f"데이터가 부족합니다. (현재: {len(daily_data)}건, 최소: 20건 필요)")
        
        logger.debug(f"조회된 일봉 데이터: {len(daily_data)}건")
        logger.info(f"Retrieved {len(daily_data)} daily records")
        
        # 🔍 Step 4: 시그널 탐지
        df = pd.DataFrame(daily_data)
        
        # ALL일 때는 UP과 DOWN 둘 다 탐지
        if request.direction == SignalDirection.ALL:
            logger.debug(f"ALL 모드: UP과 DOWN 둘 다 탐지 (버전: {version})")
            
            # UP 시그널 탐지 (버전별 함수 사용)
            up_signals_df = detect_fn(
                df, SignalDirection.UP, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            up_points = self._create_signal_points(up_signals_df, df, direction="UP")
            logger.debug(f"UP 시그널: {len(up_points)}건")
            
            # DOWN 시그널 탐지 (버전별 함수 사용)
            down_signals_df = detect_fn(
                df, SignalDirection.DOWN, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            down_points = self._create_signal_points(down_signals_df, df, direction="DOWN")
            logger.debug(f"DOWN 시그널: {len(down_points)}건")
            
            # 합치기
            signal_points = up_points + down_points
            # 날짜순 정렬
            signal_points.sort(key=lambda x: x.signal_date)
            
            up_count = len(up_points)
            down_count = len(down_points)
            
        else:
            # UP 또는 DOWN 하나만 탐지 (버전별 함수 사용)
            logger.debug(f"{request.direction.value} 시그널 탐지 (버전: {version})")
            signals_df = detect_fn(
                df, request.direction, request.lookback, request.future_window,
                request.min_change, request.max_reverse, request.flatness_k
            )
            
            direction_str = request.direction.value
            signal_points = self._create_signal_points(signals_df, df, direction=direction_str)
            
            if request.direction == SignalDirection.UP:
                up_count = len(signal_points)
                down_count = 0
            else:
                up_count = 0
                down_count = len(signal_points)
        
        logger.debug(f"탐지된 시그널: 총 {len(signal_points)}건 (UP: {up_count}, DOWN: {down_count})")
        
        logger.debug(f"최종 시그널: {len(signal_points)}건 (lookback={request.lookback}로 자동 보장)")
        
        # 💾 Step 5.5: 저장 옵션 확인 및 DB 저장
        if request.save_option == SaveOption.SAVE and len(signal_points) > 0:
            self._save_signals_to_db(
                ticker=ticker,
                request=request,
                full_df=df,
                signal_points=signal_points,
                vector_fn=vector_fn
            )
        
        # 📊 Step 6: 응답 생성
        data_start_date = df['date'].min() if len(df) > 0 else None
        data_end_date = df['date'].max() if len(df) > 0 else None
        
        response = SignalDetectionResponse(
            ticker_id=ticker_id,
            symbol=ticker.symbol,
            exchange=ticker.exchange,
            requested_direction=request.direction.value,
            total_candles=len(daily_data),
            total_signals=len(signal_points),
            up_signals=up_count,
            down_signals=down_count,
            lookback=request.lookback,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            signals=signal_points
        )
        
        logger.info("시그널 탐지 완료!")
        logger.info(f"Signal detection completed: {len(signal_points)} signals found")
        
        return response
    
    def _get_ticker(self, ticker_id: int) -> Optional[Ticker]:
        """티커 정보 조회"""
        return self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
    
    def _ensure_sufficient_data(self, ticker: Ticker, required_days: int):
        """
        데이터 충분성 확인 및 부족 시 적재
        
        Args:
            ticker: 티커 객체
            required_days: 필요한 일수
        """
        current_count = self.repo.count_final_daily_data(ticker.id)
        logger.debug(f"현재 is_final 데이터: {current_count}건 (필요: {required_days}건)")
        
        if current_count >= required_days:
            logger.debug("충분한 데이터가 있습니다.")
            return
        
        # 데이터 부족 → 추가 적재 필요
        shortage = required_days - current_count
        logger.info(f"데이터 부족 ({shortage}건) → 추가 적재 시작")
        
        # 국가별 ingestor 선택
        country = ticker.country.upper() if ticker.country else ""
        
        if country == "KR":
            self._ingest_kr_data(ticker, required_days)
        elif country == "US":
            self._ingest_us_data(ticker, required_days)
        else:
            raise ValueError(f"지원하지 않는 국가입니다: {country}")
    
    def _ingest_kr_data(self, ticker: Ticker, days: int):
        """
        국내주식 데이터 추가 적재
        
        Args:
            ticker: 티커 객체
            days: 필요한 일수
        """
        logger.info(f"국내주식 데이터 적재 시작: {ticker.symbol}:{ticker.exchange}")
        logger.info(f"Ingesting KR data for {ticker.symbol}:{ticker.exchange}")
        
        ingestor = KRDailyIngestor(self.db)
        
        # 기존 데이터의 가장 과거 날짜 확인
        min_date = self.repo.get_min_trade_date(ticker.id, is_final=True)
        if min_date:
            logger.debug(f"기존 데이터 최소 날짜: {min_date}")
        
        # 추가 데이터 적재 (days * 1.5배 정도로 넉넉하게)
        result = ingestor.sync_for_ticker_ids([ticker.id], days=int(days * 1.5))
        
        logger.info(f"국내주식 데이터 적재 완료: {result}")
        logger.info(f"KR data ingestion result: {result}")
    
    def _ingest_us_data(self, ticker: Ticker, days: int):
        """
        미국주식 데이터 추가 적재
        
        Args:
            ticker: 티커 객체
            days: 필요한 일수
        """
        logger.info(f"미국주식 데이터 적재 시작: {ticker.symbol}:{ticker.exchange}")
        logger.info(f"Ingesting US data for {ticker.symbol}:{ticker.exchange}")
        
        ingestor = USDailyIngestor(self.db)
        
        # 기존 데이터의 가장 과거 날짜 확인
        min_date = self.repo.get_min_trade_date(ticker.id, is_final=True)
        if min_date:
            logger.debug(f"기존 데이터 최소 날짜: {min_date}")
        
        # 추가 데이터 적재 (days * 1.5배 정도로 넉넉하게)
        result = ingestor.sync_for_ticker_ids([ticker.id], days=int(days * 1.5))
        
        logger.info(f"미국주식 데이터 적재 완료: {result}")
        logger.info(f"US data ingestion result: {result}")
    
    def _create_signal_points(
        self,
        signals_df: pd.DataFrame,
        full_df: pd.DataFrame,
        direction: str
    ) -> List[SignalPoint]:
        """
        탐지된 시그널을 SignalPoint 객체 리스트로 변환
        
        Args:
            signals_df: 탐지된 시그널 DataFrame
            full_df: 전체 일봉 DataFrame
            direction: 시그널 방향 (UP/DOWN)
            
        Returns:
            시그널 포인트 리스트
        """
        if len(signals_df) == 0:
            return []
        
        # 날짜 인덱스 맵 생성 (date -> 인덱스)
        date_to_idx = {d: i for i, d in enumerate(full_df['date'])}
        
        signal_points = []
        
        for _, row in signals_df.iterrows():
            signal_date = row['date']
            signal_idx = date_to_idx.get(signal_date)
            
            if signal_idx is None:
                continue
            
            # 시그널 이전 캔들 개수 = 시그널 인덱스
            prior_candles = signal_idx
            
            signal_point = SignalPoint(
                signal_date=signal_date,
                direction=direction,
                close=float(row['close']),
                change_7_24d=float(row['change_7_24d']),
                past_slope=float(row['past_slope']),
                past_std=float(row['past_std']),
                atr=float(row['atr']) if pd.notna(row['atr']) else None,
                prior_candles=prior_candles
            )
            signal_points.append(signal_point)
            logger.debug(f"{direction} 시그널: {signal_date} (앞캔들: {prior_candles}개, 변동률: {row['change_7_24d']:.2%})")
        
        return signal_points
    
    def _save_signals_to_db(
        self,
        ticker: Ticker,
        request: SignalDetectionRequest,
        full_df: pd.DataFrame,
        signal_points: List[SignalPoint],
        vector_fn
    ):
        """
        시그널을 DB에 저장 (벡터 생성 포함)
        
        Args:
            ticker: 티커 객체
            request: 탐지 요청
            full_df: 전체 일봉 DataFrame
            signal_points: 생성된 시그널 포인트 리스트 (각 포인트에 direction 포함)
            vector_fn: 벡터 생성 함수
        """
        from app.features.signals.utils.shape_vector import needs_ohlcv
        
        version_str = request.version.value  # Enum -> str
        
        logger.debug("DB 저장 시작...")
        logger.info(f"Saving {len(signal_points)} signals to DB (version: {version_str})")
        
        # 📝 Step 1: 방향별 Config 조회 또는 생성 (ALL일 때는 UP/DOWN 각각)
        configs = {}
        
        if request.direction == SignalDirection.ALL:
            # UP Config
            configs['UP'] = self.trend_repo.get_or_create_config(
                direction='UP',
                lookback=request.lookback,
                future_window=request.future_window,
                min_change=float(request.min_change),
                max_reverse=float(request.max_reverse),
                flatness_k=float(request.flatness_k),
                atr_window=7,
                version=version_str
            )
            # DOWN Config
            configs['DOWN'] = self.trend_repo.get_or_create_config(
                direction='DOWN',
                lookback=request.lookback,
                future_window=request.future_window,
                min_change=float(request.min_change),
                max_reverse=float(request.max_reverse),
                flatness_k=float(request.flatness_k),
                atr_window=7,
                version=version_str
            )
            logger.debug(f"Config ID - UP: {configs['UP'].config_id} (v{version_str}), DOWN: {configs['DOWN'].config_id} (v{version_str})")
        else:
            # UP 또는 DOWN 하나만
            direction_key = request.direction.value
            configs[direction_key] = self.trend_repo.get_or_create_config(
                direction=direction_key,
                lookback=request.lookback,
                future_window=request.future_window,
                min_change=float(request.min_change),
                max_reverse=float(request.max_reverse),
                flatness_k=float(request.flatness_k),
                atr_window=7,
                version=version_str
            )
            logger.debug(f"Config ID ({direction_key}): {configs[direction_key].config_id} (v{version_str})")
        
        logger.info(f"Configs created/retrieved: {list(configs.keys())} (version: {version_str})")
        
        # 🔢 Step 2: 날짜 인덱스 맵 생성
        date_to_idx = {d: i for i, d in enumerate(full_df['date'])}
        
        # 💾 Step 3: 각 시그널에 대해 벡터 생성 및 저장 준비
        results = []
        vector_m = 5  # PAA 리샘플링 길이
        
        for signal_point in signal_points:
            signal_date = signal_point.signal_date
            signal_direction = signal_point.direction  # UP 또는 DOWN
            signal_idx = date_to_idx.get(signal_date)
            
            if signal_idx is None or signal_idx < request.lookback:
                continue
            
            # 해당 시그널의 방향에 맞는 config_id 선택
            config = configs.get(signal_direction)
            if config is None:
                logger.warning(f"Config 없음: {signal_direction}")
                continue
            
            # 🎯 시그널 날짜 포함 lookback 기간 추출
            # 예: signal_idx=15, lookback=10 → 인덱스 6~15 (10개)
            start_idx = signal_idx - request.lookback + 1
            end_idx = signal_idx + 1
            
            lookback_candles = full_df.iloc[start_idx:end_idx]
            
            if len(lookback_candles) != request.lookback:
                logger.warning(f"시그널 {signal_date}: lookback 기간 부족 ({len(lookback_candles)} < {request.lookback})")
                continue
            
            # 🔢 벡터 생성 (버전별 함수 사용)
            try:
                # 버전에 따라 데이터 형식 결정
                if needs_ohlcv(version_str):
                    # v2, v3: OHLCV 전체 필요
                    ohlcv_data = lookback_candles.to_dict('records')
                    shape_vec = vector_fn(
                        ohlcv_data=ohlcv_data,
                        m=vector_m,
                        w_price=1.0,
                        w_volume=1.0,
                        w_candle=1.0,
                        w_meta=0.5,
                        meta_scaler="tanh"
                    )
                else:
                    # v1: prices, volumes만 필요
                    prices = lookback_candles['close'].tolist()
                    volumes = lookback_candles['volume'].tolist()
                    shape_vec = vector_fn(
                        prices=prices,
                        volumes=volumes,
                        m=vector_m,
                        w_seq=1.0,
                        w_meta=0.5,
                        meta_scaler="tanh"
                    )
                
                # NumPy array를 Python list로 변환 (PostgreSQL ARRAY 타입용)
                shape_vec_list = shape_vec.tolist()
                
                results.append({
                    "ticker_id": ticker.id,
                    "config_id": config.config_id,
                    "signal_date": signal_date,
                    "close": float(signal_point.close),
                    "change_7_24d": float(signal_point.change_7_24d),
                    "past_slope": float(signal_point.past_slope),
                    "past_std": float(signal_point.past_std),
                    "shape_vector": shape_vec_list,
                    "vector_dim": len(shape_vec_list),
                    "vector_m": vector_m,
                    "prior_candles": signal_point.prior_candles,
                    "signal_score": None  # 추후 확장용
                })
                
                logger.debug(f"{signal_direction} 벡터 생성 완료 (v{version_str}): {signal_date} (dim={len(shape_vec_list)})")
                
            except Exception as e:
                logger.error(f"벡터 생성 실패 for {signal_date}: {e}")
                continue
        
        # 💾 Step 4: DB에 일괄 저장
        if results:
            count = self.trend_repo.upsert_results(results)
            logger.debug(f"DB 저장 완료: {count}건")
        else:
            logger.warning("저장할 데이터 없음")
    
    def search_similar_signals(self, request: SimilaritySearchRequest) -> SimilaritySearchResponse:
        """
        유사도 검색 메인 로직
        
        Args:
            request: 유사도 검색 요청
            
        Returns:
            유사도 검색 응답
        """
        # 🔀 배치 처리 분기
        if request.ticker_id is None and request.save:
            return self._search_similar_signals_batch(request)
        
        # Validation: ticker_id가 없으면 에러
        if request.ticker_id is None:
            raise ValueError("ticker_id는 save=False일 때 필수입니다.")
        
        ticker_id = request.ticker_id
        reference_date = request.reference_date
        lookback = request.lookback
        top_k = request.top_k
        version = request.version.value  # Enum -> str
        
        logger.info(f"유사도 검색 시작 - 티커ID: {ticker_id}, 기준일자: {reference_date or '오늘'}, 버전: {version}, lookback: {lookback}")
        
        # 버전별 함수 로드
        vector_fn = get_vector_generator(version)
        
        # 📊 Step 1: 티커 정보 조회
        ticker = self._get_ticker(ticker_id)
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        logger.debug(f"티커 정보: {ticker.symbol}:{ticker.exchange} ({ticker.country})")
        
        # 📅 Step 2: 기준일자 결정
        if reference_date is None:
            from datetime import datetime
            reference_date = datetime.now().date()
        
        logger.debug(f"기준일자: {reference_date}")
        logger.info(f"Reference date: {reference_date}")
        
        # 📦 Step 3: 기준일자 기준 lookback 기간 캔들 조회
        all_candles = self.repo.get_daily_data_as_dict(ticker_id, order_desc=False)
        
        if len(all_candles) < lookback:
            raise ValueError(f"데이터가 부족합니다. (현재: {len(all_candles)}건, 최소: {lookback}건 필요)")
        
        # 기준일자 이하의 캔들만 필터링
        filtered_candles = [c for c in all_candles if c['date'] <= reference_date]
        
        if len(filtered_candles) < lookback:
            raise ValueError(f"기준일자({reference_date}) 이하의 데이터가 부족합니다. (현재: {len(filtered_candles)}건, 최소: {lookback}건 필요)")
        
        # 최근 lookback개 추출
        query_candles = filtered_candles[-lookback:]
        
        logger.debug(f"조회된 캔들: {len(query_candles)}개 ({query_candles[0]['date']} ~ {query_candles[-1]['date']})")
        logger.info(f"Retrieved {len(query_candles)} candles for vectorization")
        
        # 🔢 Step 4: 쿼리 벡터 생성 (버전별 함수 사용)
        from app.features.signals.utils.shape_vector import needs_ohlcv
        
        try:
            # 버전에 따라 데이터 형식 결정
            if needs_ohlcv(version):
                # v2, v3: OHLCV 전체 필요
                query_vector = vector_fn(
                    ohlcv_data=query_candles,
                    m=5,
                    w_price=1.0,
                    w_volume=1.0,
                    w_candle=1.0,
                    w_meta=0.5,
                    meta_scaler="tanh"
                )
            else:
                # v1: prices, volumes만 필요
                prices = [c['close'] for c in query_candles]
                volumes = [c['volume'] for c in query_candles]
                query_vector = vector_fn(
                    prices=prices,
                    volumes=volumes,
                    m=5,
                    w_seq=1.0,
                    w_meta=0.5,
                    meta_scaler="tanh"
                )
            
            logger.debug(f"쿼리 벡터 생성 완료 (v{version}): 차원={len(query_vector)}")
            
        except Exception as e:
            logger.error(f"벡터 생성 실패: {e}")
            raise ValueError(f"벡터 생성 실패: {str(e)}")
        
        # 🗄️ Step 5: DB에서 유사도 검색 (쿼리 벡터를 DB로 전달)
        direction_filter = request.direction_filter.value if request.direction_filter else None
        version_filter = version
        
        logger.debug(f"DB에서 유사도 검색 시작 (version={version_filter})...")
        logger.info(f"Starting similarity search in DB with direction_filter={direction_filter}, version={version_filter}")
        
        # 쿼리 벡터를 Python list로 변환 (NumPy array인 경우)
        query_vector_list = query_vector.tolist() if hasattr(query_vector, 'tolist') else list(query_vector)
        
        # DB에서 직접 유사도 계산 및 TOP K 반환
        top_similarities = self.trend_repo.search_similar_vectors(
            query_vector=query_vector_list,
            direction_filter=direction_filter,
            version_filter=version_filter,
            top_k=top_k
        )
        
        logger.debug(f"DB 검색 완료: TOP {len(top_similarities)}개 유사 시그널")
        
        if len(top_similarities) == 0:
            return SimilaritySearchResponse(
                query_ticker_id=ticker_id,
                query_symbol=ticker.symbol,
                query_exchange=ticker.exchange,
                reference_date=reference_date,
                lookback=lookback,
                vector_dim=len(query_vector),
                total_compared=0,
                similar_signals=[]
            )
        
        # 📊 Step 6: 응답 생성
        similar_signals = []
        
        for item in top_similarities:
            result = item["result"]
            direction = item["direction"]
            sim = item["similarity"]
            
            # 티커 정보 조회
            result_ticker = self._get_ticker(result.ticker_id)
            if not result_ticker:
                continue
            
            # Config 정보 조회 (relationship 사용)
            direction_str = result.config.direction if result.config else "UNKNOWN"
            
            similar_signals.append(SimilarSignal(
                result_id=result.result_id,
                ticker_id=result.ticker_id,
                symbol=result_ticker.symbol,
                exchange=result_ticker.exchange,
                signal_date=result.signal_date,
                direction=direction,
                close=float(result.close) if result.close else 0.0,
                change_7_24d=float(result.change_7_24d) if result.change_7_24d else 0.0,
                similarity=sim,
                config_id=result.config_id
            ))
        
        logger.info(f"TOP {len(similar_signals)}개 유사 시그널 반환")
        logger.info(f"Returning top {len(similar_signals)} similar signals")
        
        response = SimilaritySearchResponse(
            query_ticker_id=ticker_id,
            query_symbol=ticker.symbol,
            query_exchange=ticker.exchange,
            reference_date=reference_date,
            lookback=lookback,
            vector_dim=len(query_vector),
            total_compared=len(top_similarities),  # DB에서 직접 TOP K만 반환되므로
            similar_signals=similar_signals
        )
        
        # 📝 Step 7: save=True일 때 분석 결과 저장
        if request.save and len(similar_signals) > 0:
            logger.info(f"분석 결과 저장 시작 - 티커ID: {ticker_id}")
            
            import numpy as np
            from app.features.signals.repositories.similarity_analysis_repository import SimilarityAnalysisRepository
            from app.shared.models.ticker_i18n import TickerI18n
            
            # 한글 기업명 조회
            ticker_name_ko = None
            ticker_i18n = self.db.query(TickerI18n).filter(
                TickerI18n.ticker_id == ticker_id,
                TickerI18n.lang_code == "ko"
            ).first()
            if ticker_i18n:
                ticker_name_ko = ticker_i18n.name
            
            # 유사도 가중치 계산
            sims = np.array([s.similarity for s in similar_signals])
            chgs = np.array([
                s.change_7_24d if s.direction == "UP" else -s.change_7_24d
                for s in similar_signals
            ])
            weights = sims / np.sum(sims)
            
            # 상승/하락 마스크
            up_mask = chgs > 0
            down_mask = chgs < 0
            
            # 확률 계산
            p_up = float(np.sum(weights[up_mask]))
            p_down = float(np.sum(weights[down_mask]))
            
            # 기대 변동률 계산
            exp_up = float(np.sum(weights[up_mask] * chgs[up_mask]) / max(p_up, 1e-9)) if p_up > 0 else 0.0
            exp_down = float(np.sum(weights[down_mask] * chgs[down_mask]) / max(p_down, 1e-9)) if p_down > 0 else 0.0
            
            # TOP 1 유사도
            top_similarity = float(similar_signals[0].similarity)
            
            # DB에 저장
            similarity_repo = SimilarityAnalysisRepository(self.db)
            similarity_repo.upsert(
                ticker_id=ticker_id,
                ticker_name_ko=ticker_name_ko,
                exchange=ticker.exchange,
                p_up=p_up,
                p_down=p_down,
                exp_up=exp_up,
                exp_down=exp_down,
                top_similarity=top_similarity
            )
            
            logger.info(f"분석 결과 저장 완료 - {ticker_name_ko or ticker.symbol} - top_sim: {top_similarity:.4f}, p_up: {p_up:.4f}, p_down: {p_down:.4f}, exp_up: {exp_up:.4f}, exp_down: {exp_down:.4f}")
        
        return response
    
    def _search_similar_signals_batch(self, request: SimilaritySearchRequest) -> SimilaritySearchResponse:
        """
        🔄 전체 티커 유사도 분석 배치 처리
        
        Args:
            request: 유사도 검색 요청 (ticker_id=None, save=True)
            
        Returns:
            배치 처리 요약 응답
        """
        from datetime import datetime
        
        reference_date = request.reference_date or datetime.now().date()
        
        country_filter = request.country.value if request.country else "ALL"
        
        logger.info(f"배치 유사도 분석 시작 - 국가: {country_filter}, 날짜: {reference_date}")
        logger.info(f"Starting batch similarity analysis for country: {country_filter}")
        
        # 📊 전체 활성 티커 조회 (country 필터 적용)
        query = self.db.query(Ticker)
        
        if country_filter != "ALL":
            query = query.filter(Ticker.country == country_filter)
        
        tickers = query.all()
        logger.info(f"처리할 티커 수: {len(tickers)}개 (country: {country_filter})")
        logger.info(f"Found {len(tickers)} tickers for country: {country_filter}")
        
        # 배치 처리 결과
        total_tickers = len(tickers)
        success_count = 0
        error_count = 0
        saved_count = 0
        
        # 각 티커별로 유사도 분석
        for idx, ticker in enumerate(tickers, 1):
            try:
                logger.debug(f"[{idx}/{total_tickers}] 처리 중: {ticker.symbol}:{ticker.exchange} (ID: {ticker.id})")
                logger.info(f"Processing ticker [{idx}/{total_tickers}]: {ticker.symbol}:{ticker.exchange}")
                
                # 단일 티커 요청 생성
                single_request = SimilaritySearchRequest(
                    ticker_id=ticker.id,
                    reference_date=reference_date,
                    lookback=request.lookback,
                    top_k=request.top_k,
                    direction_filter=request.direction_filter,
                    version=request.version,
                    save=True  # 배치 모드는 항상 save=True
                )
                
                # 유사도 검색 (재귀 호출이지만 ticker_id가 있으므로 단일 처리)
                result = self.search_similar_signals(single_request)
                
                # 티커별 처리 완료 후 커밋 (독립적인 트랜잭션)
                self.db.commit()
                
                success_count += 1
                saved_count += 1  # save=True이므로 항상 저장됨
                
                logger.debug(f"{ticker.symbol}:{ticker.exchange} 완료 - 유사 시그널 {len(result.similar_signals)}건")
                
            except Exception as e:
                # 에러 발생 시 해당 티커만 롤백
                self.db.rollback()
                error_count += 1
                logger.error(f"{ticker.symbol}:{ticker.exchange} 오류: {e}", exc_info=True)
                logger.error(f"Error processing ticker {ticker.symbol}:{ticker.exchange}: {e}", exc_info=True)
        
        # 배치 처리 요약 응답
        logger.info(f"배치 처리 완료 - 총: {total_tickers}, 성공: {success_count}, 실패: {error_count}, 저장: {saved_count}")
        logger.info(f"Batch processing completed - Total: {total_tickers}, Success: {success_count}, Error: {error_count}, Saved: {saved_count}")
        
        return SimilaritySearchResponse(
            query_ticker_id=None,
            query_symbol=None,
            query_exchange=None,
            reference_date=reference_date,
            lookback=request.lookback,
            vector_dim=None,
            total_compared=0,
            similar_signals=[],
            is_batch=True,
            total_tickers=total_tickers,
            success_count=success_count,
            error_count=error_count,
            saved_count=saved_count
        )
    
    def _detect_signals_batch(self, request: SignalDetectionRequest) -> SignalDetectionResponse:
        """
        🔄 전체 티커 배치 처리
        
        Args:
            request: 시그널 탐지 요청 (ticker_id=None)
            
        Returns:
            배치 처리 요약 응답
        """
        logger.info("배치 시그널 탐지 시작 - 전체 티커 처리")
        logger.info(f"Starting batch signal detection for all tickers")
        
        # 📊 전체 활성 티커 조회
        tickers = self.db.query(Ticker).all()
        logger.info(f"처리할 티커 수: {len(tickers)}개")
        logger.info(f"Found {len(tickers)} active tickers")
        
        # 배치 처리 결과
        total_processed = 0
        total_up_signals = 0
        total_down_signals = 0
        success_count = 0
        error_count = 0
        ticker_results = []
        
        # 각 티커별로 시그널 탐지
        for idx, ticker in enumerate(tickers, 1):
            try:
                logger.debug(f"[{idx}/{len(tickers)}] 처리 중: {ticker.symbol}:{ticker.exchange} (ID: {ticker.id})")
                logger.info(f"Processing ticker [{idx}/{len(tickers)}]: {ticker.symbol}:{ticker.exchange}")
                
                # 단일 티커 요청 생성
                single_request = SignalDetectionRequest(
                    ticker_id=ticker.id,
                    days=request.days,
                    direction=request.direction,
                    save_option=request.save_option,
                    version=request.version,  # ✅ 버전 전달
                    lookback=request.lookback,
                    future_window=request.future_window,
                    min_change=request.min_change,
                    max_reverse=request.max_reverse,
                    flatness_k=request.flatness_k
                )
                
                # 시그널 탐지 (재귀 호출이지만 ticker_id가 있으므로 단일 처리)
                result = self.detect_signals(single_request)
                
                # 티커별 처리 완료 후 커밋 (독립적인 트랜잭션)
                self.db.commit()
                
                total_processed += 1
                success_count += 1
                total_up_signals += result.up_signals
                total_down_signals += result.down_signals
                
                # 티커별 요약 정보 저장
                ticker_results.append({
                    "ticker_id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "total_signals": result.total_signals,
                    "up_signals": result.up_signals,
                    "down_signals": result.down_signals,
                    "status": "success"
                })
                
                logger.debug(f"{ticker.symbol}:{ticker.exchange} 완료 - 시그널 {result.total_signals}건 (UP: {result.up_signals}, DOWN: {result.down_signals})")
                
            except Exception as e:
                # 에러 발생 시 해당 티커만 롤백
                self.db.rollback()
                error_count += 1
                logger.error(f"{ticker.symbol}:{ticker.exchange} 오류: {e}", exc_info=True)
                logger.error(f"Error processing ticker {ticker.symbol}:{ticker.exchange}: {e}", exc_info=True)
                
                ticker_results.append({
                    "ticker_id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "total_signals": 0,
                    "up_signals": 0,
                    "down_signals": 0,
                    "status": "error",
                    "error_message": str(e)
                })
        
        # 배치 처리 요약 정보
        batch_summary = {
            "total_tickers": len(tickers),
            "processed": total_processed,
            "success": success_count,
            "errors": error_count,
            "total_up_signals": total_up_signals,
            "total_down_signals": total_down_signals,
            "ticker_results": ticker_results
        }
        
        logger.info(f"배치 처리 완료! 처리 결과: {success_count}/{len(tickers)} 성공, {error_count} 실패, 총 시그널: {total_up_signals + total_down_signals}건 (UP: {total_up_signals}, DOWN: {total_down_signals})")
        logger.info(f"Batch processing completed: {success_count}/{len(tickers)} success, {error_count} errors")
        logger.info(f"Total signals: {total_up_signals + total_down_signals} (UP: {total_up_signals}, DOWN: {total_down_signals})")
        
        # 배치 응답 생성
        response = SignalDetectionResponse(
            ticker_id=None,
            symbol=None,
            exchange=None,
            requested_direction=request.direction.value,
            total_candles=0,  # 배치 모드에서는 의미 없음
            total_signals=total_up_signals + total_down_signals,
            up_signals=total_up_signals,
            down_signals=total_down_signals,
            lookback=request.lookback,
            data_start_date=None,
            data_end_date=None,
            signals=[],  # 배치 모드에서는 개별 시그널 반환하지 않음
            is_batch=True,
            batch_summary=batch_summary
        )
        
        return response

