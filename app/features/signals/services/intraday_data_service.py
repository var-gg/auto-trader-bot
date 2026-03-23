# app/features/signals/services/intraday_data_service.py
"""
5분봉 데이터 수집 서비스 (메모리 전용)
- 해외주식: 5분봉 직접 조회 (KEYB 페이징)
- 국내주식: 2분봉 → 5분봉 리샘플링
- DB 저장 없이 메모리에서만 처리
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import pandas as pd

from app.core.kis_client import KISClient
from app.core.config import KIS_OVERSEAS_EXCHANGE_MAP
from app.shared.models.ticker import Ticker


logger = logging.getLogger(__name__)


class IntradayDataService:
    """
    5분봉 데이터 수집 서비스
    - API 호출하여 메모리에서만 처리
    - 페이징 처리 (KEYB 커서 또는 날짜/시간 기준)
    - 거래소 코드 자동 변환
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.client = KISClient(db)
        self.exchange_map = KIS_OVERSEAS_EXCHANGE_MAP
    
    def fetch_us_minute_data(
        self,
        ticker: Ticker,
        candles: int = 100
    ) -> List[Dict[str, Any]]:
        """
        해외주식 5분봉 데이터 수집
        
        Args:
            ticker: 티커 객체
            candles: 필요한 캔들 개수
            
        Returns:
            5분봉 데이터 리스트 [{date, time, open, high, low, close, volume}, ...]
        """
        logger.info(f"해외주식 5분봉 수집 시작: {ticker.symbol}:{ticker.exchange}, {candles}개")
        
        # 거래소 코드 변환 (Yahoo Finance → KIS)
        kis_exchange = self.exchange_map.get(ticker.exchange.upper(), "NAS")
        logger.debug(f"거래소 코드 변환: {ticker.exchange} → {kis_exchange}")
        
        all_rows = []
        next_flag = ""  # 첫 호출은 빈 값
        keyb = ""       # 첫 호출은 빈 값
        max_iterations = (candles // 120) + 2  # 안전 마진
        
        for iteration in range(max_iterations):
            try:
                logger.debug(f"API 호출 {iteration + 1}/{max_iterations}, NEXT={next_flag or '(첫 호출)'}, KEYB={keyb or '(비어있음)'}")
                
                # KIS API 호출 (NMIN=5: 5분봉)
                response = self.client.overseas_minute_price_test(
                    EXCD=kis_exchange,  # ✅ 변환된 거래소 코드 사용
                    SYMB=ticker.symbol,
                    NMIN="5",   # 5분봉
                    PINC="1",   # ✅ 전일포함 (고정)
                    NEXT=next_flag,  # ✅ 첫 호출: "", 이후: "1"
                    KEYB=keyb,  # ✅ 첫 호출: "", 이후: 가장 과거 날짜시간
                    NREC="120"
                )
                
                # output2 파싱
                output2 = response.get("output2") or []
                if not output2:
                    logger.warning("output2가 비어있음, 종료")
                    break
                
                logger.debug(f"수신: {len(output2)}건")
                
                # 🔍 디버깅: 첫 번째 아이템 구조 확인
                if iteration == 0 and len(output2) > 0:
                    logger.debug(f"첫 번째 output2 아이템 Keys: {list(output2[0].keys())}, Sample: {output2[0]}")
                
                # 데이터 파싱 (한국 시간 기준 저장, 현지시간도 보관)
                for item in output2:
                    us_date = str(item.get("xymd") or "")  # 한국 기준 날짜
                    us_time = str(item.get("xhms") or "")  # 한국 기준 시간
                    local_date = str(item.get("xymd") or "")  # 현지 날짜 (커서용)
                    local_time = str(item.get("xhms") or "")  # 현지 시간 (커서용)
                    
                    if len(us_date) != 8 or len(us_time) != 6:
                        continue
                    
                    all_rows.append({
                        "date": us_date,  # ✅ 한국시간 저장
                        "time": us_time,
                        "local_date": local_date,  # ✅ 현지시간 보관 (커서용)
                        "local_time": local_time,
                        "open": float(item.get("open") or 0),
                        "high": float(item.get("high") or 0),
                        "low": float(item.get("low") or 0),
                        "close": float(item.get("last") or 0),
                        "volume": float(item.get("evol") or 0)
                    })
                
                # 충분한 데이터 수집 확인
                if len(all_rows) >= candles:
                    logger.debug(f"충분한 데이터 수집: {len(all_rows)}개 >= {candles}개")
                    break
                
                # 다음 페이지 KEYB 설정: output2의 가장 과거 날짜시간 (마지막 아이템)
                if len(output2) > 0:
                    last_item = output2[-1]  # 가장 과거 데이터
                    oldest_date = str(last_item.get("xymd") or "")
                    oldest_time = str(last_item.get("xhms") or "")
                    
                    if len(oldest_date) == 8 and len(oldest_time) == 6:
                        keyb = oldest_date + oldest_time  # ✅ YYYYMMDDHHMMSS
                        next_flag = "1"  # ✅ 두 번째 호출부터는 "1"
                        logger.debug(f"다음 페이지: NEXT=1, KEYB={keyb}")
                    else:
                        logger.warning("날짜/시간 형식 오류, 종료")
                        break
                else:
                    logger.warning("output2가 비어있음, 종료")
                    break
                
            except Exception as e:
                logger.error(f"API 호출 오류: {e}")
                break
        
        # ✅ 오래된 순으로 정렬 후 최근 N개만
        all_rows.sort(key=lambda x: (x["date"], x["time"]))  # 오래된 순
        result = all_rows[-candles:]  # 최근 candles개만
        
        logger.info(f"해외주식 5분봉 수집 완료: {len(result)}개")
        
        return result
    
    def fetch_kr_minute_data(
        self,
        ticker: Ticker,
        target_5min_candles: int = 100
    ) -> List[Dict[str, Any]]:
        """
        국내주식 5분봉 데이터 수집 (1분봉 → 5분봉 리샘플링)
        
        Args:
            ticker: 티커 객체
            target_5min_candles: 필요한 5분봉 캔들 개수
            
        Returns:
            5분봉 데이터 리스트 [{date, time, open, high, low, close, volume}, ...]
        """
        logger.info(f"국내주식 5분봉 수집 시작: {ticker.symbol}:{ticker.exchange}, 목표 {target_5min_candles}개 (5분봉)")
        
        # 5분봉 1개 = 1분봉 5개, 하지만 장중 시간만 있으므로 넉넉하게 반복
        all_1min_dict = {}  # ✅ 중복 제거를 위한 딕셔너리 {datetime_key: row}
        
        # 첫 호출 기준: 오늘 날짜 마지막 시간
        current_dt = datetime.now()
        date_str = current_dt.strftime("%Y%m%d")
        hour_str = "235959"
        
        # 최대 반복 횟수 (안전장치)
        max_iterations = 200
        
        for iteration in range(max_iterations):
            try:
                logger.debug(f"API 호출 {iteration + 1}, 기준={date_str} {hour_str}")
                
                # KIS API 호출 (1분봉)
                response = self.client.domestic_minute_price_test(
                    FID_INPUT_ISCD=ticker.symbol,
                    FID_INPUT_DATE_1=date_str,
                    FID_INPUT_HOUR_1=hour_str
                )
                
                # output2 파싱
                output2 = response.get("output2") or []
                if not output2:
                    logger.warning("output2가 비어있음, 종료")
                    break
                
                logger.debug(f"수신: {len(output2)}건 (1분봉)")
                
                # 🔍 디버깅: 첫 번째 아이템 구조 확인
                if iteration == 0 and len(output2) > 0:
                    logger.debug(f"첫 번째 output2 아이템 Keys: {list(output2[0].keys())}, Sample: {output2[0]}")
                
                # 1분봉 데이터 파싱 (중복 제거)
                for item in output2:
                    dt_str = str(item.get("stck_bsop_date") or "")  # YYYYMMDD
                    tm_str = str(item.get("stck_cntg_hour") or "")  # HHMMSS
                    
                    if len(dt_str) != 8 or len(tm_str) != 6:
                        continue
                    
                    # ✅ 중복 제거: 날짜+시간을 키로 사용
                    datetime_key = dt_str + tm_str
                    if datetime_key not in all_1min_dict:
                        all_1min_dict[datetime_key] = {
                            "date": dt_str,
                            "time": tm_str,
                            "open": float(item.get("stck_oprc") or 0),
                            "high": float(item.get("stck_hgpr") or 0),
                            "low": float(item.get("stck_lwpr") or 0),
                            "close": float(item.get("stck_prpr") or 0),
                            "volume": float(item.get("cntg_vol") or 0)
                        }
                
                # ✅ 주기적으로 5분봉 변환해서 충분한지 체크 (매 5회마다)
                if iteration > 0 and iteration % 5 == 0:
                    temp_rows = list(all_1min_dict.values())
                    temp_5min = self._resample_1min_to_5min(temp_rows)
                    logger.debug(f"중간 체크: 5분봉 {len(temp_5min)}개 / 목표 {target_5min_candles}개")
                    if len(temp_5min) >= target_5min_candles:
                        logger.debug(f"충분한 5분봉 수집: {len(temp_5min)}개 >= {target_5min_candles}개")
                        break
                
                # ✅ 다음 페이지: output2의 가장 과거 시간 (마지막 아이템) 사용
                if len(output2) > 0:
                    last_item = output2[-1]
                    next_date = str(last_item.get("stck_bsop_date") or "")
                    next_hour = str(last_item.get("stck_cntg_hour") or "")
                    
                    if len(next_date) != 8 or len(next_hour) != 6:
                        logger.warning("날짜/시간 형식 오류, 종료")
                        break
                    
                    # ✅ 다음 호출 기준 업데이트
                    date_str = next_date
                    hour_str = next_hour
                    logger.debug(f"다음 페이지: {date_str} {hour_str} (가장 과거 시간 포함)")
                else:
                    logger.warning("output2가 비어있음, 종료")
                    break
                
            except Exception as e:
                logger.error(f"API 호출 오류: {e}")
                break
        
        # 1분봉 → 5분봉 리샘플링
        if not all_1min_dict:
            logger.warning("수집된 데이터 없음")
            return []
        
        # ✅ 딕셔너리 → 리스트 변환
        all_1min_rows = list(all_1min_dict.values())
        logger.debug(f"중복 제거 후 1분봉: {len(all_1min_rows)}개")
        
        # 1분봉 → 5분봉 리샘플링
        result_5min = self._resample_1min_to_5min(all_1min_rows)
        logger.debug(f"리샘플링 후 5분봉: {len(result_5min)}개")
        
        # 필요한 개수만 컷 (최신 데이터부터)
        if len(result_5min) > target_5min_candles:
            result_5min = result_5min[-target_5min_candles:]  # ✅ 최근 target개만
            logger.debug(f"최종 컷팅: {len(result_5min)}개 (최신 {target_5min_candles}개)")
        
        logger.info(f"국내주식 5분봉 수집 완료: {len(result_5min)}개")
        if len(result_5min) > 0:
            logger.debug(f"기간: {result_5min[0]['date']} {result_5min[0]['time']} ~ {result_5min[-1]['date']} {result_5min[-1]['time']}")
        
        return result_5min
    
    def _resample_1min_to_5min(self, data_1min: List[Dict]) -> List[Dict]:
        """
        1분봉 → 5분봉 리샘플링
        
        Args:
            data_1min: 1분봉 데이터
            
        Returns:
            5분봉 데이터
        """
        if not data_1min:
            return []
        
        # DataFrame으로 변환
        df = pd.DataFrame(data_1min)
        
        # datetime 컬럼 생성
        df['datetime'] = pd.to_datetime(
            df['date'] + df['time'],
            format='%Y%m%d%H%M%S'
        )
        
        # datetime을 인덱스로 설정
        df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)
        
        # 5분봉으로 리샘플링
        resampled = df.resample('5min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # ✅ NaN 제거 (모든 컬럼 확인)
        resampled = resampled.dropna()  # 하나라도 NaN이면 제거
        
        # 결과 변환
        result = []
        for idx, row in resampled.iterrows():
            result.append({
                "date": idx.strftime("%Y%m%d"),
                "time": idx.strftime("%H%M%S"),
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": float(row['volume'])
            })
        
        logger.debug(f"리샘플링 완료: 1분봉 {len(data_1min)}개 → 5분봉 {len(result)}개")
        
        return result
    
    def fetch_for_similarity_kr(
        self,
        ticker: Ticker,
        reference_datetime: str | None,
        lookback: int
    ) -> List[Dict[str, Any]]:
        """
        국내주식 유사도 검색 전용 데이터 수집
        - reference_datetime 없음: 최근 1회 API 호출 → lookback만큼만
        - reference_datetime 있음: 해당 시점까지 페이징 → lookback*5(1분봉) 확보 → 5분봉 리샘플링
        
        Args:
            ticker: 티커 객체
            reference_datetime: 기준일시 (YYYYMMDD HHMMSS) 또는 None
            lookback: 5분봉 lookback 기간
            
        Returns:
            5분봉 데이터 리스트 (lookback 개수만큼)
        """
        logger.debug(f"[국내] 유사도 검색 데이터 수집: {ticker.symbol}, lookback={lookback}")
        
        # reference_datetime 없음: 최근 데이터만
        if reference_datetime is None:
            logger.debug("reference_datetime 없음 → 최근 1회 API 호출")
            current_dt = datetime.now()
            date_str = current_dt.strftime("%Y%m%d")
            hour_str = "235959"
            
            response = self.client.domestic_minute_price_test(
                FID_INPUT_ISCD=ticker.symbol,
                FID_INPUT_DATE_1=date_str,
                FID_INPUT_HOUR_1=hour_str
            )
            
            output2 = response.get("output2") or []
            if not output2:
                return []
            
            # 1분봉 파싱
            rows_1min = []
            for item in output2:
                dt_str = str(item.get("stck_bsop_date") or "")
                tm_str = str(item.get("stck_cntg_hour") or "")
                if len(dt_str) != 8 or len(tm_str) != 6:
                    continue
                rows_1min.append({
                    "date": dt_str, "time": tm_str,
                    "open": float(item.get("stck_oprc") or 0),
                    "high": float(item.get("stck_hgpr") or 0),
                    "low": float(item.get("stck_lwpr") or 0),
                    "close": float(item.get("stck_prpr") or 0),
                    "volume": float(item.get("cntg_vol") or 0)
                })
            
            # 5분봉 리샘플링
            rows_5min = self._resample_1min_to_5min(rows_1min)
            result = rows_5min[-lookback:] if len(rows_5min) >= lookback else rows_5min
            logger.debug(f"수집 완료: {len(result)}개 (목표: {lookback}개)")
            return result
        
        # reference_datetime 있음: 해당 시점을 최신으로 API 호출
        logger.debug(f"reference_datetime={reference_datetime} → 해당 시점 기준 조회")
        ref_date = reference_datetime[:8]
        ref_time = reference_datetime[9:] if len(reference_datetime) > 8 else reference_datetime[8:]
        
        all_1min_dict = {}
        # ✅ reference_datetime을 기준으로 시작
        date_str = ref_date
        hour_str = ref_time
        
        required_1min = lookback * 5  # 5분봉 lookback개 = 1분봉 lookback*5개
        max_iterations = 10  # reference_datetime 기준이므로 적은 반복으로 충분
        
        for iteration in range(max_iterations):
            logger.debug(f"API 호출 {iteration + 1}, 기준={date_str} {hour_str} (이 시점을 최신으로 과거 120개)")
            
            response = self.client.domestic_minute_price_test(
                FID_INPUT_ISCD=ticker.symbol,
                FID_INPUT_DATE_1=date_str,
                FID_INPUT_HOUR_1=hour_str
            )
            
            output2 = response.get("output2") or []
            if not output2:
                logger.warning("output2 비어있음")
                break
            
            logger.debug(f"수신: {len(output2)}건 (1분봉)")
            
            # 1분봉 파싱 (output2 = [최신 → 과거])
            for item in output2:
                dt_str = str(item.get("stck_bsop_date") or "")
                tm_str = str(item.get("stck_cntg_hour") or "")
                if len(dt_str) != 8 or len(tm_str) != 6:
                    continue
                
                datetime_key = dt_str + tm_str
                # ✅ reference_datetime 이하만 수집
                if datetime_key <= ref_date + ref_time:
                    if datetime_key not in all_1min_dict:
                        all_1min_dict[datetime_key] = {
                            "date": dt_str, "time": tm_str,
                            "open": float(item.get("stck_oprc") or 0),
                            "high": float(item.get("stck_hgpr") or 0),
                            "low": float(item.get("stck_lwpr") or 0),
                            "close": float(item.get("stck_prpr") or 0),
                            "volume": float(item.get("cntg_vol") or 0)
                        }
            
            # lookback 확보 확인
            if len(all_1min_dict) >= required_1min:
                logger.debug(f"1분봉 충분: {len(all_1min_dict)}개 >= {required_1min}개")
                break
            
            # 부족 → output2[-1](가장 과거)로 다음 호출
            if len(output2) > 0:
                last_item = output2[-1]  # 가장 과거
                date_str = str(last_item.get("stck_bsop_date") or "")
                hour_str = str(last_item.get("stck_cntg_hour") or "")
                if len(date_str) != 8 or len(hour_str) != 6:
                    logger.warning("날짜/시간 형식 오류")
                    break
                logger.debug(f"다음 호출: {date_str} {hour_str} (가장 과거 시점)")
            else:
                break
        
        # 1분봉 → 5분봉 리샘플링
        if not all_1min_dict:
            return []
        
        rows_1min = list(all_1min_dict.values())
        rows_5min = self._resample_1min_to_5min(rows_1min)
        
        # lookback만큼만 (이미 reference_datetime 이하만 수집했음)
        result = rows_5min[-lookback:] if len(rows_5min) >= lookback else rows_5min
        logger.debug(f"수집 완료: {len(result)}개 (목표: {lookback}개)")
        
        return result
    
    def fetch_for_similarity_us(
        self,
        ticker: Ticker,
        reference_datetime: str | None,
        lookback: int
    ) -> List[Dict[str, Any]]:
        """
        해외주식 유사도 검색 전용 데이터 수집
        - reference_datetime 없음: 최근 1회 API 호출 → lookback만큼만
        - reference_datetime 있음: 해당 시점까지 페이징 → lookback 확보
        
        Args:
            ticker: 티커 객체
            reference_datetime: 기준일시 (YYYYMMDD HHMMSS) 또는 None
            lookback: 5분봉 lookback 기간
            
        Returns:
            5분봉 데이터 리스트 (lookback 개수만큼)
        """
        logger.debug(f"[해외] 유사도 검색 데이터 수집: {ticker.symbol}:{ticker.exchange}, lookback={lookback}")
        
        kis_exchange = self.exchange_map.get(ticker.exchange.upper(), "NAS")
        logger.debug(f"거래소 변환: {ticker.exchange} → {kis_exchange}")
        
        # reference_datetime 없음: 최근 데이터만
        if reference_datetime is None:
            logger.debug("reference_datetime 없음 → 최근 1회 API 호출")
            
            response = self.client.overseas_minute_price_test(
                EXCD=kis_exchange,
                SYMB=ticker.symbol,
                NMIN="5",
                PINC="1",
                NEXT="",
                KEYB="",
                NREC="120"
            )
            
            output2 = response.get("output2") or []
            if not output2:
                return []
            
            # 5분봉 파싱
            rows = []
            for item in output2:
                us_date = str(item.get("xymd") or "") # kymd
                us_time = str(item.get("xhms") or "") # khms
                if len(us_date) != 8 or len(us_time) != 6:
                    continue
                rows.append({
                    "date": us_date, "time": us_time,
                    "open": float(item.get("open") or 0),
                    "high": float(item.get("high") or 0),
                    "low": float(item.get("low") or 0),
                    "close": float(item.get("last") or 0),
                    "volume": float(item.get("evol") or 0)
                })
            
            # 정렬 후 lookback만큼만
            rows.sort(key=lambda x: (x["date"], x["time"]))
            result = rows[-lookback:] if len(rows) >= lookback else rows
            logger.debug(f"수집 완료: {len(result)}개 (목표: {lookback}개)")
            return result
        
        # reference_datetime 있음: 해당 시점을 최신으로 API 호출
        logger.debug(f"reference_datetime={reference_datetime} → 해당 시점 기준 조회")
        ref_date = reference_datetime[:8]
        ref_time = reference_datetime[9:] if len(reference_datetime) > 8 else reference_datetime[8:]
        
        # ✅ KEYB 초기값: reference_datetime을 현지시간으로 변환해야 하는데...
        # 일단 한국시간 그대로 사용 (KIS API가 kymd/khms도 받아줄 가능성)
        all_rows_dict = {}
        next_flag = ""
        keyb = ref_date + ref_time  # ✅ reference_datetime을 KEYB로 사용
        max_iterations = 10  # reference_datetime 기준이므로 적은 반복으로 충분
        
        for iteration in range(max_iterations):
            logger.debug(f"API 호출 {iteration + 1}, NEXT={next_flag or '(첫호출)'}, KEYB={keyb}")
            
            response = self.client.overseas_minute_price_test(
                EXCD=kis_exchange,
                SYMB=ticker.symbol,
                NMIN="5",
                PINC="1",
                NEXT=next_flag,
                KEYB=keyb,
                NREC="120"
            )
            
            output2 = response.get("output2") or []
            if not output2:
                logger.warning("output2 비어있음")
                break
            
            logger.debug(f"수신: {len(output2)}건 (5분봉)")
            
            # 5분봉 파싱 (output2 = [최신 → 과거])
            for item in output2:
                us_date = str(item.get("xymd") or "") # kymd
                us_time = str(item.get("xhms") or "") # khms
                local_date = str(item.get("xymd") or "")
                local_time = str(item.get("xhms") or "")
                
                if len(us_date) != 8 or len(us_time) != 6:
                    continue
                
                datetime_key = us_date + us_time
                # ✅ reference_datetime 이하만 수집
                if datetime_key <= ref_date + ref_time:
                    if datetime_key not in all_rows_dict:
                        all_rows_dict[datetime_key] = {
                            "date": us_date, "time": us_time,
                            "local_date": local_date, "local_time": local_time,
                            "open": float(item.get("open") or 0),
                            "high": float(item.get("high") or 0),
                            "low": float(item.get("low") or 0),
                            "close": float(item.get("last") or 0),
                            "volume": float(item.get("evol") or 0)
                        }
            
            # lookback 확보 확인
            if len(all_rows_dict) >= lookback:
                logger.debug(f"5분봉 충분: {len(all_rows_dict)}개 >= {lookback}개")
                break
            
            # 부족 → output2[-1](가장 과거)로 다음 호출
            if len(output2) > 0:
                last_item = output2[-1]  # 가장 과거
                oldest_date = str(last_item.get("xymd") or "")
                oldest_time = str(last_item.get("xhms") or "")
                if len(oldest_date) == 8 and len(oldest_time) == 6:
                    keyb = oldest_date + oldest_time
                    next_flag = "1"
                    logger.debug(f"다음 호출: KEYB={keyb} (가장 과거 시점)")
                else:
                    logger.warning("날짜/시간 형식 오류")
                    break
            else:
                break
        
        if not all_rows_dict:
            return []
        
        # 정렬 후 lookback만큼만 (이미 reference_datetime 이하만 수집했음)
        rows = list(all_rows_dict.values())
        rows.sort(key=lambda x: (x["date"], x["time"]))  # 오래된 순
        
        result = rows[-lookback:] if len(rows) >= lookback else rows
        logger.debug(f"수집 완료: {len(result)}개 (목표: {lookback}개)")
        
        return result

