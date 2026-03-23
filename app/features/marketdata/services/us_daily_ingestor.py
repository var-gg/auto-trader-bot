# app/features/marketdata/services/us_daily_ingestor.py
from __future__ import annotations
import logging
from datetime import datetime, date
from typing import Dict, List
from sqlalchemy.orm import Session

from app.core.kis_client import KISClient
from app.features.marketdata.repositories.us_market_repository import USMarketRepository
from app.shared.models.ticker import Ticker  # symbol, exchange, id, country, type

logger = logging.getLogger(__name__)

def parse_kis_daily_payload(payload: Dict, symbol: str, exchange: str, ticker_id: int) -> List[Dict]:
    """
    📊 KIS 해외 일봉 응답 파싱 함수 (미국주식 전용)
    
    입력: KIS API에서 받은 일봉 데이터
    출력: 데이터베이스에 저장할 수 있는 형태로 가공된 리스트
    
    처리 과정:
    1️⃣ API 응답에서 일봉 배열 추출 (output2 또는 output 필드)
    2️⃣ 각 일봉 데이터를 표준 형식으로 변환
    3️⃣ 날짜별로 정렬하여 반환
    """
    logger.debug(f"[parse_kis_daily_payload] 시작 - 심볼: {symbol}, 거래소: {exchange}")
    rows: List[Dict] = []

    # 📦 Step 1: API 응답에서 일봉 데이터 배열 추출
    arr = payload.get("output2") or payload.get("output") or []
    logger.debug(f"API에서 받은 일봉 데이터 건수: {len(arr)}건")

    for item in arr:
        # 📅 Step 2: 날짜 파싱 (YYYYMMDD 형식)
        ds = str(item.get("xymd") or item.get("date") or "").strip()
        if len(ds) != 8 or not ds.isdigit():
            logger.warning(f"잘못된 날짜 형식 건너뛰기: {ds}")
            continue
        y, m, d = int(ds[:4]), int(ds[4:6]), int(ds[6:8])
        trade_dt = date(y, m, d)

        def _f(x):
            """숫자 데이터 파싱 헬퍼 (콤마 제거, float 변환)"""
            try:
                return float(str(x).replace(",", "")) if x is not None else None
            except Exception:
                return None

        # 💰 Step 3: OHLCV 데이터 추출
        logger.debug(f"{trade_dt} 일봉 데이터 파싱 중...")
        open_v  = _f(item.get("open"))   # 시가
        high_v  = _f(item.get("high"))   # 고가
        low_v   = _f(item.get("low"))    # 저가
        close_v = _f(item.get("clos"))   # 종가
        volume  = _f(item.get("tvol"))   # 거래량

        # 🔍 Step 4: 필수 데이터 검증
        if close_v is None:
            logger.warning(f"종가가 없는 데이터 건너뛰기: {trade_dt}")
            continue

        # 🏁 Step 5: 장마감 여부 판단 (모든 OHLCV 값이 있고 거래량이 있으면 마감된 것으로 간주)
        is_final = (
            open_v is not None and 
            high_v is not None and 
            low_v is not None and 
            close_v is not None and 
            volume is not None and 
            volume > 0
        )
        logger.debug(f"Is final: {is_final} (OHLCV check: O={open_v}, H={high_v}, L={low_v}, C={close_v}, V={volume})")

        # 📊 Step 6: 표준 형식으로 변환
        row = {
            "ticker_id": ticker_id,
            "trade_date": trade_dt,
            "open": open_v,
            "high": high_v,
            "low": low_v,
            "close": close_v,
            "volume": volume or 0,
            "is_final": is_final,
            "source": "KIS",
            "source_symbol": symbol,
            "source_exchange": exchange,
        }
        rows.append(row)
        logger.debug(f"{trade_dt} 일봉 데이터 파싱 완료: O={open_v}, H={high_v}, L={low_v}, C={close_v}, V={volume}, is_final={is_final}")

    # 📈 Step 7: 날짜별 정렬 (오래된 것부터)
    rows.sort(key=lambda x: x["trade_date"])
    logger.debug(f"파싱 완료: 총 {len(rows)}건의 일봉 데이터")
    return rows


class USDailyIngestor:
    """
    🇺🇸 미국주식 전용 일봉 데이터 수집 서비스
    - NMS(NASDAQ), NYQ(NYSE) 거래소만 지원
    - 기존 daily_ingestor.py의 미국주식 부분만 분리
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = KISClient(db)
        self.repo = USMarketRepository(db)

    def _load_tickers(self, pairs: List[Dict[str, str]]) -> List[Ticker]:
        """
        미국주식 티커만 조회 (NMS, NYQ 거래소만)
        pairs 예: [{"symbol":"AAPL","exchange":"NMS"}, ...]
        """
        logger.debug(f"{len(pairs)}개 미국주식 티커 정보 조회 중...")
        symbols = [p["symbol"] for p in pairs]
        exchanges = [p["exchange"] for p in pairs]
        
        # 📊 데이터베이스에서 미국주식 티커만 조회 (NMS, NYQ만)
        q = self.db.query(Ticker).filter(
            Ticker.symbol.in_(symbols),
            Ticker.exchange.in_(exchanges),
            Ticker.exchange.in_(["NMS", "NYQ"])  # 미국주식 거래소만
        )
        tk_map = {(t.symbol, t.exchange): t for t in q.all()}
        
        # 🎯 순서대로 객체 반환 (없는 것 제외)
        result = [tk_map.get((p["symbol"], p["exchange"])) for p in pairs if tk_map.get((p["symbol"], p["exchange"]))]
        logger.debug(f"데이터베이스에서 {len(result)}개 미국주식 티커 발견")
        return result

    def sync_for_ticker_ids(self, ticker_ids: List[int], days: int = 50) -> Dict[str, int]:
        """
        🇺🇸 미국주식 티커 ID 목록에 대해 일봉 데이터 수집
        
        Args:
            ticker_ids: 수집할 티커 ID 목록
            days: 조회할 거래일 수 (최근 N일)
            
        Returns:
            Dict[str, int]: 종목별 수집된 데이터 건수
        """
        from datetime import datetime, timedelta
        
        logger.info(f"미국주식 일봉 수집 시작 - 티커 {len(ticker_ids)}개, {days}일치")

        # 📊 Step 1: 티커 정보 조회 (미국주식만)
        tickers = self.db.query(Ticker).filter(
            Ticker.id.in_(ticker_ids),
            Ticker.exchange.in_(["NMS", "NYQ"])  # 미국주식 거래소만
        ).all()
        
        if not tickers:
            logger.warning("미국주식 티커를 찾을 수 없습니다.")
            return {}

        logger.info(f"미국주식 티커 {len(tickers)}개 발견")

        result_counts: Dict[str, int] = {}

        # 🔄 Step 2: 각 티커별로 일봉 데이터 수집
        for tk in tickers:
            ticker_id = tk.id
            try:
                logger.debug(f"티커 처리 중: {tk.symbol}:{tk.exchange} (ID: {ticker_id})")
                
                # 🏦 미국주식 거래소 확인
                exchange_code = tk.exchange.upper() if tk.exchange else ""
                logger.debug(f"미국주식 거래소 코드 확인: {exchange_code}")
                
                if exchange_code not in ["NMS", "NYQ"]:
                    logger.warning(f"미국주식이 아닌 거래소: {exchange_code} - 건너뛰기")
                    result_counts[f"{tk.symbol}:{tk.exchange}"] = -1  # 지원하지 않음 표시
                    continue
                
                logger.debug(f"미국주식 (NASDAQ/NYSE) 처리 시작")
                
                # 📅 여러 번 호출이 필요한지 계산 (KIS API는 BYMD 기준 100개씩만 반환)
                all_rows = []
                api_call_count = (days + 99) // 100  # 올림 계산
                logger.debug(f"필요한 API 호출 횟수: {api_call_count}회 (100개씩 최대 조회)")
                
                for i in range(api_call_count):
                    # 기준일자 계산: 오늘부터 역순으로 (i * 100)일 전
                    # 빈 값이면 오늘, 아니면 YYYYMMDD
                    if i == 0:
                        bymd = ""  # 첫 번째 호출은 오늘 기준
                        logger.debug(f"API 호출 {i+1}/{api_call_count}: BYMD=오늘")
                    else:
                        # i번째 호출은 (i * 100)일 전을 기준일로 설정
                        # 실제로는 영업일 기준이므로 넉넉하게 (i * 150)일 전으로 설정
                        target_date = datetime.now() - timedelta(days=i * 150)
                        bymd = target_date.strftime("%Y%m%d")
                        logger.debug(f"API 호출 {i+1}/{api_call_count}: BYMD={bymd}")
                    
                    # 🌐 미국주식 KIS 해외 API 호출
                    payload = self.client.daily_price(symbol=tk.symbol, exchange=tk.exchange, bymd=bymd)
                    logger.debug(f"미국주식 API 응답 받음: rt_cd={payload.get('rt_cd', 'unknown')}")
                    
                    # 🔧 미국주식 응답 데이터 파싱
                    logger.debug("미국주식 응답 데이터 파싱 중...")
                    batch_rows = parse_kis_daily_payload(payload, tk.symbol, tk.exchange, tk.id)
                    logger.debug(f"조회된 데이터: {len(batch_rows)}건")
                    all_rows.extend(batch_rows)
                    
                    # 충분한 데이터를 수집했으면 중단
                    if len(all_rows) >= days:
                        logger.debug(f"충분한 데이터 수집 완료 ({len(all_rows)}건 >= {days}건)")
                        break

                # 📊 수집된 전체 데이터 사용 (슬라이스 제거)
                logger.debug(f"총 수집된 데이터: {len(all_rows)}건 → 중복 제거 후 저장")

                # 🔧 중복 제거: 동일 trade_date가 있으면 마지막 것만 유지
                unique_rows = {}
                for row in all_rows:
                    key = row['trade_date']
                    unique_rows[key] = row  # 덮어쓰기 (마지막 것이 최신)
                
                final_rows = list(unique_rows.values())
                logger.debug(f"중복 제거 완료: {len(all_rows)}건 → {len(final_rows)}건")

                # 💾 데이터베이스에 저장 (upsert)
                logger.debug("데이터베이스 저장 중...")
                n = self.repo.upsert_daily_rows(final_rows)
                result_counts[f"{tk.symbol}:{tk.exchange}"] = n
                logger.info(f"저장 완료: {tk.symbol}:{tk.exchange} → {n}건 업서트")
                
            except Exception as e:
                logger.error(f"오류 발생: {tk.symbol}:{tk.exchange} - {e}")
                result_counts[f"{tk.symbol}:{tk.exchange}"] = 0

        # 📈 Step 3: 결과 요약 및 반환
        total_rows = sum(v for v in result_counts.values() if v > 0)
        logger.info(f"미국주식 일봉 수집 완료! 총 처리 결과: {len(result_counts)}개 종목, {total_rows}건 데이터")
        
        return result_counts

    def sync_all_tickers(self, days: int = 50) -> Dict[str, int]:
        """
        🇺🇸 모든 활성 미국주식 티커에 대해 일봉 데이터 수집
        
        Args:
            days: 조회할 거래일 수 (최근 N일)
            
        Returns:
            Dict[str, int]: 종목별 수집된 데이터 건수
        """
        logger.info(f"전체 미국주식 일봉 수집 시작 - {days}일치")

        # 📊 모든 미국주식 티커 조회
        tickers = self.db.query(Ticker).filter(
            Ticker.exchange.in_(["NMS", "NYQ"])  # 미국주식 거래소만
        ).all()
        
        if not tickers:
            logger.warning("활성 미국주식 티커를 찾을 수 없습니다.")
            return {}

        ticker_ids = [t.id for t in tickers]
        logger.info(f"활성 미국주식 티커 {len(ticker_ids)}개 발견")

        # 🔄 각 티커별로 일봉 데이터 수집
        return self.sync_for_ticker_ids(ticker_ids, days)
