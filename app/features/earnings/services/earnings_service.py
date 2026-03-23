from datetime import datetime, timedelta
import time
import pytz
import requests
import logging
import os
from collections import deque
import threading
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.core.finnhub_client import get
from app.features.earnings.repositories.earnings_repository import EarningsRepository
from app.features.earnings.models.earnings_event import EarningsEvent
from app.shared.models.ticker import Ticker

KST = pytz.timezone("Asia/Seoul")
logger = logging.getLogger(__name__)

# -------- Rate Limiter (fundamental_service.py와 동일) --------
FINNHUB_QPM = int(os.getenv("FINNHUB_QPM", "55"))
FINNHUB_QPS = min(int(os.getenv("FINNHUB_QPS", "30")), 30)


class _QpsQpmGate:
    """초당/분당 동시 제한을 모두 만족할 때만 통과시키는 간단 버킷"""
    def __init__(self, qps: int, qpm: int):
        self.qps = qps
        self.qpm = qpm
        self._sec = deque()
        self._min = deque()
        self._lock = threading.Lock()

    def acquire(self):
        now = time.time()
        with self._lock:
            while self._sec and now - self._sec[0] > 1.0:
                self._sec.popleft()
            while self._min and now - self._min[0] > 60.0:
                self._min.popleft()

            if len(self._sec) < self.qps and len(self._min) < self.qpm:
                self._sec.append(now)
                self._min.append(now)
                return True
            return False


_GATE = _QpsQpmGate(FINNHUB_QPS, FINNHUB_QPM)


def _rate_limited_finnhub_get(path: str, params: dict) -> any:
    while not _GATE.acquire():
        time.sleep(0.03)  # 짧게 대기
    return get(path, params)

class EarningsService:
    def __init__(self, db: Session):
        self.repo = EarningsRepository(db)

    def sync(self, ticker_ids: Optional[List[int]] = None):
        """ 
        1. ticker_ids가 있으면 해당 티커들만, 없으면 NMS, NYQ 거래소 전체 티커 조회
        2. 각 티커별로 calendar API로 발표 일정 수집 (estimate 위주)
        3. 각 티커별로 stock API로 실제치 보완 (always 100분기 조회)
        """
        today = datetime.now(KST).date()
        # 현재일시 기준 1년전부터 3개월 후까지
        from_date = today - timedelta(days=365)
        to_date = today + timedelta(days=90)

        # --- Step 1: 대상 티커들 조회 ---
        if ticker_ids:
            # 지정된 티커 ID들만 조회
            target_tickers = (
                self.repo.db.query(Ticker)
                .filter(Ticker.id.in_(ticker_ids))
                .filter(Ticker.exchange.in_(["NMS", "NYQ"]))
                .all()
            )
            logger.info(f"지정된 {len(ticker_ids)}개 티커 ID 중 {len(target_tickers)}개 티커의 어닝 데이터를 수집합니다.")
        else:
            # 전체 NMS, NYQ 거래소 티커들 조회
            target_tickers = (
                self.repo.db.query(Ticker)
                .filter(Ticker.exchange.in_(["NMS", "NYQ"]))
                .all()
            )
            logger.info(f"전체 {len(target_tickers)}개 티커의 어닝 데이터를 수집합니다.")

        if not target_tickers:
            logger.warning("대상 티커가 없습니다.")
            return {
                "status": "ok", 
                "range": "1년전 ~ 3개월 후", 
                "target_exchanges": ["NMS", "NYQ"],
                "processed_count": 0,
                "message": "대상 티커가 없습니다."
            }

        logger.info(f"총 {len(target_tickers)}개 티커의 어닝 데이터를 수집합니다. (QPM={FINNHUB_QPM}, QPS={FINNHUB_QPS})")

        processed_count = 0
        for idx, ticker in enumerate(target_tickers, start=1):
            logger.info(f"[{idx}/{len(target_tickers)}] {ticker.symbol} 처리 중...")
            
            # --- Step 2: calendar API 수집 ---
            if self._should_sync_calendar_earnings(ticker.symbol, today):
                self._sync_calendar_earnings(ticker.symbol, from_date, to_date)
            else:
                logger.debug(f"{ticker.symbol}: 최신 분기 데이터가 있으므로 calendar API 스킵")
            
            # --- Step 3: stock API 수집 (현재 분기가 아닌 경우만) ---
            if self._should_sync_stock_earnings(ticker.symbol, today):
                self._sync_stock_earnings(ticker.symbol)
            else:
                logger.debug(f"{ticker.symbol}: 현재 분기 데이터가 최신이므로 stock API 스킵")
            
            processed_count += 1

        return {
            "status": "ok", 
            "range": "1년전 ~ 3개월 후", 
            "target_exchanges": ["NMS", "NYQ"],
            "processed_count": processed_count,
            "total_target_count": len(target_tickers),
            "rate_limit": {"qpm": FINNHUB_QPM, "qps": FINNHUB_QPS}
        }

    def _sync_calendar_earnings(self, symbol: str, from_date, to_date):
        """특정 티커의 calendar earnings 데이터 수집"""
        try:
            calendar = _rate_limited_finnhub_get("calendar/earnings", {
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
                "symbol": symbol
            })

            for item in calendar.get("earningsCalendar", []):
                event_data = self._map_calendar_item(item)
                self.repo.upsert_event(event_data)
        except requests.HTTPError as e:
            logger.error(f"Calendar API error for {symbol}: {e}")
            raise

    def _sync_stock_earnings(self, symbol: str):
        """특정 티커의 stock earnings 데이터 수집"""
        try:
            details = _rate_limited_finnhub_get("stock/earnings", {"symbol": symbol, "limit": 100})
            for item in details:
                event_data = self._map_stock_item(item, symbol)
                self.repo.upsert_event(event_data)
        except requests.HTTPError as e:
            logger.error(f"Stock API error for {symbol}: {e}")
            raise

    def _should_sync_calendar_earnings(self, symbol: str, today) -> bool:
        """최신 분기 데이터가 있는지 확인하여 calendar API 호출 여부 결정"""
        # 해당 티커의 최신 분기 데이터 조회 (fiscal_year, fiscal_quarter 기준)
        latest_quarter = (
            self.repo.db.query(
                EarningsEvent.fiscal_year,
                EarningsEvent.fiscal_quarter,
                EarningsEvent.report_date
            )
            .filter(EarningsEvent.ticker_symbol == symbol)
            .order_by(EarningsEvent.fiscal_year.desc(), EarningsEvent.fiscal_quarter.desc())
            .first()
        )
        
        if not latest_quarter:
            # 데이터가 없으면 calendar API 호출
            return True
        
        # 현재 분기 계산
        current_year, current_quarter = self._get_current_quarter(today)
        
        # 최신 데이터가 현재 분기보다 최신이거나 같으면 스킵
        if (latest_quarter.fiscal_year > current_year or 
            (latest_quarter.fiscal_year == current_year and latest_quarter.fiscal_quarter >= current_quarter)):
            return False
        
        return True

    def _get_current_quarter(self, date) -> tuple[int, int]:
        """주어진 날짜의 분기 반환 (year, quarter)"""
        year = date.year
        month = date.month
        
        if month <= 3:
            return year, 1  # Q1
        elif month <= 6:
            return year, 2  # Q2
        elif month <= 9:
            return year, 3  # Q3
        else:
            return year, 4  # Q4

    def _should_sync_stock_earnings(self, symbol: str, today) -> bool:
        """현재 분기 데이터가 최신인지 확인하여 stock API 호출 여부 결정"""
        # 해당 티커의 최신 회계종료일자 조회
        latest_period = (
            self.repo.db.query(func.max(EarningsEvent.period_end_date))
            .filter(EarningsEvent.ticker_symbol == symbol)
            .filter(EarningsEvent.period_end_date.isnot(None))
            .scalar()
        )
        
        if not latest_period:
            # 회계종료일자 데이터가 없으면 stock API 호출
            return True
        
        # 현재 분기의 마지막 날 계산 (분기별 마지막 날)
        current_quarter_end = self._get_current_quarter_end(today)
        
        # 최신 회계종료일자가 현재 분기 마지막 날과 같거나 더 최근이면 스킵
        if latest_period >= current_quarter_end:
            return False
        
        return True

    def _get_current_quarter_end(self, date) -> datetime.date:
        """주어진 날짜의 분기 마지막 날 반환"""
        year = date.year
        month = date.month
        
        if month <= 3:
            return datetime(year, 3, 31).date()  # Q1
        elif month <= 6:
            return datetime(year, 6, 30).date()  # Q2
        elif month <= 9:
            return datetime(year, 9, 30).date()  # Q3
        else:
            return datetime(year, 12, 31).date()  # Q4

    def _map_calendar_item(self, item: dict) -> dict:
        # calendar API 날짜는 예정/확정이 혼재될 수 있으므로 의미를 분리해 저장한다.
        report_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
        is_reported = item.get("epsActual") is not None
        return {
            "ticker_symbol": item["symbol"],
            "fiscal_year": item.get("year"),
            "fiscal_quarter": item.get("quarter"),
            "report_date": report_date,  # legacy compatibility
            "confirmed_report_date": report_date if is_reported else None,
            "expected_report_date_start": None if is_reported else report_date,
            "expected_report_date_end": None if is_reported else report_date,
            "report_date_confidence": 0.95 if is_reported else 0.6,
            "report_date_kind": "confirmed" if is_reported else "expected",
            "report_time": item.get("hour"),
            "estimate_eps": item.get("epsEstimate"),
            "actual_eps": item.get("epsActual"),  # calendar API에서도 actual 제공 가능
            "estimate_revenue": item.get("revenueEstimate"),
            "actual_revenue": item.get("revenueActual"),  # calendar API에서도 actual 제공 가능
            "status": "reported" if is_reported else "scheduled",
            "source": "finnhub",
        }

    def _map_stock_item(self, item: dict, symbol: str) -> dict:
        # API는 회계분기 종료일을 period로 제공
        period_end_date = datetime.strptime(item["period"], "%Y-%m-%d").date()
        return {
            "ticker_symbol": symbol,
            "fiscal_year": item.get("year"),
            "fiscal_quarter": item.get("quarter"),
            "period_end_date": period_end_date,
            "estimate_eps": item.get("estimate"),
            "actual_eps": item.get("actual"),
            "surprise_eps": item.get("surprisePercent"),
            # Revenue는 stock/earnings에서 제공 안 함
            "status": "reported" if item.get("actual") is not None else "scheduled",
            "source": "finnhub",
        }

    def get_earnings_for_analyst(self, ticker_id: int):
        """애널리스트 AI용 어닝 정보 조회 및 포맷팅"""
        raw_data = self.repo.get_earnings_for_analyst(ticker_id)
        if not raw_data:
            return None
        
        result = {
            "ticker": raw_data["ticker"],
            "latest": None,
            "upcoming": None
        }
        
        # 최신 발표된 분기 포맷팅
        if raw_data["latest"]:
            latest = raw_data["latest"]
            # period_end_date를 기반으로 발표년도/분기 계산
            if latest.period_end_date:
                report_year, report_quarter = self._calendar_quarter(latest.period_end_date)
                quarter_str = f"Q{report_quarter} {report_year}"
            else:
                quarter_str = f"Q{latest.fiscal_quarter} {latest.fiscal_year}"  # fallback
            
            # 확정일 우선, 없으면 예상 시작일, 그래도 없으면 legacy/fiscal period fallback
            report_date = latest.preferred_report_date or latest.period_end_date
            result["latest"] = {
                "quarter": quarter_str,
                "report_date": report_date.isoformat() if report_date else None,
                "report_date_kind": latest.report_date_kind,
                "report_date_confidence": latest.report_date_confidence,
                "eps": {
                    "estimate": latest.estimate_eps,
                    "actual": latest.actual_eps,
                    "surprise_pct": latest.surprise_eps
                }
            }
        
        # 다음 예정된 분기 포맷팅
        if raw_data["upcoming"]:
            upcoming = raw_data["upcoming"]
            preferred_date = upcoming.preferred_report_date
            # 선호 발표일을 기준으로 발표년도/분기 계산
            if preferred_date:
                report_year, report_quarter = self._calendar_quarter(preferred_date)
                quarter_str = f"Q{report_quarter} {report_year}"
            else:
                quarter_str = f"Q{upcoming.fiscal_quarter} {upcoming.fiscal_year}"  # fallback
            
            result["upcoming"] = {
                "quarter": quarter_str,
                "report_date": preferred_date.isoformat() if preferred_date else None,
                "report_date_end": upcoming.preferred_report_date_end.isoformat() if upcoming.preferred_report_date_end else None,
                "report_date_kind": upcoming.report_date_kind,
                "report_date_confidence": upcoming.report_date_confidence,
                "eps_estimate": upcoming.estimate_eps,
                "revenue_estimate": upcoming.estimate_revenue,
                "report_time": upcoming.report_time
            }
        
        return result

    def _calendar_quarter(self, date) -> tuple[int, int]:
        """날짜를 기반으로 달력년도와 분기 계산"""
        q = (date.month - 1) // 3 + 1
        return date.year, q
