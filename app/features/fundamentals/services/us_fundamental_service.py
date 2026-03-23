from __future__ import annotations

import os
import logging
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple, Iterable
from itertools import islice, chain
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from sqlalchemy.orm import Session

from app.core.finnhub_client import get as finnhub_get
from app.features.fundamentals.repositories.us_fundamental_repository import FundamentalRepository
from app.shared.models.ticker import Ticker

logger = logging.getLogger(__name__)


# -------- Rate Limiter (글로벌) --------
# 무료 플랜 안전 여유를 위해 기본 QPM=55, QPS=30 (환경변수로 조정)
FINNHUB_QPM = int(os.getenv("FINNHUB_QPM", "55"))
FINNHUB_QPS = min(int(os.getenv("FINNHUB_QPS", "30")), 30)


class _QpsQpmGate:
    """초당/분당 동시 제한을 모두 만족할 때만 통과시키는 간단 버킷"""
    def __init__(self, qps: int, qpm: int):
        from collections import deque
        import threading
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


def _rate_limited_finnhub_get(path: str, params: Dict[str, Any]) -> Any:
    while not _GATE.acquire():
        time.sleep(0.03)  # 짧게 대기
    return finnhub_get(path, params)


# -------- 유틸 --------
def _chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    it = iter(seq)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _start_of_today_utc() -> datetime:
    now = datetime.utcnow()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


# -------- 서비스 --------
class FundamentalService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = FundamentalRepository(db)

    # ---------- 단건 (기존 유지) ----------
    def sync_fundamentals_by_symbol(self, symbol: str, exchange: str) -> Dict[str, Any]:
        """심볼과 거래소로 펀더멘털 데이터 동기화 (기존 유지)"""
        try:
            ticker = self.repository.get_ticker_by_symbol_exchange(symbol, exchange)
            if not ticker:
                return {"error": f"Ticker not found: {symbol} on {exchange}"}

            fundamental_data = self._fetch_fundamental_data(symbol)
            dividend_data = self._fetch_dividend_data_single(symbol)

            result = {
                "ticker_id": ticker.id,
                "symbol": symbol,
                "exchange": exchange,
                "fundamental_snapshot": None,
                "dividend_histories_added": 0
            }

            if fundamental_data:
                snapshot = self.repository.upsert_fundamental_snapshot(
                    ticker_id=ticker.id,
                    per=fundamental_data.get("per"),
                    pbr=fundamental_data.get("pbr"),
                    dividend_yield=fundamental_data.get("dividend_yield"),
                    market_cap=fundamental_data.get("market_cap"),
                    debt_ratio=fundamental_data.get("debt_ratio")
                )
                result["fundamental_snapshot"] = {
                    "id": snapshot.id,
                    "per": snapshot.per,
                    "pbr": snapshot.pbr,
                    "dividend_yield": snapshot.dividend_yield,
                    "market_cap": snapshot.market_cap,
                    "debt_ratio": snapshot.debt_ratio,
                    "updated_at": snapshot.updated_at
                }

            if dividend_data:
                added = self.repository.bulk_insert_dividends_map({
                    ticker.id: dividend_data
                })
                result["dividend_histories_added"] = added.get(ticker.id, 0)

            return result

        except Exception as e:
            logger.error(f"Error syncing fundamentals for {symbol}: {str(e)}")
            return {"error": str(e)}

    # ---------- 전체 (최적화 버전) ----------
    def sync_fundamentals_all(self) -> Dict[str, Any]:
        """
        모든 지원 거래소의 펀더멘털 동기화 (최적화)
        - 지원 거래소: NYQ, NMS
        - Finnhub: 오늘 업데이트된 스냅샷은 건너뜀(호출수 절감)
        - yfinance: 청크 벌크 다운로드(actions=True, threads=True)
        - DB: 배당 벌크 인서트(RETURNING으로 per-ticker 카운트 회수)
        """
        supported_exchanges = ["NYQ", "NMS"]
        try:
            start_ts = datetime.utcnow().isoformat()
            tickers: List[Ticker] = self.repository.get_all_tickers_by_exchanges(supported_exchanges)
            total = len(tickers)
            if total == 0:
                return {
                    "total_tickers": 0, "processed": 0, "stale_for_finnhub": 0,
                    "errors": [], "results": [], "start_time": start_ts,
                    "end_time": datetime.utcnow().isoformat(), "estimated_seconds": 0.0,
                    "notes": "No tickers in supported exchanges."
                }

            stale_tickers: List[Ticker] = tickers
            fresh_tickers: List[Ticker] = []

            # 추정 시간: Finnhub 소요 + yfinance 소요(대략)
            finnhub_est_sec = (len(stale_tickers) * 60.0) / float(max(FINNHUB_QPM, 1))
            yfin_est_sec = 120.0 if total >= 300 else 60.0
            estimated_seconds = finnhub_est_sec + yfin_est_sec

            results: List[Dict[str, Any]] = []
            errors: List[str] = []

            logger.info(
                f"[sync_all] total={total}, stale_for_finnhub={len(stale_tickers)}, "
                f"fresh={len(fresh_tickers)}, est={estimated_seconds:.1f}s"
            )

            updated_at = datetime.now(ZoneInfo("Asia/Seoul"))
            # --- 1) Finnhub 메트릭 (stale만) 수집 → 벌크 업서트 ---
            metric_payloads: List[Dict[str, Any]] = []
            for t in stale_tickers:
                try:
                    data = self._fetch_fundamental_data(t.symbol)
                    if not data:
                        continue
                    metric_payloads.append({
                        "ticker_id": t.id,
                        "per": data.get("per"),
                        "pbr": data.get("pbr"),
                        "dividend_yield": data.get("dividend_yield"),
                        "market_cap": data.get("market_cap"),
                        "debt_ratio": data.get("debt_ratio"),
                        "updated_at": updated_at
                    })
                except Exception as e:
                    msg = f"Finnhub metric error {t.symbol}: {e}"
                    logger.warning(msg)
                    errors.append(msg)

            if metric_payloads:
                inserted_total = 0
                for chunk in _chunked(metric_payloads, 400):
                    try:
                        inserted_total += self.repository.bulk_upsert_fundamental_snapshots(chunk)
                    except Exception as e:
                        msg = f"bulk_upsert_fundamental_snapshots failed: {e}"
                        logger.error(msg)
                        errors.append(msg)
                logger.info(f"[sync_all] bulk upsert snapshots done: {inserted_total}")

            results.extend([
                {
                    "ticker_id": t.id,
                    "symbol": t.symbol,
                    "exchange": t.exchange,
                    "finnhub_updated": (t in stale_tickers)
                }
                for t in tickers
            ])

            # --- 2) 배당 벌크 수집/삽입 ---
            symbol_to_ticker: Dict[str, Ticker] = {t.symbol: t for t in tickers}
            symbols = [t.symbol for t in tickers]

            since_date = (date.today() - timedelta(days=365))

            dividends_by_symbol = self._fetch_dividend_data_bulk(symbols, since_date)
            rows_map_by_tid: Dict[int, List[Dict[str, Any]]] = {}
            for sym, rows in dividends_by_symbol.items():
                t = symbol_to_ticker.get(sym)
                if not t or not rows:
                    continue
                rows_map_by_tid[t.id] = [
                    {
                        "dividend_per_share": r["dividend_per_share"],
                        "payment_date": r["payment_date"],
                        "dividend_yield": r.get("dividend_yield"),
                        "currency": r.get("currency", "USD")
                    }
                    for r in rows
                ]

            added_map: Dict[int, int] = {}
            if rows_map_by_tid:
                tid_items = list(rows_map_by_tid.items())
                for chunk_items in _chunked(tid_items, 200):
                    chunk_map = dict(chunk_items)
                    try:
                        added_part = self.repository.bulk_insert_dividends_map(chunk_map)
                        for k, v in added_part.items():
                            added_map[k] = added_map.get(k, 0) + v
                    except Exception as e:
                        msg = f"bulk_insert_dividends_map failed: {e}"
                        logger.error(msg)
                        errors.append(msg)

            result_index = {r["ticker_id"]: r for r in results}
            for tid, cnt in added_map.items():
                result_index[tid]["dividend_histories_added"] = cnt
            for r in results:
                if "dividend_histories_added" not in r:
                    r["dividend_histories_added"] = 0

            end_ts = datetime.utcnow().isoformat()
            return {
                "total_tickers": total,
                "processed": total,
                "stale_for_finnhub": len(stale_tickers),
                "errors": errors,
                "results": results,
                "start_time": start_ts,
                "end_time": end_ts,
                "estimated_seconds": estimated_seconds,
                "notes": (
                    f"Finnhub QPM={FINNHUB_QPM}, QPS={FINNHUB_QPS}. "
                    f"Dividends fetched since {since_date.isoformat()} via yfinance bulk."
                ),
            }

        except Exception as e:
            logger.error(f"Error in sync_fundamentals_all: {str(e)}")
            return {"error": str(e)}


    # ---------- 프롬프트 데이터 ----------
    def get_fundamental_prompt_data(self, ticker_id: int) -> Dict[str, Any]:
        try:
            ticker = self.repository.get_ticker_by_id(ticker_id)
            if not ticker:
                return {"error": f"Ticker not found: {ticker_id}"}
            snapshot = self.repository.get_fundamental_snapshot_by_ticker_id(ticker_id)
            dividend_histories = self.repository.get_dividend_histories_by_ticker_id(ticker_id)

            prompt_data = {
                "ticker": {
                    "id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "type": ticker.type
                },
                "fundamentals": {
                    "per": snapshot.per if snapshot else None,
                    "pbr": snapshot.pbr if snapshot else None,
                    "dividend_yield": snapshot.dividend_yield if snapshot else None,
                    "market_cap": float(snapshot.market_cap) if snapshot and snapshot.market_cap else None,
                    "debt_ratio": snapshot.debt_ratio if snapshot else None,
                    "last_updated": snapshot.updated_at.isoformat() if snapshot and snapshot.updated_at else None
                },
                "dividend_history": [
                    {
                        "dividend_per_share": float(dh.dividend_per_share),
                        "payment_date": dh.payment_date.isoformat(),
                    }
                    for dh in dividend_histories[:5]
                ]
            }
            return prompt_data

        except Exception as e:
            logger.error(f"Error getting prompt data for ticker {ticker_id}: {str(e)}")
            return {"error": str(e)}

    # ---------- 외부 데이터 호출 ----------
    def _fetch_fundamental_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Finnhub에서 펀더멘털 메트릭 조회 (레이트리밋 적용)"""
        try:
            out: Dict[str, Any] = {}
            try:
                metrics = _rate_limited_finnhub_get("stock/metric", {"symbol": symbol, "metric": "all"})
                if metrics and metrics.get("metric"):
                    m = metrics["metric"]
                    out["market_cap"] = m.get("marketCapitalization")
                    out["per"] = m.get("peNormalizedAnnual")
                    out["pbr"] = m.get("pbAnnual")
                    out["dividend_yield"] = m.get("dividendYieldIndicatedAnnual")
                    de_ratio = m.get("totalDebt/totalEquityAnnual")
                    if de_ratio is not None:
                        out["debt_ratio"] = de_ratio * 100.0  # % 단위
            except Exception as e:
                logger.warning(f"Finnhub metric fetch failed for {symbol}: {e}")
            return out if out else None
        except Exception as e:
            logger.error(f"Error fetching fundamental data for {symbol}: {e}")
            return None

    def _fetch_dividend_data_single(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """단건 배당 (기존 유지용)"""
        try:
            t = yf.Ticker(symbol)
            s = t.dividends
            if s is None or s.empty:
                return None
            rows = []
            for dt, val in s.items():
                if val and float(val) != 0.0:
                    rows.append({
                        "dividend_per_share": float(val),
                        "dividend_yield": None,
                        "payment_date": dt.date(),
                        "currency": "USD"
                    })
            return rows or None
        except Exception as e:
            logger.error(f"Error fetching dividend(single) for {symbol}: {e}")
            return None

    def _fetch_dividend_data_bulk(self, symbols: List[str], since: date) -> Dict[str, List[Dict[str, Any]]]:
        """여러 심볼에 대해 배당을 벌크 수집 (청크 + threads=True)"""
        out: Dict[str, List[Dict[str, Any]]] = {}
        # yfinance는 50~100개 배치가 안정적
        for batch in _chunked(symbols, 75):
            try:
                df = yf.download(
                    tickers=" ".join(batch),
                    start=since.isoformat(),
                    interval="1d",
                    actions=True,
                    group_by="ticker",
                    threads=True,
                    auto_adjust=False,
                    progress=False,
                )
                # 멀티/싱글 케이스 모두 처리
                if isinstance(df.columns, pd.MultiIndex):
                    # 멀티 틱커
                    for sym in batch:
                        if (sym, "Dividends") in df.columns:
                            s = df[(sym, "Dividends")].dropna()
                            if s is None or s.empty:
                                continue
                            rows = []
                            for idx, val in s.items():
                                if val and float(val) != 0.0:
                                    rows.append({
                                        "payment_date": idx.date(),
                                        "dividend_per_share": float(val),
                                        "currency": "USD"
                                    })
                            if rows:
                                out[sym] = rows
                else:
                    # 싱글 틱커
                    if "Dividends" in df.columns:
                        s = df["Dividends"].dropna()
                        if not s.empty:
                            sym = batch[0]
                            rows = []
                            for idx, val in s.items():
                                if val and float(val) != 0.0:
                                    rows.append({
                                        "payment_date": idx.date(),
                                        "dividend_per_share": float(val),
                                        "currency": "USD"
                                    })
                            if rows:
                                out[sym] = rows
            except Exception as e:
                logger.warning(f"yfinance download failed for batch(size={len(batch)}): {e}")
        return out
