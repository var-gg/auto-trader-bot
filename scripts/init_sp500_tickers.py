# python -m scripts.init_sp500_tickers

import requests
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timezone, timedelta
import logging
from app.core.db import SessionLocal
from app.models.ticker import Ticker
from app.models.ticker_i18n import TickerI18n
from app.models.ticker_fundamentals import TickerFundamentals

logger = logging.getLogger(__name__)


def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"id": "constituents"})
    if table is None:
        table = soup.find("table", {"class": "wikitable sortable"})
    if table is None:
        raise RuntimeError("⚠️ S&P500 테이블을 찾지 못했습니다.")

    tickers = []
    for row in table.findAll("tr")[1:]:
        cols = row.findAll("td")
        if not cols:
            continue
        symbol = cols[0].text.strip()
        tickers.append(symbol.replace(".", "-"))
    return tickers


db = SessionLocal()
symbols = get_sp500_tickers()
logger.info(f"총 {len(symbols)}개 심볼 수집")

for symbol in symbols:
    try:
        t = yf.Ticker(symbol)
        info = t.info
        if "longName" not in info:
            logger.warning(f"{symbol} 무시 (정보 없음)")
            continue

        # 1. Ticker 저장
        ticker = Ticker(
            symbol=symbol,
            exchange=info.get("exchange", "NASDAQ"),
            country="US",
            type="stock",
        )
        db.merge(ticker)
        db.flush()

        # 2. TickerI18n 저장 (영문 이름)
        db.merge(
            TickerI18n(
                ticker_symbol=symbol,
                lang_code="en",
                name=info.get("longName", symbol),
            )
        )

        # 3. Fundamentals 저장 (누락 고려 → get으로 안전하게)
        KST = timezone(timedelta(hours=9))
        fundamentals = TickerFundamentals(
            ticker_symbol=symbol,
            sector=info.get("sector"),
            industry=info.get("industry"),
            market_cap=info.get("marketCap"),
            beta=info.get("beta"),
            trailing_pe=info.get("trailingPE"),
            forward_pe=info.get("forwardPE"),
            trailing_eps=info.get("trailingEps"),
            price_to_book=info.get("priceToBook"),
            dividend_rate=info.get("dividendRate"),
            dividend_yield=info.get("dividendYield"),
            payout_ratio=info.get("payoutRatio"),
            profit_margins=info.get("profitMargins"),
            fifty_two_week_high=info.get("fiftyTwoWeekHigh"),
            fifty_two_week_low=info.get("fiftyTwoWeekLow"),
            last_updated=datetime.now(KST)  # 👈 서울 시간으로 기록
        )
        db.merge(fundamentals)

    except Exception as e:
        logger.error(f"{symbol} 처리 실패: {e}")

db.commit()
db.close()
