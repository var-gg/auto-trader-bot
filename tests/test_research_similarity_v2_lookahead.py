from backtest_app.historical_data.models import HistoricalBar, SymbolSessionMetadata
from backtest_app.historical_data.session_alignment import derive_session_anchor_from_date
from backtest_app.research.pipeline import _bars_until_date, _market_proxy_bars, _market_proxy_series, _sector_proxy_series


def _bars(symbol: str):
    return [
        HistoricalBar(symbol=symbol, timestamp=f"2026-01-{i:02d}", open=100 + i, high=102 + i, low=99 + i, close=101 + i, volume=1000000)
        for i in range(1, 8)
    ]


def test_proxy_bars_respect_cutoff_date():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    proxy = _market_proxy_bars(bars_by_symbol, cutoff_date="2026-01-04")
    assert proxy
    assert max(str(b.timestamp)[:10] for b in proxy) <= "2026-01-04"


def test_bars_until_date_never_includes_future_rows():
    bars = _bars("AAPL")
    clipped = _bars_until_date(bars, "2026-01-03")
    assert len(clipped) == 3
    assert all(str(b.timestamp)[:10] <= "2026-01-03" for b in clipped)


def _bars_for_dates(symbol: str, dates: list[str], closes: list[float]) -> list[HistoricalBar]:
    rows = []
    for idx, (trade_date, close) in enumerate(zip(dates, closes), start=1):
        rows.append(
            HistoricalBar(
                symbol=symbol,
                timestamp=trade_date,
                open=close - 1,
                high=close + 1,
                low=close - 2,
                close=close,
                volume=1_000_000 + idx * 100,
            )
        )
    return rows


def test_market_proxy_series_aligns_by_trade_date_instead_of_index():
    bars_by_symbol = {
        "A": _bars_for_dates("A", ["2026-01-01", "2026-01-02", "2026-01-03"], [100.0, 110.0, 120.0]),
        "B": _bars_for_dates("B", ["2026-01-01", "2026-01-03"], [200.0, 230.0]),
        "C": _bars_for_dates("C", ["2026-01-02", "2026-01-03"], [300.0, 330.0]),
    }
    proxy = _market_proxy_series(bars_by_symbol, cutoff_date="2026-01-03")
    closes = {str(bar.timestamp)[:10]: float(bar.close) for bar in proxy.bars}
    assert closes["2026-01-01"] == 150.0
    assert closes["2026-01-02"] == 205.0
    assert closes["2026-01-03"] == 226.66666666666666
    assert proxy.peer_count_by_date == {"2026-01-01": 2, "2026-01-02": 2, "2026-01-03": 3}
    assert proxy.contributing_symbols_by_date["2026-01-02"] == ["A", "C"]


def test_sector_proxy_series_marks_self_fallback_when_peer_missing():
    bars_by_symbol = {
        "AAPL": _bars("AAPL"),
        "MSFT": _bars("MSFT"),
    }
    sector_map = {"AAPL": "TECH", "MSFT": "FIN"}
    proxy = _sector_proxy_series("AAPL", bars_by_symbol, sector_map, cutoff_date="2026-01-04")
    assert proxy.fallback_to_self is True
    assert proxy.peer_count_by_date
    assert all(count == 1 for count in proxy.peer_count_by_date.values())
    assert all(symbols == ["AAPL"] for symbols in proxy.contributing_symbols_by_date.values())


def test_session_anchor_differs_between_kr_and_us_for_same_date():
    kr = SymbolSessionMetadata(symbol="005930", exchange_code="KOE", country_code="KR", exchange_tz="Asia/Seoul", session_close_local_time="15:30")
    us = SymbolSessionMetadata(symbol="AAPL", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00")
    kr_anchor = derive_session_anchor_from_date(session_date_local="2026-03-28", session_metadata=kr)
    us_anchor = derive_session_anchor_from_date(session_date_local="2026-03-28", session_metadata=us)
    assert kr_anchor["session_date_local"] == us_anchor["session_date_local"] == "2026-03-28"
    assert kr_anchor["feature_anchor_ts_utc"] != us_anchor["feature_anchor_ts_utc"]


def test_market_proxy_series_prefers_same_exchange_peers():
    bars_by_symbol = {
        "AAPL": _bars_for_dates("AAPL", ["2026-03-27", "2026-03-28"], [100.0, 101.0]),
        "MSFT": _bars_for_dates("MSFT", ["2026-03-27", "2026-03-28"], [200.0, 202.0]),
        "005930": _bars_for_dates("005930", ["2026-03-27", "2026-03-28"], [50.0, 49.0]),
    }
    session_metadata = {
        "AAPL": SymbolSessionMetadata(symbol="AAPL", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
        "MSFT": SymbolSessionMetadata(symbol="MSFT", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
        "005930": SymbolSessionMetadata(symbol="005930", exchange_code="KOE", country_code="KR", exchange_tz="Asia/Seoul", session_close_local_time="15:30"),
    }
    us_anchor = derive_session_anchor_from_date(session_date_local="2026-03-28", session_metadata=session_metadata["AAPL"])
    proxy = _market_proxy_series(
        bars_by_symbol,
        cutoff_date="2026-03-28",
        focus_symbol="AAPL",
        session_metadata_by_symbol=session_metadata,
        cutoff_anchor_ts_utc=us_anchor["feature_anchor_ts_utc"],
    )
    closes = {str(bar.timestamp)[:10]: float(bar.close) for bar in proxy.bars}
    assert closes["2026-03-28"] == 151.5
    assert proxy.same_exchange_peer_count == 2
    assert proxy.cross_exchange_proxy_used is False


def test_sector_proxy_series_marks_same_exchange_self_fallback():
    bars_by_symbol = {
        "AAPL": _bars_for_dates("AAPL", ["2026-03-27", "2026-03-28"], [100.0, 101.0]),
        "005930": _bars_for_dates("005930", ["2026-03-27", "2026-03-28"], [50.0, 49.0]),
    }
    sector_map = {"AAPL": "TECH", "005930": "TECH"}
    session_metadata = {
        "AAPL": SymbolSessionMetadata(symbol="AAPL", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
        "005930": SymbolSessionMetadata(symbol="005930", exchange_code="KOE", country_code="KR", exchange_tz="Asia/Seoul", session_close_local_time="15:30"),
    }
    proxy = _sector_proxy_series("AAPL", bars_by_symbol, sector_map, cutoff_date="2026-03-28", session_metadata_by_symbol=session_metadata, cutoff_anchor_ts_utc=derive_session_anchor_from_date(session_date_local="2026-03-28", session_metadata=session_metadata["AAPL"])["feature_anchor_ts_utc"])
    assert proxy.fallback_to_self is True
    assert proxy.same_exchange_peer_count == 1
