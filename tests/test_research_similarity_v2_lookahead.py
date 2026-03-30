from backtest_app.historical_data.models import HistoricalBar
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
