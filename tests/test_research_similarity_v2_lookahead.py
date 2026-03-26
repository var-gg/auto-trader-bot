from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import _bars_until_date, _market_proxy_bars


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
