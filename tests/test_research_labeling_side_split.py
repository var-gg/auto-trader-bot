from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import build_historical_anchors


def _bars_up(symbol: str):
    return [
        HistoricalBar(symbol=symbol, timestamp="2026-01-01", open=100, high=101, low=99, close=100.5, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-02", open=100.5, high=101.5, low=100, close=101.0, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-03", open=101.0, high=102.0, low=100.8, close=101.4, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-04", open=101.4, high=102.2, low=101.0, close=101.7, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-05", open=101.7, high=102.3, low=101.4, close=102.0, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-06", open=102.0, high=103.2, low=101.8, close=103.0, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-07", open=103.0, high=104.4, low=102.9, close=104.1, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-08", open=104.1, high=105.5, low=104.0, close=105.0, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-09", open=105.0, high=106.2, low=104.8, close=105.8, volume=1000),
        HistoricalBar(symbol=symbol, timestamp="2026-01-10", open=105.8, high=106.5, low=105.5, close=106.0, volume=1000),
    ]


def test_horizon_up_generates_buy_anchor_only():
    anchors = build_historical_anchors(
        bars_by_symbol={"AAPL": _bars_up("AAPL")},
        macro_payload={"growth": 0.0},
        market="US",
        lookback_bars=5,
        horizon_days=3,
        target_return_pct=0.08,
        stop_return_pct=0.08,
    )
    assert anchors
    assert all(a.side == "BUY" for a in anchors)
