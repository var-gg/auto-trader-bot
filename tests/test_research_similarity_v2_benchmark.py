from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import generate_similarity_candidates_rolling


def _bars(symbol: str):
    rows = []
    price = 100.0
    from datetime import date, timedelta
    start = date(2025, 10, 1)
    for i in range(84):
        d = start + timedelta(days=i)
        open_ = price
        close = price * 1.002
        high = close * 1.01
        low = open_ * 0.99
        rows.append(HistoricalBar(symbol=symbol, timestamp=d.isoformat(), open=open_, high=high, low=low, close=close, volume=1000000 + (i + 1) * 1000))
        price = close
    return rows


def test_research_similarity_v2_reports_throughput_and_panel_artifact():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT"), "NVDA": _bars("NVDA")}
    macro_history = {f"2026-01-{i:02d}": {"growth": 0.0} for i in range(1, 32)}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market="US", macro_history_by_date=macro_history, abstain_margin=0.0)
    assert isinstance(candidates, list)
    assert diagnostics["throughput"]["n_symbols"] == 3
    assert diagnostics["throughput"]["wall_clock_seconds"] >= 0.0
    assert diagnostics["throughput"]["anchor_count"] >= diagnostics["throughput"]["prototype_count"]
    assert diagnostics["signal_panel_jsonl"]
