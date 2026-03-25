from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.labeling import EventLabelingConfig, label_event_window


def _bar(day, o, h, l, c):
    return HistoricalBar(symbol="AAPL", timestamp=f"2026-01-{day:02d}", open=o, high=h, low=l, close=c, volume=1000)


def test_label_event_window_up_first():
    bars = [_bar(1, 100, 104, 99, 103), _bar(2, 103, 108, 102, 107)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=3, fee_bps=5, slippage_bps=5))
    assert out.label == "UP_FIRST"
    assert out.target_hit_day == 2
    assert out.mae_pct <= 0
    assert out.mfe_pct >= 0


def test_label_event_window_ambiguous():
    bars = [_bar(1, 100, 106, 96, 101)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=1))
    assert out.label == "AMBIGUOUS"
    assert out.ambiguous is True
