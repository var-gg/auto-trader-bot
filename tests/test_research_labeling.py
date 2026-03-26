from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.labeling import EventLabelingConfig, label_event_window


def _bar(day, o, h, l, c):
    return HistoricalBar(symbol="AAPL", timestamp=f"2026-01-{day:02d}", open=o, high=h, low=l, close=c, volume=1000)


def test_label_event_window_up_first():
    bars = [_bar(1, 100, 104, 99, 103), _bar(2, 103, 108, 102, 107)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=3, fee_bps=5, slippage_bps=5))
    assert out.label == "UP_FIRST"
    assert out.target_hit_day == 2
    assert out.side_labels["BUY"] == "UP_FIRST"
    assert out.side_labels["SELL"] == "NO_TRADE"
    assert out.mae_pct <= 0
    assert out.mfe_pct >= 0


def test_label_event_window_down_first_maps_to_short_only():
    bars = [_bar(1, 100, 101, 96, 97), _bar(2, 97, 98, 94, 95)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=3))
    assert out.label == "DOWN_FIRST"
    assert out.side_labels["BUY"] == "NO_TRADE"
    assert out.side_labels["SELL"] == "DOWN_FIRST"


def test_label_event_window_horizon_up_not_dual_routed():
    bars = [_bar(1, 100, 103, 99, 102), _bar(2, 102, 104, 101, 103)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.08, stop_return_pct=0.08, horizon_days=2, flat_return_band_pct=0.001))
    assert out.label == "HORIZON_UP"
    assert out.side_labels["BUY"] == "HORIZON_UP"
    assert out.side_labels["SELL"] == "NO_TRADE"


def test_label_event_window_flat_is_no_trade():
    bars = [_bar(1, 100, 101, 99, 100.1), _bar(2, 100.1, 100.4, 99.8, 100.0)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.08, stop_return_pct=0.08, horizon_days=2, flat_return_band_pct=0.01))
    assert out.label == "FLAT"
    assert out.no_trade is True
    assert out.side_labels["BUY"] == "NO_TRADE"
    assert out.side_labels["SELL"] == "NO_TRADE"


def test_label_event_window_ambiguous():
    bars = [_bar(1, 100, 106, 96, 101)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=1))
    assert out.label == "AMBIGUOUS"
    assert out.ambiguous is True


def test_label_event_window_blackout_extension_point_yields_no_trade():
    bars = [_bar(1, 100, 106, 96, 101)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=1, event_blackout=True, earnings_proximity_days=2))
    assert out.label == "NO_TRADE"
    assert out.no_trade is True
    assert out.diagnostics["reason"] == "event_blackout"
