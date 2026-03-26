from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.labeling import EventLabelingConfig, build_event_outcome_record, label_event_window


def _bar(day, o, h, l, c):
    return HistoricalBar(symbol="AAPL", timestamp=f"2026-01-{day:02d}", open=o, high=h, low=l, close=c, volume=1000)


def test_event_outcome_record_contains_both_side_payloads():
    bars = [_bar(1, 100, 104, 99, 103), _bar(2, 103, 108, 102, 107)]
    out = build_event_outcome_record(bars, EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=3, fee_bps=5, slippage_bps=5))
    assert out.schema_version == "event_outcome_v1"
    assert out.side_payload["BUY"]["first_touch_label"] == "UP_FIRST"
    assert "SELL" in out.side_payload
    assert "raw_path_summary" not in out.side_payload["BUY"]


def test_event_outcome_record_keeps_flat_ambiguous_no_trade_explicitly():
    flat = build_event_outcome_record([_bar(1, 100, 101, 99, 100.1), _bar(2, 100.1, 100.4, 99.8, 100.0)], EventLabelingConfig(target_return_pct=0.08, stop_return_pct=0.08, horizon_days=2, flat_return_band_pct=0.01))
    assert flat.buy.flat is True
    assert flat.sell.flat is True

    ambiguous = build_event_outcome_record([_bar(1, 100, 106, 96, 101)], EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=1))
    assert ambiguous.buy.ambiguous is True or ambiguous.sell.ambiguous is True

    no_trade = build_event_outcome_record([], EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=1))
    assert no_trade.buy.no_trade is True
    assert no_trade.sell.no_trade is True


def test_label_event_window_is_compatibility_wrapper_on_buy_side():
    bars = [_bar(1, 100, 103, 99, 102), _bar(2, 102, 104, 101, 103)]
    out = label_event_window(bars, EventLabelingConfig(target_return_pct=0.08, stop_return_pct=0.08, horizon_days=2, flat_return_band_pct=0.001))
    assert out.label == "HORIZON_UP"
    assert out.side_labels["BUY"] == "HORIZON_UP"
    assert out.side_labels["SELL"] == "HORIZON_DOWN"
    assert "side_payload" in out.diagnostics
