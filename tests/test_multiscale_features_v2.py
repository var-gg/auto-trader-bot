from backtest_app.historical_data.features import build_multiscale_feature_vector, fit_feature_scaler
from backtest_app.historical_data.models import HistoricalBar


def _bars(symbol: str, start: int = 1):
    rows = []
    price = 100.0
    for i in range(start, start + 80):
        open_ = price
        close = price * 1.002
        high = close * 1.01
        low = open_ * 0.99
        volume = 1_000_000 + i * 1000
        rows.append(HistoricalBar(symbol=symbol, timestamp=f"2026-01-{((i - 1) % 28) + 1:02d}", open=open_, high=high, low=low, close=close, volume=volume))
        price = close
    return rows


def test_multiscale_feature_vector_exposes_shape_ctx_and_metadata():
    bars = _bars("AAPL")
    market = _bars("MKT")
    sector = _bars("TECH")
    macro_history = {
        "2026-01-01": {"vix": 20.0, "rate": 4.0, "dollar": 100.0, "oil": 70.0, "breadth": 0.5},
        "2026-01-02": {"vix": 19.0, "rate": 4.1, "dollar": 99.5, "oil": 71.0, "breadth": 0.6},
    }
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=bars, market_bars=market, sector_bars=sector, macro_history=macro_history, sector_code="TECH")
    assert fv.metadata["feature_version"] == "multiscale_manual_v2"
    assert fv.metadata["shape_dim"] == len(fv.shape_vector)
    assert fv.metadata["ctx_dim"] == len(fv.ctx_vector)
    assert len(fv.embedding) == len(fv.shape_vector) + len(fv.ctx_vector)
    for key in ("ret_1", "ret_3", "ret_5", "ret_10", "ret_20", "ret_60", "realized_vol_20", "atr_pct_14", "drawdown_20", "relative_volume", "adv_percentile"):
        assert key in fv.shape_features
    for key in ("mkt_rel_ret_1", "sector_rel_ret_1", "beta_residual_20", "vol_normalized_residual_20"):
        assert key in fv.residual_features
    for key in ("vix_level", "vix_change", "vix_zscore", "breadth_level"):
        assert key in fv.context_features


def test_feature_scaler_uses_train_window_stats_only():
    rows = [
        {"ret_1": 0.01, "ret_5": 0.02, "vix_level": 20.0},
        {"ret_1": 0.02, "ret_5": 0.03, "vix_level": 22.0},
        {"ret_1": 0.03, "ret_5": 0.04, "vix_level": 24.0},
    ]
    scaler = fit_feature_scaler(rows)
    transformed = scaler.transform({"ret_1": 0.02, "ret_5": 0.03, "vix_level": 22.0})
    assert abs(transformed["ret_1"]) < 1e-8
    assert abs(transformed["ret_5"]) < 1e-8
    assert abs(transformed["vix_level"]) < 1e-8
