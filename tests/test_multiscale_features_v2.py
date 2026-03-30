from backtest_app.historical_data.features import build_multiscale_feature_vector, fit_feature_scaler, fit_feature_transform
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.pipeline import _regime_from_context_features, _regime_from_macro_raw


def _bars(symbol: str, start: int = 1):
    rows = []
    price = 100.0
    for i in range(start, start + 80):
        open_ = price
        close = price * (1.001 + ((i % 5) * 0.0003))
        high = close * 1.01
        low = open_ * 0.99
        volume = 1_000_000 + i * 1000
        rows.append(HistoricalBar(symbol=symbol, timestamp=f"2026-01-{((i - 1) % 28) + 1:02d}", open=open_, high=high, low=low, close=close, volume=volume))
        price = close
    return rows


def _macro_history(days: int = 30):
    out = {}
    for i in range(1, days + 1):
        out[f"2026-01-{i:02d}"] = {
            "vix": 18.0 + i * 0.2,
            "rate": 3.5 + i * 0.03,
            "dollar": 98.0 + i * 0.15,
            "oil": 68.0 + i * 0.25,
            "breadth": -0.3 + i * 0.04,
        }
    return out


def test_multiscale_feature_vector_exposes_shape_ctx_and_metadata():
    bars = _bars("AAPL")
    market = _bars("MKT")
    sector = _bars("TECH")
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=bars, market_bars=market, sector_bars=sector, macro_history=_macro_history(), sector_code="TECH")
    assert fv.metadata["feature_version"] == "multiscale_manual_v2"
    assert fv.metadata["shape_dim"] == len(fv.shape_vector)
    assert fv.metadata["ctx_dim"] == len(fv.ctx_vector)
    assert len(fv.embedding) == len(fv.shape_vector) + len(fv.ctx_vector)
    for key in ("ret_1", "ret_3", "ret_5", "ret_10", "ret_20", "ret_60", "realized_vol_20", "atr_pct_14", "drawdown_20", "relative_volume", "adv_percentile", "log_dollar_volume", "dollar_volume_percentile"):
        assert key in fv.shape_features
    assert "dollar_volume" not in fv.shape_features
    assert abs(float(fv.shape_features["ret_60"])) > 0.0
    for key in ("mkt_rel_ret_1", "sector_rel_ret_1", "beta_residual_20", "vol_normalized_residual_20"):
        assert key in fv.residual_features
    assert "vix_level" not in fv.context_features
    for key in ("vix_zscore_20", "vix_change_5", "vix_pct_change_20", "oil_percentile_20"):
        assert key in fv.context_features
    for key in ("vix_level", "vix_change", "vix_zscore"):
        assert key in fv.regime_context_features
    assert "breadth_percentile_20" not in fv.context_features
    assert "vix_level" not in fv.metadata["feature_keys"]
    assert "vix_level" in fv.metadata["regime_ctx_keys"]


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


def test_feature_transform_exposes_explicit_raw_to_transformed_contract():
    rows = [
        {"ret_1": 0.01, "ret_5": 0.02, "vix_level": 20.0},
        {"ret_1": 0.02, "ret_5": 0.03, "vix_level": 22.0},
        {"ret_1": 0.03, "ret_5": 0.04, "vix_level": 24.0},
    ]
    transform = fit_feature_transform(rows)
    transformed, embedding = transform.apply({"ret_1": 0.02, "ret_5": 0.03, "vix_level": 22.0})
    assert transform.version == "feature_contract_v1"
    assert transform.feature_keys == ["ret_1", "ret_5", "vix_level"]
    assert abs(transformed["ret_1"]) < 1e-8
    assert abs(transformed["ret_5"]) < 1e-8
    assert abs(transformed["vix_level"]) < 1e-8
    assert embedding == [transformed[k] for k in transform.feature_keys]


def test_ablation_shape_only_has_no_context_dims():
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history={}, sector_code="TECH")
    assert fv.metadata["ctx_dim"] == 0
    assert not fv.context_features
    assert not fv.regime_context_features


def test_ablation_shape_plus_macro_excludes_levels_by_default():
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history=_macro_history(), sector_code="TECH")
    assert "vix_level" not in fv.raw_features
    assert "rate_level" not in fv.raw_features
    assert "vix_level" in fv.raw_regime_context_features
    assert fv.metadata["use_macro_level_in_similarity"] is False


def test_ablation_shape_plus_macro_can_include_levels_when_enabled():
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history=_macro_history(), sector_code="TECH", use_macro_level_in_similarity=True)
    assert "vix_level" in fv.raw_features
    assert "rate_level" in fv.raw_features
    assert fv.metadata["use_macro_level_in_similarity"] is True


def test_ablation_shape_plus_liquidity_uses_non_absolute_dollar_volume_by_default():
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history={}, sector_code="TECH")
    assert "log_dollar_volume" in fv.raw_shape_features
    assert "dollar_volume_percentile" in fv.raw_shape_features
    assert "dollar_volume" not in fv.raw_shape_features
    fv_with_abs = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history={}, sector_code="TECH", use_dollar_volume_absolute=True)
    assert "dollar_volume" in fv_with_abs.raw_shape_features
    assert fv_with_abs.metadata["use_dollar_volume_absolute"] is True


def test_normalized_regime_context_features_are_exposed_without_raw_levels():
    fv = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history=_macro_history(), sector_code="TECH")
    assert fv.normalized_regime_context_features
    assert all(not key.endswith("_level") for key in fv.normalized_regime_context_features)
    assert set(fv.normalized_regime_context_features).issubset(set(fv.raw_context_features))
    assert fv.metadata["normalized_regime_ctx_keys"] == sorted(fv.normalized_regime_context_features.keys())
    assert all(not key.startswith("breadth_") for key in fv.normalized_regime_context_features)


def test_transform_zero_fill_bookkeeping_is_recorded():
    fv_train = build_multiscale_feature_vector(symbol="AAPL", bars=_bars("AAPL"), market_bars=_bars("MKT"), sector_bars=_bars("TECH"), macro_history=_macro_history(), sector_code="TECH")
    transform = fit_feature_transform([fv_train.raw_features])
    fv_query = build_multiscale_feature_vector(
        symbol="AAPL",
        bars=_bars("AAPL"),
        market_bars=_bars("MKT"),
        sector_bars=_bars("TECH"),
        macro_history={},
        sector_code="TECH",
        transform=transform,
    )
    missing_keys = set(fv_query.metadata["transform_missing_keys_filled_zero"])
    assert "vix_zscore_20" in missing_keys
    assert "rate_pct_change_20" in missing_keys
    assert fv_query.metadata["transformed_zero_feature_keys"]


def test_macro_freshness_features_and_breadth_missingness_are_explicit():
    fv = build_multiscale_feature_vector(
        symbol="AAPL",
        bars=_bars("AAPL"),
        market_bars=_bars("MKT"),
        sector_bars=_bars("TECH"),
        macro_history=_macro_history(),
        sector_code="TECH",
        macro_freshness_features={
            "vix_days_since_update": 1.0,
            "vix_bars_since_update": 2.0,
            "vix_is_stale": 0.0,
            "vix_age_bucket": 1.0,
        },
        additional_metadata={
            "breadth_present": False,
            "breadth_missing_reason": "canonical_source_missing",
        },
    )
    assert fv.raw_context_features["vix_days_since_update"] == 1.0
    assert fv.raw_context_features["vix_bars_since_update"] == 2.0
    assert fv.metadata["breadth_present"] is False
    assert fv.metadata["breadth_missing_reason"] == "canonical_source_missing"


def test_normalized_regime_path_is_more_scale_stable_than_raw_macro_average():
    normalized = {
        "vix_zscore_20": -1.0,
        "rate_zscore_20": -0.5,
        "dollar_zscore_20": -0.2,
        "oil_zscore_20": 0.4,
        "breadth_zscore_20": 1.2,
    }
    raw_macro_a = {"vix": 25.0, "rate": 3.5, "dollar": 101.0, "oil": 75.0, "breadth": 0.3}
    raw_macro_b = {"vix": -90.0, "rate": -8.0, "dollar": -110.0, "oil": -70.0, "breadth": -0.3}
    assert _regime_from_context_features(normalized) == "RISK_OFF"
    assert _regime_from_context_features(normalized) == _regime_from_context_features(dict(normalized))
    assert _regime_from_macro_raw(raw_macro_a) != _regime_from_macro_raw(raw_macro_b)
