from backtest_app.historical_data.local_postgres_loader import _canonical_macro_series_name, _canonicalize_macro_rows


def test_canonical_macro_series_name_maps_supported_db_series():
    assert _canonical_macro_series_name("CBOE Volatility Index: VIX") == "vix"
    assert _canonical_macro_series_name("Federal Funds Effective Rate") == "rate"
    assert _canonical_macro_series_name("Nominal Broad U.S. Dollar Index") == "dollar"
    assert _canonical_macro_series_name("Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma") == "oil"
    assert _canonical_macro_series_name("unknown") is None


def test_canonicalize_macro_rows_filters_unknown_series_and_keeps_canonical_keys():
    rows = [
        {"obs_date": "2026-01-01", "name": "CBOE Volatility Index: VIX", "value": 18.5},
        {"obs_date": "2026-01-01", "name": "Federal Funds Effective Rate", "value": 4.0},
        {"obs_date": "2026-01-01", "name": "Unknown Series", "value": 123.0},
        {"obs_date": "2026-01-01", "name": "Nominal Broad U.S. Dollar Index", "value": 120.0},
        {"obs_date": "2026-01-01", "name": "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma", "value": 70.0},
    ]
    normalized = _canonicalize_macro_rows(rows)
    assert [row["name"] for row in normalized] == ["vix", "rate", "dollar", "oil"]
    assert normalized[0]["raw_name"] == "CBOE Volatility Index: VIX"
