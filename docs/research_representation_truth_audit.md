# Research Representation Truth Audit

Current public-branch representation contract audit for `features.py`, `pipeline.py`, and `local_postgres_loader.py`.

Canonical loader macro aliases: vix, rate, dollar, oil. Similarity-disabled series: breadth.

## Contract Summary

| Item | Value | Line Reference | Risk |
| --- | --- | --- | --- |
| Similarity feature keys | adv_percentile, atr_pct_14, beta_20, beta_residual_20, body_pct, close_location, dollar_change_20, dollar_change_5, dollar_pct_change_20, dollar_pct_change_5, dollar_percentile_20, dollar_slope_5, dollar_volume_percentile, dollar_zscore_20, drawdown_20, gap_pct, log_dollar_volume, lower_wick_pct, mkt_rel_ret_1, mkt_rel_ret_20, mkt_rel_ret_5, oil_change_20, oil_change_5, oil_pct_change_20, oil_pct_change_5, oil_percentile_20, oil_slope_5, oil_zscore_20, rate_change_20, rate_change_5, rate_pct_change_20, rate_pct_change_5, rate_percentile_20, rate_slope_5, rate_zscore_20, realized_vol_20, relative_volume, ret_1, ret_10, ret_20, ret_3, ret_5, ret_60, sector_rel_ret_1, sector_rel_ret_20, sector_rel_ret_5, upper_wick_pct, vix_age_bucket, vix_bars_since_update, vix_change_20, vix_change_5, vix_days_since_update, vix_is_stale, vix_pct_change_20, vix_pct_change_5, vix_percentile_20, vix_slope_5, vix_zscore_20, vol_normalized_residual_20 | backtest_app/historical_data/features.py | acceptable but monitor |
| Regime-only keys | dollar_change, dollar_level, dollar_zscore, oil_change, oil_level, oil_zscore, rate_change, rate_level, rate_zscore, vix_change, vix_level, vix_zscore | backtest_app/historical_data/features.py | acceptable but monitor |
| Absolute macro level in similarity by default | False | backtest_app/historical_data/features.py:280 | harmless |
| Raw dollar volume in similarity by default | False | backtest_app/historical_data/features.py:207 | harmless |
| Breadth policy | diagnostics_only_v1 | backtest_app/research/pipeline.py:42 | harmless |
| Session anchor fields | session_date_local, session_close_ts_local, session_close_ts_utc, feature_anchor_ts_utc | backtest_app/historical_data/session_alignment.py:81 | acceptable but monitor |
| Session metadata object | exchange_code, country_code, exchange_tz, session_close_local_time | backtest_app/historical_data/session_alignment.py:20 | acceptable but monitor |

## Missingness Handling

| Category | Mechanism | Line Reference | Risk |
| --- | --- | --- | --- |
| exclude row | query insufficient history | backtest_app/research/pipeline.py:981 | harmless |
| exclude row | event library insufficient bars | backtest_app/research/pipeline.py:788 | harmless |
| exclude row | unknown exchange/session metadata is classified as data_quality_missing | backtest_app/research/pipeline.py:785 | likely distorts similarity |
| zero fill in raw feature function | context/liquidity/helper defaults collapse to zero when history is short or absent | backtest_app/historical_data/features.py:268 | acceptable but monitor |
| zero fill in transform stage | missing feature keys are inserted as 0.0 before scaling | backtest_app/historical_data/features.py:40 | acceptable but monitor |
| forward fill in macro loader | calendar-day macro history forward fill via last_seen | backtest_app/historical_data/local_postgres_loader.py:75 | acceptable but monitor |
| fallback to market proxy | market proxy is session-aware same-exchange by default | backtest_app/research/pipeline.py:496 | acceptable but monitor |
| fallback to self sector proxy | sector proxy falls back to the symbol itself when no same-exchange peer exists | backtest_app/research/pipeline.py:553 | likely distorts similarity |

## History Scope

| Scope | Value | Line Reference |
| --- | --- | --- |
| feature_window_bars | 60 | backtest_app/research/pipeline.py:88 |
| loaded_warmup_bars | max(120, feature_window_bars * 2) | backtest_app/historical_data/local_postgres_loader.py:106 |
| event_memory_actual_usage | feature_end_date <= decision_date and outcome_end_date < decision_date inside decision-window build | backtest_app/research/pipeline.py:796 |
| query_window_scope | spec.feature_window_bars trailing bars ending at decision_date | backtest_app/research/pipeline.py:983 |
| macro_join_scope | latest series observation whose source_ts_utc <= feature_anchor_ts_utc | backtest_app/research/pipeline.py:265 |

## Risk Matrix

| Topic | Status | Line Reference | Risk |
| --- | --- | --- | --- |
| absolute macro level in default similarity | disabled by default | backtest_app/historical_data/features.py:280 | harmless |
| raw dollar volume in default similarity | disabled by default | backtest_app/historical_data/features.py:207 | harmless |
| session alignment | exchange-local session metadata derives feature_anchor_ts_utc | backtest_app/historical_data/session_alignment.py:81 | acceptable but monitor |
| market/sector proxy alignment | same-exchange session-aware proxy | backtest_app/research/pipeline.py:559 | acceptable but monitor |
| sector proxy self fallback | still enabled for missing peers | backtest_app/research/pipeline.py:553 | likely distorts similarity |
| regime gate source | normalized regime context is primary path | backtest_app/research/pipeline.py:696 | acceptable but monitor |
| macro as-of join | anchor-time as-of join with derived source_ts_utc | backtest_app/historical_data/local_postgres_loader.py:178 | acceptable but monitor |
| breadth similarity path | policy-disabled for v1, diagnostics only, non-blocking | backtest_app/research/pipeline.py:42 | acceptable but monitor |
| calendar-day macro snapshot artifact | legacy snapshot still persisted for compatibility, as-of join uses macro_series_history | backtest_app/historical_data/local_postgres_loader.py:75 | likely distorts regime gate |
