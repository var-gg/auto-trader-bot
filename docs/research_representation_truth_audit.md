# Research Representation Truth Audit

Current public-branch representation contract audit for `features.py`, `pipeline.py`, and `local_postgres_loader.py`.

Canonical loader macro aliases: vix, rate, dollar, oil. Unmapped ctx series: breadth.

## Contract Summary

| Item | Value | Line Reference | Risk |
| --- | --- | --- | --- |
| Similarity feature keys | adv_percentile, atr_pct_14, beta_20, beta_residual_20, body_pct, breadth_change_20, breadth_change_5, breadth_pct_change_20, breadth_pct_change_5, breadth_percentile_20, breadth_slope_5, breadth_zscore_20, close_location, dollar_change_20, dollar_change_5, dollar_pct_change_20, dollar_pct_change_5, dollar_percentile_20, dollar_slope_5, dollar_volume_percentile, dollar_zscore_20, drawdown_20, gap_pct, log_dollar_volume, lower_wick_pct, mkt_rel_ret_1, mkt_rel_ret_20, mkt_rel_ret_5, oil_change_20, oil_change_5, oil_pct_change_20, oil_pct_change_5, oil_percentile_20, oil_slope_5, oil_zscore_20, rate_change_20, rate_change_5, rate_pct_change_20, rate_pct_change_5, rate_percentile_20, rate_slope_5, rate_zscore_20, realized_vol_20, relative_volume, ret_1, ret_10, ret_20, ret_3, ret_5, ret_60, sector_rel_ret_1, sector_rel_ret_20, sector_rel_ret_5, upper_wick_pct, vix_change_20, vix_change_5, vix_pct_change_20, vix_pct_change_5, vix_percentile_20, vix_slope_5, vix_zscore_20, vol_normalized_residual_20 | backtest_app/historical_data/features.py | acceptable but monitor |
| Regime-only keys | breadth_change, breadth_level, breadth_zscore, dollar_change, dollar_level, dollar_zscore, oil_change, oil_level, oil_zscore, rate_change, rate_level, rate_zscore, vix_change, vix_level, vix_zscore | backtest_app/historical_data/features.py | acceptable but monitor |
| Absolute macro level in similarity by default | False | backtest_app/historical_data/features.py:279 | harmless |
| Raw dollar volume in similarity by default | False | backtest_app/historical_data/features.py:206 | harmless |

## Missingness Handling

| Category | Mechanism | Line Reference | Risk |
| --- | --- | --- | --- |
| exclude row | query insufficient history | backtest_app/research/pipeline.py:504 | harmless |
| exclude row | event library insufficient bars | backtest_app/research/pipeline.py:376 | harmless |
| zero fill in raw feature function | context/liquidity/helper defaults collapse to zero when history is short or absent | backtest_app/historical_data/features.py:267 | acceptable but monitor |
| zero fill in raw feature function | sector residual falls back to market residual when sector proxy is absent | backtest_app/historical_data/features.py:395 | likely distorts similarity |
| zero fill in transform stage | missing feature keys are inserted as 0.0 before scaling | backtest_app/historical_data/features.py:39 | acceptable but monitor |
| forward fill in macro loader | calendar-day macro history forward fill via last_seen | backtest_app/historical_data/local_postgres_loader.py:147 | likely distorts regime gate |
| fallback to market proxy | sector residual uses market move when no sector proxy bars exist | backtest_app/historical_data/features.py:395 | likely distorts similarity |
| fallback to self sector proxy | sector proxy falls back to the symbol itself when no peer exists | backtest_app/research/pipeline.py:210 | likely distorts similarity |

## History Scope

| Scope | Value | Line Reference |
| --- | --- | --- |
| feature_window_bars | 60 | backtest_app/research/pipeline.py:76 |
| loaded_warmup_bars | max(120, feature_window_bars * 2) | backtest_app/historical_data/local_postgres_loader.py:65 |
| event_memory_actual_usage | feature_end_date <= decision_date and outcome_end_date < decision_date inside decision-window build | backtest_app/research/pipeline.py:384 |
| query_window_scope | spec.feature_window_bars trailing bars ending at decision_date | backtest_app/research/pipeline.py:506 |

## Risk Matrix

| Topic | Status | Line Reference | Risk |
| --- | --- | --- | --- |
| absolute macro level in default similarity | disabled by default | backtest_app/historical_data/features.py:279 | harmless |
| raw dollar volume in default similarity | disabled by default | backtest_app/historical_data/features.py:206 | harmless |
| market/sector proxy alignment | date aligned by trade_date union | backtest_app/research/pipeline.py:172 | acceptable but monitor |
| sector proxy self fallback | still enabled for missing peers | backtest_app/research/pipeline.py:210 | likely distorts similarity |
| regime gate source | normalized regime context is primary path | backtest_app/research/pipeline.py:296 | acceptable but monitor |
| macro loader alias mapping | canonicalizes vix/rate/dollar/oil; breadth remains unmapped in current DB source | backtest_app/historical_data/local_postgres_loader.py:31 | acceptable but monitor |
| calendar-day macro forward fill | still enabled | backtest_app/historical_data/local_postgres_loader.py:152 | likely distorts regime gate |
