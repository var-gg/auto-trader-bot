# Live vs Shared vs Backtest Map

Scope focus requested:
- bootstrap
- premarket signal / risk / history
- trading-hybrid open / intraday
- fill collection
- order repository
- risk controller
- pm signal v1 / v2

Goal of this document:
1. map controller -> service -> repository -> DB/external API chain
2. highlight active money path
3. highlight pure-calculation candidates
4. mark v1 / v2 / legacy mixing points

---

## 1. High-level map

```text
Cloud Scheduler / HTTP
  -> controllers
    -> live orchestration services / engines
      -> repositories + DB + external APIs
      -> pure math helpers / planners
      -> executor_service
        -> order_repository
          -> DB write + broker submit
```

Important current reality:
- the **production runtime path is PM-based**
- but some **legacy hybrid/signal stack still participates** in open/intraday logic
- pure planning logic is spread across both PM and legacy hybrid files

---

## 2. Bootstrap chain

## 2.1 Route chain

```text
GET /kis-test/bootstrap
  -> app/features/kis_test/controllers/bootstrap_controller.py::run_bootstrap
    -> BootstrapService.run_bootstrap
       -> _refresh_tokens
          -> TokenRefreshService.refresh_expiring_tokens
       -> _ingest_fred_data
          -> FredSyncService.bulk_sync_since
          -> MacroRepository.get_active_series
       -> _ingest_yahoo_data
          -> YahooIndexService.ingest_data
       -> _refresh_premarket_risk
          -> HeadlineRiskService.refresh_snapshot
       -> _update_signals
          -> PMSignalServiceV2.update_signals_v2   (bootstrap path says v2)
```

## 2.2 Classification

| Layer | Current file | Bucket | Notes |
|---|---|---|---|
| Controller | `bootstrap_controller.py` | live-only | HTTP orchestration |
| Orchestrator | `bootstrap_service.py` | live-only | step coordinator |
| Pure candidate | none substantial | - | bootstrap itself is not shared |
| External side effects | token/FRED/Yahoo/risk/signal updates | live-only | production runtime |

## 2.3 Active money path relevance

Bootstrap is upstream of the money path because it refreshes:
- KIS auth
- macro context
- Yahoo market context
- PM risk snapshot
- PM signal table

It does not place orders directly, but it prepares the inputs that later drive orders.

---

## 3. PM signal v1 / v2 chain

## 3.1 Current route-default chain (v1)

```text
POST|GET /api/premarket/signals/update
  -> pm_signal_controller.update_pm_signals_*
    -> PMSignalService.update_signals          [v1 route default]
       -> _get_latest_config_id               -> trading.optuna_vector_config
       -> _load_config                        -> trading.optuna_vector_config
       -> _insert_signal_run_header           -> trading.pm_signal_run_header
       -> _get_tickers                        -> trading.ticker / filters
       -> build_context_vector                -> macro/yahoo tables
       -> per ticker:
          -> _fetch_ohlcv                     -> trading.ohlcv_daily
          -> build_shape_vector               -> pure vector math
          -> _ann_search / _full_scan_search  -> trading.target_vecidx_cfg_{config_id}
          -> _compute_signal                  -> pure PM scoring kernel
          -> snapshot/history upsert          -> PM history tables
       -> CALL trading.update_pm_best_signal(config_id)
          -> materialize latest pm_best_signal
```

## 3.2 Bootstrap chain (v2)

```text
BootstrapService._update_signals
  -> PMSignalServiceV2.update_signals_v2      [bootstrap path uses v2]
     -> same overall DB chain
     -> adds diagnostics / distribution logging
     -> still calls trading.update_pm_best_signal(config_id)
```

## 3.3 Pure calculation cluster inside PM signal path

```text
vector_builder.py
  -> compute_log_returns
  -> compute_log_volume_returns
  -> paa_transform
  -> build_shape_vector
  -> normalization helpers

pm_signal_service(_v2)
  -> rerank logic
  -> _logsumexp_tau
  -> _compute_signal
```

## 3.4 Classification map

| Node | Type | Bucket |
|---|---|---|
| `pm_signal_controller.py` | HTTP | live-only |
| `PMSignalService` | compute + persistence | mixed |
| `PMSignalServiceV2` | compute + persistence | mixed |
| `vector_builder.py` pure helpers | math | shared-candidate |
| config / ticker / OHLCV loaders | DB | live-only |
| stored proc call | DB side effect | live-only |
| PM scoring kernel | pure math | shared-candidate |

## 3.5 v1/v2 mixed state

| Mixed point | Observation |
|---|---|
| route `/api/premarket/signals/update` | still uses `PMSignalService` v1 |
| bootstrap `_update_signals` | uses `PMSignalServiceV2.update_signals_v2()` |
| docs / production understanding | runtime can be discussed as v2-ish while route code still points at v1 |

This is the most important PM v1/v2 coexistence point to preserve in refactor docs.

---

## 4. Premarket risk chain

## 4.1 Route chain

```text
POST /api/premarket/risk/refresh
  -> pm_risk_controller.refresh_risk_snapshot
    -> require_internal_scheduler_auth
    -> HeadlineRiskService.refresh_snapshot
       -> _fetch_headlines                -> trading.kis_news
       -> _score_headlines                -> OpenAI / responses_json
       -> _normalize                      -> pure normalization policy
       -> _insert_snapshot                -> trading.market_headline_risk_snapshot

GET /api/premarket/risk/latest
  -> get_latest_risk_snapshot
    -> HeadlineRiskService.get_latest_active_snapshot
       -> trading.market_headline_risk_snapshot
```

## 4.2 Pure candidates
- `_normalize`
- `_buy_multiplier`
- `_sell_multiplier`

## 4.3 Live-only chain
- internal auth gate
- DB headline fetch
- LLM scoring
- snapshot insert/query

---

## 5. Premarket history / outcome chain

## 5.1 Route chain

```text
POST /api/premarket/history/backfill-unfilled-reasons
  -> PMHistoryBatchService.backfill_unfilled_reasons
     -> trading.pm_order_execution_history scan
     -> _infer_unfilled_reason              [pure mapping]
     -> trading.pm_order_execution_history update

POST /api/premarket/history/compute-outcomes
  -> PMHistoryBatchService.compute_tplus_outcomes
     -> trading.pm_order_execution_history scan
     -> _load_tplus_close                   -> trading.ohlcv_daily
     -> _label_from_pnl_bps                 [pure labeling]
     -> trading.pm_outcome_tplus_history upsert

POST /api/premarket/history/postprocess
  -> PMHistoryBatchService.run_postprocess
     -> backfill_unfilled_reasons
     -> compute_tplus_outcomes
```

## 5.2 Classification

| Node | Type | Bucket |
|---|---|---|
| controller auth + scheduler endpoints | execution | live-only |
| batch service methods | mixed | mixed |
| `_infer_unfilled_reason` | pure | shared-candidate |
| `_label_from_pnl_bps` | pure | shared-candidate |

---

## 6. PM active set / open-session chain

## 6.1 Candidate-selection chain

```text
PMActiveSetService.get_pm_active_candidates
  -> OptunaRepository.get_pm_best_signals
     -> trading.pm_best_signal
     -> join trading.optuna_target_vectors
     -> join ticker / analyst recommendation state
  -> _get_latest_prices
     -> trading.ohlcv_daily
  -> _estimate_atr
     -> trading.ohlcv_daily
  -> OptunaRepository.get_ladder_params
     -> trading.optuna_vector_config.ladder_params
```

## 6.2 PM open execution planning chain

```text
HybridTraderEngine.run_open_greedy
  -> imports plan_pm_open_buy_orders, plan_pm_take_profit_orders from pm_open_session_service

plan_pm_open_buy_orders
  -> PMActiveSetService.get_pm_active_candidates
  -> HeadlineRiskService.get_discount_multiplier
     -> market_headline_risk_snapshot
  -> ladder / budget / multiplier helpers
  -> generate_pm_adaptive_ladder
  -> returns BUY order plans (not yet submitted)

plan_pm_take_profit_orders
  -> PositionMaturityRepository.check_position_maturity
     -> trading.order_fill / broker_order / order_leg / order_plan
     -> trading.ohlcv_daily
  -> sell-intensity helpers / ladder helpers
  -> returns SELL order plans
```

## 6.3 Execution chain after planning

```text
HybridTraderEngine.run_open_greedy
  -> executor_service.persist_batch_and_execute
     -> order_repository.create_order_batch
        -> trading.order_batch INSERT
     -> order_repository.create_plan_with_legs
        -> trading.order_plan INSERT
        -> trading.order_leg INSERT
        -> _submit_leg_to_broker (immediate)
           -> KISClient / broker APIs
           -> trading.broker_order
           -> PM execution-history append helpers
```

## 6.4 Classification

| Node | Type | Bucket |
|---|---|---|
| `PMActiveSetService` | mixed | mixed |
| `pm_open_session_service` | mixed | mixed, main extraction target |
| `pm_ladder_generator` | pure | shared-candidate |
| `PositionMaturityRepository` | side-effect DB | live-only |
| `persist_batch_and_execute` | side-effect execution | live-only |
| `order_repository` write/submit path | side-effect execution | live-only |

## 6.5 Pure subgraph inside PM open path

- `_required_discount`
- `_sigmoid`
- `_compute_pm_sell_intensity`
- `_rescale_legs_by_intensity`
- `_apply_news_risk_multiplier_to_legs`
- `_apply_news_bull_multiplier_to_sell_legs`
- `_apply_earnings_day_buy_multiplier_to_legs`
- `_apply_earnings_day_sell_multiplier_to_legs`
- `allocate_symbol_budgets_pm`
- ladder generation helpers

These should become the future PM domain planner kernel.

---

## 7. PM intraday + hybrid intraday chain

## 7.1 Top-level chain

```text
POST /api/trading-hybrid/{kr|us}/intraday
  -> trading_hybrid_controller.run_*_intraday
    -> runbooks.run_*_intraday
       -> _check_market_open
       -> _sync_profit_and_account
          -> TradeRealizedPnlService
          -> AssetSnapshotService
          -> Domestic/OverseasFillCollectionService
       -> HybridTraderEngine.run_intraday_cycle
```

## 7.2 Inside `run_intraday_cycle`

```text
run_intraday_cycle
  -> load_latest_account_snapshot
  -> load_latest_positions
  -> load_pending_orders
  -> compute_bucket_caps
  -> after-hours gates / cleanup paths
     -> cancel_negative_signal_pending_orders
  -> regular-market path:
     -> close_negative_signal_positions
     -> get_pm_intraday_active_set                     [PM active pool]
     -> predict_5min_window                            [legacy intraday predictions]
     -> plan_intraday_actions                          [legacy/hybrid planner]
     -> apply_rebalancing_rules
     -> enforce_intraday_stops
     -> near_close_cleanup (conditional)
     -> persist_batch_and_execute
```

## 7.3 Key mixed point

This is a major hybrid boundary:
- **candidate universe** comes from PM signal world
- **price timing / intraday behavior** still comes from legacy 5-min signal world

That mixed state must be explicitly preserved during refactor.

## 7.4 Classification

| Node | Type | Bucket |
|---|---|---|
| controller / runbooks | execution | live-only |
| `HybridTraderEngine.run_intraday_cycle` | orchestration | live-only |
| `get_pm_intraday_active_set` | mixed/light | mixed |
| `predict_5min_window` | side-effect DB/model query | live-only or legacy support |
| `plan_intraday_actions` | mixed | shared-candidate after adapter split |
| `apply_rebalancing_rules` | mixed | shared-candidate after adapter split |
| `enforce_intraday_stops` | mixed | mostly live-only |
| `near_close_cleanup` | mixed | mostly live-only |
| `persist_batch_and_execute` | execution | live-only |

---

## 8. Fill collection / sync chain

## 8.1 Chain

```text
runbooks._sync_profit_and_account(market)
  -> TradeRealizedPnlService.collect_and_save_realized_pnl
  -> AssetSnapshotService.collect_*_account_snapshot
  -> DomesticFillCollectionService.collect_domestic_fills OR
     OverseasFillCollectionService.collect_overseas_fills
```

## 8.2 Role in system

This is not planning logic. It is runtime state synchronization before live trading.
It is fully `live-only` and belongs under `live_app.sync` or equivalent.

## 8.3 Active money path

This is directly upstream of every order cycle because it refreshes:
- buying power
- positions
- fills
- realized pnl

Without this sync, order planning uses stale state.

---

## 9. Order repository / active money path map

## 9.1 Current chain

```text
planner returns order plans
  -> executor_service.persist_batch_and_execute
     -> create_order_batch
     -> create_plan_with_legs
        -> order_plan insert
        -> order_leg insert
        -> _submit_leg_to_broker
           -> KIS client resolution
           -> domestic/overseas order submit
           -> broker response parse
           -> broker_order write/update
           -> PM execution history append
```

## 9.2 Pure helpers inside same file

- `compute_bucket_caps`
- `extract_reject_reason`

These are the only clearly shared candidates in this module.

## 9.3 Risk note

This module is the tightest coupling point between:
- order policy
- persistence
- broker adapter
- PM observability history

If this is not split carefully, parity tests will remain weak because planning and execution stay entangled.

---

## 10. Legacy / older backtest chain

## 10.1 Direct backtest API

```text
POST|GET /api/signals/backtest-vec40
  -> backtest_controller
    -> BacktestService.backtest_vec40
       -> SignalRepository.get_daily_data_as_dict
       -> vec40 shape vector creation
       -> Vec40Repository.search_similar_vectors
       -> sliding-window evaluation
```

## 10.2 Older signal-detection chain

```text
SignalDetectionService.detect_signals
  -> get_trend_detector(version)
  -> get_vector_generator(version)
  -> ticker lookup
  -> ensure data (KR/US ingest if short)
  -> signal detection on df
  -> optional save to DB
```

## 10.3 Why it matters for refactor

This zone is the closest thing to an existing `backtest_app`, but it is:
- older architecture
- partially production-coupled
- not aligned with PM best-signal stack

So it should be treated as:
- input material for future backtest app
- not as the authority for current live PM logic

---

## 11. Active money path summary

These files/functions form the current **active money path** and must be visually separable from pure math:

### Entry/orchestration
- `trading_hybrid_controller.py`
- `runbooks.py`
- `HybridTraderEngine.run_open_greedy`
- `HybridTraderEngine.run_intraday_cycle`

### State sync / external IO
- holiday services
- account snapshot services
- fill collection services
- token / marketdata refresh in bootstrap

### Planning+execution boundary
- `pm_open_session_service.py` planners
- `intraday_session_service.py` planners
- `risk_controller.py`
- `executor_service.py`
- `order_repository.py`

### Broker / DB side effects
- `create_order_batch`
- `create_plan_with_legs`
- `_submit_leg_to_broker`
- PM execution-history append

---

## 12. Pure calculation candidate summary

These can plausibly move into `shared/domain` while preserving existing routes and schedulers:

### PM signal math
- PAA / returns / normalization / vector shaping
- rerank logic
- `_logsumexp_tau`
- PM score transform

### Order planning math
- tick rounding
- bucket-cap math
- ladder generation
- sell-intensity scaling
- quantity rescaling
- risk / earnings multiplier transforms
- affordability / granularity budget allocation

### Outcome / labeling
- pnl-to-label mapping
- reject-reason normalization
- unfilled-reason inference

---

## 13. Mixed / danger zones to preserve explicitly

| Zone | Why dangerous |
|---|---|
| PM signal route=v1 vs bootstrap=v2 | easy to accidentally “standardize” during refactor |
| PM active-set + legacy 5min intraday prediction | active pool and price timing come from different generations |
| `order_repository.py` | planning-adjacent helpers and broker side effects in one file |
| `open_session_service.py` legacy vs PM open session | older hybrid planner still coexists with current PM planner |
| risk controller | stop rules are mixed with live DB/order operations |

These should be called out in code comments/tests before moving files.
