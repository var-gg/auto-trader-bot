# Refactor Target - live_app vs shared/domain vs backtest_app

Scope: current `auto-trader-bot` repo, code-read only. No route / scheduler / filename changes proposed in this step.

## Target architecture

- `shared/domain`
- `live_app`
- `backtest_app`
- `parity_tests`

This document classifies current code into:
- `live-only`
- `shared-candidate`
- `backtest-only`
- `legacy/deprecated`

It also judges each major function/class as:
- `pure calculation`
- `side-effect execution`
- `mixed`

---

## 1. Executive summary

### Current repo shape

The repo already contains two different lineages:

1. **Active money path / production runtime**
   - `kis_test.bootstrap` orchestration
   - `premarket` signal/risk/history
   - `trading_hybrid` open/intraday execution
   - order persistence + broker submission
   - account / fill collection / holiday checks

2. **Older research/backtest lineage**
   - `signals` feature set (`signal_detection_service`, `backtest_service`, vec40/similarity stack)
   - legacy pattern-detection and similarity search
   - still partially imported by current live code in hybrid services

### Most important refactor fact

The repo **does not yet cleanly separate decision logic from execution logic**.

The main blockers are:
- `pm_open_session_service.py` and `trading_hybrid/services/*` mix:
  - ranking / budgeting / ladder math / maturity logic
  - with DB reads and live order-plan side effects nearby
- `order_repository.py` mixes:
  - persistence
  - broker adapter
  - PM execution-history append
- `runbooks.py` mixes:
  - sync/fill collection
  - holiday gating
  - active trading entrypoints

### Strongest shared/domain candidates

These are the best initial extraction targets:

- `app/features/premarket/utils/vector_builder.py`
- `app/features/premarket/utils/pm_ladder_generator.py`
- pure helper blocks inside:
  - `pm_open_session_service.py`
  - `trading_hybrid/services/open_session_service.py`
  - `trading_hybrid/services/intraday_session_service.py`
- `trading_hybrid/policy/tuning.py`
- pure labeling helpers from `pm_history_batch_service.py`
- budget allocation / ladder shaping / intensity math

### Strongest live-only zones

- controllers under `kis_test`, `premarket`, `trading_hybrid`
- `runbooks.py`
- `executor_service.py`
- most of `order_repository.py`
- `HeadlineRiskService.refresh_snapshot()` (LLM + DB)
- token refresh / market data ingest / fill collection / KIS submission

### Strongest legacy/deprecated zones

- `app/features/signals/controllers/backtest_controller.py`
- `app/features/signals/services/backtest_service.py`
- much of `app/features/signals/services/signal_detection_service.py`
- vec40 / similarity-analysis stack used by older hybrid logic
- `trading_hybrid/services/open_session_service.py` as older pre-PM open logic

---

## 2. Classification by area

## 2.1 Bootstrap

### Files
- `app/features/kis_test/controllers/bootstrap_controller.py`
- `app/features/kis_test/services/bootstrap_service.py`

### Classification
- `bootstrap_controller.py` → `live-only`
- `BootstrapService` → `live-only`

### Why
This is orchestration glue for production bootstrap:
- token refresh
- FRED ingest
- Yahoo ingest
- premarket risk refresh
- PM signal update

It is not reusable backtest logic. It should stay in `live_app`.

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| `run_bootstrap` controller | side-effect execution | HTTP entrypoint + request handling |
| `BootstrapService.run_bootstrap` | side-effect execution | step orchestration |
| `_refresh_tokens` | side-effect execution | token refresh |
| `_ingest_fred_data` | side-effect execution | external ingest + DB |
| `_ingest_yahoo_data` | side-effect execution | external ingest + DB |
| `_refresh_premarket_risk` | side-effect execution | DB + LLM-backed service |
| `_update_signals` | side-effect execution | delegates PM signal update |

### Refactor note
Keep as `live_app.bootstrap`. Do not move into shared/domain.

---

## 2.2 Premarket signal engine

### Files
- `app/features/premarket/controllers/pm_signal_controller.py`
- `app/features/premarket/services/pm_signal_service.py` (v1 active route)
- `app/features/premarket/services/pm_signal_service_v2.py` (improved but not route-default)
- `app/features/premarket/utils/vector_builder.py`
- `app/features/premarket/models/optuna_models.py`
- `app/features/premarket/repositories/optuna_repository.py`

### Classification
- controller → `live-only`
- `PMSignalService` / `PMSignalServiceV2` → `mixed`
- `vector_builder.py` → `shared-candidate`
- `optuna_models.py` → `live-only` DB schema layer
- `optuna_repository.py` → `live-only` repository, but returns data for shared logic

### Why
The PM signal services contain two layers at once:
- **shared-worthy domain math**
  - shape/context vector generation
  - reranking
  - log-sum-exp evidence aggregation
  - signal probability transform
- **live-only execution**
  - config load from DB
  - ticker fetch
  - run header insert
  - `pm_best_signal` writes
  - stored procedure call

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| `pm_signal_controller.*` | side-effect execution | HTTP only |
| `PMSignalService.update_signals` | mixed | compute + DB writes + proc call |
| `PMSignalService.get_signals` | side-effect execution | DB query API |
| `PMSignalService.test_pm_signal` | mixed | compute path + DB sourcing |
| `_get_latest_config_id`, `_load_config`, `_get_tickers`, `_fetch_ohlcv`, `_ann_search`, `_full_scan_search`, `_insert_signal_run_header`, `_upsert_snapshot_history` | side-effect execution | DB access |
| `_logsumexp_tau` | pure calculation | math only |
| `_compute_signal` | pure calculation | rerank + probability + signal transform |
| `build_shape_vector` | pure calculation | vector math |
| `build_context_vector` | mixed | DB read + vector math |
| `pgvector_to_numpy`, `cosine_similarity`, normalization helpers | pure calculation | math conversion |
| `PMSignalServiceV2.update_signals_v2` | mixed | same split as v1, improved diagnostics |

### Shared/domain extraction candidates
1. `vector_builder` pure helpers
2. PM signal scoring kernel from `_compute_signal`
3. shape/context rerank logic
4. config-independent query/result DTOs

### Refactor note
Target end-state:
- `shared/domain/premarket/signal_math.py`
- `shared/domain/premarket/vector_math.py`
- `live_app/premarket/signal_runtime.py` for DB loading + persistence

---

## 2.3 Premarket risk snapshot

### Files
- `app/features/premarket/controllers/pm_risk_controller.py`
- `app/features/premarket/services/headline_risk_service.py`

### Classification
- controller → `live-only`
- `HeadlineRiskService` → mostly `live-only`, with a few `shared-candidate` helper functions

### Why
This service is production-only because it depends on:
- recent headline fetch from DB
- LLM scoring
- snapshot persistence
- live TTL behavior

But some mapping logic is generic.

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| `refresh_risk_snapshot` controller | side-effect execution | HTTP auth + call |
| `HeadlineRiskService.refresh_snapshot` | mixed | DB fetch + LLM + normalization + DB insert |
| `get_latest_active_snapshot`, `get_latest_snapshot`, `ensure_active_snapshot` | side-effect execution | DB + freshness policy |
| `get_discount_multiplier`, `get_sell_markup_multiplier` | mixed | DB-sourced + simple mapping |
| `_fetch_headlines` | side-effect execution | DB query |
| `_score_headlines` | side-effect execution | external LLM call |
| `_normalize` | pure calculation | maps scored object |
| `_buy_multiplier`, `_sell_multiplier` | pure calculation | deterministic policy math |

### Shared/domain extraction candidates
- `_normalize`
- `_buy_multiplier`
- `_sell_multiplier`
- compact risk-note formatting policy shared with order planning

---

## 2.4 Premarket history / outcome postprocess

### Files
- `app/features/premarket/controllers/pm_history_batch_controller.py`
- `app/features/premarket/services/pm_history_batch_service.py`

### Classification
- controller → `live-only`
- service → `mixed`

### Why
Batch execution is live-only, but the label / inference helpers are backtest-friendly.

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| controller endpoints | side-effect execution | scheduler/internal API |
| `backfill_unfilled_reasons` | mixed | DB scan/update + pure inference helper |
| `compute_tplus_outcomes` | mixed | DB scan/upsert + deterministic outcome labeling |
| `run_postprocess` | side-effect execution | orchestration |
| `_infer_unfilled_reason` | pure calculation | message/code mapping |
| `_load_tplus_close` | side-effect execution | DB query |
| `_label_from_pnl_bps` | pure calculation | deterministic label mapping |

### Shared/domain extraction candidates
- outcome labeling policy (`_label_from_pnl_bps`)
- unfilled-reason classification policy
- horizon constant model

### Backtest value
This is a good seed for `shared/domain/outcomes.py` and later parity tests.

---

## 2.5 Premarket active-set / open / intraday

### Files
- `app/features/premarket/services/pm_active_set_service.py`
- `app/features/premarket/services/pm_open_session_service.py`
- `app/features/premarket/services/pm_intraday_session_service.py`
- `app/features/premarket/repositories/position_maturity_repository.py`
- `app/features/premarket/utils/pm_ladder_generator.py`

### Classification
- `pm_active_set_service.py` → `mixed`
- `pm_open_session_service.py` → `mixed`, major extraction target
- `pm_intraday_session_service.py` → `mixed/light`
- `position_maturity_repository.py` → `live-only`
- `pm_ladder_generator.py` → `shared-candidate`

### Why
This area is the best candidate for the future `shared/domain` because it contains:
- active candidate ranking
- budget allocation
- ladder generation
- intensity scaling
- buy/sell discount/markup transforms
- maturity decisions

But the current files also do DB lookups and live planning assembly.

### Function judgment (important subset)
| Symbol | Type | Reason |
|---|---|---|
| `PMActiveSetService.get_pm_active_candidates` | mixed | repo query + enrich + default ladder params |
| `_get_latest_prices` | side-effect execution | DB query |
| `_estimate_atr` | mixed | DB query + deterministic ATR calc |
| `_required_discount` | pure calculation | simple policy math |
| `_sigmoid` | pure calculation | math |
| `_compute_pm_sell_intensity` | pure calculation | deterministic intensity |
| `_rescale_legs_by_intensity` | pure calculation | quantity redistribution |
| `_apply_news_risk_multiplier_to_legs` | pure calculation | ladder transform |
| `_apply_news_bull_multiplier_to_sell_legs` | pure calculation | ladder transform |
| `_apply_earnings_day_*_to_legs` | pure calculation | ladder transform |
| `allocate_symbol_budgets_pm` | pure calculation if inputs are pre-enriched | no direct IO |
| `plan_pm_open_buy_orders` | mixed | DB access + planning |
| `plan_pm_take_profit_orders` | mixed | DB positions + maturity + planning |
| `get_pm_intraday_active_set` | mixed/light | thin DB-backed selector |
| `PositionMaturityRepository.*` | side-effect execution | DB only |
| `pm_ladder_generator.generate_pm_adaptive_ladder` | pure calculation | domain candidate |
| `pm_ladder_generator.qty_from_budget` | pure calculation | domain candidate |

### Shared/domain extraction candidates
- ladder generators
- budget allocators
- sell-intensity math
- risk/earnings multiplier transforms
- planning DTOs (`OrderIntent`, `LadderLeg`, `CandidateScore`, etc.)

### Refactor note
This area should become the first real `shared/domain/premarket_execution` package.

---

## 2.6 Trading-hybrid controller / engine / services

### Files
- `app/features/trading_hybrid/controllers/trading_hybrid_controller.py`
- `app/features/trading_hybrid/engines/hybrid_trader_engine.py`
- `app/features/trading_hybrid/engines/runbooks.py`
- `app/features/trading_hybrid/services/open_session_service.py`
- `app/features/trading_hybrid/services/intraday_session_service.py`
- `app/features/trading_hybrid/services/risk_controller.py`
- `app/features/trading_hybrid/services/executor_service.py`
- `app/features/trading_hybrid/policy/tuning.py`
- `app/features/trading_hybrid/utils/*`

### Classification
- controller → `live-only`
- `hybrid_trader_engine.py` → `live-only orchestration`, but it invokes shared-worthy planners
- `runbooks.py` → `live-only`
- `open_session_service.py` → `legacy/deprecated` + `shared-candidate` pure helpers inside
- `intraday_session_service.py` → `mixed`
- `risk_controller.py` → `mixed`, mostly live-only
- `executor_service.py` → `live-only`
- `policy/tuning.py` → `shared-candidate`
- `utils/ticks.py`, `utils/timebars.py`, `utils/ladder_generator.py` → mostly `shared-candidate` except session-clock helpers may remain live support

### Why
Current production engine already shifted to PM-based open/intraday paths, but older hybrid logic still exists and is still imported in places.

### Function judgment (important subset)
| Symbol | Type | Reason |
|---|---|---|
| controller endpoints | side-effect execution | HTTP only |
| `HybridTraderEngine.run_open_greedy` | side-effect execution | orchestration + persistence |
| `HybridTraderEngine.run_intraday_cycle` | side-effect execution | orchestration + sync + risk + execution |
| `runbooks._check_market_open` | side-effect execution | holiday service / API |
| `runbooks._sync_profit_and_account` | side-effect execution | fill collection + account snapshot |
| `persist_batch_and_execute` | side-effect execution | batch persistence + submit |
| `compute_bucket_caps` | pure calculation | deterministic caps |
| `open_session_service.allocate_symbol_budgets` | pure calculation with pre-enriched inputs | reusable |
| `_compute_sell_intensity` | pure calculation | reusable |
| `_rescale_sell_legs_by_intensity` | pure calculation | reusable |
| `plan_intraday_actions` | mixed | planning math + references to DB helpers / pending state conventions |
| `apply_rebalancing_rules` | mixed | order-state aware rule engine |
| `risk_controller.enforce_intraday_stops` | mixed | stop rule + DB/order execution |
| `risk_controller.near_close_cleanup` | mixed | cleanup policy + DB/order execution |
| `risk_controller.cancel_negative_signal_pending_orders` | mixed | policy + DB/order ops |
| `_session_times_kst`, `_is_us_dst` | pure calculation | reusable session policy |
| `ticks.round_to_tick` | pure calculation | reusable |
| `ladder_generator.generate_unified_adaptive_ladder` | pure calculation | reusable |

### Refactor note
- `runbooks.py` and controller stay in `live_app`.
- `HybridTraderEngine` remains `live_app` orchestrator.
- move pure planning and policy math into `shared/domain`.
- old open-session service should be marked legacy once PM path is parity-covered.

---

## 2.7 Order repository / active money path

### File
- `app/features/trading_hybrid/repositories/order_repository.py`

### Classification
- mostly `live-only`
- but contains several `shared-candidate` pure helpers

### Why
This file currently combines four roles:
1. bucket math
2. DB persistence
3. broker adapter
4. PM order-execution-history append

That is the most dangerous coupling in the repo from refactor perspective.

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| `compute_bucket_caps` | pure calculation | domain policy |
| `create_order_batch` | side-effect execution | DB insert |
| `create_plan_with_legs` | side-effect execution | DB insert + immediate submission |
| `submit_to_broker` | side-effect execution | broker submit |
| `_load_leg_context` | side-effect execution | DB query |
| `_kis_client_or_none` | side-effect execution | external adapter creation |
| `extract_reject_reason` | pure calculation | response normalization |
| `_resolve_pm_run_id` | side-effect execution | DB query |
| `_insert_pm_order_execution_history` | side-effect execution | DB append |
| `_submit_leg_to_broker` | side-effect execution | live money path |
| cancel/replace/get_pending/get_blocked helpers | side-effect execution | DB/order state |

### Refactor note
Future split should be:
- `shared/domain/order_policy.py` → cap math, reason normalization DTOs
- `live_app/order_persistence.py`
- `live_app/broker/kis_executor.py`
- `live_app/history/pm_execution_writer.py`

---

## 2.8 Legacy signals / vec40 backtest stack

### Files
- `app/features/signals/controllers/backtest_controller.py`
- `app/features/signals/services/backtest_service.py`
- `app/features/signals/services/signal_detection_service.py`
- `app/features/signals/repositories/intraday_signal_repository.py`
- similarity / vec40 / similarity-analysis related files

### Classification
- `backtest_controller.py` → `backtest-only` or `legacy/deprecated`
- `backtest_service.py` → `backtest-only` + legacy
- `signal_detection_service.py` → `legacy/deprecated`, but some pattern-label logic is reusable
- `intraday_signal_repository.py` → mostly `live-only` data access, but tied to legacy signal lineage

### Why
This stack clearly predates current PM best-signal architecture.
It uses:
- vec40 similarity search
- old signal detection versions
- explicit backtest endpoint
- older hybrid open-session dependencies

This is the natural source area for future `backtest_app`, but **not** as-is. It needs isolation from production API concerns.

### Function judgment
| Symbol | Type | Reason |
|---|---|---|
| `backtest_vec40` controller/service | mixed, but backtest-only | analytics path, not live money |
| `SignalDetectionService.detect_signals` | mixed | data ensure/ingest + compute + optional save |
| `_create_signal_points` | pure calculation | DTO transform |
| `_ensure_sufficient_data`, `_ingest_kr_data`, `_ingest_us_data` | side-effect execution | ingest |
| `_save_signals_to_db` | side-effect execution | persistence |
| intraday repo CRUD/upsert/search | side-effect execution | DB queries |

### Refactor note
Treat as:
- source material for `backtest_app`
- plus a `legacy` namespace until parity migration is complete

---

## 3. Best candidates for `shared/domain`

## Tier 1 - extract first

| Current location | Candidate | Why |
|---|---|---|
| `premarket/utils/vector_builder.py` | vector math package | core PM signal math |
| `premarket/utils/pm_ladder_generator.py` | PM ladder policy | directly reusable in backtest/live |
| `trading_hybrid/utils/ladder_generator.py` | generic ladder policy | core order-plan generation |
| `trading_hybrid/utils/ticks.py` | tick rounding | pure market-rule helper |
| `trading_hybrid/policy/tuning.py` | policy config | needed by both live/backtest |
| pure helper blocks in `pm_open_session_service.py` | budget/intensity/risk transforms | strong parity value |
| pure helper blocks in `open_session_service.py` | older intensity/budget logic | shared if still needed |
| `_label_from_pnl_bps` in `pm_history_batch_service.py` | outcome labeling | parity / evaluation |
| `_infer_unfilled_reason` in `pm_history_batch_service.py` | reject classification | useful across replay/live |
| `_logsumexp_tau`, PM signal scoring kernel | PM score engine | main shared model logic |

## Tier 2 - extract after DTO boundaries exist

| Current location | Candidate | Why |
|---|---|---|
| `PMActiveSetService` planning DTO assembly | candidate ranking policy | depends on repo output now |
| `plan_pm_open_buy_orders` | open planner | needs IO separation |
| `plan_pm_take_profit_orders` | exit planner | needs positions/history adapters |
| `plan_intraday_actions` | intraday planner | needs state adapter abstraction |
| `risk_controller` rule predicates | risk policy engine | currently coupled to submit/persist |

---

## 4. Active money path vs pure calculation boundary

## Active money path (must stay readable and isolated)

These are the functions where real-money side effects occur or are one call away:

- `runbooks._sync_profit_and_account`
- `HybridTraderEngine.run_open_greedy`
- `HybridTraderEngine.run_intraday_cycle`
- `executor_service.persist_batch_and_execute`
- `order_repository.create_order_batch`
- `order_repository.create_plan_with_legs`
- `order_repository._submit_leg_to_broker`
- `risk_controller.enforce_intraday_stops`
- `risk_controller.near_close_cleanup`
- fill collection services invoked from runbooks
- token refresh / market data ingest / holiday checks / KIS calls

## Pure-calculation cluster (should move together)

- vector normalization / returns / PAA / cosine logic
- PM signal evidence aggregation math
- budget allocation
- ladder generation
- tick rounding
- sell-intensity / risk multiplier transforms
- outcome label mapping
- reject-reason normalization

This split is visible enough in current code to support the next refactor phase.

---

## 5. Recommended first extraction boundaries

## Boundary A - PM signal math
- Inputs: preloaded OHLCV, context vector, candidate vectors, config
- Outputs: signal score, best target, diagnostics
- No DB/session inside

## Boundary B - order planning
- Inputs: candidate list, account caps, positions, tuning, market micro-rules
- Outputs: `OrderPlan[]`, `SkippedReason[]`
- No DB writes, no broker calls

## Boundary C - outcome evaluation
- Inputs: entry price, horizon prices, labels/policy params
- Outputs: outcome DTOs
- No DB inside

## Boundary D - broker/persistence adapters
- DB writes
- KIS submission
- execution history append
- fill collection

---

## 6. Immediate implications for next phase

1. **Do not split repo first. Split logic layers first.**
2. **Create parity tests around extracted planners before replacing live path.**
3. **Keep PM v1/v2 coexistence explicit until route default is decided.**
4. **Mark older `signals` + legacy hybrid open path as non-authoritative for new design.**
5. **Refactor `order_repository.py` early** because it is the highest-risk mixed file on the execution boundary.

---

## 7. Provisional package landing map

### `shared/domain`
- `premarket/signal_math.py`
- `premarket/vector_math.py`
- `premarket/order_planning.py`
- `execution/ladder_policy.py`
- `execution/ticks.py`
- `execution/budgeting.py`
- `risk/headline_policy.py`
- `outcomes/labels.py`

### `live_app`
- current controllers
- bootstrap orchestration
- DB repositories
- KIS broker adapter
- account/fill sync
- PM runtime loaders and writers
- hybrid runtime engine

### `backtest_app`
- replay runners
- historical data adapters
- simulated broker
- optuna / evaluation runners
- reports

### `parity_tests`
- PM signal math parity
- ladder generation parity
- order-plan parity
- outcome labeling parity
- live-vs-backtest decision parity on frozen fixtures
