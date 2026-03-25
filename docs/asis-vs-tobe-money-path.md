# AS-IS vs TO-BE Money Path Map

Created: 2026-03-25
Branch under review: `public-release-20260323` (`1b67bef`)
Baseline: `docs/runtime-replay-corpus.md` + `tests/replay_fixtures/*`

## Classification legend
- **active-same**: same live endpoint and materially same live path still active
- **active-moved**: same live endpoint still active, but orchestration moved behind command/schedule seam
- **changed-contract**: live endpoint remains but response/behavior/side effect contract changed materially
- **missing**: AS-IS live case exists in production truth, but TO-BE path is not fully accounted for
- **deprecated**: code path still exists but should not be treated as active money path for current live validation

## Summary table

| Live function | AS-IS truth anchor | TO-BE entry | TO-BE path | Classification | Evidence / rationale |
|---|---|---|---|---|---|
| Bootstrap preopen | `/kis-test/bootstrap`, Cloud Run request at 2026-03-24T07:00:01Z, PM run `34` | same endpoint via `app/features/kis_test/controllers/bootstrap_controller.py` | controller -> `live_app.application.bootstrap_commands.RunBootstrapCommand` -> `BootstrapService` | **active-moved** | Endpoint remains active, but orchestration/logging moved into `live_app.application.*` command seam. Live behavior still delegates to original `BootstrapService`. |
| PM signal generation US | `/recommendations/batch-generate`, PM run `34` | same endpoint via `app/features/recommendation/controllers/us_batch_recommendation_controller.py` | controller -> `UsBatchRecommendationService` | **active-same** | Active endpoint still included in `app/controllers/api_router.py`; PM run headers/decision history still written by premarket services. No new live seam yet for this path. |
| PM signal generation KR | `/recommendations/kr/batch-generate`, PM run `33` | same endpoint via `app/features/recommendation/controllers/kr_batch_recommendation_controller.py` | controller -> `KrBatchRecommendationService` | **active-same** | Same reasoning as US path. Endpoint still active and routed. |
| PM signal history write | DB: `pm_signal_run_header`, `pm_candidate_decision_history`, `pm_signal_snapshot_history` | internal via recommendation / bootstrap flows | `pm_signal_service_v2` inserts run header + candidate history + snapshot history | **active-same** | Side-effect tables are still written in current code; no new repository/adapter seam replaces them yet. |
| PM risk refresh | Bootstrap step + `pm_risk_controller` router included | likely bootstrap step and/or direct PM risk controller | bootstrap -> `BootstrapService`; direct PM risk route remains in old app router | **missing** | Active production truth for dedicated PM risk refresh endpoint not yet fixture-anchored. Code exists, router exists, but AS-IS→TO-BE equivalence is not fully proven from truth corpus yet. |
| PM history batch / outcomes | old router present in app router | `live_app.application.history_commands` + old controller routes | old controllers still routed; new command seam exists for slot dispatcher | **active-moved** | Housekeeping/postprocess seam exists in `schedule_slots.py`, but old controller routes also remain active. Migration is partial. |
| KR open trading | `/api/trading-hybrid/kr/open`, batch `7156` | same endpoint via `trading_hybrid_controller.py` | controller -> `RunTradingHybridCommand.run_open(KR)` -> `runbooks.run_kr_open` -> `HybridTraderEngine` -> repositories/DB/broker | **active-moved** | Endpoint remains same, but live orchestration moved behind `live_app.application.trading_commands`. Core engine/repository path still old/live. |
| US open trading | `/api/trading-hybrid/us/open`, batches `7175`,`7176` | same endpoint | controller -> `RunTradingHybridCommand.run_open(US)` -> `runbooks.run_us_open` -> engine -> repositories/DB/broker | **active-moved** | Same as KR open. |
| KR intraday trading | `/api/trading-hybrid/kr/intraday`, batch `7181` | same endpoint | controller -> `RunTradingHybridCommand.run_intraday(KR)` -> `runbooks.run_kr_intraday` -> engine -> repositories/DB/broker | **active-moved** | Same endpoint, command seam inserted, engine path still legacy/live. |
| US intraday trading | `/api/trading-hybrid/us/intraday`, batch `7219` | same endpoint | controller -> `RunTradingHybridCommand.run_intraday(US)` -> `runbooks.run_us_intraday` -> engine -> repositories/DB/broker | **active-moved** | Same endpoint, command seam inserted, engine path still legacy/live. |
| Trading fill sync inside runbooks | AS-IS DB truth shows order_fill rows updating around trade cycles | no separate new endpoint; still in runbook pre-sync | `_sync_profit_and_account()` -> `DomesticFillCollectionService` / `OverseasFillCollectionService` | **active-same** | Trade-cycle path still explicitly runs fill collection before engine execution. No live migration away from those services. |
| Dedicated fill collection API KR | code path only so far, no scheduler truth yet | `/domestic-fill-collection/collect` | controller -> `DomesticFillCollectionService` -> `order_fill` upsert | **missing** | Code path exists and is included in router, but current corpus has not connected production scheduler/request truth to it. |
| Dedicated fill collection API US | code path only so far, no scheduler truth yet | `/overseas-fill-collection/collect` | controller -> `OverseasFillCollectionService` -> `order_fill` upsert | **missing** | Same as KR dedicated fill API. |
| Slot-based dispatch layer | no direct AS-IS scheduler truth yet | `live_app.application.schedule_slots.ScheduleSlotDispatcher` | slot -> bootstrap/trading/history commands | **deprecated for active path / future target** | This seam exists for TO-BE unification, but current Scheduler truth still calls old public HTTP endpoints directly. Do not count it as active production path yet. |
| Shared/domain planning seam | no direct AS-IS production endpoint | `live_app.application.planning_commands.BuildOrderPlanCommand` | shared/domain only | **deprecated for active path / future target** | Useful for parity tests, not current live production orchestration. Backtest/shared-domain planning must not be confused with active live money path. |

## Detailed path notes

### 1) Bootstrap
**AS-IS truth**
- Scheduler jobs `kr_signals` / `us_signals` call `/kis-test/bootstrap`
- Cloud Run request log confirms GET `/kis-test/bootstrap`
- PM signal run headers and candidate decisions appear downstream in DB

**TO-BE path**
- `bootstrap_controller.py` still owns public endpoint
- endpoint now delegates to `RunBootstrapCommand`
- `RunBootstrapCommand` still delegates to `BootstrapService`

**Judgment**
- Live behavior is still anchored in old service logic
- orchestration and structured logging moved outward
- therefore **active-moved**, not fully replaced

### 2) PM signal / risk / history
**AS-IS truth**
- US/KR recommendation endpoints are active in Scheduler + Cloud Run logs
- DB confirms `pm_signal_run_header` and `pm_candidate_decision_history`

**TO-BE path**
- recommendation controllers remain active in old router
- `pm_signal_service_v2` still writes signal run header / snapshot / decision history
- `pm_risk_controller` and `pm_history_batch_controller` are still included in `api_router`
- new command seams exist for history/risk/slots, but they are not proven active scheduler entrypoints yet

**Judgment**
- signal generation: **active-same**
- risk refresh: **missing** proof at truth level
- history batch/outcome processing: **active-moved** only partially, because old active router remains and slot dispatcher is not yet proven to be scheduler-facing

### 3) Trading hybrid KR/US open/intraday
**AS-IS truth**
- Scheduler calls exact `/api/trading-hybrid/{market}/{phase}` endpoints
- DB anchors show `order_batch -> order_plan -> order_leg -> broker_order -> order_fill`

**TO-BE path**
- public endpoints unchanged
- controller now delegates to `RunTradingHybridCommand`
- command delegates to `runbooks.*`
- runbooks still perform market-open checks, profit/account sync, fill sync, and then invoke `HybridTraderEngine`
- engine/repositories still own broker submission and DB writes

**Judgment**
- controller/application layer moved
- engine/repository side effects remain mostly in place
- live money path is **active-moved**

### 4) Fill collection
**AS-IS truth**
- `order_fill` rows clearly update in production DB
- trade runbooks explicitly call fill collectors during trading cycle pre-sync
- dedicated fill collection scheduler trigger not yet identified from current inventory snapshot

**TO-BE path**
- dedicated routes `/domestic-fill-collection/collect` and `/overseas-fill-collection/collect` exist and are still routed
- trade runbooks still call those services directly during open/intraday prep

**Judgment**
- in-trade fill sync: **active-same**
- dedicated collection APIs: **missing** truth linkage

## What is equal vs not equal from live perspective

### Equal enough to call live-equivalent now
- trading-hybrid endpoint contract at HTTP entry level
- bootstrap endpoint existence and downstream bootstrap service execution
- PM signal write path into `pm_signal_run_header` and `pm_candidate_decision_history`
- trade-cycle pre-sync use of fill collection services

### Not yet proven equal
- slot dispatcher as real scheduler-facing active path
- PM risk refresh parity as a dedicated live case
- dedicated fill collection API as a production-triggered active money path
- any shared/domain planning seam as a live replacement for current engine path

## Explicit non-goals for this comparison
- backtest runtime parity is excluded from live money-path classification
- shared/domain planning seam is not treated as active live path unless production truth points to it
- code presence alone is not sufficient to mark a path active
