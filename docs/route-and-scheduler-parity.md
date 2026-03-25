# Route and Scheduler Parity

Created: 2026-03-25
Reference branch: `public-release-20260323`
Baseline: production truth from `docs/runtime-replay-corpus.md`

## Goal
Verify that existing live triggers (scheduler / manual / webhook-style HTTP entrypoints) are still accepted by TO-BE at the route-contract layer.

This pass focuses on:
- route path
- HTTP method
- auth requirement
- required params / body shape
- response-code expectation at ingress layer
- internal command / service dispatch target

It intentionally does **not** treat backtest or shared-domain-only paths as live parity proof.

## Added contract tests
- `tests/contracts/test_live_routes_trading_and_bootstrap.py`
- `tests/contracts/test_live_routes_premarket.py`
- `tests/contracts/test_live_routes_fill_collection.py`

## Route parity table

| Area | AS-IS trigger | TO-BE route | Method | Auth | Contract parity | Internal dispatch |
|---|---|---|---|---|---|---|
| Bootstrap | Scheduler: `/kis-test/bootstrap` | `/kis-test/bootstrap` | `GET` | none | path/method preserved | `RunBootstrapCommand` -> `BootstrapService` |
| PM signals US | Scheduler: `/recommendations/batch-generate` | `/recommendations/batch-generate` | `POST` | none | path/method preserved | `UsBatchRecommendationService` |
| PM signals KR | Scheduler: `/recommendations/kr/batch-generate` | `/recommendations/kr/batch-generate` | `POST` | none | path/method preserved | `KrBatchRecommendationService` |
| PM signals direct update | manual/internal API | `/api/premarket/signals/update` | `GET`,`POST` | none | both forms still accepted | `UpdatePMSignalsCommand` |
| PM signals query | manual/internal API | `/api/premarket/signals` | `GET` | none | preserved | `GetPMSignalsQuery` |
| PM signal test | manual/internal API | `/api/premarket/signals/test` | `GET` | none | preserved | `TestPMSignalQuery` |
| PM risk refresh | internal scheduler/manual | `/api/premarket/risk/refresh` | `POST` | `X-Scheduler-Token` or `Authorization: Bearer` | explicit auth required | `RefreshRiskSnapshotCommand` |
| PM risk latest | internal/manual | `/api/premarket/risk/latest` | `GET` | same auth | explicit auth required | `GetLatestRiskSnapshotQuery` |
| PM history backfill | internal scheduler/manual | `/api/premarket/history/backfill-unfilled-reasons` | `POST` | same auth | preserved | `BackfillUnfilledReasonsCommand` |
| PM history outcomes | internal scheduler/manual | `/api/premarket/history/compute-outcomes` | `POST` | same auth | preserved | `ComputeOutcomesCommand` |
| PM history postprocess | internal scheduler/manual | `/api/premarket/history/postprocess` | `POST` | same auth | preserved | `RunHistoryPostprocessCommand` |
| Trading KR open | Scheduler: `/api/trading-hybrid/kr/open` | `/api/trading-hybrid/kr/open` | `POST` | none | path/method preserved | `RunTradingHybridCommand.run_open(market='KR')` |
| Trading US open | Scheduler: `/api/trading-hybrid/us/open` | `/api/trading-hybrid/us/open` | `POST` | none | path/method preserved | `RunTradingHybridCommand.run_open(market='US')` |
| Trading KR intraday | Scheduler: `/api/trading-hybrid/kr/intraday` | `/api/trading-hybrid/kr/intraday` | `POST` | none | path/method preserved | `RunTradingHybridCommand.run_intraday(market='KR')` |
| Trading US intraday | Scheduler: `/api/trading-hybrid/us/intraday` | `/api/trading-hybrid/us/intraday` | `POST` | none | path/method preserved | `RunTradingHybridCommand.run_intraday(market='US')` |
| Fill collection KR | manual/batch path not fully truth-anchored | `/domestic-fill-collection/collect` | `POST` | none | route active in code | `DomesticFillCollectionService.collect_domestic_fills` |
| Fill collection US | manual/batch path not fully truth-anchored | `/overseas-fill-collection/collect` | `POST` | none | route active in code | `OverseasFillCollectionService.collect_overseas_fills` |

## What the tests lock down

### Trading + bootstrap
The tests verify that:
- live paths still resolve at the same URL and method
- query parameter `test_mode` is still accepted for trading routes
- bootstrap still accepts query parameters like `skip_token_refresh` / `fred_lookback_days`
- requests dispatch into the expected command layer with route/slot metadata attached

### Premarket
The tests verify that:
- PM signal update accepts both `POST` JSON body and `GET` querystring form
- PM query/test endpoints still accept prior contract shapes
- PM risk and history routes enforce scheduler auth
- authenticated requests dispatch into the expected command/query classes

### Fill collection
The tests verify that:
- dedicated KR/US fill collection routes still exist
- `days_back` query param is accepted
- stats endpoints still return successfully at route level

## Changed-contract list
These are the contract-level items that changed or carry risk, even when routes still exist.

### 1) Bootstrap metadata contract drift
- public route is preserved
- but controller currently stamps fixed metadata (`slot=US_PREOPEN`, `strategy_version=pm-core-v2`)
- risk: KR/US scheduler calls can be mislabeled at observability layer
- classification: `changed-contract`

### 2) Trading response envelope is wrapped through `wrap_trading_result`
- path/method are preserved
- ingress now consistently passes through `live_app.api.responses.wrap_trading_result`
- risk: callers depending on exact legacy response wording may see envelope differences even if semantic success remains
- classification: `changed-contract` (response-shape sensitivity)

### 3) PM risk/history now require explicit internal auth
- route paths are present and testable
- auth contract is strict: missing token returns `401`, missing config returns `503`
- if any legacy caller lacked these headers, parity breaks at ingress
- classification: `changed-contract`

### 4) Command seam inserted for bootstrap/trading
- route contract preserved externally
- internal dispatch target changed from direct controller->service/engine to controller->command->service/engine
- acceptable if no new business branching is introduced at controller layer
- classification: `active-moved`, not a route break by itself

### 5) Fill collection remains route-active but scheduler parity is not yet proven
- dedicated routes exist and accept requests
- current production truth corpus has not yet tied scheduler jobs to these routes
- classification: `missing` at scheduler parity level, though route contract exists

## Verification status
### Verified now
- route modules import successfully
- contract test files added for bootstrap / premarket / trading-hybrid / fill collection
- route-to-command traceability documented

### Not fully executed in this environment
- full `pytest` run was not executed because `pytest` is not installed in the current default Python environment
- route-level smoke via direct FastAPI/TestClient import succeeded

## Bottom line
At the ingress layer, the important live routes still exist and are mostly parity-preserving.
The main remaining risks are not missing paths, but:
- metadata mislabeling,
- auth strictness on internal PM routes,
- response-envelope sensitivity,
- and missing scheduler truth for dedicated fill collection.
