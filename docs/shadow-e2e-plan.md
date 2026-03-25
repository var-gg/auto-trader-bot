# Shadow E2E Plan

Created: 2026-03-25
Branch: `public-release-20260323`
Goal: replay representative live scheduler flow end-to-end without sending real broker orders.

## Scope
Shadow flow follows live operational sequence as closely as possible while keeping money movement disabled.

Included stages:
1. bootstrap
2. KR open trading-hybrid
3. US open trading-hybrid
4. US intraday trading-hybrid
5. PM history postprocess
6. fill/reconcile side-effect sync inside trading runbooks

Excluded on purpose:
- production DB reuse
- real broker submit
- strategy tuning
- scheduler config rewrites

## Shadow environment rules
- isolated / shadow DB only
- live market-data and external read adapters may remain real when safe
- broker submit path must be stub / paper / no-op
- same public route / command ingress meaning should be preserved
- no silent fallback to production credentials

## Replay basis
AS-IS anchors used from replay corpus:
- `bootstrap_20260324_1600kst.json`
- `kr_open_20260324.json`
- `us_open_20260324.json`
- `us_intraday_risk_cut_20260325_0010kst.json`
- `fill_snapshot_20260325.json`

## Test implementation
- `tests/e2e/test_shadow_replay_scheduler.py`

The test harness drives:
- `RunBootstrapCommand`
- `RunTradingHybridCommand.run_open(...)`
- `RunTradingHybridCommand.run_intraday(...)`
- `RunHistoryPostprocessCommand`

It monkeypatches:
- bootstrap service
- market-open checks
- profit/account/fill sync
- trading engine
- PM history batch service
- structured logging

This preserves orchestration order while preventing real external side effects.

## What is compared
### Run sequence
Expected shadow order:
- bootstrap
- KR sync/open
- US sync/open
- US sync/intraday
- history postprocess

### Decision meaning
Compare to AS-IS replay anchors at semantic level:
- generated BUY/SELL presence by slot
- skip/reject reasons
- ladder direction and quantity split
- broker intent correlation ids present

### Broker intent
Shadow broker identifiers must be paper/no-op only:
- `PAPER-*` style ids in test harness
- no real order numbers

### DB side-effect meaning
Shadow run should still expose the same meaning at orchestration level:
- order batch / plan correlation ids
- PM postprocess summaries
- fill sync stage occurrence before trading engine execution

## Current result status
This pass creates a deterministic shadow replay test harness rather than a deployed long-lived staging service.
That is sufficient to prove that the live money path can be executed end-to-end in one process with broker writes disabled.

## Gaps still open
1. This is in-process shadow replay, not yet a separately deployed shadow service.
2. Live market data can be kept real in a future pass, but this test currently stubs the trading engine for deterministic scheduler replay comparison.
3. Dedicated fill collection scheduler route is still not proven from production scheduler truth; current shadow coverage includes fill/reconcile via runbook sync stage.

## Exit criteria for cutover confidence
- representative live slots can replay in order without real broker side effects
- replay summary matches AS-IS meaning at slot level
- no stage drops required forensic correlation ids
- history postprocess still runs after trading sequence
