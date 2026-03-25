# Shadow Run Result Report

Created: 2026-03-25
Mode: deterministic in-process shadow replay
Broker mode: no-op / paper ids only
DB mode: test-double only (no production DB sharing)

## Covered replay sequence
1. bootstrap
2. KR open
3. US open
4. US intraday
5. PM history postprocess

## Result summary
### bootstrap
- status: PASS
- meaning: bootstrap orchestration completes and emits structured run log

### KR open
- status: PASS
- meaning: shadow run generates SELL-side intent and correlation ids
- broker intent: paper-only (`PAPER-*`)

### US open
- status: PASS
- meaning: shadow run generates BUY-side intent with two-leg ladder and 1/1 split
- broker intent: paper-only (`PAPER-*`)

### US intraday
- status: PASS
- meaning: shadow run generates risk-cut SELL intent and skip reason `RISK`
- broker intent: paper-only (`PAPER-*`)

### PM history postprocess
- status: PASS
- meaning: unfilled reason backfill + outcome computation can still run after trading stages

## Sequence verification
Observed shadow sequence:
- bootstrap.log
- sync.KR
- engine.open.KR
- trading.log
- sync.US
- engine.open.US
- trading.log
- sync.US
- engine.intraday.US
- trading.log
- history.postprocess

This is consistent with the intended live scheduler progression, modulo the absence of separate dedicated fill-collection route replay.

## Semantic comparison vs AS-IS anchors
- KR open retains sell-side execution intent
- US open retains buy-side execution intent
- US intraday retains sell/risk reaction intent
- correlation ids (`order_batch_ids`, `order_plan_ids`, broker request/response ids) remain present
- paper broker ids prevent live money movement while preserving outbound intent meaning

## What this proves
- representative live cases can be replayed end-to-end in shadow mode
- command/runbook orchestration is traversable without real broker connectivity
- bootstrap -> trading -> postprocess chain is intact
- core live money path can complete with broker disabled

## What this does not prove yet
- full separate deployed shadow environment parity
- full live market-data response parity during replay
- dedicated fill-collection scheduler endpoint replay parity
- parity against every production slot/job variant

## Overall assessment
Shadow E2E is good enough to support a controlled next step.
The remaining risk is not basic orchestration survivability, but expanding shadow coverage to more slots and binding it to a true isolated staging runtime when needed.
