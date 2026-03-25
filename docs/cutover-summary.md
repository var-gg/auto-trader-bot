# Cutover Summary

Created: 2026-03-25
Status: not approved for full cutover yet

## Objective
Move live auto-trading from AS-IS to TO-BE without changing money-going meaning.

## What is already prepared
1. Route / scheduler ingress parity documented
2. Decision parity harness added
3. Broker outbound / external adapter parity tests added
4. DB write / next-read parity documented
5. Shadow E2E scheduler replay harness added
6. Small-canary / cutover-gate / rollback rules documented

## Current recommendation
Do **not** do full cutover.
Use a **parity-first, minimal-canary** approach rather than broad runtime E2E.
Run the following next:
1. finish parity / contract / DB side-effect confidence on representative anchors
2. run runtime preflight in the real TO-BE runtime context
3. run one broker-safe canary on **US open** only
4. produce side-by-side AS-IS vs TO-BE canary report
5. if clean, run smallest-money canary on the same market/slot
6. only then consider widening

## What this does *not* mean
- it does **not** require end-to-end runtime testing of every slot / market / scheduler path before deployment
- it does **not** treat every runtime error as a regression
- it does **not** justify repeated local retries from an invalid shell/runtime context

The standard is:
- preserve AS-IS money-going meaning,
- prove that with parity evidence,
- then confirm with a small number of representative live canaries.

## Why US open first
- replay anchor exists
- shadow replay already covers it
- core live money path is exercised
- scope can stay narrow

## Blocking risks still acknowledged
- fill collection scheduler endpoint truth is still incomplete
- full deployed shadow environment is not yet proven
- broker idempotency contract is still weaker than ideal
- PM run linkage inference should be treated carefully

## Full cutover is allowed only when
- cutover gates in `docs/cutover-gates.md` are all green
- rollback path in `docs/rollback-runbook.md` is operator-ready
- broker-safe canary and smallest-money canary both pass
- no concurrent strategy/Optuna/config experiments are running
