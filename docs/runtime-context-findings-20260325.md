# Runtime Context Findings — 2026-03-25

## Local workspace shell
`python scripts/check_canary_env.py` => FAIL

Observed blocker in this shell:
- `KIS_CANO is missing or placeholder`

This means local shell execution is not an admissible broker-safe canary context.

## Repo evidence suggesting real runtime exists elsewhere
- `ops/state/trade_monitor_latest.json` says Cloud Run remains Ready=True on revision `auto-trader-bot-00076-2sp` with 100% traffic in project `curioustore`
- `ops/runtime_inventory_run_services_20260325.json` contains runtime service env inventory entries for:
  - `KIS_APPKEY`
  - `KIS_CANO`
  - `KIS_VIRTUAL`
  - `KIS_VIRTUAL_CANO`

## Implication
The productive next step is to run the canary in the actual runtime context that owns those env bindings, not from this placeholder local shell.
