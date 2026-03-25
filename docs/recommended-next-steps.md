# Recommended Next Steps

Created: 2026-03-25

## Immediate next action
Run runtime preflight with `python scripts/check_canary_env.py`.
Only if that passes, run **Phase 0 broker-safe canary** for **US open only** and write the result using `docs/canary-report-template.md`.

## Ordered work
1. Execute broker-safe US open canary
2. Fill `docs/canary-report-template.md`
3. Review against `docs/cutover-gates.md`
4. If all green, schedule smallest-money US open canary
5. Do not widen to KR or intraday until US open canary is clean

## Explicit holds
- hold Optuna / backtest / strategy tuning
- hold scheduler topology changes
- hold broad config cleanup
- hold full cutover

## Decision rule
If there is any uncertainty whether the canary passed, treat it as **not passed** and do not widen.
