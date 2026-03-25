# Transition Next Actions Bundle

Created: 2026-03-25
Goal: continue the cutover toward the refactored runtime without wasting cycles on invalid local canary attempts.

## What is done already
- route / scheduler / DB write-read / external I/O / decision parity docs prepared
- US open broker-safe canary attempted
- crash blockers reduced:
  - earnings schema mismatch fixed
  - PM sell formatting bug fixed
  - invalid OVRS snapshot now fails explicitly instead of pretending success
- runtime env preflight added: `scripts/check_canary_env.py`

## Current truth
Local workspace shell is **not** a valid canary execution context.
Reason:
- `python scripts/check_canary_env.py` fails here
- last observed local blocker: `KIS_CANO is missing or placeholder`

## Therefore
The next productive work is **not** another local canary retry.
The next productive work is to move to the real TO-BE runtime context and perform the same gate there.

## Next action bundle
1. In the real TO-BE runtime context, run:
   - `python scripts/check_canary_env.py`
2. If preflight passes, run:
   - US open only broker-safe canary (`/api/trading-hybrid/us/open?test_mode=true`)
3. Fill:
   - `docs/canary-report-template.md`
4. Compare against:
   - `tests/replay_fixtures/us_open_20260324.json`
5. Gate result:
   - pass -> schedule smallest-money canary
   - fail -> stop and record blocker

## Hard rule
Do not widen to KR or intraday until the above US open report is clean.

## Why this is the correct next step
Any further local retry from the current shell is low-value because runtime env is invalid here. That would create more noise, not more certainty.
