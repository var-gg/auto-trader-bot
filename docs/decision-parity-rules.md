# Decision Parity Rules

Created: 2026-03-25
Scope: semantic parity for live decision generation without broker calls.

## Goal
For the same effective candidate input, TO-BE live planning must produce the same money-going meaning as AS-IS.

This does **not** require byte-identical output.
It **does** require that the decision remains semantically equivalent for execution.

## Current harness
- `tests/parity/test_decision_parity_live_replay.py`
- fixtures:
  - `tests/fixtures/decision_parity_us_open_buy_fixture.json`
  - `tests/fixtures/decision_parity_kr_open_sell_fixture.json`
  - `tests/fixtures/decision_parity_budget_skip_fixture.json`

The fixtures are replay-derived adapters, not raw broker fixtures.
They encode the decision meaning observed from AS-IS runtime replay and feed it into the TO-BE planning seam (`BuildOrderPlanCommand`).

## Compared fields
The harness compares the following semantic fields:
- symbol
- side
- requested quantity
- leg count
- quantity split by leg
- relative ladder ordering
- first-leg price gap band
- far-leg price gap band
- skip code (when skipped)
- required metadata keys (currently reverse_breach_day)
- policy/strategy version carried in fixture expectation

## Allowed diffs
These are acceptable if the money-going meaning is unchanged:
1. `plan_id` format differences
2. rationale text wording differences
3. exact metadata extras beyond the required subset
4. small non-material price rounding differences **within the allowed price gap band**
5. observability-only fields not used for order intent

## Disallowed diffs
These fail parity:
1. side changes (BUY vs SELL)
2. symbol/selected candidate changes
3. skip vs execute decision changes
4. leg count changes that alter execution shape materially
5. quantity split changes that alter exposure materially
6. ladder direction inversion
   - BUY ladder not below current price
   - SELL ladder not above current price
7. first-leg or far-leg price distances moving outside allowed semantic band
8. skip code changing to a different operational meaning
9. required decision metadata drifting without explicit acceptance

## Current fixture expectations
### US open buy replay
Expected meaning:
- BUY remains BUY
- 2 total quantity
- 2 ladder legs
- 1/1 split
- both legs below current price
- first leg close to current price (sub-1%)
- far leg materially wider (3%+ band)

### KR open sell replay
Expected meaning:
- SELL remains SELL
- 2 total quantity
- 2 ladder legs
- 1/1 split
- both legs above current price
- first leg around high-2% band
- far leg wider around 4%+ band

### Budget skip case
Expected meaning:
- remains skipped
- skip code stays `BUDGET`

## Output style on failure
Parity failures should be immediately readable and include:
- fixture name
- field-level problem list
- canonical actual view
- canonical expected view

## Non-goals
- no broker call
- no strategy parameter tuning
- no claiming equivalence because of refactor intent alone
- no mixing backtest parity with live parity proof in this harness
