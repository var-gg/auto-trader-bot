# Parity Test Policy

This step adds parity tests whose core purpose is:

> same canonical input -> live planning path and backtest planning path produce the same `OrderPlan`

Created:
- `tests/parity/*`
- `docs/parity-test-policy.md`

---

## What parity means here

Parity compares **planning outputs**, not execution outputs.

Compared fields:
- symbol
- side
- ladder legs
- quantity split
- key decision metadata
- policy version
- risk/skip reason

Not compared:
- live broker fill results vs simulated backtest fill results
- environment-driven timestamps
- provider-specific execution ids

This is intentional.
Execution and fill mechanics differ by runtime and should not be falsely treated as equivalent.

---

## Covered parity cases

## 1. PM buy/open planning parity
Fixture:
- `tests/fixtures/parity_pm_open_fixture.json`

Test:
- `test_pm_open_plan_parity`

Verifies:
- same symbol/side
- same requested quantity/budget
- same ladder leg count
- same per-leg quantity split
- same per-leg limit price
- same core decision metadata

## 2. Intraday-related planning parity
Fixture:
- `tests/fixtures/parity_intraday_fixture.json`

Test:
- `test_intraday_plan_parity`

Verifies:
- same canonical intraday plan shape for same signal-style input

## 3. Risk gate / skip / reject parity
Fixture:
- `tests/fixtures/parity_risk_skip_fixture.json`

Test:
- `test_risk_gate_skip_parity`

Verifies:
- same skip/reject outcome when budget/risk gate blocks planning
- drift shows up directly in structured skip payload diff

## 4. Outcome labeling parity
Test:
- `test_outcome_label_parity`

Verifies:
- WIN / LOSS / FLAT classification stays identical across planning surfaces

---

## Why drift is visible immediately

Parity assertions compare compact canonical views of `OrderPlan`.

When drift happens, the failure prints:
- LIVE view
- BACKTEST view

This makes it obvious which field changed:
- price ladder
- quantity split
- skip reason
- metadata

That is more useful than comparing opaque strings or large raw dumps.

---

## CI suitability

These tests are CI-friendly because:
- fixture-only input
- no external API calls
- no broker dependency
- no live DB writes
- deterministic canonical output comparisons

This means strategy edits can be checked for parity before and after changes.

---

## Important boundary

Do **not** interpret parity as “live and backtest execution must match”.

Parity only guarantees:
- same input facts
- same planning core
- same canonical planning output

It does **not** guarantee:
- same fill timing
- same gap/open behavior as broker
- same slippage or queue effects

Those belong to execution-model validation, not planning parity.

---

## Recommended CI usage

At minimum, parity suite should be run whenever changes touch:
- `shared/domain/*`
- `live_app/application/planning_commands.py`
- `backtest_app/runner/*`
- candidate normalization or ladder generation code

Suggested intent:
- fail fast on planning drift
- require explicit golden/parity update when a real strategy change is intended

---

## Current policy summary

- planning parity: required
- fill parity: not required
- fixture-only: required
- deterministic output diff: required
- runtime/env hidden normalization: forbidden
