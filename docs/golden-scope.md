# Golden Scope

This stage freezes representative current behavior before refactor.

Principles:
- preserve current behavior first
- no route changes
- no scheduler changes
- no logic fixes in this stage
- external APIs replaced with mock/fixture inputs
- golden outputs stored as readable JSON

---

## Covered representative flows

## 1. Bootstrap orchestration
- fixture: `tests/fixtures/bootstrap_request.json`
- golden: `tests/golden/bootstrap.golden.json`
- test: `tests/golden/test_runtime_goldens.py::test_bootstrap_golden`

Frozen shape:
- step order
- success/fail/skip counts
- per-step result summaries

Reason:
- protects orchestration sequencing before bootstrap refactor

---

## 2. PM signal update v1
- fixture: `tests/fixtures/pm_signal_fixture.json`
- golden: `tests/golden/pm_signal_v1.golden.json`
- test: `test_pm_signal_v1_golden`

Frozen shape:
- `signal_1d`
- best target identity and score
- reason code (`TOO_FEW` etc.)
- up/down candidate counts

Reason:
- protects current scoring kernel output on fixed synthetic ranked inputs

---

## 3. PM signal update v2
- fixture: `tests/fixtures/pm_signal_fixture.json`
- golden: `tests/golden/pm_signal_v2.golden.json`
- test: `test_pm_signal_v2_golden`

Frozen shape:
- same core output contract as v1

Reason:
- makes current v1/v2 coexistence explicit and comparable

---

## 4. PM risk refresh normalization
- fixture: `tests/fixtures/pm_risk_normalize_fixture.json`
- golden: `tests/golden/pm_risk_refresh.golden.json`
- test: `test_pm_risk_refresh_golden`

Frozen shape:
- normalized regime/risk fields
- buy/sell multiplier outputs
- ttl / reason string

Reason:
- protects policy mapping while external headline/LLM calls stay mocked out

---

## 5. Trading-hybrid KR/US open planning surrogate
- fixture: `tests/fixtures/pm_open_candidates.json`
- golden: `tests/golden/pm_open_plan.golden.json`
- test: `test_pm_open_plan_golden`

Frozen shape:
- selected symbols
- budget allocation map
- ladder leg counts
- per-leg quantities
- per-leg prices

Reason:
- locks key open-session planning behavior without real DB/broker calls

Note:
- this freezes the **planning kernel** rather than the full controller/runbook shell
- enough to detect changes in shared-domain extraction targets

---

## 6. Trading-hybrid intraday current behavior surrogate
- fixture: `tests/fixtures/intraday_preds.json`
- golden: `tests/golden/intraday_plan.golden.json`
- test: `test_intraday_golden_preserves_current_error`

Frozen shape:
- current raised error type/message fragment under fixture input

Reason:
- this stage explicitly preserves current behavior, including odd behavior
- observed current behavior: `plan_intraday_actions(...)` raises `NameError` referencing `db`
- this is intentionally **not fixed here**; it is captured as current state

This satisfies the “do not repair first, preserve first” rule.

---

## 7. Fill collection / sync
- fixture: `tests/fixtures/fill_collection_fixture.json`
- golden: `tests/golden/fill_collection_sync.golden.json`
- test: `test_fill_collection_sync_golden`

Frozen shape:
- call order
- pnl save count
- snapshot id
- fill processed/upserted counts

Reason:
- protects live pre-trade sync orchestration without live provider calls

---

## 8. Order write intent / live money path shape
- fixture: `tests/fixtures/golden_write_intent_fixture.json`
- golden: `tests/golden/write_intent.golden.json`
- test: `test_write_intent_golden`

Frozen shape:
- BUY/SELL batch creation order
- plan note shape
- leg count per plan
- total submitted leg count
- representative reject reason normalization

Reason:
- this is the most important surrogate for the live money path boundary
- it freezes DB write intent shape and submission fan-out without real broker traffic

---

## Not yet fully covered in this stage

The following are only partially covered or covered via planning surrogates:
- full `trading-hybrid KR open` controller/runbook shell
- full `trading-hybrid US open` controller/runbook shell
- full `trading-hybrid KR intraday` controller/runbook shell
- full `trading-hybrid US intraday` controller/runbook shell
- full PM history postprocess batch

Why not fully yet:
- those shells are highly coupled to DB/account/fill state
- this step prioritizes freezing core decision outputs and write intent shape first
- next steps can add broader frozen orchestration cases once adapter seams are introduced

---

## Readability conventions

Goldens are stored as JSON and intentionally keep only human-reviewable keys:
- no volatile timestamps unless necessary
- no large raw payload dumps
- no unreadable binary snapshots
- emphasis on decision shape, leg shape, notes/reasons, and call order

This makes diff review practical during refactor.

---

## Acceptance mapping

### Same input -> same core result
Covered by all golden JSON comparisons.

### Diff readable by humans
Covered by JSON-only goldens with compact decision-oriented fields.

### Live money path representative cases frozen
Covered at minimum by:
- bootstrap orchestration
- PM signal v1/v2
- PM risk policy normalization
- open planning ladder/budget output
- fill sync order
- order write intent / reject reason normalization
- current intraday failure mode preservation
