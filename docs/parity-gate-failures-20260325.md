# Parity Gate Failures — 2026-03-25

## Execution context
Validated with repo venv after installing `pytest` into `venv`.

## Representative bundle results
### 1. Decision parity
Command:
- `venv\\Scripts\\python.exe -m pytest tests\\parity\\test_decision_parity_live_replay.py -q`

Result:
- 2 failed, 1 passed

Observed failures:
- `decision_parity_kr_open_sell_fixture.json`
  - `last_leg_pct` expected range `[0.04, 0.07]`
  - actual `0.07007575757575757`
- another failure in the same file (`test_us_open_buy_decision_parity`) also failed in this run and should be inspected from the test output/log detail

Interpretation:
- there is real decision-semantic drift or an overly strict tolerance band
- this is a true gate issue until explained or fixed

### 2. DB side-effect parity
Command:
- `venv\\Scripts\\python.exe -m pytest tests\\parity\\test_db_side_effect_parity_core.py -q`

Result:
- 1 failed, 4 passed

Observed failure:
- `NameError: name 'params' is not defined`
- location: test fixture/responder lambda in `tests/parity/test_db_side_effect_parity_core.py`

Interpretation:
- this is a test bug, not product evidence
- fix test harness first, then rerun

### 3. Route contract tests
Command:
- `venv\\Scripts\\python.exe -m pytest tests\\contracts\\test_live_routes_trading_and_bootstrap.py tests\\contracts\\test_live_routes_premarket.py tests\\contracts\\test_live_routes_fill_collection.py -q`

Result:
- 2 failed, 3 passed

Observed failures:
- `AttributeError` in `tests/contracts/test_live_routes_premarket.py`
- expected monkeypatch target missing:
  - `app.features.premarket.controllers.pm_signal_controller.UpdatePMSignalsCommand`

Interpretation:
- either the refactor changed the controller dispatch surface
- or the contract test still points to an AS-IS symbol that no longer exists
- this is a real ingress/dispatch parity blocker until reconciled

### 4. Shadow replay E2E
Command:
- `venv\\Scripts\\python.exe -m pytest tests\\e2e\\test_shadow_replay_scheduler.py -q`

Result:
- 2 failed

Observed failures:
- async tests are not supported in current pytest setup
- missing async plugin (`pytest-asyncio` or equivalent)

Interpretation:
- this is a test-environment gap, not yet product evidence
- install/configure async pytest plugin, then rerun

## Current blocker classification
### Real gate blockers
- decision parity drift
- premarket controller dispatch contract mismatch

### Test harness blockers
- DB parity test NameError in responder lambda
- missing async pytest plugin for shadow replay tests

## Immediate next action bundle
1. inspect and fix / explain decision parity drift
2. reconcile `pm_signal_controller` contract surface vs contract tests
3. fix DB parity test bug (`params` NameError)
4. install/configure async pytest plugin and rerun shadow replay tests
