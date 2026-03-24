# Legacy / Deprecated Candidates

Scope: identify code that is likely legacy, transitional, or non-authoritative for the target architecture.

This is **not** a deletion proposal.
It is a map for:
- migration planning
- parity test scope
- protecting live paths while refactoring

---

## 1. Highest-confidence legacy candidates

## 1.1 Old backtest API

### Files
- `app/features/signals/controllers/backtest_controller.py`
- `app/features/signals/services/backtest_service.py`

### Why marked legacy/deprecated
- explicit older `vec40` terminology
- separate signals-era backtest path, not PM best-signal architecture
- no evidence this is the current production PM pipeline
- useful as backtest material, but not authoritative for live PM

### Keep for now because
- it is the clearest surviving backtest-oriented codepath
- it may provide fixtures, evaluation ideas, and replay mechanics for `backtest_app`

### Migration note
Move conceptually to `backtest_app/legacy_vec40` until replaced by PM-parity backtests.

---

## 1.2 Old signal-detection lineage

### Files
- `app/features/signals/services/signal_detection_service.py`
- related `signals` models/repositories/utils under `app/features/signals/*`

### Why marked legacy/deprecated
- older versioned trend detection / shape-vector flow
- optional on-demand ingestion inside signal detection service
- tied to similarity/vec40 ecosystem rather than PM `optuna_target_vectors` / `pm_best_signal`
- current live PM path no longer centers this service

### Still relevant because
- historical clue for original “meaningful rise window” detection logic
- may still feed legacy hybrid intraday paths
- provides extraction material for future backtest tooling

### Refactor caution
Do not assume it is dead. Some hybrid services still depend on `signals` lineage.

---

## 1.3 Legacy hybrid open-session planner

### File
- `app/features/trading_hybrid/services/open_session_service.py`

### Why marked legacy/deprecated
- current `HybridTraderEngine.run_open_greedy()` imports PM open planners from `premarket/services/pm_open_session_service.py`
- file still contains older active-set / adaptive ladder / intensity logic built around legacy signal stack
- name suggests it used to be the authoritative open-session planner but is no longer the selected runtime path

### What to preserve
- pure helpers are still valuable:
  - `_required_discount`
  - `_compute_sell_intensity`
  - `_rescale_sell_legs_by_intensity`
  - `allocate_symbol_budgets`
- this file is legacy as a runtime module, not necessarily as a math source

### Migration note
- extract pure helper logic to shared domain
- then demote remaining runtime shell to `legacy/`

---

## 1.4 Legacy similarity-analysis / vec40 artifacts

### Likely files / areas
- `app/features/signals/models/similarity_analysis.py`
- similarity repositories / vec40 repository / similarity docs
- old alembic revisions with `add_simil*`

### Why marked legacy/deprecated
- naming indicates older similarity-analysis era
- PM architecture now uses `optuna_target_vectors`, `pm_best_signal`, `target_vecidx_cfg_*`
- old similarity tables likely predate current PM library design

### Keep for now because
- schema history helps identify how old backtest system evolved
- may contain conversion hints for parity datasets

---

## 2. Transitional, not fully legacy but mixed-state candidates

## 2.1 PM signal v1 route path

### Files
- `app/features/premarket/services/pm_signal_service.py`
- `app/features/premarket/services/pm_signal_service_v2.py`
- `app/features/premarket/controllers/pm_signal_controller.py`
- `app/features/kis_test/services/bootstrap_service.py`

### Mixed-state observation
- public PM route still points to **v1** (`PMSignalService`)
- bootstrap path internally points to **v2** (`PMSignalServiceV2.update_signals_v2()`)

### Why this is transitional
- v2 clearly documents improvements over v1
- but controller default has not been moved
- therefore v1 is not safe to call “dead”, and v2 is not safe to call “fully authoritative” yet

### Status label
- `transitional / mixed-state`, not deletion candidate yet

### Required before deprecation
- route-default decision
- parity tests between v1 and v2 on frozen inputs
- runtime inventory: who calls which path in production

---

## 2.2 Hybrid intraday: PM candidate pool + legacy 5-minute predictor

### Files
- `app/features/trading_hybrid/engines/hybrid_trader_engine.py`
- `app/features/premarket/services/pm_intraday_session_service.py`
- `app/features/trading_hybrid/repositories/intraday_signal_repository.py`
- `app/features/trading_hybrid/services/intraday_session_service.py`
- `app/features/signals/...` prediction lineage

### Why transitional
Current intraday path is hybrid in the literal sense:
- PM signal picks the active universe
- older 5-minute prediction stack still drives intraday price timing / action logic

This is not cleanly legacy or modern. It is a mixed bridge state.

### Status label
- `transitional / mixed-state`

### Refactor implication
Keep it explicitly labeled in docs/tests so refactor does not accidentally flatten two distinct decision layers into one.

---

## 2.3 Risk controller utility pile

### File
- `app/features/trading_hybrid/services/risk_controller.py`

### Why transitional
- contains real runtime order-management side effects
- also contains reusable rule predicates / time policy helpers
- probably grew organically as a “safety toolbox” rather than a clean layer

### Status label
- `mixed-state utility pile`

### What may become legacy later
- once stop/cleanup logic is split into domain rules + execution adapters, this file may shrink drastically or disappear as a monolith

---

## 3. Live-only but old-looking - do NOT treat as legacy yet

These may look old or dense, but they are still clearly on the active runtime path.

### Keep as active
- `app/features/kis_test/services/bootstrap_service.py`
- `app/features/premarket/services/pm_open_session_service.py`
- `app/features/premarket/services/pm_active_set_service.py`
- `app/features/trading_hybrid/engines/hybrid_trader_engine.py`
- `app/features/trading_hybrid/engines/runbooks.py`
- `app/features/trading_hybrid/services/executor_service.py`
- `app/features/trading_hybrid/repositories/order_repository.py`

### Why not legacy
They participate directly in:
- bootstrap
- active PM order planning
- live intraday cycles
- broker submission
- fill / account state sync

Even if ugly or mixed, they are active.

---

## 4. Legacy candidate table

| File / area | Proposed label | Confidence | Reason |
|---|---|---:|---|
| `signals/controllers/backtest_controller.py` | backtest-only legacy | High | vec40-era backtest endpoint |
| `signals/services/backtest_service.py` | backtest-only legacy | High | old backtest service not tied to PM runtime |
| `signals/services/signal_detection_service.py` | legacy lineage / source material | High | older trend-detection architecture |
| vec40 / similarity-analysis stack | legacy lineage | High | superseded conceptually by PM optuna target vectors |
| `trading_hybrid/services/open_session_service.py` | legacy runtime shell, shared-helper source | High | not current open runtime path |
| `pm_signal_service.py` v1 | transitional | Medium | route-default still active |
| `pm_signal_service_v2.py` | transitional successor | Medium | bootstrap uses it, route doesn’t |
| hybrid intraday predictor stack | transitional | Medium | PM + legacy mixed runtime |
| `risk_controller.py` monolith | mixed-state utility pile | Medium | refactor target, not deprecation target |

---

## 5. Suggested treatment rules during refactor

## Rule 1
Do not delete anything marked legacy before parity fixtures exist.

## Rule 2
When extracting shared/domain math, prefer harvesting from legacy files first if:
- helper is pure
- helper is battle-tested
- helper still matches current runtime semantics

## Rule 3
Mark transitional zones explicitly in code comments/tests:
- PM signal v1 vs v2
- PM active-set vs legacy intraday predictor

## Rule 4
Treat old backtest stack as a **fixture source**, not as the future architecture.

---

## 6. What parity_tests should cover before deprecation

### Before deprecating old open-session runtime pieces
- same candidate set -> same budget allocation
- same market/tuning -> same ladder output
- same position/gain state -> same sell-intensity scaling

### Before deprecating PM signal v1
- frozen OHLCV/config/candidate vectors -> same or intentionally changed score outputs under v2
- route behavior inventory completed

### Before deprecating vec40 backtest path
- new PM-parity backtest can reproduce at least:
  - signal count accounting
  - horizon outcome labeling
  - aggregate return metrics on fixed fixtures

---

## 7. Final practical reading

### Safe to call legacy now
- `signals/backtest_controller.py`
- `signals/backtest_service.py`
- most vec40/similarity-analysis stack
- `trading_hybrid/services/open_session_service.py` as authoritative runtime path

### Safe to call transitional now
- `pm_signal_service.py` vs `pm_signal_service_v2.py`
- hybrid intraday PM+5min blend

### Definitely active, not legacy
- bootstrap
- PM active set / PM open planner
- runbooks / engine
- executor / order repository / fill sync
