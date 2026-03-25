# Live Path Gap List

Created: 2026-03-25
Reference branch: `public-release-20260323`

## Purpose
List every currently known live-path gap between AS-IS production truth and TO-BE classification.

## Gaps

### 1) PM risk refresh is not fully truth-anchored
- **Status:** `missing`
- **Why:** code path exists (`pm_risk_controller`, bootstrap step), but replay corpus does not yet isolate a dedicated AS-IS runtime case with endpoint/request/DB side effects.
- **Impact:** cannot yet claim live-equivalent PM risk behavior across refactor boundary.
- **Needed evidence:** dedicated request log + DB side-effect snapshot for one or more PM risk refresh runs.

### 2) Fill collection scheduler/trigger path is not fully identified
- **Status:** `missing`
- **Why:** `order_fill` DB truth exists, and in-runbook fill sync is confirmed, but dedicated scheduler/request evidence for `/domestic-fill-collection/collect` or `/overseas-fill-collection/collect` is not yet tied from production truth.
- **Impact:** dedicated fill collection cannot yet be marked active or equal.
- **Needed evidence:** Cloud Scheduler job or Cloud Run request log explicitly hitting dedicated fill endpoints, plus linked `order_fill` side effects.

### 3) Slot dispatcher is not yet an active scheduler-facing production path
- **Status:** `deprecated for active validation` / future target
- **Why:** `ScheduleSlotDispatcher` exists and wires bootstrap/trading/history commands, but production Scheduler truth still targets public HTTP endpoints directly.
- **Impact:** do not use slot dispatcher as proof that live cutover already happened.
- **Needed evidence:** scheduler inventory or controller route proving slots are the real runtime ingress.

### 4) Legacy controllers remain active alongside new application seams
- **Status:** `changed topology`
- **Why:** public endpoints still live in old app router while delegating into `live_app.application.*` commands.
- **Impact:** migration is partial; active path is hybrid, not fully centralized.
- **Needed action:** document each endpoint as controller->command->legacy service/engine chain until old controller surface is retired.

### 5) Shared/domain planning seam is not a live money path yet
- **Status:** `deprecated for active validation`
- **Why:** `BuildOrderPlanCommand` exists for parity/testing, but production truth does not show it as scheduler-facing or execution-owning.
- **Impact:** cannot claim planning refactor equals live parity by itself.
- **Needed evidence:** active live endpoint/command path delegating to shared/domain for real trading decisions.

### 6) Bootstrap slot metadata is still partially hardcoded
- **Status:** `changed-contract risk`
- **Why:** bootstrap controller currently stamps metadata with fixed `slot=US_PREOPEN` and `strategy_version=pm-core-v2` regardless of actual scheduler source.
- **Impact:** observability can mislabel KR/US preopen runs.
- **Needed action:** derive slot/strategy metadata from real route/job context.

### 7) Structured logging improved, but full external-correlation parity is not finished
- **Status:** `changed-contract risk`
- **Why:** current TO-BE path now emits local correlation ids for trading, but broker external request/response ids are still approximated by local fallback keys in many cases.
- **Impact:** production incident traceability is improved but not fully external-system-native.
- **Needed action:** capture broker-native request/response identifiers where available.

## Cases already explainable end-to-end
These are not gaps for basic path mapping anymore:
- `/kis-test/bootstrap`
- `/recommendations/batch-generate`
- `/recommendations/kr/batch-generate`
- `/api/trading-hybrid/kr/open`
- `/api/trading-hybrid/us/open`
- `/api/trading-hybrid/kr/intraday`
- `/api/trading-hybrid/us/intraday`

## Review rule
Do not mark any gap resolved until there is:
1. production truth anchor (Scheduler or request log),
2. TO-BE code path confirmation, and
3. DB/external side-effect link via correlation key.
