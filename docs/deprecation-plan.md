# Deprecation Plan

This document classifies runtime paths as:
- active
- deprecated
- delete-candidate

Goal:
- avoid multiple active execution paths
- make migration status explicit
- prevent silent legacy branching from persisting forever

Primary source of truth:
- `live_app/config/schedule_manifest.json`

---

## Active

### PM signal generation
Active path:
- `PMSignalServiceV2`
- manifest label: `pm_signal_update -> v2`

Reason:
- current bootstrap and planning direction are aligned toward v2
- parity/pure-domain work is being anchored around the new command seam

### Bootstrap orchestration
Active path:
- `BootstrapService`
- ingress through `/kis-test/bootstrap`
- internal ownership through `live_app.application.bootstrap_commands.RunBootstrapCommand`

### Open trading path
Active path:
- `trading_hybrid` open runbook via `live_app.application.trading_commands.RunTradingHybridCommand.run_open`

### Intraday trading path
Active path:
- `trading_hybrid` intraday runbook via `live_app.application.trading_commands.RunTradingHybridCommand.run_intraday`

### PM history postprocess
Active path:
- `PMHistoryBatchService.run_postprocess`
- command seam through `live_app.application.history_commands.RunHistoryPostprocessCommand`

---

## Deprecated

### PM signal v1 update path
Deprecated:
- old v1 update logic still reachable through legacy service wiring

Policy:
- no new behavior should be added there
- any parity-critical work should target the command seam and v2-aligned domain path

### Direct controller orchestration
Deprecated:
- controllers directly owning business sequencing

Policy:
- controllers may remain as ingress, but should not grow orchestration branches

---

## Delete-candidate

### Dedicated `/api/premarket/signals/update/v2` ingress
Delete-candidate once default path is fully switched and validated.

Why:
- keeping both default and explicit v2 ingress long-term creates dual active readings
- the desired end state is a single active PM signal update path

### Legacy inline planning code that bypasses command seams
Delete-candidate as service internals migrate toward:
- `shared/domain`
- `live_app.application` commands
- explicit adapters

---

## Transition rules

1. Only one path should be marked active per business capability.
2. Deprecated paths can remain temporarily, but must not receive new strategy branching.
3. Delete-candidate paths should have a clear trigger for removal:
   - parity stable
   - manifest active path confirmed
   - ingress usage switched

---

## Current single-reading summary

If someone asks “what is the active operating path now?” the answer should be:
- PM signal update: v2
- preopen orchestration: bootstrap command path
- open trading: trading_hybrid command path
- intraday trading: trading_hybrid command path
- history cleanup: PM history postprocess command path

That is the intended single active reading for this stage.
