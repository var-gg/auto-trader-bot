# Domain Canonical Model

This step defines the canonical language for `shared/domain`.

Goal:
- represent trading decisions and execution outcomes without DB / HTTP / broker / Cloud Run / Optuna / pandas coupling
- allow both live executor and backtest simulator to consume the same `OrderPlan`
- make serialization stable for fixtures, golden tests, replay, and parity tests

Implemented at:
- `shared/domain/models/base.py`
- `shared/domain/models/enums.py`
- `shared/domain/models/market.py`
- `shared/domain/models/signal.py`
- `shared/domain/models/execution.py`

---

## Design rules

## 1. No runtime/vendor payload coupling
Forbidden in domain models:
- SQLAlchemy ORM models
- FastAPI request/response models
- KIS response payload fields as canonical structure
- Cloud Run env/session objects
- pandas DataFrame / numpy ndarray dependency
- DB session / transaction / HTTP request objects

Allowed:
- Python primitives
- `dataclass`
- `Enum`
- `datetime/date`
- `dict/list` with JSON-serializable contents

## 2. Domain types are transport-neutral
A domain object should be valid whether it comes from:
- live runtime
- backtest replay
- paper trading
- fixture replay
- parity test harness

## 3. Canonical language, not storage schema
These models are not ORM tables.
They are the common semantic contract between systems.

---

## Type overview

## `MarketSnapshot`
Represents the market state used by decision logic.

Examples of information it may contain:
- market open/closed
- reference price / last close
- volatility snapshot
- macro/news regime summary

Why needed:
- lets planners consume market state without knowing where it came from
- live source may be DB/API
- backtest source may be replayed historical tape

## `SignalCandidate`
Represents a ranked opportunity or candidate produced by any signal engine.

Examples:
- PM best-signal candidate
- intraday predictor output normalized into canonical form
- backtest-generated candidate from historical scan

Why needed:
- different signal systems can produce a shared candidate object
- planner should not care whether source was PM v1, PM v2, vec40, or intraday model

## `StrategyContext`
Represents strategy-level context for a decision.

Examples:
- strategy id/version
- venue label (`live`, `backtest`, `paper`)
- account equity / buying power snapshot
- policy parameters used in that run

Why needed:
- planners need operating context, but not DB/session objects

## `RiskPolicy`
Represents risk constraints and risk bias.

Examples:
- discount multiplier
- sell markup multiplier
- blocked symbols
- max symbol weight
- stop/time-stop policy parameters

Why needed:
- risk state should be passed in as data, not hidden in services/env

## `ExecutionIntent`
Represents the high-level intent before an order plan exists.

Examples:
- “buy NVDA with high priority”
- “reduce AAPL due to maturity”
- “skip new buys because risk-off regime”

Why needed:
- useful intermediate step for planners and parity tests
- allows separation between candidate scoring and full plan generation

## `LadderLeg`
Represents a single execution leg.

Examples:
- limit buy 13 @ 118.78
- limit sell 4 @ 214.20

Why needed:
- both live executor and backtest simulator can consume the same leg model

## `OrderPlan`
Represents the full actionable order decision.

Contains:
- side
- rationale
- requested budget/quantity
- ladder legs
- risk notes
- metadata

Why needed:
- this is the core shared contract
- live executor can persist and submit it
- backtest simulator can replay and fill-simulate it

## `FillOutcome`
Represents what happened after execution attempt.

Examples:
- fully filled
- partially filled
- rejected with normalized reject reason
- cancelled

Why needed:
- live path and backtest path need a common result shape
- backtest can synthesize fills with same schema used by live results

## `OutcomeRecord`
Represents post-trade evaluation outcome.

Examples:
- T+1 / T+3 / T+5 label
- pnl_bps
- exit price

Why needed:
- unify live postprocess and backtest evaluation outputs

---

## Why these models pass the constraints

## No HTTP/DB/broker details
The models do **not** contain:
- request objects
- response objects
- session handles
- SQLAlchemy columns
- broker-specific nested payloads

## No vendor payload reuse
For example:
- `FillOutcome.reject_code` / `reject_message` are normalized fields
- not a raw KIS payload dump
- raw provider data can still be stored in `metadata` if needed, but that is optional and non-canonical

## No env/time/session mixing
- decision timestamps are explicit fields (`generated_at`, `decision_time`, `as_of`)
- no hidden “now” lookup inside the model layer
- no env or DB-driven defaults

## Serializable
All domain objects inherit `DomainModel`, which provides:
- `to_dict()`
- `from_dict()`

Supported conversions:
- `Enum` <-> string value
- `datetime/date` <-> ISO string
- nested dataclasses
- lists/dicts of JSON-compatible values

This makes them suitable for:
- fixtures
- golden tests
- snapshotting
- event logs
- replay

---

## Example: same `OrderPlan` for live and backtest

```python
from datetime import datetime
from shared.domain.models import OrderPlan, LadderLeg, Side, OrderType, ExecutionVenue

plan = OrderPlan(
    plan_id="pm-us-20260324-nvda-buy",
    symbol="NVDA",
    ticker_id=101,
    side=Side.BUY,
    generated_at=datetime.fromisoformat("2026-03-24T09:00:00+09:00"),
    status="READY",
    rationale="PM signal strong; risk-adjusted ladder approved",
    venue=ExecutionVenue.LIVE,
    requested_budget=2100.0,
    requested_quantity=17,
    legs=[
        LadderLeg(
            leg_id="leg-1",
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            quantity=13,
            limit_price=118.78,
            rationale="near leg"
        ),
        LadderLeg(
            leg_id="leg-2",
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            quantity=4,
            limit_price=115.17,
            rationale="deep leg"
        ),
    ],
    risk_notes=["riskM=1.20", "earnings_day=false"],
)
```

### Live executor can do
- persist batch/order_plan/order_leg
- submit each leg to broker
- convert broker responses into `FillOutcome`

### Backtest simulator can do
- replay each leg against historical bars
- simulate fills/slippage
- emit the same `FillOutcome` schema

This is the key proof that the canonical model works.

---

## Proposed mapping direction

## live_app -> domain
- DB rows / PM signal rows -> `SignalCandidate`
- account snapshot -> `StrategyContext`
- headline risk snapshot -> `RiskPolicy`
- current market state -> `MarketSnapshot`
- planner output -> `OrderPlan`
- broker result -> `FillOutcome`

## backtest_app -> domain
- historical replay state -> `MarketSnapshot`
- historical signal event -> `SignalCandidate`
- run config -> `StrategyContext`
- replay risk regime -> `RiskPolicy`
- simulator decision -> `OrderPlan`
- simulated fills -> `FillOutcome`
- evaluation result -> `OutcomeRecord`

---

## What is intentionally *not* modeled yet

To keep this step small and stable, the following were not overfit yet:
- account position snapshot model
- portfolio state aggregate model
- vendor order-id model
- LLM/headline raw score object
- Optuna-specific config object
- broker routing/exchange-specific payload schema

These can be added later if needed, but only if they remain transport-neutral.

---

## Validation against requested checks

### Can same `OrderPlan` be used by live executor and backtest simulator?
Yes.
`OrderPlan` + `LadderLeg` contain only canonical execution fields and no live-only dependencies.

### Are HTTP/DB/broker details removed from domain types?
Yes.
The domain layer only contains dataclasses, enums, and serializable fields.
No ORM, FastAPI, KIS, Cloud Run, session, env, or pandas dependency exists in these models.

---

## Next expected step

Once adapters are introduced, the next natural work is:
1. add mapper functions from live rows/services -> canonical domain objects
2. add planner signatures that consume only domain objects
3. add parity tests where live and backtest both run on the same domain fixtures
