# auto-trader-bot Agent Map

## North Star
- Protect `live_app` as the current AS-IS operating path.
- Use `backtest_app` as the TOBE research runtime.
- Do not mix strategy discovery with live canary or cutover work.
- Propose live uplift only through explicit promotion gates.

## Runtime Lanes
- `live-cutover`: preserve money-going meaning and external contracts.
- `shadow-replay`: replay live orchestration without broker side effects.
- `research-discovery`: run strategy, feature, and parameter exploration in `backtest_app`.
- `promotion`: connect research evidence to replay, shadow, and canary eligibility.

Read the lane contract first: [docs/harness/runtime-lanes.md](docs/harness/runtime-lanes.md)

## Task Classes
- `live-parity`
- `shadow-replay`
- `research-run`
- `promotion-eval`
- `harness-fix`
- `docs-only`

Classification and lane mapping live here: [docs/harness/task-taxonomy.md](docs/harness/task-taxonomy.md)

## Source of Truth Order
1. This file
2. [docs/harness/README.md](docs/harness/README.md)
3. Active runtime docs such as [docs/cutover-gates.md](docs/cutover-gates.md), [docs/shadow-e2e-plan.md](docs/shadow-e2e-plan.md), and [docs/research_run_protocol.md](docs/research_run_protocol.md)
4. Replay fixtures and run outputs under `tests/replay_fixtures/` and `runs/`
5. Code
6. Historical notes only when newer evidence is missing

Full precedence rules: [docs/harness/source-of-truth.md](docs/harness/source-of-truth.md)

## Hard Prohibitions
- Do not rewire the active live money path without source-of-truth evidence.
- Do not treat `shared/domain` as the live money path unless runtime proof exists.
- Do not mix research tuning with active canary or cutover changes.
- Do not report harness or environment failures as product regressions.
- Do not claim success without tests, evals, or explicit evidence references.

Failure classes and rerun policy: [docs/harness/failure-attribution.md](docs/harness/failure-attribution.md)

## Required Output Shape
- Task classification
- Files read
- Invariants
- Changes made
- Tests or evals run
- Artifacts produced
- Failure attribution
- Remaining risk
- Next smallest step

Evidence contract: [docs/harness/evidence-contract.md](docs/harness/evidence-contract.md)

## Promotion Rule
- Research results are not live candidates by default.
- Promotion requires reproducible backtest evidence, replay-anchor parity, shadow replay, and canary eligibility.

Promotion flow: [docs/harness/promotion-gate.md](docs/harness/promotion-gate.md)

## Read in This Order
1. [docs/harness/README.md](docs/harness/README.md)
2. [docs/harness/runtime-lanes.md](docs/harness/runtime-lanes.md)
3. [docs/harness/source-of-truth.md](docs/harness/source-of-truth.md)
4. [docs/harness/failure-attribution.md](docs/harness/failure-attribution.md)
5. Existing lane docs linked from the overlay

## Repo Pointers
- Local setup and default mirror-first backtest path: [README.md](README.md)
- Live boundary: [docs/live-app-boundary.md](docs/live-app-boundary.md)
- Live vs shared vs backtest map: [docs/live-vs-shared-vs-backtest-map.md](docs/live-vs-shared-vs-backtest-map.md)
- Structured logging contract: [docs/structured-logging-and-metrics.md](docs/structured-logging-and-metrics.md)
- Local backtest Postgres runbook: [docs/local-backtest-postgres.md](docs/local-backtest-postgres.md)

## Follow-Up Hygiene
- Keep deep rules in `docs/harness/*` and existing `docs/*`; keep this file short.
- When new evidence changes behavior or truth, update the owning doc and add or adjust a harness test.
