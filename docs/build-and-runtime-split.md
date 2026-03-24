# Build and Runtime Split

This step separates live deployment artifacts from backtest/research runtime artifacts.

Created:
- `Dockerfile.live`
- `Dockerfile.backtest`
- `requirements-common.txt`
- `requirements-live.txt`
- `requirements-backtest.txt`
- `.github/workflows/live-tests.yml`
- `.github/workflows/backtest-parity-tests.yml`

---

## Goal

Keep Cloud Run production images focused on `live_app`, while allowing `backtest_app` to evolve as a separate non-deployed runtime.

This means:
- live image should not carry heavy research dependencies
- backtest runtime should not require live-only secrets or Cloud Run assumptions
- CI should validate live and backtest paths independently

---

## Dependency split

## `requirements-common.txt`
Contains lightweight shared dependencies used by both runtimes.

Examples:
- numpy
- pydantic
- requests/http primitives

## `requirements-live.txt`
Contains:
- FastAPI / uvicorn / gunicorn
- SQLAlchemy / alembic / psycopg2 / pgvector
- live integrations (OpenAI, Google AI Platform, feeds, parsing)

Important:
- heavy research/backtest libraries are intentionally excluded

## `requirements-backtest.txt`
Contains:
- pandas
- optuna
- matplotlib
- seaborn
- jupyter / notebook
- pyarrow
- scipy

Important:
- no live DB/broker/web deployment stack is required here

## `requirements.txt`
Now acts only as a backward-compatible wrapper:
- `-r requirements-live.txt`

This keeps old tooling from breaking while making the runtime split explicit.

---

## Docker split

## `Dockerfile.live`
Purpose:
- Cloud Run / production live runtime

Includes:
- live requirements only
- Cloud SQL proxy install
- web startup script
- HTTP healthcheck

Does **not** intentionally install backtest-only packages like:
- optuna
- notebook/jupyter
- plotting stack
- heavy analytics extras

## `Dockerfile.backtest`
Purpose:
- non-deployed backtest/research runtime

Includes only:
- backtest requirements
- `backtest_app`
- `shared`
- `tests` fixtures

Entrypoint:
- `python -m backtest_app.runner`

This container is batch/CLI-oriented and not tied to Cloud Run request serving.

## `Dockerfile`
Kept as a backward-compatible default and aligned with live build behavior.
Production should use `Dockerfile.live` explicitly.

---

## CI split

## `live-tests.yml`
Runs when live-side files change.

Checks:
- install `requirements-live.txt`
- import live boundary modules
- run pure decision golden tests relevant to live/shared planning seam

## `backtest-parity-tests.yml`
Runs when backtest/parity-related files change.

Checks:
- install `requirements-backtest.txt`
- run backtest execution model tests
- run parity tests
- verify CLI backtest runner works from fixture input

This gives clean separation between production-facing validation and research/parity validation.

---

## Why this satisfies the constraints

### No backtest dependency dump into production image
Satisfied.
`Dockerfile.live` installs `requirements-live.txt`, which excludes optuna/notebook/plotting-heavy libs.

### No live-secret/env requirement for backtest runtime
Satisfied by design.
`Dockerfile.backtest` and `requirements-backtest.txt` do not include FastAPI/Cloud SQL/live broker stack as required runtime pieces.
Backtest runner is CLI/fixture based.

### No runtime-mode if/else spread
Satisfied.
Split is file-level/build-level, not via global RUN_MODE branches in application code.

---

## Expected operational effect

### Cloud Run artifact should be lighter
Compared with the previous single-image setup, the live image no longer needs to include:
- pandas
- optuna
- matplotlib
- seaborn
- jupyter/notebook
- pyarrow
- scipy

That reduces unnecessary production footprint and dependency surface.

### Backtest runtime is independently installable
Backtest can now be installed separately with:

```bash
pip install -r requirements-backtest.txt
python -m backtest_app.runner \
  --scenario-id demo-us-open \
  --market US \
  --start-date 2026-03-01 \
  --end-date 2026-03-24 \
  --symbols NVDA,AAPL \
  --data tests/fixtures/backtest_historical_fixture.json
```

---

## Validation intent

The split should be considered healthy when:
- Cloud Run build uses `Dockerfile.live`
- live CI passes without needing backtest-only packages
- backtest/parity CI passes without live deployment secrets
- fixture-based backtest runner remains independently executable
