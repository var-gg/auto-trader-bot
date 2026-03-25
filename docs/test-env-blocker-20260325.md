# Test Environment Blocker — 2026-03-25

## Finding
The repo virtual environment exists, but `pytest` was not installed in it.

Confirmed by:
- `venv\\Scripts\\python.exe -m pytest --version` -> `No module named pytest`
- `venv\\Scripts\\python.exe -m pip show pytest` -> package not found

## Interpretation
This is a tooling gap in the local validation environment, not a parity regression signal.

## Action
Install `pytest` into the repo venv, then run the representative validation bundle:
- decision parity
- DB side-effect parity
- route contract tests
- shadow replay E2E
