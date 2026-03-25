# Runtime Environment Checklist

Created: 2026-03-25
Purpose: block false-start canary runs before attempting TO-BE cutover validation.

## Rule
Do not run US open broker-safe canary until this checklist is green.

## Required runtime env
- `KIS_APPKEY`
- `KIS_APPSECRET`
- `KIS_CANO`
- `KIS_ACNT_PRDT_CD`
- if `KIS_VIRTUAL=true`, also `KIS_VIRTUAL_CANO`

## Invalid values
Any of the following count as invalid for canary:
- empty value
- `00000000`
- `00000000-00`
- `CHANGE_ME`
- `your_account*`

## Preflight command
```powershell
python scripts/check_canary_env.py
```

Expected result:
- `"ok": true`
- no entries in `errors`

## Current blocker observed in this workspace shell
Observed on 2026-03-25:
- `KIS_CANO=00000000`
- `KIS_VIRTUAL_CANO=00000000`

Meaning:
- local canary attempts from this shell are invalid until runtime env is aligned

## After preflight passes
1. run **US open only** broker-safe canary
2. write `docs/canary-report-template.md`
3. compare against `tests/replay_fixtures/us_open_20260324.json`
4. do not widen scope until report is clean
