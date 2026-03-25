# Validation Strategy — Parity First, Minimal Canary

Created: 2026-03-25

## Core idea
The deployment goal is **not** to perform exhaustive runtime E2E on every market/slot/path.
If AS-IS is already operationally acceptable, the smarter validation strategy is:

1. prove semantic preservation against AS-IS using parity evidence
2. use a small number of representative live canaries to catch runtime-only gaps
3. widen only after those representative gates are clean

## Why this is the right model
Runtime E2E is expensive, noisy, and time-window dependent.
Some runtime stops/errors are correct behavior because of:
- market closed
- holiday window
- broker-safe mode
- missing/invalid runtime preflight

Therefore runtime validation must ask:
- was the behavior correct for this context?
not merely:
- did the endpoint return success?

## Main validation layers
### 1. Parity / contract / side-effect validation
This is the main gate.

Covers:
- ingress parity
- decision parity
- external adapter parity
- DB write/read parity
- representative shadow replay parity

### 2. Minimal live canary validation
This is the final reality check, not the main proof.

Recommended order:
1. US open broker-safe canary
2. US open smallest-money canary
3. only then expand further

## Practical rule
Do not spend cycles trying to execute every runtime path live before cutover.
Spend cycles proving that TO-BE preserves AS-IS meaning, then verify with a few representative live gates.
