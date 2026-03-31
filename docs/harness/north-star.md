# North Star

The repo has two distinct runtime goals that must stay separate until evidence says otherwise.

## Operating Principle
- `live_app` is the protected AS-IS operating path.
- `backtest_app` is the TOBE research runtime.
- `shared/domain` is a planning seam, not proof of active live ingress.
- Strategy discovery does not run in parallel with live canary or cutover work.
- Live uplift happens only through explicit promotion gates.

## Why the Overlay Exists
The repo already contains deep documents for parity, shadow replay, local mirror research, structured logging, and known truth gaps.
What was missing was an overlay that helps an agent read those documents in the right order and classify failures correctly.

## Non-Goals for This Slice
- No broker or order-path behavior changes.
- No scheduler topology changes.
- No promotion of research output directly into live runtime.
- No rewrite of cutover or research docs beyond cross-linking.

## Primary References
- Live boundary: [../live-app-boundary.md](../live-app-boundary.md)
- Live/runtime map: [../live-vs-shared-vs-backtest-map.md](../live-vs-shared-vs-backtest-map.md)
- Research protocol: [../research_run_protocol.md](../research_run_protocol.md)
- Cutover gates: [../cutover-gates.md](../cutover-gates.md)
