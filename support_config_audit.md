official_classification: metadata_application_mismatch
medium best1/best2 failure must not be used as final support-family evidence under the current strict audit policy

Strict C result:
- Completed `best1` summary lacks `metadata_application.expected/observed`: True.
- Completed `best2` summary lacks `metadata_application.expected/observed`: True.
- Completed `best1` summary exposes current verdict fields (`authoritative`, `verdict_eligible`, `exclusion_reasons`): False.
- Completed `best2` summary exposes current verdict fields (`authoritative`, `verdict_eligible`, `exclusion_reasons`): False.
- Current-schema probe path: `A:\vargg-workspace\30_trading\auto-trader-bot\runs\medium_viability_check\20260329_e21ff63_best1_rerun\medium_viability_summary.json`.
- Current-schema probe medium runs present: `best1`.
- Probe `best1` authoritative=False, verdict_eligible=False, exclusion_reasons=["child_failed", "non_authoritative", "metadata_not_applied"].

Why C and why the official path stops here:
- Official classification is `metadata_application_mismatch` because completed `best1`/`best2` artifacts cannot be audited with the current `metadata_application` contract.
- `medium_gate_census`, `gate_family_viability_check.py`, and `label_family_viability_check.py` stay blocked because `Completed best1/best2 artifacts do not expose current-schema metadata_application.expected/observed or authoritative/verdict_eligible/exclusion_reasons, and the only current-schema probe is a partial best1 rerun.`

Supplemental inference:
- Tiny source top-2 metadata are distinct: True.
- Legacy completed medium manifests match tiny source metadata: True.
- Legacy completed mapping: `best1 -> tiny_sweep_kt6_tk5_kw1_ess1p5`, `best2 -> tiny_ess_gate_disabled`.
- Current probe mapping: `best1 -> tiny_ess_gate_disabled`, `best2 -> tiny_sweep_kt6_tk5_kw1_ess1p5`.
- Driver support dedupe present: False.
- These supplemental facts do not upgrade the official classification: True.

Unblock conditions:
- Audit policy changes to `Infer A` or `Hybrid`, or
- Both `best1` and `best2` are re-collected as completed authoritative current-schema artifacts.

Execution guardrails:
- No new medium reruns were started by this audit.
- `medium_gate_census`, `gate_family_viability`, `label_family_viability`, `matrix`, and `optuna` remain out of scope until the unblock condition is met.
