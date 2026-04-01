from backtest_app.research.pre_optuna import build_pre_optuna_evidence


def _row(
    decision_date: str,
    symbol: str,
    *,
    buy_q10: float,
    buy_q50: float,
    buy_q90: float,
    buy_mixture_ess: float,
    buy_top1_weight_share: float,
    buy_consensus_signature: str,
    buy_prototype_pool_size: int = 4,
    buy_positive_weight_candidate_count: int = 3,
    regime_code: str = "RISK_ON",
    sector_code: str = "TECH",
) -> dict:
    return {
        "decision_date": decision_date,
        "symbol": symbol,
        "chosen_side_before_deploy": "BUY",
        "query_regime_code": regime_code,
        "query_sector_code": sector_code,
        "buy_q10": buy_q10,
        "buy_q50": buy_q50,
        "buy_q90": buy_q90,
        "buy_uncertainty": 0.01,
        "buy_effective_sample_size": buy_mixture_ess,
        "buy_mixture_ess": buy_mixture_ess,
        "buy_member_mixture_ess": buy_mixture_ess,
        "buy_top1_weight_share": buy_top1_weight_share,
        "buy_member_top1_weight_share": buy_top1_weight_share,
        "buy_cumulative_weight_top3": min(1.0, buy_top1_weight_share + 0.25),
        "buy_member_cumulative_weight_top3": min(1.0, buy_top1_weight_share + 0.25),
        "buy_member_support_sum": 24.0,
        "buy_consensus_signature": buy_consensus_signature,
        "buy_member_consensus_signature": buy_consensus_signature,
        "buy_prototype_pool_size": buy_prototype_pool_size,
        "buy_ranked_candidate_count": 6,
        "buy_positive_weight_candidate_count": buy_positive_weight_candidate_count,
        "buy_positive_weight_member_count": buy_positive_weight_candidate_count,
        "buy_pre_truncation_candidate_count": 5,
        "buy_member_pre_truncation_count": 5,
        "buy_member_candidate_count": 5,
        "buy_top_matches_summary": "[]",
        "buy_member_top_matches_summary": "[{\"member_key\": \"aaa:2025-04-01:BUY\", \"weight_share\": 0.6}, {\"member_key\": \"aaa:2025-03-28:BUY\", \"weight_share\": 0.4}]",
        "sell_q10": -0.03,
        "sell_q50": -0.01,
        "sell_q90": 0.0,
        "sell_uncertainty": 0.02,
        "sell_effective_sample_size": 1.0,
        "sell_top_matches_summary": "[]",
    }


def test_build_pre_optuna_evidence_marks_repeated_tight_consensus_family_ready():
    rows = [
        _row("2025-04-01", "AAA", buy_q10=0.015, buy_q50=0.028, buy_q90=0.040, buy_mixture_ess=2.4, buy_top1_weight_share=0.60, buy_consensus_signature="hash-a|hash-b"),
        _row("2025-04-01", "BBB", buy_q10=0.014, buy_q50=0.027, buy_q90=0.039, buy_mixture_ess=2.3, buy_top1_weight_share=0.62, buy_consensus_signature="hash-a|hash-b"),
        _row("2025-04-02", "CCC", buy_q10=0.016, buy_q50=0.029, buy_q90=0.041, buy_mixture_ess=2.5, buy_top1_weight_share=0.58, buy_consensus_signature="hash-a|hash-b"),
        _row("2025-04-03", "DDD", buy_q10=0.013, buy_q50=0.026, buy_q90=0.038, buy_mixture_ess=2.2, buy_top1_weight_share=0.63, buy_consensus_signature="hash-a|hash-b"),
        _row("2025-04-03", "EEE", buy_q10=0.012, buy_q50=0.025, buy_q90=0.037, buy_mixture_ess=2.1, buy_top1_weight_share=0.64, buy_consensus_signature="hash-a|hash-b"),
    ]

    evidence = build_pre_optuna_evidence(rows)
    packet = evidence["pre_optuna_packet"]
    annotated_rows = evidence["forecast_rows"]
    family_table = evidence["pattern_family_table"]

    assert packet["pre_optuna_ready"] is True
    assert packet["verdict"] == "optuna_ready"
    assert packet["eligible_policy_family_count"] == 1
    assert packet["next_optuna_target_scope"] == "tight_consensus_only"
    assert any(bool(row["recurring_family"]) for row in annotated_rows)
    assert any(row["policy_family"] == "tight_consensus" for row in annotated_rows)
    assert all("|hash-a|hash-b|" not in row["pattern_key"] for row in annotated_rows) is False
    assert family_table[0]["recurring_family"] is True


def test_build_pre_optuna_evidence_separates_single_prototype_collapse_from_no_repeated_patterns():
    collapse_rows = [
        _row("2025-04-01", "AAA", buy_q10=0.0, buy_q50=0.020, buy_q90=0.020, buy_mixture_ess=1.0, buy_top1_weight_share=1.0, buy_consensus_signature="hash-collapse", buy_prototype_pool_size=1, buy_positive_weight_candidate_count=1),
        _row("2025-04-02", "BBB", buy_q10=0.0, buy_q50=0.021, buy_q90=0.021, buy_mixture_ess=1.0, buy_top1_weight_share=1.0, buy_consensus_signature="hash-collapse", buy_prototype_pool_size=1, buy_positive_weight_candidate_count=1),
        _row("2025-04-03", "CCC", buy_q10=0.0, buy_q50=0.022, buy_q90=0.022, buy_mixture_ess=1.0, buy_top1_weight_share=1.0, buy_consensus_signature="hash-collapse", buy_prototype_pool_size=1, buy_positive_weight_candidate_count=1),
    ]
    collapse_packet = build_pre_optuna_evidence(collapse_rows)["pre_optuna_packet"]
    assert collapse_packet["verdict"] == "not_ready_single_prototype_collapse"
    assert collapse_packet["single_prototype_collapse_share"] == 1.0

    sparse_rows = [
        _row("2025-04-01", "AAA", buy_q10=0.006, buy_q50=0.018, buy_q90=0.050, buy_mixture_ess=1.7, buy_top1_weight_share=0.72, buy_consensus_signature="hash-a"),
        _row("2025-04-02", "BBB", buy_q10=0.004, buy_q50=0.017, buy_q90=0.051, buy_mixture_ess=1.8, buy_top1_weight_share=0.74, buy_consensus_signature="hash-b"),
        _row("2025-04-03", "CCC", buy_q10=0.005, buy_q50=0.019, buy_q90=0.052, buy_mixture_ess=1.6, buy_top1_weight_share=0.73, buy_consensus_signature="hash-c"),
    ]
    sparse_packet = build_pre_optuna_evidence(sparse_rows)["pre_optuna_packet"]
    assert sparse_packet["verdict"] == "not_ready_no_repeated_patterns"


def test_build_pre_optuna_evidence_uses_member_level_collapse_source_of_truth():
    rows = [
        {
            **_row("2025-04-01", "AAA", buy_q10=0.01, buy_q50=0.02, buy_q90=0.03, buy_mixture_ess=2.0, buy_top1_weight_share=0.55, buy_consensus_signature="member-a|member-b", buy_prototype_pool_size=1),
            "buy_member_candidate_count": 4,
            "buy_positive_weight_member_count": 3,
            "buy_member_pre_truncation_count": 4,
            "buy_member_top1_weight_share": 0.55,
            "buy_member_mixture_ess": 2.0,
            "buy_member_consensus_signature": "member-a|member-b",
            "buy_member_top_matches_summary": "[{\"member_key\": \"AAA:2025-03-01:BUY\", \"weight_share\": 0.55}, {\"member_key\": \"AAA:2025-03-02:BUY\", \"weight_share\": 0.45}]",
        },
        {
            **_row("2025-04-02", "BBB", buy_q10=0.01, buy_q50=0.02, buy_q90=0.03, buy_mixture_ess=2.1, buy_top1_weight_share=0.54, buy_consensus_signature="member-a|member-b", buy_prototype_pool_size=1),
            "buy_member_candidate_count": 4,
            "buy_positive_weight_member_count": 3,
            "buy_member_pre_truncation_count": 4,
            "buy_member_top1_weight_share": 0.54,
            "buy_member_mixture_ess": 2.1,
            "buy_member_consensus_signature": "member-a|member-b",
            "buy_member_top_matches_summary": "[{\"member_key\": \"BBB:2025-03-01:BUY\", \"weight_share\": 0.54}, {\"member_key\": \"BBB:2025-03-02:BUY\", \"weight_share\": 0.46}]",
        },
        {
            **_row("2025-04-03", "CCC", buy_q10=0.01, buy_q50=0.02, buy_q90=0.03, buy_mixture_ess=2.2, buy_top1_weight_share=0.53, buy_consensus_signature="member-a|member-b", buy_prototype_pool_size=1),
            "buy_member_candidate_count": 4,
            "buy_positive_weight_member_count": 3,
            "buy_member_pre_truncation_count": 4,
            "buy_member_top1_weight_share": 0.53,
            "buy_member_mixture_ess": 2.2,
            "buy_member_consensus_signature": "member-a|member-b",
            "buy_member_top_matches_summary": "[{\"member_key\": \"CCC:2025-03-01:BUY\", \"weight_share\": 0.53}, {\"member_key\": \"CCC:2025-03-02:BUY\", \"weight_share\": 0.47}]",
        },
        {
            **_row("2025-04-03", "DDD", buy_q10=0.01, buy_q50=0.02, buy_q90=0.03, buy_mixture_ess=2.0, buy_top1_weight_share=0.56, buy_consensus_signature="member-a|member-b", buy_prototype_pool_size=1),
            "buy_member_candidate_count": 4,
            "buy_positive_weight_member_count": 3,
            "buy_member_pre_truncation_count": 4,
            "buy_member_top1_weight_share": 0.56,
            "buy_member_mixture_ess": 2.0,
            "buy_member_consensus_signature": "member-a|member-b",
            "buy_member_top_matches_summary": "[{\"member_key\": \"DDD:2025-03-01:BUY\", \"weight_share\": 0.56}, {\"member_key\": \"DDD:2025-03-02:BUY\", \"weight_share\": 0.44}]",
        },
        {
            **_row("2025-04-04", "EEE", buy_q10=0.01, buy_q50=0.02, buy_q90=0.03, buy_mixture_ess=2.0, buy_top1_weight_share=0.57, buy_consensus_signature="member-a|member-b", buy_prototype_pool_size=1),
            "buy_member_candidate_count": 4,
            "buy_positive_weight_member_count": 3,
            "buy_member_pre_truncation_count": 4,
            "buy_member_top1_weight_share": 0.57,
            "buy_member_mixture_ess": 2.0,
            "buy_member_consensus_signature": "member-a|member-b",
            "buy_member_top_matches_summary": "[{\"member_key\": \"EEE:2025-03-01:BUY\", \"weight_share\": 0.57}, {\"member_key\": \"EEE:2025-03-02:BUY\", \"weight_share\": 0.43}]",
        },
    ]
    packet = build_pre_optuna_evidence(rows)["pre_optuna_packet"]
    assert packet["verdict"] == "optuna_ready"


def test_build_pre_optuna_evidence_reports_contract_or_environment_when_rows_are_missing():
    packet = build_pre_optuna_evidence([])["pre_optuna_packet"]
    assert packet["verdict"] == "not_ready_contract_or_environment"
    assert packet["pre_optuna_ready"] is False
