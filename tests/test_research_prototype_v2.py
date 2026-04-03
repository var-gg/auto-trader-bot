import json
from pathlib import Path

import pytest

from backtest_app.research import prototype as prototype_module
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.models import EventOutcomeRecord, ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes, build_prototype_snapshot_from_event_memory, build_state_prototypes_from_event_memory
from backtest_app.research.repository import ExactCosineCandidateIndex, load_prototypes_asof
from backtest_app.research.scoring import ScoringConfig, build_decision_surface, score_candidates_exact


def _anchor(symbol, ref_date, embedding, *, side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.05, mae=-0.01, mfe=0.07, quality=0.8):
    return ResearchAnchor(symbol=symbol, anchor_code="SIM_V2", reference_date=ref_date, anchor_date=ref_date, side=side, embedding=embedding, shape_vector=embedding, ctx_vector=[], vector_version="research_similarity_v2_multiscale", embedding_model="manual-multiscale", vector_dim=len(embedding), anchor_quality=quality, mae_pct=mae, mfe_pct=mfe, days_to_hit=2, after_cost_return_pct=ret, realized_return_pct=ret, regime_code=regime, sector_code=sector, liquidity_score=liq, metadata={"feature_version": "multiscale_manual_v2"})


def _event(symbol, event_date, outcome_end_date, embedding=None, buy_ret=0.03, sell_ret=-0.03):
    embedding = embedding or [1.0, 0.0]
    return EventOutcomeRecord(symbol=symbol, event_date=event_date, outcome_end_date=outcome_end_date, schema_version="event_outcome_v1", path_summary={"regime_code": "RISK_ON", "sector_code": "TECH", "liquidity_bucket": "HIGH", "embedding": embedding}, side_outcomes={"BUY": {"after_cost_return_pct": buy_ret, "mae_pct": -0.01, "mfe_pct": 0.04}, "SELL": {"after_cost_return_pct": sell_ret, "mae_pct": -0.04, "mfe_pct": 0.01}}, diagnostics={"regime_code": "RISK_ON", "sector_code": "TECH", "embedding": embedding, "quality_score": 0.9, "liquidity_score": 0.9})


def test_build_anchor_prototypes_legacy_wrapper_still_works():
    anchors = [_anchor("AAPL", "2026-01-01", [1.0, 0.0], side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08)]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1, memory_version="memory_asof_v1"), as_of_date="2026-01-10")
    assert len(prototypes) == 1
    assert prototypes[0].metadata["legacy_wrapper"] is True


def test_build_state_prototypes_from_event_memory_keeps_buy_and_sell_inside_same_prototype():
    events = [_event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0], buy_ret=0.04, sell_ret=-0.04), _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.99, 0.01], buy_ret=0.02, sell_ret=-0.02)]
    prototypes = build_state_prototypes_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    assert len(prototypes) == 1
    proto = prototypes[0]
    assert "BUY" in proto.side_stats
    assert "SELL" in proto.side_stats
    assert proto.side_stats["BUY"]["support_count"] == 2
    assert proto.side_stats["SELL"]["support_count"] == 2
    assert proto.metadata["spec_hash"] == "spec-1"
    assert proto.representative_hash
    assert proto.prototype_membership["member_refs"]


def test_score_candidates_exact_and_surface_use_same_prototype_pool_for_both_sides():
    events = [_event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0], buy_ret=0.08, sell_ret=-0.01), _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.98, 0.02], buy_ret=0.06, sell_ret=-0.02)]
    prototypes = build_state_prototypes_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    buy_scores = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1), candidate_index=ExactCosineCandidateIndex(), side="BUY")
    sell_scores = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1), candidate_index=ExactCosineCandidateIndex(), side="SELL")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=prototypes, regime_code="RISK_ON", sector_code="TECH", candidate_index=ExactCosineCandidateIndex())
    assert buy_scores and sell_scores
    assert buy_scores[0].prototype_id == sell_scores[0].prototype_id
    assert surface.diagnostics["shared_neighbor_pool"] is True


def test_build_prototype_snapshot_from_event_memory_is_deterministic_and_keeps_lineage(tmp_path):
    events = [_event("AAPL", "2026-01-01", "2026-01-05", buy_ret=0.04, sell_ret=-0.04), _event("MSFT", "2026-01-02", "2026-01-06", buy_ret=0.02, sell_ret=-0.02)]
    snap1 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    snap2 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    assert snap1["prototypes"][0]["prototype_id"] == snap2["prototypes"][0]["prototype_id"]
    assert snap1["prototypes"][0]["side_stats"]["BUY"]["support_count"] == 2
    assert snap1["prototypes"][0]["side_stats"]["SELL"]["support_count"] == 2
    store = JsonResearchArtifactStore(str(tmp_path))
    manifest_path = store.save_prototype_snapshot(run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1", payload=snap1)
    assert Path(manifest_path).exists()
    assert (Path(manifest_path).parent / "parts").exists()
    loaded = load_prototypes_asof(artifact_store=store, run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1")
    assert loaded
    assert loaded[0].prototype_id == snap1["prototypes"][0]["prototype_id"]


def test_build_state_prototypes_fast_path_matches_legacy_exactly():
    events = [
        _event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0], buy_ret=0.04, sell_ret=-0.04),
        _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.99, 0.01], buy_ret=0.02, sell_ret=-0.02),
        _event("NVDA", "2026-01-03", "2026-01-07", embedding=[0.1, 0.9], buy_ret=0.03, sell_ret=-0.03),
    ]
    legacy = prototype_module._build_state_prototypes_from_event_memory_legacy(
        event_records=events,
        as_of_date="2026-01-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
    )
    fast = build_state_prototypes_from_event_memory(
        event_records=events,
        as_of_date="2026-01-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
    )
    assert [item.__dict__ for item in fast] == [item.__dict__ for item in legacy]


def test_build_state_prototypes_checkpoint_resume_matches_full_run(tmp_path):
    events = [
        _event(f"SYM{i:04d}", f"2026-01-{(i % 20) + 1:02d}", "2026-02-01", embedding=[1.0, 0.0], buy_ret=0.01, sell_ret=-0.01)
        for i in range(1001)
    ]
    checkpoint_path = tmp_path / "prototype_checkpoint.pkl"
    interrupted = False
    history: list[dict] = []

    def _progress(payload: dict) -> None:
        history.append(dict(payload))
        if payload.get("phase") == "prototype_cluster" and int(payload.get("prototype_rows_done") or 0) >= 1000:
            raise RuntimeError("interrupt-after-checkpoint")

    with pytest.raises(RuntimeError, match="interrupt-after-checkpoint"):
        build_state_prototypes_from_event_memory(
            event_records=events,
            as_of_date="2026-02-10",
            memory_version="memory_asof_v1",
            spec_hash="spec-1",
            checkpoint_path=str(checkpoint_path),
            progress_callback=_progress,
        )
    interrupted = True
    assert interrupted is True
    assert checkpoint_path.exists()
    assert checkpoint_path.with_name("prototype_rows").exists()
    assert checkpoint_path.with_name("prototype_norms.npy").exists()
    assert checkpoint_path.with_name("prototype_representatives.npy").exists()
    resumed = build_state_prototypes_from_event_memory(
        event_records=events,
        as_of_date="2026-02-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
        checkpoint_path=str(checkpoint_path),
        resume_from_checkpoint=True,
    )
    full = build_state_prototypes_from_event_memory(
        event_records=events,
        as_of_date="2026-02-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
    )
    assert [item.__dict__ for item in resumed] == [item.__dict__ for item in full]


def test_load_prototype_snapshot_keeps_legacy_json_compatibility(tmp_path):
    run_dir = tmp_path / "r1"
    run_dir.mkdir(parents=True, exist_ok=True)
    legacy_payload = {
        "as_of_date": "2026-01-10",
        "memory_version": "memory_asof_v1",
        "spec_hash": "spec-1",
        "snapshot_id": "snap-1",
        "prototype_count": 1,
        "prototypes": [
            build_state_prototypes_from_event_memory(
                event_records=[_event("AAPL", "2026-01-01", "2026-01-05")],
                as_of_date="2026-01-10",
                memory_version="memory_asof_v1",
                spec_hash="spec-1",
            )[0].__dict__
        ],
    }
    (run_dir / "prototype_snapshot.json").write_text(json.dumps(legacy_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    store = JsonResearchArtifactStore(str(tmp_path))
    loaded = load_prototypes_asof(artifact_store=store, run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1")
    assert len(loaded) == 1
    assert loaded[0].prototype_id == legacy_payload["prototypes"][0]["prototype_id"]


def test_build_state_prototypes_emits_running_counters_and_matches_block_sizes(tmp_path):
    events = [
        _event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0]),
        _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.99, 0.01]),
        _event("NVDA", "2026-01-03", "2026-01-07", embedding=[0.98, 0.02]),
    ]
    progress_events: list[dict] = []
    block_one = build_state_prototypes_from_event_memory(
        event_records=events,
        as_of_date="2026-01-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
        checkpoint_path=str(tmp_path / "prototype_checkpoint.pkl"),
        comparison_block_size=1,
        progress_callback=lambda payload: progress_events.append(dict(payload)),
    )
    block_many = build_state_prototypes_from_event_memory(
        event_records=events,
        as_of_date="2026-01-10",
        memory_version="memory_asof_v1",
        spec_hash="spec-1",
        comparison_block_size=256,
    )
    assert [item.__dict__ for item in block_one] == [item.__dict__ for item in block_many]
    prototype_cluster = [event for event in progress_events if event.get("phase") == "prototype_cluster"]
    assert prototype_cluster
    assert any(int(event.get("prototype_rows_done") or 0) > 0 for event in prototype_cluster)
    assert any(int(event.get("cluster_count") or 0) > 0 for event in prototype_cluster)


@pytest.mark.parametrize(
    ("label", "after_cost_return_pct", "flags", "expected_counts", "expected_probs"),
    [
        ("UP_FIRST", 0.03, {}, {"target_first_count": 1, "stop_first_count": 0, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 0}, {"p_target_first": 1.0, "p_stop_first": 0.0, "p_flat": 0.0, "p_ambiguous": 0.0, "p_no_trade": 0.0}),
        ("DOWN_FIRST", -0.03, {}, {"target_first_count": 0, "stop_first_count": 1, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 0}, {"p_target_first": 0.0, "p_stop_first": 1.0, "p_flat": 0.0, "p_ambiguous": 0.0, "p_no_trade": 0.0}),
        ("FLAT", 0.0, {"flat": True}, {"target_first_count": 0, "stop_first_count": 0, "flat_count": 1, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 0}, {"p_target_first": 0.0, "p_stop_first": 0.0, "p_flat": 1.0, "p_ambiguous": 0.0, "p_no_trade": 0.0}),
        ("AMBIGUOUS", 0.0, {"ambiguous": True}, {"target_first_count": 0, "stop_first_count": 0, "flat_count": 0, "ambiguous_count": 1, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 0}, {"p_target_first": 0.0, "p_stop_first": 0.0, "p_flat": 0.0, "p_ambiguous": 1.0, "p_no_trade": 0.0}),
        ("NO_TRADE", 0.0, {"no_trade": True}, {"target_first_count": 0, "stop_first_count": 0, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 1, "horizon_up_count": 0, "horizon_down_count": 0}, {"p_target_first": 0.0, "p_stop_first": 0.0, "p_flat": 0.0, "p_ambiguous": 0.0, "p_no_trade": 1.0}),
        ("HORIZON_UP", 0.02, {}, {"target_first_count": 0, "stop_first_count": 0, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 1, "horizon_down_count": 0}, {"p_target_first": 0.0, "p_stop_first": 0.0, "p_flat": 0.0, "p_ambiguous": 0.0, "p_no_trade": 0.0}),
        ("HORIZON_DOWN", -0.02, {}, {"target_first_count": 0, "stop_first_count": 0, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 1}, {"p_target_first": 0.0, "p_stop_first": 0.0, "p_flat": 0.0, "p_ambiguous": 0.0, "p_no_trade": 0.0}),
    ],
)
def test_build_state_prototypes_reconstructs_side_counts_from_labels(label, after_cost_return_pct, flags, expected_counts, expected_probs):
    side_payload = {
        "first_touch_label": label,
        "after_cost_return_pct": after_cost_return_pct,
        "mae_pct": -0.01,
        "mfe_pct": 0.02,
        **flags,
    }
    event = EventOutcomeRecord(
        symbol="AAPL",
        event_date="2026-01-01",
        outcome_end_date="2026-01-05",
        schema_version="event_outcome_v1",
        path_summary={"regime_code": "RISK_ON", "sector_code": "TECH", "liquidity_bucket": "HIGH", "embedding": [1.0, 0.0]},
        side_outcomes={"BUY": side_payload, "SELL": dict(side_payload)},
        diagnostics={"regime_code": "RISK_ON", "sector_code": "TECH", "embedding": [1.0, 0.0], "quality_score": 0.9, "liquidity_score": 0.9},
    )
    proto = build_state_prototypes_from_event_memory(event_records=[event], as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")[0]
    stats = proto.side_stats["BUY"]
    for key, expected in expected_counts.items():
        assert stats[key] == expected
    for key, expected in expected_probs.items():
        assert stats[key] == expected
