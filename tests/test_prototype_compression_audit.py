from backtest_app.research.labeling import EventLabelingConfig, build_event_outcome_record
from backtest_app.research.models import EventOutcomeRecord, StatePrototype
from backtest_app.research.prototype import aggregate_prototype_compression_batches, build_prototype_compression_audit


def _event(symbol: str, event_date: str, regime: str, sector: str) -> EventOutcomeRecord:
    outcome = build_event_outcome_record([], EventLabelingConfig(target_return_pct=0.05, stop_return_pct=0.03, horizon_days=5))
    return EventOutcomeRecord(
        symbol=symbol,
        event_date=event_date,
        outcome_end_date=None,
        schema_version="event_outcome_v1",
        side_outcomes=outcome.side_payload,
        diagnostics={"regime_code": regime, "sector_code": sector},
        path_summary={"regime_code": regime, "sector_code": sector},
    )


def test_prototype_compression_audit_distinguishes_raw_event_count_from_prototype_count():
    events = [
        _event("AAA", "2025-03-01", "RISK_ON", "TECH"),
        _event("BBB", "2025-03-02", "RISK_ON", "TECH"),
        _event("CCC", "2025-03-03", "RISK_OFF", "ENERGY"),
    ]
    prototypes = [
        StatePrototype(
            prototype_id="p1",
            anchor_code="STATE_MEMORY_V1",
            embedding=[1.0, 0.0],
            member_count=2,
            representative_symbol="AAA",
            representative_date="2025-03-01",
            representative_hash="hash-a",
            regime_code="RISK_ON",
            sector_code="TECH",
            support_count=2,
            decayed_support=2.0,
            prototype_membership={"lineage": [{"ref": "AAA:2025-03-01"}, {"ref": "BBB:2025-03-02"}]},
        ),
        StatePrototype(
            prototype_id="p2",
            anchor_code="STATE_MEMORY_V1",
            embedding=[0.0, 1.0],
            member_count=1,
            representative_symbol="CCC",
            representative_date="2025-03-03",
            representative_hash="hash-c",
            regime_code="RISK_OFF",
            sector_code="ENERGY",
            support_count=1,
            decayed_support=1.0,
            prototype_membership={"lineage": [{"ref": "CCC:2025-03-03"}]},
        ),
    ]
    audit = build_prototype_compression_audit(event_records=events, prototypes=prototypes, as_of_date="2025-04-01")
    assert audit["event_record_count"] == 3
    assert audit["prototype_count"] == 2
    assert audit["compression_ratio"] == 1.5
    assert audit["table_row"]["event_record_count"] == 3
    assert audit["table_row"]["prototype_count"] == 2
    aggregate = aggregate_prototype_compression_batches([audit])
    assert aggregate["batch_count"] == 1
    assert aggregate["event_record_count_total"] == 3
    assert aggregate["prototype_count_total"] == 2
    assert aggregate["table_rows"][0]["compression_ratio"] == 1.5
