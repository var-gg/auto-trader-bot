from app.features.premarket.services.pm_open_session_service import _format_pm_buy_risk_note


def test_format_pm_buy_risk_note_includes_core_audit_fields():
    note = _format_pm_buy_risk_note(
        risk_multiplier=1.73,
        risk_snapshot_id=42,
        risk_meta={
            "policy": "core",
            "status": "refreshed",
            "freshness": "auto_refreshed_snapshot",
            "reason": "geopolitical escalation and oil shock risk",
        },
    )

    assert "riskM=1.73" in note
    assert "riskSnap=42" in note
    assert "riskState=core/refreshed/auto_refreshed_snapshot" in note
    assert "riskWhy=" in note


def test_format_pm_buy_risk_note_handles_missing_snapshot():
    note = _format_pm_buy_risk_note(
        risk_multiplier=1.0,
        risk_snapshot_id=None,
        risk_meta={
            "policy": "core",
            "status": "fallback-error",
            "freshness": "error",
            "reason": "core-policy:error:TimeoutError",
        },
    )

    assert "riskM=1.00" in note
    assert "riskSnap=none" in note
    assert "riskState=core/fallback-error/error" in note
