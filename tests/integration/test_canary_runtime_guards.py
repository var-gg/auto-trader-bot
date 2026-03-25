from __future__ import annotations

from app.features.portfolio.services.asset_snapshot_service import AssetSnapshotService
from app.features.trading_hybrid.engines import runbooks


class _DummyDB:
    pass


def test_ovrs_snapshot_rejects_placeholder_account(monkeypatch):
    class _DummyClient:
        def overseas_present_balance_test(self, **kwargs):
            raise AssertionError('should not call external API with placeholder account')

    svc = AssetSnapshotService(_DummyDB())
    svc.kis_client = _DummyClient()

    monkeypatch.setattr('app.features.portfolio.services.asset_snapshot_service.settings.KIS_VIRTUAL', False)
    monkeypatch.setattr('app.features.portfolio.services.asset_snapshot_service.settings.KIS_CANO', '00000000')
    monkeypatch.setattr('app.features.portfolio.services.asset_snapshot_service.settings.KIS_ACNT_PRDT_CD', '01')

    out = svc.collect_ovrs_account_snapshot(account_uid=None)
    assert out['success'] is False
    assert 'placeholder account configuration' in out['error']


async def test_sync_profit_and_account_fails_when_snapshot_fails(monkeypatch):
    class _DummyPnl:
        def __init__(self, db):
            pass
        async def collect_and_save_realized_pnl(self, *args, **kwargs):
            return {'total_saved': 0}

    class _DummyAsset:
        def __init__(self, db):
            pass
        def collect_ovrs_account_snapshot(self, account_uid=None):
            return {'success': False, 'error': 'placeholder account configuration'}

    monkeypatch.setattr(runbooks, 'TradeRealizedPnlService', _DummyPnl)
    monkeypatch.setattr(runbooks, 'AssetSnapshotService', _DummyAsset)

    try:
        await runbooks._sync_profit_and_account(_DummyDB(), market='US')
    except RuntimeError as e:
        assert 'OVRS account snapshot failed' in str(e)
    else:
        raise AssertionError('expected RuntimeError when OVRS snapshot fails')
