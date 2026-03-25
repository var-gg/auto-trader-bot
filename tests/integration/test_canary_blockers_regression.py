from __future__ import annotations

from datetime import date

from app.core import kis_client as kis_client_module
from app.core.kis_client import KISClient
from app.features.premarket.services.pm_open_session_service import _is_earnings_day


class _CaptureClient(KISClient):
    def __init__(self):
        self.calls = []
        self.base_url = 'https://example.test'

    def _make_request(self, url, tr_id, params, retry_count=0, extra_headers=None):
        self.calls.append({
            'url': url,
            'tr_id': tr_id,
            'params': params,
            'retry_count': retry_count,
            'extra_headers': extra_headers,
        })
        return {'rt_cd': '0', 'output1': [], 'output2': []}


class _FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping


class _FakeResult:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _CaptureDB:
    def __init__(self, row=None):
        self.row = row
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append({'sql': str(stmt), 'params': params})
        return _FakeResult(self.row)


def test_is_earnings_day_uses_runtime_schema_safe_query():
    db = _CaptureDB(_FakeRow({'exists': 1}))

    out = _is_earnings_day(db, 'AAPL', date(2026, 3, 25))

    assert out is True
    sql = db.calls[0]['sql']
    assert 'e.report_date = :asof_date' in sql
    assert 'confirmed_report_date' not in sql
    assert 'expected_report_date_start' not in sql


def test_present_balance_uses_virtual_account_when_enabled(monkeypatch):
    client = _CaptureClient()
    monkeypatch.setattr(kis_client_module.settings, 'KIS_VIRTUAL', True)
    monkeypatch.setattr(kis_client_module.settings, 'KIS_VIRTUAL_CANO', '99999999')
    monkeypatch.setattr(kis_client_module.settings, 'KIS_CANO', '11111111')
    monkeypatch.setattr(kis_client_module.settings, 'KIS_ACNT_PRDT_CD', '01')
    monkeypatch.setattr(kis_client_module.settings, 'KIS_TR_ID_PRESENT_BALANCE', 'VTRP6504R')

    client.present_balance(wcrc_frcr_dvsn_cd='02', natn_cd='840', tr_mket_cd='00', inqr_dvsn_cd='00')

    call = client.calls[0]
    assert call['params']['CANO'] == '99999999'
    assert call['params']['ACNT_PRDT_CD'] == '01'
    assert call['params']['WCRC_FRCR_DVSN_CD'] == '02'
