from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.features.kis_test.models.kis_test_models import BootstrapRequest, BootstrapResponse
from app.features.trading_hybrid.engines import runbooks
from live_app.application.bootstrap_commands import RunBootstrapCommand
from live_app.application.context import RunContext
from live_app.application.history_commands import RunHistoryPostprocessCommand
from live_app.application.trading_commands import RunTradingHybridCommand


@pytest.fixture
def anyio_backend():
    return 'asyncio'


class _DummyDB:
    pass


class _BootstrapStub:
    def __init__(self, db):
        self.db = db

    async def run_bootstrap(self, request):
        return BootstrapResponse(
            overall_success=True,
            total_steps=5,
            successful_steps=5,
            failed_steps=0,
            skipped_steps=0,
            total_duration_seconds=1.1,
            started_at='2026-03-24T07:00:00Z',
            completed_at='2026-03-24T07:00:01Z',
            steps=[],
        )


class _FakeHistorySummary:
    class _Unfilled:
        scanned = 5
        updated = 4
        unresolved = 1

    class _Outcomes:
        scanned = 7
        upserted = 6
        skipped_missing_price = 1

    unfilled = _Unfilled()
    outcomes = _Outcomes()


async def _shadow_run_sequence(monkeypatch):
    events = []

    monkeypatch.setattr('live_app.application.bootstrap_commands.BootstrapService', _BootstrapStub)
    monkeypatch.setattr('live_app.application.bootstrap_commands.build_live_run_log', lambda **kwargs: events.append({"stage": "bootstrap.log", **kwargs}))
    monkeypatch.setattr('live_app.application.trading_commands.build_live_run_log', lambda **kwargs: events.append({"stage": "trading.log", **kwargs}))

    async def fake_sync(db, market):
        events.append({"stage": f"sync.{market}", "market": market, "processed": 7, "upserted": 6})

    monkeypatch.setattr(runbooks, '_check_market_open', lambda db, market: True)
    monkeypatch.setattr(runbooks, '_sync_profit_and_account', fake_sync)

    class _FakeEngine:
        def __init__(self, db, config):
            self.db = db
            self.config = config

        def run_open_greedy(self):
            market = self.config.market
            result = {
                'buy_plans': [{'ticker_id': 15365, 'symbol': 'AAPL', 'legs': [{'quantity': 1, 'limit_price': 107.1}, {'quantity': 1, 'limit_price': 104.2}], 'execution_correlation': [{'leg_id': 1, 'broker_order_id': 901, 'broker_order_no': 'PAPER-901'}]}] if market == 'US' else [],
                'sell_plans': [{'ticker_id': 15313, 'symbol': '005930', 'legs': [{'quantity': 1, 'limit_price': 71200}, {'quantity': 1, 'limit_price': 73300}], 'execution_correlation': [{'leg_id': 2, 'broker_order_id': 902, 'broker_order_no': 'PAPER-902'}]}] if market == 'KR' else [],
                'skipped': [{'symbol': 'SYM_SKIP', 'reason': 'BUDGET'}],
                'summary': {'buy_count': 1 if market == 'US' else 0, 'sell_count': 1 if market == 'KR' else 0, 'skip_count': 1},
                'correlation': {'order_batch_ids': [f'{market}-B1'], 'order_plan_ids': [f'{market}-P1'], 'broker_request_ids': [f'{market}-BRQ1'], 'broker_response_ids': [f'{market}-BRS1']},
            }
            events.append({"stage": f"engine.open.{market}", "result": result})
            return result

        def run_intraday_cycle(self):
            market = self.config.market
            result = {
                'buy_plans': [],
                'sell_plans': [{'ticker_id': 128, 'symbol': 'CSGP', 'legs': [{'quantity': 1, 'limit_price': 41.28}], 'execution_correlation': [{'leg_id': 3, 'broker_order_id': 903, 'broker_order_no': 'PAPER-903'}]}] if market == 'US' else [],
                'skipped': [{'symbol': 'AAPL', 'reason': 'RISK'}],
                'summary': {'buy_count': 0, 'sell_count': 1 if market == 'US' else 0, 'skip_count': 1},
                'correlation': {'order_batch_ids': [f'{market}-B2'], 'order_plan_ids': [f'{market}-P2'], 'broker_request_ids': [f'{market}-BRQ2'], 'broker_response_ids': [f'{market}-BRS2']},
            }
            events.append({"stage": f"engine.intraday.{market}", "result": result})
            return result

    monkeypatch.setattr(runbooks, 'HybridTraderEngine', _FakeEngine)

    class _FakePMHistoryService:
        def __init__(self, db):
            self.db = db

        def run_postprocess(self, **kwargs):
            events.append({"stage": 'history.postprocess', **kwargs})
            return _FakeHistorySummary()

    monkeypatch.setattr('live_app.application.history_commands.PMHistoryBatchService', _FakePMHistoryService)

    db = _DummyDB()
    bootstrap_ctx = RunContext(actor='shadow', channel='e2e', invoked_at=datetime(2026, 3, 24, 16, 0, 0, tzinfo=timezone.utc), metadata={'slot': 'US_PREOPEN', 'strategy_version': 'pm-core-v2'})
    trade_ctx_kr = RunContext(actor='shadow', channel='e2e', invoked_at=datetime(2026, 3, 24, 23, 31, 0, tzinfo=timezone.utc), metadata={'slot': 'KR_OPEN', 'strategy_version': 'live-shadow'})
    trade_ctx_us = RunContext(actor='shadow', channel='e2e', invoked_at=datetime(2026, 3, 25, 8, 1, 0, tzinfo=timezone.utc), metadata={'slot': 'US_OPEN', 'strategy_version': 'live-shadow'})
    intraday_ctx_us = RunContext(actor='shadow', channel='e2e', invoked_at=datetime(2026, 3, 25, 15, 10, 0, tzinfo=timezone.utc), metadata={'slot': 'US_INTRADAY', 'strategy_version': 'live-shadow'})
    history_ctx = RunContext(actor='shadow', channel='e2e', invoked_at=datetime(2026, 3, 25, 21, 0, 0, tzinfo=timezone.utc), metadata={'slot': 'POSTPROCESS'})

    bootstrap = await RunBootstrapCommand(db).execute(BootstrapRequest(skip_token_refresh=True, fred_lookback_days=14), bootstrap_ctx)
    kr_open = await RunTradingHybridCommand(db).run_open(market='KR', test_mode=True, ctx=trade_ctx_kr)
    us_open = await RunTradingHybridCommand(db).run_open(market='US', test_mode=True, ctx=trade_ctx_us)
    us_intraday = await RunTradingHybridCommand(db).run_intraday(market='US', test_mode=True, ctx=intraday_ctx_us)
    history = RunHistoryPostprocessCommand(db).execute(backfill_lookback_days=7, backfill_limit=2000, outcome_lookback_days=14, outcome_limit=5000, ctx=history_ctx)

    return {
        'bootstrap': bootstrap.model_dump(),
        'kr_open': kr_open,
        'us_open': us_open,
        'us_intraday': us_intraday,
        'history': {
            'unfilled_scanned': history.unfilled.scanned,
            'unfilled_updated': history.unfilled.updated,
            'outcomes_scanned': history.outcomes.scanned,
            'outcomes_updated': history.outcomes.upserted,
        },
        'events': events,
    }


@pytest.mark.anyio
async def test_shadow_scheduler_replay_sequence(monkeypatch):
    out = await _shadow_run_sequence(monkeypatch)
    assert out['bootstrap']['overall_success'] is True
    assert out['kr_open']['summary']['sell_count'] == 1
    assert out['us_open']['summary']['buy_count'] == 1
    assert out['us_intraday']['summary']['sell_count'] == 1
    assert out['history']['outcomes_updated'] == 6

    stages = [e['stage'] for e in out['events']]
    assert stages[:2] == ['bootstrap.log', 'sync.KR']
    assert 'engine.open.KR' in stages
    assert 'engine.open.US' in stages
    assert 'engine.intraday.US' in stages
    assert stages[-1] == 'history.postprocess'

    us_open = out['us_open']
    assert us_open['buy_plans'][0]['execution_correlation'][0]['broker_order_no'].startswith('PAPER-')
    assert us_open['correlation']['order_batch_ids'] == ['US-B1']
    assert out['us_intraday']['skipped'][0]['reason'] == 'RISK'


@pytest.mark.anyio
async def test_shadow_replay_matches_asis_semantic_anchors(monkeypatch):
    out = await _shadow_run_sequence(monkeypatch)

    assert out['kr_open']['summary']['sell_count'] >= 1
    assert out['us_open']['summary']['buy_count'] >= 1
    assert out['us_intraday']['summary']['sell_count'] >= 1

    us_open_plan = out['us_open']['buy_plans'][0]
    assert us_open_plan['legs'][0]['limit_price'] > us_open_plan['legs'][1]['limit_price']
    assert [leg['quantity'] for leg in us_open_plan['legs']] == [1, 1]

    kr_open_plan = out['kr_open']['sell_plans'][0]
    assert kr_open_plan['legs'][0]['limit_price'] < kr_open_plan['legs'][1]['limit_price']
    assert [leg['quantity'] for leg in kr_open_plan['legs']] == [1, 1]

    assert out['history']['unfilled_updated'] >= 1
