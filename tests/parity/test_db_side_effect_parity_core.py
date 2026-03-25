from __future__ import annotations

from datetime import datetime, timedelta, timezone, date

from app.features.premarket.repositories.position_maturity_repository import PositionMaturityRepository
from app.features.premarket.services.headline_risk_service import HeadlineRiskService
from app.features.premarket.services.pm_history_batch_service import PMHistoryBatchService
from app.features.trading_hybrid.repositories import order_repository
from app.features.trading_hybrid.repositories import portfolio_repository


class _FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping
        for k, v in mapping.items():
            setattr(self, k, v)


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def mappings(self):
        class _M:
            def __init__(self, rows):
                self._rows = rows
            def first(self):
                return self._rows[0] if self._rows else None
            def all(self):
                return self._rows
            def __iter__(self):
                return iter(self._rows)
        return _M(self._rows)

    def scalar(self):
        return self._scalar_value


class _NoopQuery:
    def filter(self, *args, **kwargs):
        return self
    def first(self):
        return None
    def scalar(self):
        return 3


class _CaptureDB:
    def __init__(self, responders=None):
        self.responders = responders or []
        self.calls = []
        self.commits = 0

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append({"sql": sql, "params": params})
        for predicate, result in self.responders:
            if predicate(sql, params):
                return result(sql, params) if callable(result) else result
        return _FakeResult()

    def query(self, *args, **kwargs):
        return _NoopQuery()

    def commit(self):
        self.commits += 1


def test_order_write_set_preserves_next_read_keys(monkeypatch):
    db = _CaptureDB(responders=[
        (lambda sql, params: 'INSERT INTO trading.order_batch' in sql, _FakeResult(scalar_value=7001)),
        (lambda sql, params: 'INSERT INTO trading.order_plan' in sql, _FakeResult(scalar_value=7101)),
        (lambda sql, params: 'INSERT INTO trading.order_leg' in sql, lambda sql, params: _FakeResult(scalar_value=7201 if params['qty'] == 1 else 7202)),
    ])
    submitted = []
    monkeypatch.setattr(order_repository, '_submit_leg_to_broker', lambda db_, leg_id, test_mode=False: submitted.append({"leg_id": leg_id, "test_mode": test_mode}))

    batch_id = order_repository.create_order_batch(db, datetime.now(timezone.utc), 'BUY', 'USD', {"route": "/api/trading-hybrid/us/open", "slot": "US_OPEN"})
    plan_id = order_repository.create_plan_with_legs(db, batch_id, {
        "ticker_id": 15365,
        "reference": {"recommendation_id": 34},
        "note": "replay parity plan",
        "reverse_breach_day": 5,
        "legs": [
            {"type": "LIMIT", "side": "BUY", "quantity": 1, "limit_price": 107.10},
            {"type": "LIMIT", "side": "BUY", "quantity": 2, "limit_price": 104.25},
        ],
    }, 'BUY', test_mode=True)

    assert batch_id == 7001
    assert plan_id == 7101
    plan_insert = next(c for c in db.calls if 'INSERT INTO trading.order_plan' in c['sql'])
    assert plan_insert['params']['ticker_id'] == 15365
    assert plan_insert['params']['rid'] == 34
    assert plan_insert['params']['rbd'] == 5
    assert plan_insert['params']['action'] == 'BUY'
    leg_inserts = [c for c in db.calls if 'INSERT INTO trading.order_leg' in c['sql']]
    assert len(leg_inserts) == 2
    assert submitted == [{"leg_id": 7201, "test_mode": True}, {"leg_id": 7202, "test_mode": True}]


def test_pending_order_read_meaning_depends_on_submitted_without_fill():
    now = datetime.now(timezone.utc)
    db = _CaptureDB(responders=[
        (lambda sql, params: 'WITH latest_submitted_orders AS' in sql and 'FROM trading.order_leg ol' in sql,
         _FakeResult(rows=[_FakeRow({
             'broker_order_id': 991,
             'leg_id': 7201,
             'plan_id': 7101,
             'ticker_id': 15365,
             'symbol': 'AAPL',
             'side': 'BUY',
             'quantity': 1,
             'limit_price': 107.10,
             'order_type': 'LIMIT',
             'order_number': 'OD001',
             'submitted_at': now,
             'exchange': 'NASDAQ',
             'country': 'US',
         })]))
    ])

    rows = portfolio_repository.load_pending_orders(db, 'US')
    assert len(rows) == 1
    assert rows[0]['broker_order_id'] == 991
    assert rows[0]['plan_id'] == 7101
    assert rows[0]['symbol'] == 'AAPL'


def test_pm_order_execution_history_supports_outcome_postprocess():
    now = datetime.now(timezone.utc)
    inserts = []
    db = _CaptureDB(responders=[
        (lambda sql, params: 'FROM trading.pm_order_execution_history' in sql and 'COALESCE(avg_fill_price' in sql,
         _FakeResult(rows=[_FakeRow({'id': 1, 'run_id': 34, 'ticker_id': 15365, 'symbol': 'AAPL', 'entry_price': 100.0, 'entry_date': date(2026, 3, 20)})])),
        (lambda sql, params: 'WITH ranked AS' in sql, _FakeResult(rows=[_FakeRow({'close': 103.0})])),
        (lambda sql, params: 'INSERT INTO trading.pm_outcome_tplus_history' in sql,
         lambda sql, params: inserts.append(params) or _FakeResult()),
    ])

    summary = PMHistoryBatchService(db).compute_tplus_outcomes(lookback_days=14, limit=500)
    assert summary.scanned == 1
    assert summary.upserted == 3
    assert summary.skipped_missing_price == 0
    assert len(inserts) == 3
    assert {x['horizon_days'] for x in inserts} == {1, 3, 5}
    assert all(x['run_id'] == 34 for x in inserts)
    assert all(x['ticker_id'] == 15365 for x in inserts)
    assert db.commits == 1


def test_position_maturity_read_path_depends_on_filled_buy_and_reverse_breach_day(monkeypatch):
    class _DummyOptunaRepo:
        def __init__(self, db):
            pass
        def get_latest_promoted_config(self):
            class _Cfg:
                future = 12
            return _Cfg()

    monkeypatch.setattr('app.features.premarket.repositories.position_maturity_repository.OptunaRepository', _DummyOptunaRepo)
    db = _CaptureDB(responders=[
        (lambda sql, params: 'SELECT op.reverse_breach_day' in sql,
         _FakeResult(rows=[_FakeRow({'reverse_breach_day': 5})])),
        (lambda sql, params: 'SELECT MAX(of.filled_at) as last_filled_at' in sql,
         _FakeResult(rows=[_FakeRow({'last_filled_at': datetime(2026, 3, 20, tzinfo=timezone.utc)})])),
    ])
    repo = PositionMaturityRepository(db)
    monkeypatch.setattr(repo, 'count_business_days_held', lambda ticker_id, from_date: 5)

    out = repo.check_position_maturity(15365, 'AAPL')
    assert out['future_days'] == 5
    assert out['maturity_source'] == 'reverse_breach_day'
    assert out['business_days_held'] == 5
    assert out['is_matured'] is True


def test_headline_risk_latest_active_snapshot_requires_ttl_viable_write():
    now = datetime.now(timezone.utc)
    active = {'id': 11, 'market_scope': 'US', 'as_of_at': now - timedelta(minutes=5), 'expires_at': now + timedelta(minutes=10), 'discount_multiplier': 1.25}
    db = _CaptureDB(responders=[
        (lambda sql, params: 'FROM trading.market_headline_risk_snapshot' in sql and 'expires_at > NOW()' in sql,
         _FakeResult(rows=[active]))
    ])

    snap = HeadlineRiskService(db).get_latest_active_snapshot('US')
    assert snap['id'] == 11
    assert snap['discount_multiplier'] == 1.25
