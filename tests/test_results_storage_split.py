from backtest_app.results.sql_models import LiveMetricRecord, LiveRunRecord, LiveTradeRecord, ResearchMetricRecord, ResearchRunRecord, ResearchTradeRecord
from backtest_app.results.store import SqlResultStore


def test_sql_models_are_physically_split_by_schema_and_table():
    assert ResearchRunRecord.__table__.schema == "research_results"
    assert ResearchTradeRecord.__table__.schema == "research_results"
    assert ResearchMetricRecord.__table__.schema == "research_results"
    assert LiveRunRecord.__table__.schema == "live_results"
    assert LiveTradeRecord.__table__.schema == "live_results"
    assert LiveMetricRecord.__table__.schema == "live_results"
    assert ResearchRunRecord.__tablename__ != LiveRunRecord.__tablename__


def test_sql_store_selects_live_models_without_research_namespace():
    store = SqlResultStore("sqlite+pysqlite:///:memory:", namespace="live")
    assert store.RunRecord is LiveRunRecord
    assert store.TradeRecord is LiveTradeRecord
    assert store.MetricRecord is LiveMetricRecord
