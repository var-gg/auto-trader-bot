from logging.config import fileConfig
import sys, os

# 환경 변수 로드 (가장 먼저!)
from dotenv import load_dotenv
load_dotenv()

from alembic import context
from sqlalchemy import create_engine
from app.core.db import Base, get_database_url

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 모든 모델 import (항상 전체 import 유지!)
import app.shared.models.ticker
import app.shared.models.ticker_i18n
import app.shared.models.theme
import app.shared.models.theme_i18n
import app.shared.models.ticker_theme
import app.shared.models.sector
import app.shared.models.sector_i18n
import app.shared.models.industry
import app.shared.models.industry_i18n
import app.shared.models.ticker_industry
import app.shared.models.gpt_call_log
import app.shared.models.market_holiday
import app.shared.models.dart_corp_code
import app.core.models.kis_token
import app.features.fundamentals.models.fundamental_snapshot
import app.features.fundamentals.models.dividend_history
import app.features.fundamentals.models.ticker_vector
import app.features.earnings.models.earnings_event
import app.features.news.models.news
import app.features.news.models.news_summary
import app.features.news.models.news_theme
import app.features.news.models.news_ticker
import app.features.news.models.news_exchange
import app.features.news.models.news_vector
import app.features.news.models.news_anchor_vector
import app.features.news.models.kis_news
import app.features.fred.models.macro_group
import app.features.fred.models.macro_group_series
import app.features.fred.models.macro_data_series
import app.features.fred.models.macro_data_series_value
import app.features.fred.models.macro_data_series
import app.features.fred.models.macro_data_series_value
import app.features.yahoo_finance.models.yahoo_index_series
import app.features.yahoo_finance.models.yahoo_index_timeseries
import app.features.marketdata.models.ohlcv_daily
import app.features.recommendation.models.analyst_recommendation
import app.features.portfolio.models.portfolio_snapshot
import app.features.portfolio.models.trading_models
import app.features.portfolio.models.asset_snapshot
import app.features.portfolio.models.trade_realized_pnl
import app.features.signals.models.trend_detection_config
import app.features.signals.models.trend_detection_result
import app.features.signals.models.intraday_signal_detection_config
import app.features.signals.models.intraday_signal_detection_result
import app.features.signals.models.trend_detection_result_vec40
import app.features.trading_hybrid.models.trading_hybrid_models
import app.features.signals.models.similarity_analysis

target_metadata = Base.metadata

# ✅ include_name 대신 include_object 사용
# ---- trading 스키마만 포함하는 필터 (수정판) ----
def include_object(obj, name, type_, reflected, compare_to):
    if type_ == "table":
        # ✅ 항상 obj에서 schema 읽기 (reflected와 무관)
        return getattr(obj, "schema", None) == "trading"

    # 인덱스/제약조건은 소속 테이블의 스키마로 판별
    if type_ in (
        "index",
        "unique_constraint",
        "foreign_key_constraint",
        "primary_key",
        "check_constraint",
    ):
        table = getattr(obj, "table", None)
        if table is not None:
            return getattr(table, "schema", None) == "trading"
        return True  # 테이블 못 찾으면 보수적으로 포함

    return True

def run_migrations_offline() -> None:
    # 앱 런타임과 동일한 DB 해석 경로 사용 (Proxy/환경 우선순위 일치)
    url = str(get_database_url())
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,                 # 스키마 정보를 활용
        version_table_schema="trading",       # 버전 테이블을 trading 스키마에 둠
        include_object=include_object,        # ✅ 여기!
        compare_type=False,
        compare_server_default=False          # 노이즈 줄이기 (원하면 True)
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    # 앱 런타임과 동일한 DB 해석 경로 사용 (Proxy/환경 우선순위 일치)
    connectable = create_engine(get_database_url())
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,             # 스키마 정보를 활용
            version_table_schema="trading",
            include_object=include_object,    # ✅ 여기!
            compare_type=True,
            compare_server_default=False
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
