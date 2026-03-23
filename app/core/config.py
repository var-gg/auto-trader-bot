# app/core/config.py
import os
from typing import Dict
from dotenv import load_dotenv

# .env 파일 로드 (로컬 환경에서만 필요, Cloud Run에서는 환경변수 직접 주입)
load_dotenv()

PLACEHOLDER_ACCOUNT_VALUES = {"", "00000000", "00000000-00", "CHANGE_ME", "your_account", "your_account_number"}


def is_cloud_runtime() -> bool:
    return os.getenv("K_SERVICE") is not None or os.getenv("CLOUD_CODE_ENV", "false").lower() == "true"


def require_runtime_env() -> None:
    """Fail fast when required runtime secrets/account settings are missing in deploy environments."""
    if not is_cloud_runtime():
        return

    required = {
        "KIS_APPKEY": os.getenv("KIS_APPKEY", "").strip(),
        "KIS_APPSECRET": os.getenv("KIS_APPSECRET", "").strip(),
        "KIS_CANO": os.getenv("KIS_CANO", "").strip(),
        "KIS_ACNT_PRDT_CD": os.getenv("KIS_ACNT_PRDT_CD", "").strip(),
    }

    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required runtime environment variables: {', '.join(missing)}")

    placeholder_keys = [
        key for key, value in required.items()
        if value in PLACEHOLDER_ACCOUNT_VALUES or value.lower().startswith("your_")
    ]
    if placeholder_keys:
        raise RuntimeError(f"Refusing to start with placeholder runtime values: {', '.join(placeholder_keys)}")

    if os.getenv("KIS_VIRTUAL", "false").lower() == "true":
        virtual_cano = os.getenv("KIS_VIRTUAL_CANO", "").strip()
        if not virtual_cano:
            raise RuntimeError("Missing required runtime environment variable: KIS_VIRTUAL_CANO")
        if virtual_cano in PLACEHOLDER_ACCOUNT_VALUES or virtual_cano.lower().startswith("your_"):
            raise RuntimeError("Refusing to start with placeholder runtime value: KIS_VIRTUAL_CANO")

# ── DB ─────────────────────────────────────────────────────
DB_URL = os.getenv("DB_URL")
DB_SCHEMA = os.getenv("DB_SCHEMA", "trading")

# ── 시간/타임존 ─────────────────────────────────────────────
KST_ZONE = "Asia/Seoul"
MAX_CONTENT_RETRY = int(os.getenv("MAX_CONTENT_RETRY", "2"))

# ── OpenAI ─────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL_CLASSIFY = os.getenv("OPENAI_MODEL_CLASSIFY", "gpt-4o-mini")
MODEL_SUMMARIZE = os.getenv("OPENAI_MODEL_SUMMARIZE", "gpt-5-nano")
MODEL_TICKER_PICK = os.getenv("OPENAI_MODEL_TICKER_PICK", "gpt-4o-mini")
MODEL_ANALYST_AI = os.getenv("OPENAI_MODEL_ANALYST_AI", "gpt-5-nano")
MODEL_BUY_ORDER_AI = os.getenv("OPENAI_MODEL_BUY_ORDER_AI", "gpt-5-mini")

# ── News Risk Multiplier (Premarket) ──────────────────────
# NOTE:
# - PM BUY 뉴스 리스크는 핵심 리스크 정책이다.
# - ENABLE_NEWS_RISK_MULTIPLIER 는 하위호환/관찰용으로만 남기며,
#   live PM BUY execution path 는 이 값에 의존해 비활성화되지 않는다.
ENABLE_NEWS_RISK_MULTIPLIER = os.getenv("ENABLE_NEWS_RISK_MULTIPLIER", "false").lower() == "true"
NEWS_RISK_MAX_MULTIPLIER = float(os.getenv("NEWS_RISK_MAX_MULTIPLIER", "3.0"))
NEWS_RISK_DEFAULT_TTL_MIN = int(os.getenv("NEWS_RISK_DEFAULT_TTL_MIN", "120"))
NEWS_RISK_PROVIDER = os.getenv("NEWS_RISK_PROVIDER", "openai")
NEWS_RISK_MODEL = os.getenv("NEWS_RISK_MODEL", "gpt-5-nano")
NEWS_RISK_API_KEY = os.getenv("NEWS_RISK_API_KEY", os.getenv("OPENAI_API_KEY", ""))
NEWS_RISK_MIN_HEADLINES = int(os.getenv("NEWS_RISK_MIN_HEADLINES", "8"))
NEWS_RISK_GLOBAL_BLEND_WEIGHT = float(os.getenv("NEWS_RISK_GLOBAL_BLEND_WEIGHT", "0.35"))
ENABLE_NEWS_BULL_MULTIPLIER = os.getenv("ENABLE_NEWS_BULL_MULTIPLIER", "false").lower() == "true"
NEWS_BULL_MAX_MULTIPLIER = float(os.getenv("NEWS_BULL_MAX_MULTIPLIER", "1.8"))

# ===== Logging Configuration =====
# 로그 레벨은 CLOUD_CODE_ENV 환경변수로 자동 제어됩니다
# Cloud Code: 모든 로그 ERROR, 로컬: 모든 로그 DEBUG

# ===== GPT Logging =====
GPT_LOG_ENABLED = os.getenv("GPT_LOG_ENABLED", "true").lower() == "true"
GPT_LOG_MAX_PROMPT_CHARS = int(os.getenv("GPT_LOG_MAX_PROMPT_CHARS", "16000"))
GPT_LOG_MAX_RESPONSE_CHARS = int(os.getenv("GPT_LOG_MAX_RESPONSE_CHARS", "16000"))

# ── RSS (필요 시만 유지) ───────────────────────────────────
RSS_SOURCES = [
    {"name": "CNBC Top News",   "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "language":"en","country":"us","timezone":"US/Eastern"},
    {"name": "CNBC World News", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "language":"en","country":"us","timezone":"US/Eastern"},
    #{"name": "Financial Times World",   "url": "https://www.ft.com/world?format=rss",   "language":"en","country":"uk","timezone":"Europe/London"},
    #{"name": "Financial Times Markets", "url": "https://www.ft.com/markets?format=rss", "language":"en","country":"uk","timezone":"Europe/London"},
    {"name": "NPR News", "url": "https://feeds.npr.org/1002/rss.xml", "language":"en","country":"us","timezone":"US/Eastern"},
    {"name": "한국경제 금융", "url": "https://www.hankyung.com/feed/finance", "language":"ko","country":"kr","timezone":"Asia/Seoul"},
    {"name": "한국경제 경제", "url": "https://www.hankyung.com/feed/economy", "language":"ko","country":"kr","timezone":"Asia/Seoul"},
    {"name": "한국경제 IT", "url": "https://www.hankyung.com/feed/it", "language":"ko","country":"kr","timezone":"Asia/Seoul"},
    {"name": "한국경제 부동산", "url": "https://www.hankyung.com/feed/realestate", "language":"ko","country":"kr","timezone":"Asia/Seoul"}
]

# Finnhub (프로젝트에서 실제 사용하지 않으면 .env 포함 값 제거 가능)
FINNHUB_BASE_URL = os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# FRED
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = os.getenv("FRED_BASE_URL", "https://api.stlouisfed.org/fred")
FRED_REQUEST_TIMEOUT = int(os.getenv("FRED_REQUEST_TIMEOUT", "20"))
FRED_CALL_INTERVAL_MS = int(os.getenv("FRED_CALL_INTERVAL_MS", "150"))
FRED_DEFAULT_LOOKBACK_DAYS = int(os.getenv("FRED_DEFAULT_LOOKBACK_DAYS", "30"))

# 프롬프트 그룹(지표 섹터) & 포인트 수
MACRO_PROMPT_GROUPS = [
    {"code":"INFLATION", "name":"Inflation", "series":["CPIAUCSL","CPILFESL","PCEPI"]},
    {"code":"LABOR",     "name":"Labor",     "series":["UNRATE","PAYEMS","JTSJOL"]},
    {"code":"GROWTH",    "name":"Growth",    "series":["GDPC1","INDPRO","RSXFS"]},
    {"code":"RATES",     "name":"Rates",     "series":["FEDFUNDS","T10Y2Y","M2SL"]},
]
MACRO_PROMPT_POINTS_PER_SERIES = int(os.getenv("MACRO_PROMPT_POINTS_PER_SERIES", "6"))

# === KIS (Korea Investment & Securities) OpenAPI ===
KIS_BASE_URL: str = "https://openapi.koreainvestment.com:9443"
KIS_VIRTUAL_BASE_URL: str = "https://openapivts.koreainvestment.com:29443"
KIS_APPKEY: str = os.getenv("KIS_APPKEY", "")
KIS_VIRTUAL_APPKEY: str = os.getenv("KIS_VIRTUAL_APPKEY", "")
KIS_APPSECRET: str = os.getenv("KIS_APPSECRET", "")
KIS_VIRTUAL_APPSECRET: str = os.getenv("KIS_VIRTUAL_APPSECRET", "")
KIS_VIRTUAL: bool = os.getenv("KIS_VIRTUAL", "false").lower() == "true"

# 토큰 TTL (초) - 안전버퍼 포함
KIS_TOKEN_TTL: int = int(os.getenv("KIS_TOKEN_TTL", "3300"))

# 거래소 코드 매핑 (Yahoo Finance → KIS; 국장 확장 대비 포함)
KIS_OVERSEAS_EXCHANGE_MAP: Dict[str, str] = {
    # Yahoo Finance 코드 → KIS 코드
    "NMS": "NAS",      # NASDAQ
    "NYQ": "NYS",      # NYSE
    "ASE": "AMS",      # AMEX
    "ARCA": "ARC",     # NYSE Arca
    "BATS": "BAT",     # CBOE BATS
    "KOE": "KSP",      # 한국거래소 (KOSPI)
    "KOSDAQ": "KSQ",   # KOSDAQ
    # 표준명도 지원 (하위 호환성)
    "NASDAQ": "NAS",
    "NYSE": "NYS", 
    "AMEX": "AMS",
    "KOSPI": "KSP",
}

# 기간별시세 TR ID (실전/모의 환경별로 다를 수 있음)
KIS_TR_ID_DAILYPRICE: str = os.getenv("KIS_TR_ID_DAILYPRICE", "HHDFS76240000")

# 해외주식현재가상세 TR ID
KIS_TR_ID_PRICE_DETAIL: str = os.getenv("KIS_TR_ID_PRICE_DETAIL", "HHDFS00000300")

# 체결기준현재 잔고 조회 TR ID
KIS_TR_ID_PRESENT_BALANCE: str = os.getenv("KIS_TR_ID_PRESENT_BALANCE", "CTRP6504R")

# 주문 TR ID (실전/모의)
KIS_TR_ID_ORDER_BUY_US: str = os.getenv("KIS_TR_ID_ORDER_BUY_US", "TTTT1002U")  # 미국 매수
KIS_TR_ID_ORDER_SELL_US: str = os.getenv("KIS_TR_ID_ORDER_SELL_US", "TTTT1006U")  # 미국 매도
KIS_TR_ID_ORDER_BUY_US_VIRTUAL: str = os.getenv("KIS_TR_ID_ORDER_BUY_US_VIRTUAL", "VTTT1002U")  # 모의 미국 매수
KIS_TR_ID_ORDER_SELL_US_VIRTUAL: str = os.getenv("KIS_TR_ID_ORDER_SELL_US_VIRTUAL", "VTTT1001U")  # 모의 미국 매도

# 국내주식 실적추정 TR ID (실전만 지원)
KIS_TR_ID_ESTIMATE_PERFORM: str = os.getenv("KIS_TR_ID_ESTIMATE_PERFORM", "HHKST668300C0")
KIS_TR_ID_FINANCIAL_RATIO: str = os.getenv("KIS_TR_ID_FINANCIAL_RATIO", "FHKST66430300")
KIS_TR_ID_DIVIDEND_SCHEDULE: str = os.getenv("KIS_TR_ID_DIVIDEND_SCHEDULE", "HHKDB66910200")
KIS_TR_ID_STOCK_BASIC_INFO: str = os.getenv("KIS_TR_ID_STOCK_BASIC_INFO", "CTPF1002R")
KIS_TR_ID_DOMESTIC_DAILY_PRICE: str = os.getenv("KIS_TR_ID_DOMESTIC_DAILY_PRICE", "FHKST01010400")
KIS_TR_ID_DOMESTIC_HOLIDAY_CHECK: str = os.getenv("KIS_TR_ID_DOMESTIC_HOLIDAY_CHECK", "CTCA0903R")


# DART API 설정
DART_API_KEY: str = os.getenv("DART_API_KEY", "")

# Google Vertex AI 설정
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service-account-key.json")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GOOGLE_CLOUD_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION")
VERTEX_AI_EMBEDDING_MODEL = os.getenv("VERTEX_AI_EMBEDDING_MODEL", "textembedding-gecko@001")

# 계좌 정보
KIS_CANO: str = os.getenv("KIS_CANO", "00000000")  # 종합계좌번호 (placeholder)
KIS_VIRTUAL_CANO: str = os.getenv("KIS_VIRTUAL_CANO", "00000000")  # 모의투자 종합계좌번호 (placeholder)
KIS_ACNT_PRDT_CD: str = os.getenv("KIS_ACNT_PRDT_CD", "01")  # 계좌상품코드

# 덮어쓰기 겹침(휴장 보정용)
DAILY_OVERLAP_DAYS: int = int(os.getenv("DAILY_OVERLAP_DAYS", "3"))
# 레이트리밋 완충
KIS_REQUEST_INTERVAL_MS: int = int(os.getenv("KIS_REQUEST_INTERVAL_MS", "120"))

# 뉴스→티커 추출 파라미터
THEME_FULL_SCORE_DEFAULT: float = float(os.getenv("THEME_FULL_SCORE_DEFAULT", "1.0"))  # 만점 테마 기준
NEWS_TIME_WINDOW_HOURS_DEFAULT: int = int(os.getenv("NEWS_TIME_WINDOW_HOURS_DEFAULT", "24"))  # 기본 1일

# 토큰 만료 여유(초) - expires_in에서 뺌 (기본 60초)
KIS_TOKEN_SKEW_SECONDS: int = int(os.getenv("KIS_TOKEN_SKEW_SECONDS", "60"))


# Settings 클래스
class Settings:
    def __init__(self):
        self.DB_URL = DB_URL
        self.DB_SCHEMA = DB_SCHEMA
        self.KST_ZONE = KST_ZONE
        self.MAX_CONTENT_RETRY = MAX_CONTENT_RETRY
        self.OPENAI_API_KEY = OPENAI_API_KEY
        self.MODEL_CLASSIFY = MODEL_CLASSIFY
        self.MODEL_SUMMARIZE = MODEL_SUMMARIZE
        self.MODEL_TICKER_PICK = MODEL_TICKER_PICK
        self.MODEL_ANALYST_AI = MODEL_ANALYST_AI
        self.MODEL_BUY_ORDER_AI = MODEL_BUY_ORDER_AI
        self.ENABLE_NEWS_RISK_MULTIPLIER = ENABLE_NEWS_RISK_MULTIPLIER
        self.NEWS_RISK_MAX_MULTIPLIER = NEWS_RISK_MAX_MULTIPLIER
        self.NEWS_RISK_DEFAULT_TTL_MIN = NEWS_RISK_DEFAULT_TTL_MIN
        self.NEWS_RISK_PROVIDER = NEWS_RISK_PROVIDER
        self.NEWS_RISK_MODEL = NEWS_RISK_MODEL
        self.NEWS_RISK_API_KEY = NEWS_RISK_API_KEY
        self.NEWS_RISK_MIN_HEADLINES = NEWS_RISK_MIN_HEADLINES
        self.NEWS_RISK_GLOBAL_BLEND_WEIGHT = NEWS_RISK_GLOBAL_BLEND_WEIGHT
        self.ENABLE_NEWS_BULL_MULTIPLIER = ENABLE_NEWS_BULL_MULTIPLIER
        self.NEWS_BULL_MAX_MULTIPLIER = NEWS_BULL_MAX_MULTIPLIER
        self.GPT_LOG_ENABLED = GPT_LOG_ENABLED
        self.GPT_LOG_MAX_PROMPT_CHARS = GPT_LOG_MAX_PROMPT_CHARS
        self.GPT_LOG_MAX_RESPONSE_CHARS = GPT_LOG_MAX_RESPONSE_CHARS
        self.RSS_SOURCES = RSS_SOURCES
        self.FINNHUB_BASE_URL = FINNHUB_BASE_URL
        self.FINNHUB_API_KEY = FINNHUB_API_KEY
        self.FRED_API_KEY = FRED_API_KEY
        self.FRED_BASE_URL = FRED_BASE_URL
        self.FRED_REQUEST_TIMEOUT = FRED_REQUEST_TIMEOUT
        self.FRED_CALL_INTERVAL_MS = FRED_CALL_INTERVAL_MS
        self.FRED_DEFAULT_LOOKBACK_DAYS = FRED_DEFAULT_LOOKBACK_DAYS
        self.MACRO_PROMPT_GROUPS = MACRO_PROMPT_GROUPS
        self.MACRO_PROMPT_POINTS_PER_SERIES = MACRO_PROMPT_POINTS_PER_SERIES
        self.KIS_BASE_URL = KIS_BASE_URL
        self.KIS_VIRTUAL_BASE_URL = KIS_VIRTUAL_BASE_URL
        self.KIS_APPKEY = KIS_APPKEY
        self.KIS_VIRTUAL_APPKEY = KIS_VIRTUAL_APPKEY
        self.KIS_APPSECRET = KIS_APPSECRET
        self.KIS_VIRTUAL_APPSECRET = KIS_VIRTUAL_APPSECRET
        self.KIS_VIRTUAL = KIS_VIRTUAL
        self.KIS_TOKEN_TTL = KIS_TOKEN_TTL
        self.KIS_OVERSEAS_EXCHANGE_MAP = KIS_OVERSEAS_EXCHANGE_MAP
        self.KIS_TR_ID_DAILYPRICE = KIS_TR_ID_DAILYPRICE
        self.KIS_TR_ID_PRICE_DETAIL = KIS_TR_ID_PRICE_DETAIL
        self.KIS_TR_ID_PRESENT_BALANCE = KIS_TR_ID_PRESENT_BALANCE
        self.KIS_TR_ID_ORDER_BUY_US = KIS_TR_ID_ORDER_BUY_US
        self.KIS_TR_ID_ORDER_SELL_US = KIS_TR_ID_ORDER_SELL_US
        self.KIS_TR_ID_ORDER_BUY_US_VIRTUAL = KIS_TR_ID_ORDER_BUY_US_VIRTUAL
        self.KIS_TR_ID_ORDER_SELL_US_VIRTUAL = KIS_TR_ID_ORDER_SELL_US_VIRTUAL
        self.KIS_TR_ID_ESTIMATE_PERFORM = KIS_TR_ID_ESTIMATE_PERFORM
        self.KIS_TR_ID_DOMESTIC_DAILY_PRICE = KIS_TR_ID_DOMESTIC_DAILY_PRICE
        self.KIS_TR_ID_DOMESTIC_HOLIDAY_CHECK = KIS_TR_ID_DOMESTIC_HOLIDAY_CHECK
        self.DART_API_KEY = DART_API_KEY
        self.KIS_CANO = KIS_CANO
        self.KIS_VIRTUAL_CANO = KIS_VIRTUAL_CANO
        self.KIS_ACNT_PRDT_CD = KIS_ACNT_PRDT_CD
        self.DAILY_OVERLAP_DAYS = DAILY_OVERLAP_DAYS
        self.KIS_REQUEST_INTERVAL_MS = KIS_REQUEST_INTERVAL_MS
        self.THEME_FULL_SCORE_DEFAULT = THEME_FULL_SCORE_DEFAULT
        self.NEWS_TIME_WINDOW_HOURS_DEFAULT = NEWS_TIME_WINDOW_HOURS_DEFAULT
        self.KIS_TOKEN_SKEW_SECONDS = KIS_TOKEN_SKEW_SECONDS


def get_settings():
    return Settings()
