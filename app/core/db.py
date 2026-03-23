# app/core/db.py
import os
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine, MetaData, URL, event, text
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import DB_URL, DB_SCHEMA

logger = logging.getLogger(__name__)

# trading 스키마 지정
metadata = MetaData(schema=DB_SCHEMA)
Base = declarative_base(metadata=metadata)

# Cloud Code 환경 감지
# CLOUD_CODE_ENV가 "true"로 설정되면 is_cloud_code = True
# 환경변수가 없거나 "false"면 is_cloud_code = False (로컬 환경)
is_cloud_code = os.getenv("CLOUD_CODE_ENV", "false").lower() == "true"


def get_env_with_priority(key: str, default: str = None) -> str:
    """
    환경변수 가져오기 (실제 우선순위: VS Code 디버거 > 시스템 환경변수 > .env 파일)
    """
    value = os.getenv(key)
    if value:
        return value
    
    if default:
        return default
    
    return None


def fix_password_url_encoding(db_url: str) -> str:
    """
    비밀번호에 특수문자가 포함된 URL의 인코딩 문제를 해결
    예: password!@# -> password%21%40%23
    """
    if "!@#" in db_url or any(char in db_url for char in ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')']):
        logger.warning("⚠️ 비밀번호에 특수문자가 포함되어 있습니다.")
        logger.warning("⚠️ URL 인코딩을 적용합니다.")
        
        # 비밀번호 부분을 찾아서 인코딩
        import re
        pattern = r'://([^:]+):([^@]+)@'
        match = re.search(pattern, db_url)
        
        if match:
            username = match.group(1)
            password = match.group(2)
            encoded_password = quote_plus(password)
            
            # 인코딩된 비밀번호로 URL 교체
            fixed_url = db_url.replace(f":{password}@", f":{encoded_password}@")
            logger.debug(f"  • 원본 비밀번호: {password}")
            logger.debug(f"  • 인코딩된 비밀번호: {encoded_password}")
            logger.debug(f"  • 수정된 URL: {fixed_url}")
            
            return fixed_url
    
    return db_url


def get_database_url():
    """
    환경에 따라 적절한 데이터베이스 URL을 반환
    - Cloud Run: Unix socket을 통한 Cloud SQL 직접 연결
    - 로컬: Cloud SQL Proxy 사용 (PROXY_HOST:PROXY_PORT 환경변수 지원)
    """
    
    logger.info(f"🔧 데이터베이스 연결 설정:")
    logger.info(f"  • 환경: {'Cloud Run' if is_cloud_code else '로컬'}")
    
    # 환경변수에서 DB 정보 가져오기
    DB_USER = get_env_with_priority("DB_USER")
    DB_PASS = get_env_with_priority("DB_PASS")
    DB_NAME = get_env_with_priority("DB_NAME")
    
    if is_cloud_code:
        # Cloud Run 환경: Unix socket 사용
        INSTANCE_CONNECTION_NAME = get_env_with_priority("INSTANCE_CONNECTION_NAME")
        
        logger.info(f"  • 사용자: {DB_USER}")
        logger.info(f"  • 데이터베이스: {DB_NAME}")
        logger.info(f"  • 인스턴스 연결명: {INSTANCE_CONNECTION_NAME}")
        
        # 필수 환경변수 검증
        if not all([DB_USER, DB_PASS, DB_NAME, INSTANCE_CONNECTION_NAME]):
            logger.error("❌ Cloud Run 환경에서 필수 환경변수 누락")
            raise ValueError(
                "Cloud Run 환경에서는 다음 환경변수가 필요합니다: "
                "DB_USER, DB_PASS, DB_NAME, INSTANCE_CONNECTION_NAME"
            )
        
        # Unix socket을 통한 연결
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            query={"host": f"/cloudsql/{INSTANCE_CONNECTION_NAME}"}
        )
        
        logger.info(f"🔗 Cloud Run 연결 URL: {url.render_as_string(hide_password=True)}")
        return url
    else:
        # 로컬 환경: Cloud SQL Proxy 사용
        PROXY_HOST = get_env_with_priority("PROXY_HOST", "127.0.0.1")
        PROXY_PORT = get_env_with_priority("PROXY_PORT", "5432")
        
        logger.info(f"  • 사용자: {DB_USER}")
        logger.info(f"  • 데이터베이스: {DB_NAME}")
        logger.info(f"  • 프록시 호스트: {PROXY_HOST}")
        logger.info(f"  • 프록시 포트: {PROXY_PORT}")
        
        if not all([DB_USER, DB_PASS, DB_NAME]):
            # DB_URL 폴백 (기존 방식 지원)
            if DB_URL:
                logger.warning(f"⚠️ [Local] Using DB_URL fallback (환경변수 DB_USER/DB_PASS/DB_NAME 미설정)")
                logger.debug(f"[Local] DB_URL: {DB_URL}")
                # DB_URL에 특수문자가 있을 수 있으므로 인코딩 처리
                fixed_url = fix_password_url_encoding(DB_URL)
                return fixed_url
            else:
                logger.error("❌ 로컬 환경에서 필수 환경변수 누락")
                raise ValueError(
                    "로컬 환경에서는 다음 환경변수가 필요합니다: "
                    "DB_USER, DB_PASS, DB_NAME (그리고 선택적으로 PROXY_HOST, PROXY_PORT)"
                )
        
        # Cloud SQL Proxy를 통한 연결
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASS,
            host=PROXY_HOST,
            port=int(PROXY_PORT),
            database=DB_NAME
        )
        
        logger.info(f"🔗 [Local] Using Cloud SQL Proxy at {PROXY_HOST}:{PROXY_PORT}")
        logger.debug(f"🔗 Final DB URL: {repr(url.render_as_string(hide_password=False))}")
        
        return url

# 데이터베이스 URL 생성
# 로깅이 설정되지 않았을 수 있으므로 기본 로깅 사용
import logging
_temp_logger = logging.getLogger(__name__)
if not _temp_logger.handlers:
    # 로깅이 설정되지 않았으면 기본 핸들러 추가
    _temp_logger.setLevel(logging.INFO)
    if not _temp_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"))
        _temp_logger.addHandler(handler)

_temp_logger.info("🚀 데이터베이스 엔진 초기화 시작...")
_temp_logger.info("🔍 환경변수 상태 확인:")
_temp_logger.info(f"  • CLOUD_CODE_ENV: {os.getenv('CLOUD_CODE_ENV', 'NOT_SET')}")
_temp_logger.info(f"  • DB_USER: {os.getenv('DB_USER', 'NOT_SET')}")
_temp_logger.info(f"  • DB_PASS: {'***' if os.getenv('DB_PASS') else 'NOT_SET'}")
_temp_logger.info(f"  • DB_NAME: {os.getenv('DB_NAME', 'NOT_SET')}")
_temp_logger.info(f"  • PROXY_HOST: {os.getenv('PROXY_HOST', 'NOT_SET (기본값: 127.0.0.1)')}")
_temp_logger.info(f"  • PROXY_PORT: {os.getenv('PROXY_PORT', 'NOT_SET (기본값: 5432)')}")
_temp_logger.info("  • 우선순위: VS Code 디버거(launch.json) > 시스템 환경변수 > .env 파일")

try:
    database_url = get_database_url()
    _temp_logger.info(f"✅ 데이터베이스 URL 생성 완료")
except Exception as e:
    _temp_logger.error(f"❌ 데이터베이스 URL 생성 실패: {e}", exc_info=True)
    raise

# echo 설정 (Cloud Code에서는 echo=False, 로컬에서는 echo=True)
db_echo = not is_cloud_code

# SQLAlchemy 엔진 & 세션
# DB 연결 타임아웃 설정 (로컬 환경에서 DB 연결 실패 시 빠른 실패)
connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))  # 기본 5초
engine = create_engine(
    database_url, 
    echo=False, 
    future=True,
    pool_pre_ping=True,  # 연결 상태 확인
    pool_recycle=3600,   # 1시간마다 연결 재생성
    connect_args={
        "connect_timeout": connect_timeout,  # 연결 타임아웃 (초)
        "options": "-c statement_timeout=10000"  # 쿼리 실행 타임아웃 10초
    },
    pool_timeout=10,  # 풀에서 연결 가져오기 타임아웃 (초)
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

@event.listens_for(engine, "connect")
def set_search_path(dbapi_connection, connection_record):
    """데이터베이스 연결 시 search_path를 trading 스키마로 설정"""
    import logging
    logger = logging.getLogger(__name__)
    with dbapi_connection.cursor() as cur:
        # trading 우선!
        cur.execute("SET search_path TO trading, public;")

# FastAPI Depends() 주입용 세션
def get_db():
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.debug("DB 세션 생성 시작")
        db = SessionLocal()
        logger.debug("DB 세션 생성 완료")
        
        # 연결 테스트 (빠른 실패를 위해)
        try:
            db.execute(text("SELECT 1"))
            logger.debug("DB 연결 테스트 성공")
        except Exception as e:
            logger.error(f"DB 연결 테스트 실패: {e}", exc_info=True)
            db.close()
            raise
        
        yield db
    except Exception as e:
        logger.error(f"DB 세션 오류: {e}", exc_info=True)
        raise
    finally:
        try:
            db.close()
            logger.debug("DB 세션 종료")
        except:
            pass
