# app/core/logging_config.py
import logging.config
import os

def setup_logging():
    """
    로깅 설정
    - Cloud Code 배포: 모든 로그 ERROR 레벨
    - 로컬 테스트: 모든 로그 DEBUG 레벨
    """
    
    # Cloud Code 환경 감지
    is_cloud_code = os.getenv("CLOUD_CODE_ENV", "false").lower() == "true"
    
    # 환경별 로그 레벨 설정
    if is_cloud_code:
        # Cloud Code 배포: 모든 로그 ERROR만
        log_level = "ERROR"
        db_log_level = "ERROR"
        app_log_level = "ERROR"
        uvicorn_log_level = "ERROR"
    else:
        # 로컬 테스트: 모든 로그 DEBUG
        log_level = "INFO"
        db_log_level = "INFO"
        app_log_level = "INFO" 
        uvicorn_log_level = "ERROR"
    
    # 로그 포맷터 설정
    formatters = {
        "default": {
            "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        },
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        },
        "json": {
            "format": '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
        }
    }
    
    # 핸들러 설정
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json" if is_cloud_code else "detailed",
            "stream": "ext://sys.stdout",
        },
    }
    
    # Cloud Code 환경에서는 JSON 로그 추가
    if is_cloud_code:
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/app/logs/app.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
            "formatter": "json",
        }
    
    # 로거 설정
    loggers = {
        "app": {
            "level": app_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "app.core": {
            "level": app_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "app.core.db": {
            "level": app_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "level": "ERROR",
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "sqlalchemy.pool": {
            "level": db_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "sqlalchemy.dialects": {
            "level": db_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "alembic": {
            "level": log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "uvicorn": {
            "level": uvicorn_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "uvicorn.access": {
            "level": uvicorn_log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
        "fastapi": {
            "level": "ERROR",  # FastAPI 워닝 숨기기
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
            "propagate": False,
        },
    }
    
    # 로깅 설정 적용
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "handlers": handlers,
        "loggers": loggers,
        "root": {
            "level": log_level,
            "handlers": ["console"] + (["file"] if is_cloud_code else []),
        },
    })
    
    # Cloud Code 환경에서 로그 디렉토리 생성
    if is_cloud_code:
        os.makedirs("/app/logs", exist_ok=True)
