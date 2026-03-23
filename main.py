from fastapi import FastAPI
from app.controllers.api_router import api_router
from app.core.logging_config import setup_logging
import logging

# --- 로깅 초기화 ---
setup_logging()
logger = logging.getLogger("main")

# --- FastAPI 앱 정의 ---
app = FastAPI(
    title="Auto Trader Bot",
    description="""
    ## 🤖 AI 기반 주식 자동투자 시스템
    
    ### 주요 기능
    - **뉴스 파이프라인**: RSS 수집 → AI 분석 → 테마/티커 매핑
    - **경제지표 수집**: FRED API 기반 매크로 데이터 수집
    - **어닝 데이터**: 실적 일정/결과 수집 및 분석
    - **주가데이터**: KIS API 기반 일봉/분봉 수집
    - **기술지표**: MA, RSI, 볼린저밴드, 거래량 분석
    
    ### 워크플로우
    1. 뉴스 수집 및 AI 분석
    2. 경제지표 및 어닝 데이터 수집
    3. 주가데이터 수집 및 기술지표 계산
    4. 애널리스트 AI를 통한 포지션 추천 (개발 예정)
    5. 트레이더 AI를 통한 실제 매매 실행 (개발 예정)
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "Auto Trader Bot Team",
        "email": "support@autotrader.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    }
)

# --- 기본 테스트 엔드포인트 ---
@app.get(
    "/",
    summary="서버 상태 확인",
    description="Auto Trader Bot API 서버의 실행 상태를 확인합니다.",
    response_description="서버 상태 정보와 버전 정보를 반환합니다."
)
async def root():
    """서버 상태 확인"""
    return {"message": "Auto Trader Bot API is running!", "version": "1.0.0"}

@app.get(
    "/health",
    summary="헬스 체크",
    description="API 서버의 건강 상태를 확인합니다. 시스템 모니터링용으로 사용됩니다.",
    response_description="서버의 건강 상태와 현재 시간을 반환합니다."
)
async def health_check():
    """헬스 체크"""
    return {"status": "healthy", "timestamp": "2025-01-26"}

# --- 라우터 등록 ---
app.include_router(api_router)

# --- 이벤트 훅: 서버 시작/종료 로그 ---
@app.on_event("startup")
async def startup_event():
    logger.debug("🚀 Auto Trader Bot service started")
    
    # 런타임 필수 env/secret 검증 (Cloud Run 등 배포 환경에서는 fail-fast)
    from app.core import config as settings
    settings.require_runtime_env()

    # KIS 설정값 로그 출력
    logger.info("=== GPT Configuration ===")
    logger.info(f"MODEL_SUMMARIZE: {settings.MODEL_SUMMARIZE}")
    logger.info(f"MODEL_ANALYST_AI: {settings.MODEL_ANALYST_AI}")
    logger.info("=========================")

@app.on_event("shutdown")
async def shutdown_event():
    logger.debug("🛑 Auto Trader Bot service stopped")
