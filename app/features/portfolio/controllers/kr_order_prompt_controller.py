from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session
import json
import logging

from app.core.db import get_db
from app.features.portfolio.services.kr_buy_order_prompt_service import KrBuyOrderPromptService
from app.features.portfolio.services.kr_sell_order_prompt_service import KrSellOrderPromptService
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portfolio/kr", tags=["[국내주식] portfolio"])

@router.get(
    "/buy-order-prompt",
    response_class=PlainTextResponse,
    summary="국내주식 매수주문 프롬프트 생성",
    description="""
    국내주식 매수주문생성용 프롬프트를 생성합니다.
    
    **처리 과정:**
    1. 자산 스냅샷 조회: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    2. 추천서 조회: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='KR', confidence_score 내림차순)
    3. 현재가 조회: /marketdata/kr/sync/price-detail/{ticker_id} API를 통한 실시간 가격 조회
    4. 프롬프트 구성: 시스템 메시지 + 컨텍스트 데이터 + 정책 가이드라인 + 출력 형식
    5. 프롬프트 반환: PlainTextResponse로 텍스트 직접 반환
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    - 추천서: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='KR', is_latest=True)
    - 현재가: /marketdata/kr/sync/price-detail/{ticker_id} API
    
    **프롬프트 구성:**
    1. 시스템 메시지 (국내주식 매수트레이더AI 역할 정의)
    2. 컨텍스트 데이터 (포트폴리오 + KR LONG 추천서 + 현재가)
    3. 정책 가이드라인 (자본 관리, 리스크 관리, 체결 전략)
    4. 출력 형식 (JSON 스키마)
    
    **국내주식 특화 요소:**
    - 호가단위 준수 (1원, 5원, 10원, 50원, 100원, 500원, 1000원)
    - 거래시간 고려 (정규장 09:00-15:30, 시간외단일가 16:00-18:00)
    - 상한가/하한가 30% 변동폭 제한
    - 한국 주식시장 특성 및 규제 반영
    - 개인투자자 보호 규정 준수
    
    **출력 예시:**
    ```
    ### [SYSTEM PROMPT]
    당신은 국내주식 전문 매수트레이더 AI입니다...
    
    ### [USER INPUT]
    **계좌 정보 (KRW 기준)**
    - 현금 잔고: 1,000,000원
    - 매수가능금액: 800,000원
    ...
    
    [OUTPUT]
    {
      "batch": {
        "mode": "BUY",
        "currency": "KRW",
        "available_cash": 800000,
        "notes": "국내주식 매수 전략"
      },
      "plans": [...]
    }
    ```
    """
)
async def generate_kr_buy_order_prompt(
    db: Session = Depends(get_db)
):
    """국내주식 매수주문 프롬프트를 생성합니다."""
    try:
        logger.info("🏦 Generating KR buy order prompt...")
        
        # 프롬프트 서비스 생성
        prompt_service = KrBuyOrderPromptService(db)
        
        # 프롬프트 생성
        prompt_text = prompt_service.generate_buy_order_prompt()
        
        # [OUTPUT] 섹션과 스키마 추가
        schema_text = json.dumps(ORDER_BATCH_SCHEMA, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt_text}\n\n[OUTPUT]\n{schema_text}"
        
        logger.info("✅ KR buy order prompt generated successfully")
        
        # PlainTextResponse로 프롬프트 텍스트 직접 반환
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to generate KR buy order prompt: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 매수주문 프롬프트 생성 실패: {str(e)}"
        )


@router.get(
    "/sell-order-prompt",
    response_class=PlainTextResponse,
    summary="국내주식 매도주문 프롬프트 생성",
    description="""
    국내주식 매도주문생성용 프롬프트를 생성합니다.
    
    **처리 과정:**
    1. 자산 스냅샷 조회: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    2. 추천서 조회: 현재 보유종목의 최근 추천서 (country='KR', is_latest=True, LONG/SHORT 모두 포함)
    3. 현재가 조회: /marketdata/kr/sync/price-detail/{ticker_id} API를 통한 실시간 가격 조회
    4. 프롬프트 구성: 시스템 메시지 + 컨텍스트 데이터 + 정책 가이드라인 + 출력 형식
    5. 프롬프트 반환: PlainTextResponse로 텍스트 직접 반환
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    - 추천서: analyst_recommendation 테이블의 현재 보유종목 최근 추천서 (country='KR', is_latest=True)
    - 현재가: /marketdata/kr/sync/price-detail/{ticker_id} API
    
    **프롬프트 구성:**
    1. 시스템 메시지 (국내주식 매도트레이더AI 역할 정의)
    2. 컨텍스트 데이터 (포트폴리오 + KR 추천서 + 현재가)
    3. 정책 가이드라인 (자본 관리, 리스크 관리, 체결 전략)
    4. 출력 형식 (JSON 스키마)
    
    **국내주식 특화 요소:**
    - 호가단위 준수 (1원, 5원, 10원, 50원, 100원, 500원, 1000원)
    - 거래시간 고려 (정규장 09:00-15:30, 시간외단일가 16:00-18:00)
    - 상한가/하한가 30% 변동폭 제한
    - 한국 주식시장 특성 및 규제 반영
    - 개인투자자 보호 규정 준수
    
    **출력 예시:**
    ```
    ### [SYSTEM PROMPT]
    당신은 국내주식 전문 매도트레이더 AI입니다...
    
    ### [USER INPUT]
    **계좌 정보 (KRW 기준)**
    - 현금 잔고: 1,000,000원
    - 총 평가액: 5,000,000원
    ...
    
    [OUTPUT]
    {
      "batch": {
        "mode": "SELL",
        "currency": "KRW",
        "available_cash": 5000000,
        "notes": "국내주식 매도 전략"
      },
      "plans": [...]
    }
    ```
    """
)
async def generate_kr_sell_order_prompt(
    db: Session = Depends(get_db)
):
    """국내주식 매도주문 프롬프트를 생성합니다."""
    try:
        logger.info("🏦 Generating KR sell order prompt...")
        
        # 프롬프트 서비스 생성
        prompt_service = KrSellOrderPromptService(db)
        
        # 프롬프트 생성
        prompt_text = prompt_service.generate_sell_order_prompt()
        
        # [OUTPUT] 섹션과 스키마 추가
        schema_text = json.dumps(ORDER_BATCH_SCHEMA, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt_text}\n\n[OUTPUT]\n{schema_text}"
        
        logger.info("✅ KR sell order prompt generated successfully")
        
        # PlainTextResponse로 프롬프트 텍스트 직접 반환
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to generate KR sell order prompt: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 매도주문 프롬프트 생성 실패: {str(e)}"
        )
