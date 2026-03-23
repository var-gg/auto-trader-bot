# app/features/portfolio/controllers/kis_controller.py

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field

from app.core.db import get_db
from app.features.portfolio.services.us_portfolio_service import PortfolioService
from app.features.portfolio.services.us_buy_order_prompt_service import BuyOrderPromptService
from app.features.portfolio.services.us_sell_order_prompt_service import SellOrderPromptService
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA
from app.features.portfolio.services.us_buy_order_ai_service import BuyOrderAIService
from app.features.portfolio.services.us_sell_order_ai_service import SellOrderAIService

router = APIRouter(prefix="/portfolio", tags=["[미국주식] portfolio"])


class BuyOrderBatchResponse(BaseModel):
    """매수주문 배치 생성 및 실행 응답 모델"""
    batch_id: Optional[int] = Field(None, description="배치 ID (휴장시 None)")
    asof_kst: str = Field(..., description="생성 시각 (KST)")
    mode: str = Field(..., description="배치 모드 (BUY)")
    currency: str = Field(..., description="계좌 통화")
    available_cash: float = Field(..., description="가용 현금")
    notes: str = Field(..., description="설계 메모")
    plans_count: int = Field(..., description="실행 계획 수")
    skipped_count: int = Field(..., description="제외된 계획 수")
    executed_orders: List[Dict[str, Any]] = Field(..., description="실행된 주문 목록")
    is_market_closed: bool = Field(..., description="휴장 여부")
    generated_at: str = Field(..., description="생성 시간 (ISO 형식)")
    message: Optional[str] = Field(None, description="휴장시 메시지")


class SellOrderBatchResponse(BaseModel):
    """매도주문 배치 생성 및 실행 응답 모델"""
    batch_id: Optional[int] = Field(None, description="배치 ID (휴장시 None)")
    asof_kst: str = Field(..., description="생성 시각 (KST)")
    mode: str = Field(..., description="배치 모드 (SELL)")
    currency: str = Field(..., description="계좌 통화")
    available_cash: float = Field(..., description="가용 현금")
    notes: str = Field(..., description="설계 메모")
    plans_count: int = Field(..., description="실행 계획 수")
    skipped_count: int = Field(..., description="제외된 계획 수")
    executed_orders: List[Dict[str, Any]] = Field(..., description="실행된 주문 목록")
    is_market_closed: bool = Field(..., description="휴장 여부")
    generated_at: str = Field(..., description="생성 시간 (ISO 형식)")
    message: Optional[str] = Field(None, description="휴장시 메시지")


@router.get(
    "/kis/balance",
    summary="미국주식 포트폴리오 잔고 조회",
    description="""
    KIS API를 통해 미국주식 포트폴리오의 현재 잔고를 조회합니다.
    
    **처리 과정:**
    1. KIS API 호출: 체결기준 현재 잔고 조회 (미국주식 전용)
    2. 데이터 검증: API 응답 성공 여부 확인
    3. 스냅샷 저장: 성공 시 포트폴리오 스냅샷을 데이터베이스에 저장
    4. 응답 반환: KIS API 원본 응답을 그대로 반환 (bypass)
    
    **파라미터:**
    - wcrc_frcr_dvsn_cd: "01" (원화 기준)
    - natn_cd: "840" (미국)
    - tr_mket_cd: "00" (전체 거래시장)
    - inqr_dvsn_cd: "00" (체결기준 조회)
    
    **데이터 소스:**
    - KIS API: 체결기준 현재 잔고 조회
    - 저장 테이블: kis_portfolio_snapshot, kis_position_execbasis, kis_currency_summary, kis_account_totals
    
    **특징:**
    - 미국주식 전용 잔고 조회
    - 실시간 포트폴리오 스냅샷 생성
    - 체계적인 포지션 및 통화별 요약 데이터 저장
    
    **사용 예시:**
    - `GET /portfolio/kis/balance` (미국주식 포트폴리오 잔고 조회)
    """,
    response_description="KIS API 원본 응답 (bypass)"
)
async def get_present_balance(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """미국주식 포트폴리오의 현재 잔고를 조회합니다."""
    try:
        service = PortfolioService(db)
        result = service.get_present_balance(
            wcrc_frcr_dvsn_cd="01",  # 원화 기준
            natn_cd="840",              # 전체 국가
            tr_mket_cd="00",           # 전체 거래시장
            inqr_dvsn_cd="00"        # 체결기준 조회
        )
        return result
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Balance inquiry failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"잔고 조회 실패: {str(e)}")


@router.post(
    "/kis/order",
    summary="미국주식 주문 실행",
    description="""
    KIS API를 통해 미국주식을 주문합니다.
    
    **처리 과정:**
    1. 파라미터 검증: 주문 방식별 필수 파라미터 확인
    2. KIS API 호출: 실제 주문 실행
    3. 브로커 주문 저장: 주문 결과를 broker_order 테이블에 저장
    4. 응답 반환: KIS API 원본 응답을 그대로 반환 (bypass)
    
    **주문 방식별 파라미터:**
    - **LIMIT**: 지정가 주문 - `price` 필수
    - **MARKET**: 시장가 주문 - `price` 무시됨
    - **LOC**: 장마감시장가 주문 - `price` 필수
    
    **데이터 소스:**
    - KIS API: 미국주식 주문 실행
    - 저장 테이블: broker_order (주문 결과 저장)
    
    **특징:**
    - 미국주식 전용 주문 (NASD/NYSE)
    - 실시간 주문 실행 및 결과 추적
    - 체계적인 주문 이력 관리
    
    **사용 예시:**
    - `POST /portfolio/kis/order?order_type=buy&symbol=AAPL&quantity=10&price=150.00&order_method=LIMIT&exchange=NASD`
    """,
    response_description="KIS API 원본 응답 (bypass)"
)
async def order_stock(
    order_type: str = Query(..., description="주문유형 (buy/sell)", regex="^(buy|sell)$"),
    symbol: str = Query(..., description="종목코드 (예: AAPL)", min_length=1, max_length=10),
    quantity: str = Query(..., description="주문수량", regex="^[1-9][0-9]*$"),
    price: Optional[str] = Query(None, description="주문단가 (LIMIT/LOC일 때 필수, MARKET일 때 생략)"),
    order_method: str = Query("LIMIT", description="주문방식", regex="^(LIMIT|MARKET|LOC)$"),
    exchange: str = Query("NASD", description="거래소코드 (NASD/NYSE)", regex="^(NASD|NYSE)$"),
    leg_id: Optional[int] = Query(None, description="주문레그 ID (선택사항)"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """미국주식을 주문합니다."""
    try:
        # 파라미터 검증
        if order_method in ["LIMIT", "LOC"] and not price:
            raise HTTPException(
                status_code=400, 
                detail=f"{order_method} 주문에는 price 파라미터가 필수입니다."
            )
        
        service = PortfolioService(db)
        result = service.order_stock(
            order_type=order_type,
            symbol=symbol,
            quantity=quantity,
            price=price,
            order_method=order_method,
            exchange=exchange,
            leg_id=leg_id
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Stock order failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"주문 실패: {str(e)}")


@router.get(
    "/buy-order-prompt",
    response_class=PlainTextResponse,
    summary="미국주식 매수주문 프롬프트 생성",
    description="""
    미국주식 매수주문생성용 프롬프트를 생성합니다.
    
    **처리 과정:**
    1. 자산 스냅샷 조회: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    2. 추천서 조회: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='US', confidence_score 내림차순)
    3. 현재가 조회: /marketdata/sync/price-detail API를 통한 실시간 가격 조회
    4. 프롬프트 구성: 시스템 메시지 + 컨텍스트 데이터 + 정책 가이드라인 + 출력 형식
    5. 프롬프트 반환: PlainTextResponse로 텍스트 직접 반환
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    - 추천서: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='US', is_latest=True)
    - 현재가: /marketdata/sync/price-detail API
    
    **프롬프트 구성:**
    1. 시스템 메시지 (매수트레이더AI 역할 정의)
    2. 컨텍스트 데이터 (포트폴리오 + LONG 추천서 + 현재가)
    3. 정책 가이드라인 (자본 관리, 리스크 관리, 체결 전략)
    4. 출력 형식 (JSON 스키마)
    
    **특징:**
    - 미국주식 전용 매수주문 프롬프트
    - 실시간 데이터 기반 프롬프트 생성
    - 체계적인 매수 전략 수립 지원
    
    **사용 예시:**
    - `GET /portfolio/buy-order-prompt` (미국주식 매수주문 프롬프트 생성)
    """,
    response_description="생성된 매수주문 프롬프트 텍스트 (줄바꿈 포함)"
)
def generate_buy_order_prompt(
    db: Session = Depends(get_db)
):
    """미국주식 매수주문 프롬프트를 생성합니다."""
    try:
        buy_order_prompt_service = BuyOrderPromptService(db)
        prompt_text = buy_order_prompt_service.generate_buy_order_prompt()
        
        # [OUTPUT] 섹션과 스키마 추가
        import json
        schema_text = json.dumps(ORDER_BATCH_SCHEMA, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt_text}\n\n[OUTPUT]\n{schema_text}"
        
        # PlainTextResponse로 프롬프트 텍스트 직접 반환
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"매수주문 프롬프트 생성 중 오류가 발생했습니다: {str(e)}"
        )


@router.get(
    "/sell-order-prompt",
    response_class=PlainTextResponse,
    summary="미국주식 매도주문 프롬프트 생성",
    description="""
    미국주식 매도주문생성용 프롬프트를 생성합니다.
    
    **처리 과정:**
    1. 자산 스냅샷 조회: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    2. 보유종목 추천서 조회: 현재 보유종목의 최근 추천서 (country='US', LONG/SHORT 구분없이)
    3. 현재가 조회: /marketdata/sync/price-detail API를 통한 실시간 가격 조회
    4. 프롬프트 구성: 시스템 메시지 + 컨텍스트 데이터 + 정책 가이드라인 + 출력 형식
    5. 프롬프트 반환: PlainTextResponse로 텍스트 직접 반환
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    - 추천서: analyst_recommendation 테이블의 최근 추천서 (현재 보유 종목 중 country='US'만)
    - 현재가: /marketdata/sync/price-detail API
    
    **핵심 로직:**
    1. 현재 보유 종목의 ticker_id로 최근 추천서 조회 (LONG/SHORT 모두)
    2. 트레이더AI가 자체적으로 SHORT는 민감하게, LONG은 신중하게 판단
    3. 익절/손절 타이밍을 유리한 가격대에 미리 대응
    
    **프롬프트 구성:**
    1. 시스템 메시지 (매도트레이더AI 역할 정의)
    2. 컨텍스트 데이터 (포트폴리오 + 최근 추천서 + 현재가)
    3. 정책 가이드라인 (SHORT 민감, LONG 신중)
    4. 출력 형식 (JSON 스키마)
    
    **특징:**
    - 미국주식 전용 매도주문 프롬프트
    - 보유종목 기반 매도 전략 수립
    - 실시간 데이터 기반 프롬프트 생성
    
    **사용 예시:**
    - `GET /portfolio/sell-order-prompt` (미국주식 매도주문 프롬프트 생성)
    """,
    response_description="생성된 매도주문 프롬프트 텍스트 (줄바꿈 포함)"
)
def generate_sell_order_prompt(
    db: Session = Depends(get_db)
):
    """미국주식 매도주문 프롬프트를 생성합니다."""
    try:
        sell_order_prompt_service = SellOrderPromptService(db)
        prompt_text = sell_order_prompt_service.generate_sell_order_prompt()
        
        # [OUTPUT] 섹션과 스키마 추가
        import json
        schema_text = json.dumps(ORDER_BATCH_SCHEMA, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt_text}\n\n[OUTPUT]\n{schema_text}"
        
        # PlainTextResponse로 프롬프트 텍스트 직접 반환
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"매도주문 프롬프트 생성 중 오류가 발생했습니다: {str(e)}"
        )


@router.post(
    "/buy-order-batch",
    response_model=BuyOrderBatchResponse,
    summary="미국주식 매수주문 배치 AI 생성 및 즉시 실행 ★★★",
    description="""
    AI를 통해 미국주식 매수주문 배치를 생성하고 즉시 실행합니다.
    
    **처리 과정:**
    0. **휴장 확인**: `/marketdata/is-market-closed` 서비스로 휴장 여부 확인
    1. **자산 스냅샷 갱신**: `/asset-snapshots/collect/ovrs` API를 통해 해외 자산 스냅샷 수집
    2. **프롬프트 생성**: 최신 자산 스냅샷 + 유효한 LONG 추천서들 (country='US'만)
    3. **GPT AI 호출**: 매수주문 설계자 AI가 주문 배치 생성
    4. **데이터베이스 저장**: OrderBatch → OrderPlan → OrderLeg 구조로 저장
    5. **주문 즉시 실행**: 실제 KIS API를 통해 주문 실행
    6. **결과 반환**: 생성된 배치 정보 및 실행 결과
    
    **휴장 처리:**
    - 휴장 시: 전체 로직 skip, `is_market_closed: true` 반환
    - 개장 시: 정상적으로 매수주문 배치 생성 및 실행
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    - 추천서: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='US', is_latest=True)
    - 현재가: /marketdata/sync/price-detail API를 통한 실시간 가격 조회
    
    **AI 모델:** GPT를 사용하여 매수주문 배치 설계
    **저장 테이블:** order_batch, order_plan, order_leg, broker_order
    
    **특징:**
    - **스마트 휴장 감지**: 주말/공휴일 자동 감지하여 불필요한 처리 방지
    - **실시간 포트폴리오 갱신**: 매번 KIS API를 통해 최신 잔고 조회
    - **지능형 주문 설계**: GPT AI를 통한 체계적인 매수 전략 수립
    - **실행 계획(EXECUTE)과 제외 계획(SKIP) 모두 저장**
    - **각 계획은 여러 레그(LIMIT/LOC)로 구성 가능** (MARKET 제거)
    - **체계적인 주문 추적 및 관리 지원**
    - **실제 KIS API 주문 실행 및 BrokerOrder 테이블 연동**
    
    **사용 예시:**
    - `POST /portfolio/buy-order-batch` (미국주식 매수주문 배치 AI 생성 + 즉시 실행)
    """,
    response_description="생성된 매수주문 배치 정보 및 실행 결과"
)
def generate_buy_order_batch(
    db: Session = Depends(get_db)
):
    """AI를 통해 미국주식 매수주문 배치를 생성하고 즉시 실행합니다."""
    try:
        buy_order_ai_service = BuyOrderAIService(db)
        result = buy_order_ai_service.generate_buy_order_batch()
        
        return BuyOrderBatchResponse(
            batch_id=result["batch_id"],
            asof_kst=result["asof_kst"],
            mode=result["mode"],
            currency=result["currency"],
            available_cash=result["available_cash"],
            notes=result["notes"],
            plans_count=result["plans_count"],
            skipped_count=result["skipped_count"],
            executed_orders=result["executed_orders"],
            is_market_closed=result["is_market_closed"],
            generated_at=result["generated_at"],
            message=result.get("message")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"매수주문 배치 생성 중 오류가 발생했습니다: {str(e)}"
        )


@router.post(
    "/sell-order-batch",
    response_model=SellOrderBatchResponse,
    summary="미국주식 매도주문 배치 AI 생성 및 즉시 실행 ★★★",
    description="""
    AI를 통해 미국주식 매도주문 배치를 생성하고 즉시 실행합니다.
    
    **처리 과정:**
    0. **휴장 확인**: `/marketdata/is-market-closed` 서비스로 휴장 여부 확인
    1. **자산 스냅샷 갱신**: `/asset-snapshots/collect/ovrs` API를 통해 해외 자산 스냅샷 수집
    2. **프롬프트 생성**: 현재 보유종목의 최근 추천서 기반 매도 프롬프트 (country='US'만)
    3. **GPT AI 호출**: AI를 통한 매도주문 배치 생성
    4. **데이터베이스 저장**: OrderBatch, OrderPlan, OrderLeg 테이블 저장
    5. **주문 실행**: 실제 KIS API를 통한 매도주문 실행
    6. **결과 반환**: 생성된 배치 정보 및 실행 결과
    
    **휴장 처리:**
    - 휴장 시: 전체 로직 skip, `is_market_closed: true` 반환
    - 개장 시: 정상적으로 매도주문 배치 생성 및 실행
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 OVRS 스냅샷
    - 추천서: analyst_recommendation 테이블의 최근 추천서 (현재 보유 종목 중 country='US'만)
    - 현재가: /marketdata/sync/price-detail API를 통한 실시간 가격 조회
    
    **AI 모델:** GPT를 사용하여 매도주문 배치 설계
    **저장 테이블:** order_batch, order_plan, order_leg, broker_order
    
    **특징:**
    - **스마트 휴장 감지**: 시장 휴장 시 자동 중단
    - **실시간 포트폴리오 갱신**: 최신 보유종목 정보 반영
    - **지능형 매도 전략**: SHORT/LONG 추천서 구분 대응
    - **안전한 주문 설계**: 포지션 한도(25~35%) 내 매도 집행
    - **체계적인 주문 관리**: 배치 단위 주문 추적
    - **실제 KIS API 주문 실행 및 BrokerOrder 테이블 연동**
    
    **사용 예시:**
    - `POST /portfolio/sell-order-batch` (미국주식 매도주문 배치 AI 생성 + 즉시 실행)
    """,
    response_description="생성된 매도주문 배치 정보 및 실행 결과"
)
async def generate_sell_order_batch_endpoint(
    db: Session = Depends(get_db)
):
    """AI를 통해 미국주식 매도주문 배치를 생성하고 즉시 실행합니다."""
    try:
        sell_order_ai_service = SellOrderAIService(db)
        result = sell_order_ai_service.generate_sell_order_batch()
        
        return SellOrderBatchResponse(
            batch_id=result["batch_id"],
            asof_kst=result["asof_kst"],
            mode=result["mode"],
            currency=result["currency"],
            available_cash=result["available_cash"],
            notes=result["notes"],
            plans_count=result["plans_count"],
            skipped_count=result["skipped_count"],
            executed_orders=result["executed_orders"],
            is_market_closed=result["is_market_closed"],
            generated_at=result["generated_at"],
            message=result.get("message")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"매도주문 배치 생성 중 오류가 발생했습니다: {str(e)}"
        )


