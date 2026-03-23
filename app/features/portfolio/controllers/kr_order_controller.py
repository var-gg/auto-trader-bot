from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from app.core.db import get_db
from app.features.portfolio.services.kr_order_service import KrOrderService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portfolio/kr", tags=["[국내주식] portfolio"])

class KrOrderRequest(BaseModel):
    """국내주식 주문 요청"""
    symbol: str = Field(..., description="종목코드 (6자리)", example="005930")
    side: str = Field(..., description="매수/매도", enum=["BUY", "SELL"])
    quantity: int = Field(..., description="주문수량", gt=0, example=10)
    price: float = Field(..., description="주문단가 (시장가일 경우 0)", ge=0, example=70000.0)
    order_type: str = Field(default="LIMIT", description="주문유형", enum=["LIMIT", "LOC"], example="LIMIT")
    notes: Optional[str] = Field(None, description="메모", example="테스트 주문")

class KrOrderResponse(BaseModel):
    """국내주식 주문 응답"""
    success: bool = Field(..., description="주문 성공 여부")
    order_id: Optional[str] = Field(None, description="주문번호")
    message: str = Field(..., description="응답 메시지")
    details: Optional[Dict[str, Any]] = Field(None, description="상세 정보")

@router.post(
    "/order",
    response_model=KrOrderResponse,
    summary="국내주식 주문 실행",
    description="""
    국내주식 주문을 실행합니다.
    
    **지원 주문 유형:**
    - LIMIT: 지정가 주문 (ORD_DVSN: 00)
    - LOC: 장후 시간외 주문 (ORD_DVSN: 06)
    
    **KIS API 매핑:**
    - 매수: TTTC0012U (실전) / VTTC0012U (모의)
    - 매도: TTTC0011U (실전) / VTTC0011U (모의)
    
    **고정값:**
    - 거래소: KRX (한국거래소)
    - 매도유형: 01 (일반매도)
    - 계좌정보: 환경변수에서 자동 설정
    
    **필수 입력:**
    - symbol: 종목코드 (6자리)
    - side: BUY/SELL
    - quantity: 주문수량
    - price: 주문단가 (LOC 주문시에도 필수)
    """
)
async def execute_kr_order(
    request: KrOrderRequest,
    db: Session = Depends(get_db)
):
    """국내주식 주문을 실행합니다."""
    try:
        logger.info(f"🏦 국내주식 주문 실행 시작: {request.symbol} {request.side} {request.quantity}주 @ {request.price}")
        
        # 주문 서비스 생성
        order_service = KrOrderService(db)
        
        # 주문 실행
        result = await order_service.execute_order(
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            order_type=request.order_type,
            notes=request.notes
        )
        
        if result["success"]:
            logger.info(f"✅ 국내주식 주문 성공: {result.get('order_id')}")
            return KrOrderResponse(
                success=True,
                order_id=result.get("order_id"),
                message=result.get("message", "주문이 성공적으로 실행되었습니다."),
                details=result.get("details")
            )
        else:
            logger.error(f"❌ 국내주식 주문 실패: {result.get('message')}")
            raise HTTPException(
                status_code=400,
                detail=f"국내주식 주문 실패: {result.get('message')}"
            )
            
    except Exception as e:
        logger.error(f"❌ 국내주식 주문 실행 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 주문 실행 중 오류 발생: {str(e)}"
        )

