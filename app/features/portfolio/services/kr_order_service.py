from sqlalchemy.orm import Session
from typing import Dict, Any, List, Optional
from app.core.kis_client import KISClient
from app.core import config as settings
import logging

logger = logging.getLogger(__name__)

class KrOrderService:
    """국내주식 주문 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
    
    def execute_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        order_type: str = "LIMIT",
        notes: Optional[str] = None,
        leg_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        국내주식 주문을 실행합니다.
        
        Args:
            symbol: 종목코드 (6자리)
            side: BUY/SELL
            quantity: 주문수량
            price: 주문단가 (시장가시 0)
            order_type: LIMIT/MARKET
            notes: 메모
            
        Returns:
            주문 실행 결과
        """
        try:
            logger.info(f"🏦 국내주식 주문 실행: {symbol} {side} {quantity}주 @ {price} ({order_type})")
            
            # 계좌 정보 설정
            cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
            acnt_prdt_cd = settings.KIS_ACNT_PRDT_CD
            
            # 주문 구분 설정
            if order_type == "LIMIT":
                ord_dvsn = "00"  # 지정가
            elif order_type == "LOC":
                ord_dvsn = "06"  # 장후 시간외
            else:
                raise ValueError(f"지원하지 않는 주문 유형: {order_type}. 지원 유형: LIMIT, LOC")
            
            # 매도유형 설정 (매도시에만)
            sll_type = "01" if side == "SELL" else None  # 일반매도
            
            # 주문 파라미터 구성
            order_params = {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt_prdt_cd,
                "PDNO": symbol,
                "ORD_DVSN": ord_dvsn,
                "ORD_QTY": str(quantity),
                "ORD_UNPR": str(int(price)) if price > 0 else "0",
                "EXCG_ID_DVSN_CD": "KRX"  # 한국거래소 고정
            }
            
            # 매도시에만 SLL_TYPE 추가
            if side == "SELL":
                order_params["SLL_TYPE"] = sll_type
            
            logger.info(f"📋 주문 파라미터: {order_params}")
            
            # KIS API 호출
            if side == "BUY":
                result = self.kis_client.order_cash_buy(**order_params)
            elif side == "SELL":
                result = self.kis_client.order_cash_sell(**order_params)
            else:
                raise ValueError(f"지원하지 않는 매매 구분: {side}")
            
            # 응답 처리
            if result.get("rt_cd") == "0":
                output = result.get("output", {})
                order_id = output.get("ODNO")
                
                logger.info(f"✅ 국내주식 주문 성공: {symbol} {side} {quantity}주, 주문번호: {order_id}")
                
                # BrokerOrder 생성
                self._create_broker_order(result, leg_id=leg_id)
                
                return {
                    "success": True,
                    "order_id": order_id,
                    "message": "주문이 성공적으로 실행되었습니다.",
                    "details": {
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "price": price,
                        "order_type": order_type,
                        "order_time": output.get("ORD_TMD"),
                        "exchange_code": output.get("KRX_FWDG_ORD_ORGNO"),
                        "notes": notes
                    }
                }
            else:
                error_msg = result.get("msg1", "알 수 없는 오류")
                logger.error(f"❌ 국내주식 주문 실패: {error_msg}")
                
                return {
                    "success": False,
                    "message": f"주문 실패: {error_msg}",
                    "details": {
                        "error_code": result.get("msg_cd"),
                        "error_message": error_msg,
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "price": price,
                        "order_type": order_type
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ 국내주식 주문 실행 중 오류: {str(e)}")
            return {
                "success": False,
                "message": f"주문 실행 중 오류 발생: {str(e)}",
                "details": {
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "price": price,
                    "order_type": order_type,
                    "error": str(e)
                }
            }
    
    def _create_broker_order(self, kis_response: Dict[str, Any], leg_id: Optional[int] = None) -> None:
        """
        KIS 주문 응답을 기반으로 BrokerOrder 레코드를 생성합니다.
        
        Args:
            kis_response: KIS API 응답
            leg_id: OrderLeg ID (선택사항)
        """
        from app.features.portfolio.models.trading_models import BrokerOrder
        from datetime import datetime, timezone
        
        # KIS 응답에서 필요한 정보 추출
        rt_cd = kis_response.get("rt_cd", "")
        output = kis_response.get("output", {})
        order_number = output.get("ODNO", "")  # 주문번호
        routing_org_code = output.get("KRX_FWDG_ORD_ORGNO", "")  # 거래소 전송 조직번호
        
        # 주문 상태 결정
        if rt_cd == "0":
            status = "SUBMITTED"  # 주문 성공
        else:
            status = "REJECTED"   # 주문 실패
        
        # BrokerOrder 레코드 생성
        broker_order = BrokerOrder(
            leg_id=leg_id,  # NULL 허용 (직접 주문시)
            order_number=order_number,
            routing_org_code=routing_org_code,
            payload=kis_response,  # 전체 응답을 JSONB로 저장
            status=status,
            submitted_at=datetime.now(timezone.utc),
            reject_code=kis_response.get("msg_cd", "") if status == "REJECTED" else None,
            reject_message=kis_response.get("msg1", "") if status == "REJECTED" else None
        )
        
        self.db.add(broker_order)
        self.db.commit()
        
        logger.info(f"📝 BrokerOrder 생성 완료: ID={broker_order.id}, 주문번호={order_number}, 상태={status}")
    
