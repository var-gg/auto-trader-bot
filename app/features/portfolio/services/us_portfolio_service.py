from typing import Dict, Any
from sqlalchemy.orm import Session
from app.core.kis_client import KISClient
from app.features.portfolio.repositories.us_portfolio_snapshot_repository import PortfolioSnapshotRepository
from app.core import config as settings
import logging

class PortfolioService:
    """포트폴리오 관련 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
        self.snapshot_repo = PortfolioSnapshotRepository(db)
    
    def get_present_balance(self, wcrc_frcr_dvsn_cd: str = "01", natn_cd: str = "", tr_mket_cd: str = "", inqr_dvsn_cd: str = "02") -> Dict[str, Any]:
        """
        체결기준현재 잔고 조회
        - KIS API를 호출하여 스냅샷을 저장한 후 응답을 그대로 반환 (bypass)
        
        Parameters:
        - wcrc_frcr_dvsn_cd: 원화외화구분코드 (01: 원화, 02: 외화)
        - natn_cd: 국가코드 (빈값: 전체)
        - tr_mket_cd: 거래시장코드 (빈값: 전체)
        - inqr_dvsn_cd: 조회구분코드 (02: 체결기준)
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"=== Portfolio Balance Request ===")
            logger.info(f"Parameters: wcrc_frcr_dvsn_cd={wcrc_frcr_dvsn_cd}, natn_cd={natn_cd}, tr_mket_cd={tr_mket_cd}, inqr_dvsn_cd={inqr_dvsn_cd}")
            logger.info(f"KIS_VIRTUAL: {settings.KIS_VIRTUAL}")
            logger.info(f"KIS_CANO: {settings.KIS_CANO}")
            logger.info(f"KIS_VIRTUAL_CANO: {settings.KIS_VIRTUAL_CANO}")
            
            # KIS API 호출
            logger.info("Calling KIS API...")
            result = self.kis_client.present_balance(
                wcrc_frcr_dvsn_cd=wcrc_frcr_dvsn_cd,
                natn_cd=natn_cd,
                tr_mket_cd=tr_mket_cd,
                inqr_dvsn_cd=inqr_dvsn_cd
            )
            
            logger.info(f"KIS API Response: rt_cd={result.get('rt_cd')}, msg_cd={result.get('msg_cd')}, msg1={result.get('msg1')}")
            
            # 성공적인 응답인 경우 스냅샷 저장
            if result.get("rt_cd") == "0":
                logger.info("✅ KIS API call successful, attempting to save snapshot...")
                
                try:
                    # 계좌 ID 결정 (가상환경에 따라)
                    account_id = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
                    logger.info(f"Selected account_id: {account_id}")
                    
                    # 시장 범위 결정
                    venue_scope = "US" if natn_cd == "840" else "ALL"
                    logger.info(f"Selected venue_scope: {venue_scope}")
                    
                    # 응답 데이터 확인
                    output1_count = len(result.get("output1", []))
                    output2_count = len(result.get("output2", []))
                    output3_exists = result.get("output3") is not None
                    logger.info(f"Response data: output1={output1_count} items, output2={output2_count} items, output3={'exists' if output3_exists else 'missing'}")
                    
                    # 스냅샷 저장
                    logger.info("Calling snapshot repository...")
                    snapshot_id = self.snapshot_repo.save_portfolio_snapshot(
                        account_id=account_id,
                        venue_scope=venue_scope,
                        raw_response=result
                    )
                    
                    logger.info(f"✅ Portfolio snapshot saved successfully: {snapshot_id}")
                    
                except Exception as e:
                    # 스냅샷 저장 실패는 로그만 남기고 API 응답은 그대로 반환
                    logger.error(f"❌ Failed to save portfolio snapshot: {str(e)}")
                    logger.error(f"Exception type: {type(e).__name__}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
            else:
                logger.warning(f"⚠️ KIS API call failed, skipping snapshot save. rt_cd={result.get('rt_cd')}, msg1={result.get('msg1')}")
            
            # 응답을 그대로 반환 (bypass)
            logger.info("Returning KIS API response (bypass)")
            return result
            
        except Exception as e:
            # 에러 발생 시 에러 정보를 포함한 응답 반환
            return {
                "rt_cd": "1",  # 에러 코드
                "msg_cd": "ERROR",
                "msg1": f"잔고 조회 중 오류 발생: {str(e)}",
                "output1": [],
                "output2": [],
                "output3": None
            }

    def order_stock(self, order_type: str, symbol: str, quantity: str, price: str = None, order_method: str = "LIMIT", exchange: str = "NASD", leg_id: int = None) -> Dict[str, Any]:
        """
        미국 주식 주문 (매수/매도)
        - KIS API를 호출하고 브로커 주문 정보를 저장한 후 응답을 반환
        
        Parameters:
        - order_type: 주문유형 ("buy": 매수, "sell": 매도)
        - symbol: 종목코드 (예: AAPL)
        - quantity: 주문수량
        - price: 주문단가 (LIMIT/LOC일 때 필수, MARKET일 때 None)
        - order_method: 주문방식 ("LIMIT", "MARKET", "LOC")
        - exchange: 거래소코드 (기본값: NASD)
        - leg_id: 주문레그 ID (선택사항, 직접 주문시 None)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # KIS API 호출
            logger.info(f"🚀 Placing KIS order: {order_type} {symbol} {quantity} {order_method}@{price or 'MARKET'}")
            result = self.kis_client.order_stock(
                order_type=order_type,
                symbol=symbol,
                quantity=quantity,
                price=price,
                order_method=order_method,
                exchange=exchange
            )
            
            # 브로커 주문 정보 저장
            try:
                self._save_broker_order(result, order_method, leg_id)
                logger.info("✅ Broker order saved successfully")
            except Exception as save_error:
                logger.error(f"❌ Failed to save broker order: {str(save_error)}")
                # 주문은 성공했지만 저장 실패는 로그만 남기고 계속 진행
            
            # 응답을 그대로 반환 (bypass)
            return result
            
        except Exception as e:
            logger.error(f"❌ Order failed: {str(e)}")
            # 에러 발생 시 에러 정보를 포함한 응답 반환
            return {
                "rt_cd": "1",  # 에러 코드
                "msg_cd": "ERROR",
                "msg1": f"주문 중 오류 발생: {str(e)}",
                "KRX_FWDG_ORD_ORGNO": "",
                "ODNO": "",
                "ORD_TMD": ""
            }
    
    def _save_broker_order(self, kis_response: Dict[str, Any], order_method: str, leg_id: int = None) -> None:
        """
        KIS 주문 응답을 broker_order 테이블에 저장합니다.
        
        Parameters:
        - kis_response: KIS API 응답
        - order_method: 주문방식 ("LIMIT", "MARKET", "LOC")
        - leg_id: 주문레그 ID (선택사항)
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
        
        # 주문 방식별 ord_dvsn 매핑 (KIS 클라이언트와 동일)
        ord_dvsn_mapping = {
            "LIMIT": "00",   # 지정가
            "MARKET": "01",  # 시장가  
            "LOC": "02"      # 장마감시장가
        }
        ord_dvsn = ord_dvsn_mapping.get(order_method, "00")
        
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
        
        # 데이터베이스에 저장
        self.db.add(broker_order)
        self.db.commit()
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"💾 Broker order saved: ID={broker_order.id}, Status={status}, Order#={order_number}")
