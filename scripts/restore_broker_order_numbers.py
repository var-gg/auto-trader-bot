#!/usr/bin/env python3
"""
BrokerOrder 테이블의 누락된 주문번호와 거래소 조직번호를 복원하는 스크립트

기존 SUBMITTED 상태의 BrokerOrder 레코드들 중에서:
- order_number가 비어있거나 NULL인 경우
- routing_org_code가 비어있거나 NULL인 경우

payload에서 ODNO와 KRX_FWDG_ORD_ORGNO를 추출하여 복원합니다.
"""

import sys
import os
import logging
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.core.db import get_db
from app.features.portfolio.models.trading_models import BrokerOrder

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("restore_broker_order_numbers")


def get_broker_orders_with_missing_data(db: Session) -> List[BrokerOrder]:
    """
    주문번호나 거래소 조직번호가 누락된 BrokerOrder 레코드들을 조회
    
    Args:
        db: 데이터베이스 세션
        
    Returns:
        List[BrokerOrder]: 복원이 필요한 BrokerOrder 레코드들
    """
    return db.query(BrokerOrder).filter(
        and_(
            BrokerOrder.status == "SUBMITTED",  # 성공한 주문만
            or_(
                BrokerOrder.order_number.is_(None),  # 주문번호가 NULL
                BrokerOrder.order_number == "",      # 주문번호가 빈 문자열
                BrokerOrder.routing_org_code.is_(None),  # 거래소 조직번호가 NULL
                BrokerOrder.routing_org_code == ""       # 거래소 조직번호가 빈 문자열
            )
        )
    ).all()


def extract_order_data_from_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    payload에서 주문번호와 거래소 조직번호를 추출
    
    Args:
        payload: KIS API 응답 payload
        
    Returns:
        Dict[str, str]: 추출된 데이터 {"order_number": "...", "routing_org_code": "..."}
    """
    result = {"order_number": "", "routing_org_code": ""}
    
    if not payload:
        return result
    
    # output 객체에서 데이터 추출
    output = payload.get("output", {})
    if output:
        result["order_number"] = output.get("ODNO", "")
        result["routing_org_code"] = output.get("KRX_FWDG_ORD_ORGNO", "")
    
    return result


def restore_broker_order_data(db: Session) -> Dict[str, int]:
    """
    BrokerOrder 테이블의 누락된 데이터를 복원
    
    Args:
        db: 데이터베이스 세션
        
    Returns:
        Dict[str, int]: 복원 결과 통계
    """
    logger.info("🔍 누락된 데이터가 있는 BrokerOrder 레코드 조회 중...")
    
    # 복원이 필요한 레코드들 조회
    broker_orders = get_broker_orders_with_missing_data(db)
    
    if not broker_orders:
        logger.info("✅ 복원이 필요한 레코드가 없습니다.")
        return {"total": 0, "restored": 0, "failed": 0, "skipped": 0}
    
    logger.info(f"📋 총 {len(broker_orders)}개의 레코드에서 복원 작업을 시작합니다.")
    
    stats = {
        "total": len(broker_orders),
        "restored": 0,
        "failed": 0,
        "skipped": 0
    }
    
    for broker_order in broker_orders:
        try:
            logger.info(f"🔄 BrokerOrder ID {broker_order.id} 처리 중...")
            
            # payload에서 데이터 추출
            extracted_data = extract_order_data_from_payload(broker_order.payload)
            
            if not extracted_data["order_number"] and not extracted_data["routing_org_code"]:
                logger.warning(f"⚠️ BrokerOrder ID {broker_order.id}: payload에서 데이터를 추출할 수 없습니다.")
                stats["skipped"] += 1
                continue
            
            # 데이터 복원
            updated = False
            
            if not broker_order.order_number and extracted_data["order_number"]:
                broker_order.order_number = extracted_data["order_number"]
                updated = True
                logger.info(f"  📝 주문번호 복원: {extracted_data['order_number']}")
            
            if not broker_order.routing_org_code and extracted_data["routing_org_code"]:
                broker_order.routing_org_code = extracted_data["routing_org_code"]
                updated = True
                logger.info(f"  📝 거래소 조직번호 복원: {extracted_data['routing_org_code']}")
            
            if updated:
                stats["restored"] += 1
                logger.info(f"✅ BrokerOrder ID {broker_order.id} 복원 완료")
            else:
                stats["skipped"] += 1
                logger.info(f"⏭️ BrokerOrder ID {broker_order.id} 복원 불필요 (이미 데이터 존재)")
                
        except Exception as e:
            logger.error(f"❌ BrokerOrder ID {broker_order.id} 복원 실패: {str(e)}")
            stats["failed"] += 1
    
    # 변경사항 커밋
    try:
        db.commit()
        logger.info("💾 모든 변경사항이 데이터베이스에 저장되었습니다.")
    except Exception as e:
        logger.error(f"❌ 데이터베이스 커밋 실패: {str(e)}")
        db.rollback()
        raise
    
    return stats


def main():
    """메인 실행 함수"""
    logger.info("🚀 BrokerOrder 주문번호 복원 스크립트 시작")
    
    try:
        # 데이터베이스 세션 생성
        db = next(get_db())
        
        # 복원 작업 실행
        stats = restore_broker_order_data(db)
        
        # 결과 출력
        logger.info("📊 복원 작업 완료!")
        logger.info(f"  총 처리: {stats['total']}개")
        logger.info(f"  복원 성공: {stats['restored']}개")
        logger.info(f"  복원 실패: {stats['failed']}개")
        logger.info(f"  건너뜀: {stats['skipped']}개")
        
        if stats["restored"] > 0:
            logger.info("🎉 주문번호와 거래소 조직번호가 성공적으로 복원되었습니다!")
        else:
            logger.info("ℹ️ 복원할 데이터가 없거나 이미 모든 데이터가 완전합니다.")
            
    except Exception as e:
        logger.error(f"❌ 스크립트 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()
            logger.info("🔒 데이터베이스 연결이 종료되었습니다.")


if __name__ == "__main__":
    main()
