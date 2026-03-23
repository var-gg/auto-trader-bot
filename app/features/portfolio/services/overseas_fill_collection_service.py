# app/features/portfolio/services/overseas_fill_collection_service.py

from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from app.core.kis_client import KISClient
from app.features.portfolio.models.trading_models import BrokerOrder, OrderFill, OrderLeg
from app.features.kis_test.models.kis_test_models import KISOverseasOrderRequest, KISOverseasOrderResponse

logger = logging.getLogger(__name__)


class OverseasFillCollectionService:
    """해외주식 체결정보 수집 서비스
    
    KIS API를 통해 해외주식 체결정보를 수집하고 order_fill 테이블에 upsert합니다.
    주기적으로 실행되어 체결상태를 업데이트합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
    
    async def collect_overseas_fills(self, days_back: int = 7) -> Dict[str, Any]:
        """해외주식 체결정보 수집 (일별로 소분해서 조회)
        
        Args:
            days_back: 조회할 일수 (기본값: 7일)
            
        Returns:
            Dict[str, Any]: 수집 결과 통계
        """
        try:
            logger.info(f"해외주식 체결정보 수집 시작 - {days_back}일간 (일별 조회)")
            
            # 날짜 범위 설정
            end_date = datetime.now()
            
            # 전체 통계 카운터
            total_processed = 0
            total_upserted = 0
            total_skipped = 0
            
            # 각 날짜별로 조회 (체결/미체결 각각)
            for day_offset in range(days_back):
                target_date = end_date - timedelta(days=day_offset)
                date_str = target_date.strftime("%Y%m%d")
                
                logger.info(f"📅 {date_str} 조회 시작 ({day_offset + 1}/{days_back})")
                
                # 1. 체결 조회 (CCLD_NCCS_DVSN="01")
                filled_result = await self._collect_overseas_fills_single_day(date_str, "01")
                
                if filled_result["success"]:
                    total_processed += filled_result.get("processed_count", 0)
                    total_upserted += filled_result.get("upserted_count", 0)
                    total_skipped += filled_result.get("skipped_count", 0)
                
                # 2. 미체결 조회 (CCLD_NCCS_DVSN="02")
                unfilled_result = await self._collect_overseas_fills_single_day(date_str, "02")
                
                if unfilled_result["success"]:
                    total_processed += unfilled_result.get("processed_count", 0)
                    total_upserted += unfilled_result.get("upserted_count", 0)
                    total_skipped += unfilled_result.get("skipped_count", 0)
                
                day_total = filled_result.get('processed_count', 0) + unfilled_result.get('processed_count', 0)
                day_upserted = filled_result.get('upserted_count', 0) + unfilled_result.get('upserted_count', 0)
                logger.info(f"✅ {date_str} 완료 - 처리: {day_total}건, 업서트: {day_upserted}건")
            
            # 커밋
            self.db.commit()
            
            logger.info(f"해외주식 체결정보 수집 완료 - {days_back}일간, 처리: {total_processed}건, 업서트: {total_upserted}건, 스킵: {total_skipped}건")
            
            return {
                "success": True,
                "processed_count": total_processed,
                "upserted_count": total_upserted,
                "skipped_count": total_skipped,
                "days_collected": days_back,
                "message": f"체결정보 수집 완료: {days_back}일간, 처리 {total_processed}건, 업서트 {total_upserted}건, 스킵 {total_skipped}건"
            }
            
        except Exception as e:
            logger.error(f"해외주식 체결정보 수집 중 오류: {str(e)}")
            self.db.rollback()
            return {
                "success": False,
                "error": str(e),
                "processed_count": 0,
                "upserted_count": 0,
                "skipped_count": 0
            }
    
    async def _collect_overseas_fills_single_day(self, date_str: str, ccld_nccs_dvsn: str = "00") -> Dict[str, Any]:
        """하루치 해외주식 체결정보 수집
        
        Args:
            date_str: 조회 날짜 (YYYYMMDD)
            ccld_nccs_dvsn: 체결구분 (00: 전체, 01: 체결, 02: 미체결)
            
        Returns:
            Dict[str, Any]: 수집 결과
        """
        try:
            processed_count = 0
            upserted_count = 0
            skipped_count = 0
            
            # KIS API 호출 (하루만 조회)
            result = self.kis_client.overseas_order_history_test(
                CANO="00000000",
                ACNT_PRDT_CD="01",
                PDNO="",
                ORD_STRT_DT=date_str,  # 하루만
                ORD_END_DT=date_str,    # 하루만
                SLL_BUY_DVSN="00",  # 전체
                CCLD_NCCS_DVSN=ccld_nccs_dvsn,  # 체결구분
                OVRS_EXCG_CD="%",  # 전체 거래소
                SORT_SQN="AS",  # 역순
                ORD_DT="",
                ORD_GNO_BRNO="",
                ODNO="",
                CTX_AREA_NK200="",
                CTX_AREA_FK200=""
            )
            
            if result.get("rt_cd") != "0":
                return {
                    "success": False,
                    "error": f"KIS API 호출 실패: {result.get('msg1', 'Unknown error')}",
                    "processed_count": 0,
                    "upserted_count": 0,
                    "skipped_count": 0
                }
            
            # 체결정보 처리
            output_data = result.get("output", [])
            if not output_data:
                return {
                    "success": True,
                    "processed_count": 0,
                    "upserted_count": 0,
                    "skipped_count": 0
                }
            
            logger.debug(f"{date_str} (체결구분: {ccld_nccs_dvsn}): {len(output_data)}건")
            
            for order_data in output_data:
                try:
                    processed_count += 1
                    upserted = await self._process_order_fill(order_data)
                    if upserted:
                        upserted_count += 1
                    else:
                        skipped_count += 1
                        
                except Exception as e:
                    logger.error(f"체결정보 처리 중 오류 (주문번호: {order_data.get('odno', 'unknown')}): {str(e)}")
                    skipped_count += 1
                    continue
            
            return {
                "success": True,
                "processed_count": processed_count,
                "upserted_count": upserted_count,
                "skipped_count": skipped_count
            }
            
        except Exception as e:
            logger.error(f"{date_str} 체결정보 수집 중 오류: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "processed_count": 0,
                "upserted_count": 0,
                "skipped_count": 0
            }
    
    async def _process_order_fill(self, order_data: Dict[str, Any]) -> bool:
        """개별 주문 체결정보 처리
        
        Args:
            order_data: KIS API 응답의 개별 주문 데이터
            
        Returns:
            bool: 업서트 성공 여부
        """
        try:
            # 필수 필드 추출
            order_number = order_data.get("odno")
            orgn_order_number = order_data.get("orgn_odno")
            
            if not order_number:
                logger.warning(f"필수 필드 누락 - order_number: {order_number}")
                return False
            
            # broker_order 조회 (주문번호로 먼저 조회)
            broker_order = self.db.query(BrokerOrder).filter(
                BrokerOrder.order_number == order_number
            ).first()
            
            # 못 찾으면 원주문번호로 다시 조회 (취소/정정 주문의 경우)
            if not broker_order and orgn_order_number:
                broker_order = self.db.query(BrokerOrder).filter(
                    BrokerOrder.order_number == orgn_order_number
                ).first()
                if broker_order:
                    logger.debug(f"원주문번호로 broker_order 찾음 - odno: {order_number}, orgn_odno: {orgn_order_number}")
            
            if not broker_order:
                logger.debug(f"broker_order 없음 (스킵) - order_number: {order_number}, pdno: {order_data.get('pdno')}, ovrs_excg_cd: {order_data.get('ovrs_excg_cd')}")
                return False
            
            # 체결정보 추출
            fill_qty = self._parse_int(order_data.get("ft_ccld_qty", "0"))
            fill_price = self._parse_decimal(order_data.get("ft_ccld_amt3", "0"))
            filled_at = self._parse_datetime(order_data.get("dmst_ord_dt", ""), order_data.get("thco_ord_tmd", ""))
            
            # 해외주식의 경우 ord_dt, ord_tmd를 먼저 확인 (dmst_ord_dt가 없을 수 있음)
            if not filled_at or filled_at == datetime.now():
                filled_at = self._parse_datetime(order_data.get("ord_dt", ""), order_data.get("ord_tmd", ""))
            
            # 체결상태 결정
            fill_status = self._determine_fill_status(order_data, broker_order)
            
            # ✅ 미체결(fill_qty=0)은 order_fill에 저장하지 않음 (실제 체결만 기록)
            if fill_qty == 0:
                logger.debug(f"미체결 스킵 (order_fill 생성 안함) - order_number: {order_number}, fill_status: {fill_status}")
                return False
            
            # ✅ 취소/거부 상태도 실제 체결이 아니므로 스킵
            if fill_status in ['CANCELLED', 'REJECTED']:
                logger.debug(f"{fill_status} 스킵 (order_fill 생성 안함) - order_number: {order_number}")
                return False
            
            # order_fill upsert (실제 체결만 기록: PARTIAL, FULL)
            existing_fill = self.db.query(OrderFill).filter(
                and_(
                    OrderFill.broker_order_id == broker_order.id,
                    OrderFill.fill_qty == fill_qty,
                    OrderFill.fill_price == fill_price,
                    OrderFill.filled_at == filled_at
                )
            ).first()
            
            if existing_fill:
                # 기존 체결정보 업데이트
                existing_fill.fill_status = fill_status
                logger.debug(f"체결정보 업데이트 - order_number: {order_number}, fill_status: {fill_status}")
            else:
                # 새 체결정보 생성
                new_fill = OrderFill(
                    broker_order_id=broker_order.id,
                    fill_qty=fill_qty,
                    fill_price=fill_price,
                    fill_status=fill_status,
                    filled_at=filled_at
                )
                self.db.add(new_fill)
                logger.info(f"체결정보 생성 - order_number: {order_number}, fill_status: {fill_status}, fill_qty: {fill_qty}")
            
            return True
            
        except Exception as e:
            logger.error(f"체결정보 처리 중 오류: {str(e)}")
            return False
    
    def _determine_fill_status(self, order_data: Dict[str, Any], broker_order: BrokerOrder) -> str:
        """체결상태 결정
        
        Args:
            order_data: KIS API 응답의 개별 주문 데이터
            broker_order: 브로커 주문 정보
            
        Returns:
            str: 체결상태 (UNFILLED/PARTIAL/FULL/CANCELLED/REJECTED)
        """
        # KIS API의 처리상태명 확인
        prcs_stat_name = order_data.get("prcs_stat_name", "")
        
        if "거부" in prcs_stat_name:
            return "REJECTED"
        elif "취소" in prcs_stat_name:
            return "CANCELLED"
        
        # 체결수량과 주문수량 비교
        fill_qty = self._parse_int(order_data.get("ft_ccld_qty", "0"))
        order_qty = self._parse_int(order_data.get("ft_ord_qty", "0"))
        
        if fill_qty == 0:
            return "UNFILLED"
        elif fill_qty < order_qty:
            return "PARTIAL"
        else:
            return "FULL"
    
    def _parse_int(self, value: str) -> int:
        """문자열을 정수로 변환"""
        try:
            return int(value) if value else 0
        except (ValueError, TypeError):
            return 0
    
    def _parse_decimal(self, value: str) -> Decimal:
        """문자열을 Decimal로 변환"""
        try:
            return Decimal(value) if value else Decimal("0")
        except (ValueError, TypeError):
            return Decimal("0")
    
    def _parse_datetime(self, date_str: str, time_str: str) -> datetime:
        """날짜와 시간 문자열을 datetime으로 변환"""
        try:
            if not date_str or not time_str:
                return datetime.now()
            
            # 날짜 형식: YYYYMMDD
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            # 시간 형식: HHMMSS
            hour = int(time_str[:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])
            
            return datetime(year, month, day, hour, minute, second)
            
        except (ValueError, TypeError, IndexError):
            logger.warning(f"날짜/시간 파싱 실패 - date: {date_str}, time: {time_str}")
            return datetime.now()
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 조회"""
        try:
            # 최근 7일간의 체결정보 통계
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            total_fills = self.db.query(OrderFill).join(BrokerOrder).filter(
                OrderFill.filled_at >= start_date
            ).count()
            
            status_counts = {}
            for status in ['UNFILLED', 'PARTIAL', 'FULL', 'CANCELLED', 'REJECTED']:
                count = self.db.query(OrderFill).join(BrokerOrder).filter(
                    and_(
                        OrderFill.filled_at >= start_date,
                        OrderFill.fill_status == status
                    )
                ).count()
                status_counts[status] = count
            
            return {
                "period": f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
                "total_fills": total_fills,
                "status_counts": status_counts
            }
            
        except Exception as e:
            logger.error(f"수집 통계 조회 중 오류: {str(e)}")
            return {
                "error": str(e)
            }

