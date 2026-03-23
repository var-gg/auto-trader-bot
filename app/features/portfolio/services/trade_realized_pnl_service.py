# app/features/portfolio/services/trade_realized_pnl_service.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta
from decimal import Decimal

from app.features.portfolio.repositories.trade_realized_pnl_repository import TradeRealizedPnlRepository
from app.features.kis_test.services.kis_test_service import KISTestService
from app.features.kis_test.models.kis_test_models import (
    KISDomesticProfitRequest,
    KISOverseasProfitRequest
)
import logging

logger = logging.getLogger(__name__)


class TradeRealizedPnlService:
    """일자별 종목 손익 서비스
    
    KIS API를 통해 손익 데이터를 조회하고 통합 저장하는 서비스를 제공합니다.
    해외주식과 국내주식의 손익 데이터를 통합하여 일관된 형태로 저장합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repository = TradeRealizedPnlRepository(db)
        self.kis_service = KISTestService(db)
    
    async def collect_and_save_realized_pnl(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """손익 데이터 수집 및 저장
        
        KIS API를 통해 해외주식과 국내주식의 손익 데이터를 조회하고 통합 저장합니다.
        
        Args:
            start_date: 조회 시작일 (YYYYMMDD 형식)
            end_date: 조회 종료일 (YYYYMMDD 형식)
            
        Returns:
            Dict[str, Any]: 수집 및 저장 결과
        """
        try:
            logger.info(f"손익 데이터 수집 시작: {start_date} ~ {end_date}")
            
            results = {
                'period': f"{start_date} ~ {end_date}",
                'overseas_count': 0,
                'domestic_count': 0,
                'total_saved': 0,
                'errors': []
            }
            
            # 해외주식 손익 데이터 수집
            try:
                overseas_data = await self._collect_overseas_profit_data(start_date, end_date)
                if overseas_data:
                    saved_count = len(self.repository.bulk_create_or_update(overseas_data))
                    results['overseas_count'] = saved_count
                    logger.info(f"해외주식 손익 데이터 저장 완료: {saved_count}건")
                else:
                    logger.info("해외주식 손익 데이터 없음")
                    
            except Exception as e:
                error_msg = f"해외주식 손익 데이터 수집 중 오류: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            # 국내주식 손익 데이터 수집
            try:
                domestic_data = await self._collect_domestic_profit_data(start_date, end_date)
                if domestic_data:
                    saved_count = len(self.repository.bulk_create_or_update(domestic_data))
                    results['domestic_count'] = saved_count
                    logger.info(f"국내주식 손익 데이터 저장 완료: {saved_count}건")
                else:
                    logger.info("국내주식 손익 데이터 없음")
                    
            except Exception as e:
                error_msg = f"국내주식 손익 데이터 수집 중 오류: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            results['total_saved'] = results['overseas_count'] + results['domestic_count']
            
            logger.info(f"손익 데이터 수집 완료: 총 {results['total_saved']}건 저장")
            return results
            
        except Exception as e:
            logger.error(f"손익 데이터 수집 중 오류 발생: {str(e)}")
            raise
    
    async def _collect_overseas_profit_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """해외주식 손익 데이터 수집
        
        KIS API를 통해 해외주식 손익 데이터를 조회하고 통합 형태로 변환합니다.
        
        Args:
            start_date: 조회 시작일 (YYYYMMDD 형식)
            end_date: 조회 종료일 (YYYYMMDD 형식)
            
        Returns:
            List[Dict[str, Any]]: 변환된 해외주식 손익 데이터 리스트
        """
        try:
            # KIS API 호출
            request = KISOverseasProfitRequest(
                INQR_STRT_DT=start_date,
                INQR_END_DT=end_date
            )
            
            response = await self.kis_service.test_overseas_profit(request)
            
            if response.rt_cd != "0" or not response.output1:
                logger.warning(f"해외주식 손익 데이터 조회 실패: {response.msg1}")
                return []
            
            # 데이터 변환
            converted_data = []
            for item in response.output1:
                try:
                    # Pydantic 모델이면 딕셔너리로 변환, 이미 딕셔너리면 그대로 사용
                    if hasattr(item, 'model_dump'):
                        item_dict = item.model_dump()
                    elif hasattr(item, 'dict'):
                        item_dict = item.dict()
                    else:
                        item_dict = item
                    
                    # 날짜 변환 (YYYYMMDD -> date)
                    trade_date = datetime.strptime(item_dict.get('trad_day', ''), '%Y%m%d').date()
                    
                    # 수치 데이터 변환
                    def safe_decimal(value, default=0):
                        try:
                            return Decimal(str(value)) if value and str(value).replace('-', '').replace('.', '').isdigit() else default
                        except:
                            return default
                    
                    converted_item = {
                        'trade_date': trade_date,
                        'market_type': 'US',
                        'exchange_code': item_dict.get('ovrs_excg_cd', ''),
                        'symbol': item_dict.get('ovrs_pdno', ''),
                        'instrument_name': item_dict.get('ovrs_item_name', ''),
                        'currency_code': 'USD',  # 기본값, 실제로는 API에서 가져와야 함
                        
                        'buy_qty': safe_decimal(item_dict.get('slcl_qty')),
                        'buy_price': safe_decimal(item_dict.get('pchs_avg_pric')),
                        'buy_amount': safe_decimal(item_dict.get('frcr_pchs_amt1')),
                        
                        'sell_qty': safe_decimal(item_dict.get('slcl_qty')),
                        'sell_price': safe_decimal(item_dict.get('avg_sll_unpr')),
                        'sell_amount': safe_decimal(item_dict.get('frcr_sll_amt_smtl1')),
                        
                        'realized_pnl': safe_decimal(item_dict.get('ovrs_rlzt_pfls_amt')),
                        'pnl_rate': safe_decimal(item_dict.get('pftrt')),
                        
                        'fee': safe_decimal(item_dict.get('stck_sll_tlex')),
                        'tax': None,  # 해외주식은 제세금 없음
                        'interest': None,  # 해외주식은 대출이자 없음
                        'exchange_rate': safe_decimal(item_dict.get('exrt')),
                        
                        'note': f"해외주식 API 조회: {start_date}~{end_date}"
                    }
                    
                    converted_data.append(converted_item)
                    
                except Exception as e:
                    logger.warning(f"해외주식 손익 데이터 변환 중 오류 (건너뜀): {str(e)}")
                    continue
            
            logger.info(f"해외주식 손익 데이터 변환 완료: {len(converted_data)}건")
            return converted_data
            
        except Exception as e:
            logger.error(f"해외주식 손익 데이터 수집 중 오류 발생: {str(e)}")
            raise
    
    async def _collect_domestic_profit_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """국내주식 손익 데이터 수집
        
        KIS API를 통해 국내주식 손익 데이터를 조회하고 통합 형태로 변환합니다.
        
        Args:
            start_date: 조회 시작일 (YYYYMMDD 형식)
            end_date: 조회 종료일 (YYYYMMDD 형식)
            
        Returns:
            List[Dict[str, Any]]: 변환된 국내주식 손익 데이터 리스트
        """
        try:
            # KIS API 호출
            request = KISDomesticProfitRequest(
                INQR_STRT_DT=start_date,
                INQR_END_DT=end_date
            )
            
            response = await self.kis_service.test_domestic_profit(request)
            
            if response.rt_cd != "0" or not response.output1:
                logger.warning(f"국내주식 손익 데이터 조회 실패: {response.msg1}")
                return []
            
            # 데이터 변환
            converted_data = []
            for item in response.output1:
                try:
                    # Pydantic 모델이면 딕셔너리로 변환, 이미 딕셔너리면 그대로 사용
                    if hasattr(item, 'model_dump'):
                        item_dict = item.model_dump()
                    elif hasattr(item, 'dict'):
                        item_dict = item.dict()
                    else:
                        item_dict = item
                    
                    # 날짜 변환 (YYYYMMDD -> date)
                    trade_date = datetime.strptime(item_dict.get('trad_dt', ''), '%Y%m%d').date()
                    
                    # 수치 데이터 변환
                    def safe_decimal(value, default=0):
                        try:
                            return Decimal(str(value)) if value and str(value).replace('-', '').replace('.', '').isdigit() else default
                        except:
                            return default
                    
                    converted_item = {
                        'trade_date': trade_date,
                        'market_type': 'KR',
                        'exchange_code': None,  # 국내주식은 거래소 코드 없음
                        'symbol': item_dict.get('pdno', ''),
                        'instrument_name': item_dict.get('prdt_name', ''),
                        'currency_code': 'KRW',  # 국내주식은 원화
                        
                        'buy_qty': safe_decimal(item_dict.get('buy_qty')),
                        'buy_price': safe_decimal(item_dict.get('pchs_unpr')),
                        'buy_amount': safe_decimal(item_dict.get('buy_amt')),
                        
                        'sell_qty': safe_decimal(item_dict.get('sll_qty')),
                        'sell_price': safe_decimal(item_dict.get('sll_pric')),
                        'sell_amount': safe_decimal(item_dict.get('sll_amt')),
                        
                        'realized_pnl': safe_decimal(item_dict.get('rlzt_pfls')),
                        'pnl_rate': safe_decimal(item_dict.get('pfls_rt')),
                        
                        'fee': safe_decimal(item_dict.get('fee')),
                        'tax': safe_decimal(item_dict.get('tl_tax')),
                        'interest': safe_decimal(item_dict.get('loan_int')),
                        'exchange_rate': None,  # 국내주식은 환율 없음
                        
                        'note': f"국내주식 API 조회: {start_date}~{end_date}"
                    }
                    
                    converted_data.append(converted_item)
                    
                except Exception as e:
                    logger.warning(f"국내주식 손익 데이터 변환 중 오류 (건너뜀): {str(e)}")
                    continue
            
            logger.info(f"국내주식 손익 데이터 변환 완료: {len(converted_data)}건")
            return converted_data
            
        except Exception as e:
            logger.error(f"국내주식 손익 데이터 수집 중 오류 발생: {str(e)}")
            raise
    
    def get_realized_pnl_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """기간별 손익 데이터 조회
        
        저장된 손익 데이터를 조회합니다.
        
        Args:
            start_date: 조회 시작일
            end_date: 조회 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            List[Dict[str, Any]]: 조회된 손익 데이터 리스트
        """
        try:
            records = self.repository.get_by_date_range(start_date, end_date, market_type)
            
            # Dict 형태로 변환
            results = []
            for record in records:
                result = {
                    'id': record.id,
                    'trade_date': record.trade_date.isoformat(),
                    'market_type': record.market_type,
                    'exchange_code': record.exchange_code,
                    'symbol': record.symbol,
                    'instrument_name': record.instrument_name,
                    'currency_code': record.currency_code,
                    'buy_qty': float(record.buy_qty) if record.buy_qty else None,
                    'buy_price': float(record.buy_price) if record.buy_price else None,
                    'buy_amount': float(record.buy_amount) if record.buy_amount else None,
                    'sell_qty': float(record.sell_qty) if record.sell_qty else None,
                    'sell_price': float(record.sell_price) if record.sell_price else None,
                    'sell_amount': float(record.sell_amount) if record.sell_amount else None,
                    'realized_pnl': float(record.realized_pnl) if record.realized_pnl else None,
                    'pnl_rate': float(record.pnl_rate) if record.pnl_rate else None,
                    'fee': float(record.fee) if record.fee else None,
                    'tax': float(record.tax) if record.tax else None,
                    'interest': float(record.interest) if record.interest else None,
                    'exchange_rate': float(record.exchange_rate) if record.exchange_rate else None,
                    'note': record.note,
                    'created_at': record.created_at.isoformat() if record.created_at else None,
                    'updated_at': record.updated_at.isoformat() if record.updated_at else None
                }
                results.append(result)
            
            logger.info(f"손익 데이터 조회 완료: {len(results)}건")
            return results
            
        except Exception as e:
            logger.error(f"손익 데이터 조회 중 오류 발생: {str(e)}")
            raise
    
    def get_summary_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> Dict[str, Any]:
        """기간별 손익 요약 정보 조회
        
        저장된 손익 데이터의 요약 정보를 조회합니다.
        
        Args:
            start_date: 조회 시작일
            end_date: 조회 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            Dict[str, Any]: 요약 정보 딕셔너리
        """
        try:
            summary = self.repository.get_summary_by_date_range(start_date, end_date, market_type)
            
            logger.info(f"손익 요약 정보 조회 완료: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"손익 요약 정보 조회 중 오류 발생: {str(e)}")
            raise
    
    def delete_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> int:
        """기간별 손익 데이터 삭제
        
        지정된 기간의 손익 데이터를 삭제합니다.
        
        Args:
            start_date: 삭제 시작일
            end_date: 삭제 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            int: 삭제된 레코드 수
        """
        try:
            deleted_count = self.repository.delete_by_date_range(start_date, end_date, market_type)
            
            logger.info(f"손익 데이터 삭제 완료: {deleted_count}건")
            return deleted_count
            
        except Exception as e:
            logger.error(f"손익 데이터 삭제 중 오류 발생: {str(e)}")
            raise
