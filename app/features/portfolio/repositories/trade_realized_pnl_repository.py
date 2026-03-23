# app/features/portfolio/repositories/trade_realized_pnl_repository.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert
from datetime import date, datetime
from decimal import Decimal

from app.features.portfolio.models.trade_realized_pnl import TradeRealizedPnl
import logging

logger = logging.getLogger(__name__)


class TradeRealizedPnlRepository:
    """일자별 종목 손익 데이터 저장소
    
    TradeRealizedPnl 모델에 대한 데이터베이스 작업을 처리합니다.
    KIS API에서 조회한 손익 데이터를 저장하고 조회하는 기능을 제공합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_or_update(self, data: Dict[str, Any]) -> TradeRealizedPnl:
        """손익 데이터 생성 또는 업데이트
        
        일자별, 시장별, 종목별로 유니크한 데이터를 생성하거나 업데이트합니다.
        이미 존재하는 데이터가 있으면 업데이트하고, 없으면 새로 생성합니다.
        
        Args:
            data: 저장할 손익 데이터 딕셔너리
            
        Returns:
            TradeRealizedPnl: 저장된 손익 데이터 객체
        """
        try:
            # 유니크 제약 조건에 맞는 키 생성
            unique_keys = {
                'market_type': data['market_type'],
                'trade_date': data['trade_date'],
                'symbol': data['symbol']
            }
            
            # 기존 데이터 조회
            existing_record = self.db.query(TradeRealizedPnl).filter(
                and_(
                    TradeRealizedPnl.market_type == unique_keys['market_type'],
                    TradeRealizedPnl.trade_date == unique_keys['trade_date'],
                    TradeRealizedPnl.symbol == unique_keys['symbol']
                )
            ).first()
            
            if existing_record:
                # 기존 데이터 업데이트
                for key, value in data.items():
                    if hasattr(existing_record, key) and value is not None:
                        setattr(existing_record, key, value)
                
                existing_record.updated_at = datetime.now()
                self.db.commit()
                self.db.refresh(existing_record)
                
                logger.info(f"손익 데이터 업데이트 완료: {unique_keys}")
                return existing_record
            else:
                # 새 데이터 생성
                new_record = TradeRealizedPnl(**data)
                self.db.add(new_record)
                self.db.commit()
                self.db.refresh(new_record)
                
                logger.info(f"손익 데이터 생성 완료: {unique_keys}")
                return new_record
                
        except Exception as e:
            logger.error(f"손익 데이터 저장 중 오류 발생: {str(e)}")
            self.db.rollback()
            raise
    
    def bulk_create_or_update(self, data_list: List[Dict[str, Any]]) -> List[TradeRealizedPnl]:
        """손익 데이터 일괄 생성 또는 업데이트
        
        여러 개의 손익 데이터를 일괄 처리합니다.
        
        Args:
            data_list: 저장할 손익 데이터 리스트
            
        Returns:
            List[TradeRealizedPnl]: 저장된 손익 데이터 객체 리스트
        """
        try:
            results = []
            
            for data in data_list:
                result = self.create_or_update(data)
                results.append(result)
            
            logger.info(f"손익 데이터 일괄 저장 완료: {len(data_list)}건")
            return results
            
        except Exception as e:
            logger.error(f"손익 데이터 일괄 저장 중 오류 발생: {str(e)}")
            self.db.rollback()
            raise
    
    def get_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> List[TradeRealizedPnl]:
        """기간별 손익 데이터 조회
        
        지정된 기간 내의 손익 데이터를 조회합니다.
        
        Args:
            start_date: 조회 시작일
            end_date: 조회 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            List[TradeRealizedPnl]: 조회된 손익 데이터 리스트
        """
        try:
            query = self.db.query(TradeRealizedPnl).filter(
                and_(
                    TradeRealizedPnl.trade_date >= start_date,
                    TradeRealizedPnl.trade_date <= end_date
                )
            )
            
            if market_type:
                query = query.filter(TradeRealizedPnl.market_type == market_type)
            
            results = query.order_by(TradeRealizedPnl.trade_date.desc(), TradeRealizedPnl.symbol).all()
            
            logger.info(f"손익 데이터 조회 완료: {len(results)}건 (기간: {start_date} ~ {end_date}, 시장: {market_type or '전체'})")
            return results
            
        except Exception as e:
            logger.error(f"손익 데이터 조회 중 오류 발생: {str(e)}")
            raise
    
    def get_by_symbol(self, symbol: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[TradeRealizedPnl]:
        """종목별 손익 데이터 조회
        
        특정 종목의 손익 데이터를 조회합니다.
        
        Args:
            symbol: 종목코드
            start_date: 조회 시작일 (선택사항)
            end_date: 조회 종료일 (선택사항)
            
        Returns:
            List[TradeRealizedPnl]: 조회된 손익 데이터 리스트
        """
        try:
            query = self.db.query(TradeRealizedPnl).filter(TradeRealizedPnl.symbol == symbol)
            
            if start_date:
                query = query.filter(TradeRealizedPnl.trade_date >= start_date)
            
            if end_date:
                query = query.filter(TradeRealizedPnl.trade_date <= end_date)
            
            results = query.order_by(TradeRealizedPnl.trade_date.desc()).all()
            
            logger.info(f"종목별 손익 데이터 조회 완료: {symbol}, {len(results)}건")
            return results
            
        except Exception as e:
            logger.error(f"종목별 손익 데이터 조회 중 오류 발생: {str(e)}")
            raise
    
    def get_by_trade_date(self, trade_date: date) -> List[TradeRealizedPnl]:
        """특정 일자의 손익 데이터 조회
        
        특정 매매일자의 모든 손익 데이터를 조회합니다.
        
        Args:
            trade_date: 매매일자
            
        Returns:
            List[TradeRealizedPnl]: 조회된 손익 데이터 리스트
        """
        try:
            results = self.db.query(TradeRealizedPnl).filter(
                TradeRealizedPnl.trade_date == trade_date
            ).order_by(TradeRealizedPnl.market_type, TradeRealizedPnl.symbol).all()
            
            logger.info(f"일자별 손익 데이터 조회 완료: {trade_date}, {len(results)}건")
            return results
            
        except Exception as e:
            logger.error(f"일자별 손익 데이터 조회 중 오류 발생: {str(e)}")
            raise
    
    def delete_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> int:
        """기간별 손익 데이터 삭제
        
        지정된 기간 내의 손익 데이터를 삭제합니다.
        
        Args:
            start_date: 삭제 시작일
            end_date: 삭제 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            int: 삭제된 레코드 수
        """
        try:
            query = self.db.query(TradeRealizedPnl).filter(
                and_(
                    TradeRealizedPnl.trade_date >= start_date,
                    TradeRealizedPnl.trade_date <= end_date
                )
            )
            
            if market_type:
                query = query.filter(TradeRealizedPnl.market_type == market_type)
            
            deleted_count = query.delete(synchronize_session=False)
            self.db.commit()
            
            logger.info(f"손익 데이터 삭제 완료: {deleted_count}건 (기간: {start_date} ~ {end_date}, 시장: {market_type or '전체'})")
            return deleted_count
            
        except Exception as e:
            logger.error(f"손익 데이터 삭제 중 오류 발생: {str(e)}")
            self.db.rollback()
            raise
    
    def get_summary_by_date_range(self, start_date: date, end_date: date, market_type: Optional[str] = None) -> Dict[str, Any]:
        """기간별 손익 요약 정보 조회
        
        지정된 기간 내의 손익 데이터 요약 정보를 조회합니다.
        
        Args:
            start_date: 조회 시작일
            end_date: 조회 종료일
            market_type: 시장 구분 ('KR', 'US', None: 전체)
            
        Returns:
            Dict[str, Any]: 요약 정보 딕셔너리
        """
        try:
            from sqlalchemy import func
            
            query = self.db.query(TradeRealizedPnl).filter(
                and_(
                    TradeRealizedPnl.trade_date >= start_date,
                    TradeRealizedPnl.trade_date <= end_date
                )
            )
            
            if market_type:
                query = query.filter(TradeRealizedPnl.market_type == market_type)
            
            # 집계 쿼리
            summary = query.with_entities(
                func.count(TradeRealizedPnl.id).label('total_count'),
                func.sum(TradeRealizedPnl.realized_pnl).label('total_pnl'),
                func.sum(TradeRealizedPnl.buy_amount).label('total_buy_amount'),
                func.sum(TradeRealizedPnl.sell_amount).label('total_sell_amount'),
                func.sum(TradeRealizedPnl.fee).label('total_fee'),
                func.sum(TradeRealizedPnl.tax).label('total_tax'),
                func.sum(TradeRealizedPnl.interest).label('total_interest')
            ).first()
            
            result = {
                'period': f"{start_date} ~ {end_date}",
                'market_type': market_type or '전체',
                'total_count': summary.total_count or 0,
                'total_pnl': float(summary.total_pnl) if summary.total_pnl else 0.0,
                'total_buy_amount': float(summary.total_buy_amount) if summary.total_buy_amount else 0.0,
                'total_sell_amount': float(summary.total_sell_amount) if summary.total_sell_amount else 0.0,
                'total_fee': float(summary.total_fee) if summary.total_fee else 0.0,
                'total_tax': float(summary.total_tax) if summary.total_tax else 0.0,
                'total_interest': float(summary.total_interest) if summary.total_interest else 0.0
            }
            
            logger.info(f"손익 요약 정보 조회 완료: {result}")
            return result
            
        except Exception as e:
            logger.error(f"손익 요약 정보 조회 중 오류 발생: {str(e)}")
            raise
