"""
KIS API를 활용한 펀더멘털 데이터 수집 서비스
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, func

from app.core.kis_client import KISClient
from app.features.fundamentals.models.fundamental_snapshot import FundamentalSnapshot
from app.features.fundamentals.models.dividend_history import DividendHistory
from app.shared.models.ticker import Ticker
import logging

logger = logging.getLogger(__name__)


class KISFundamentalService:
    """KIS API를 활용한 펀더멘털 데이터 수집 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
    
    def collect_fundamentals(self, ticker_id: int) -> Dict[str, Any]:
        """
        ticker_id를 받아서 펀더멘털 데이터를 수집하고 저장
        
        Returns:
            Dict[str, Any]: 수집 결과 요약
        """
        try:
            # 1. ticker_id로 ticker 정보 조회
            logger.info(f"🔍 DEBUG: Looking up ticker_id: {ticker_id}")
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                raise ValueError(f"Ticker ID {ticker_id} not found")
            
            logger.info(f"🔍 DEBUG: Found ticker - ID: {ticker.id}, Symbol: {ticker.symbol}, Country: {ticker.country}, Exchange: {ticker.exchange}")
            
            # 한국 주식만 지원 (exchange가 KOSPI, KOSDAQ인 경우)
            if ticker.country != "KR":
                raise ValueError(f"Only Korean stocks are supported. Ticker {ticker.symbol} is from {ticker.country}")
            
            # symbol에서 종목코드 추출 (예: 005930.KQ -> 005930)
            symbol = ticker.symbol
            if "." in symbol:
                symbol = symbol.split(".")[0]
            
            logger.info(f"🔍 DEBUG: Extracted symbol: {symbol} (from {ticker.symbol})")
            logger.info(f"Collecting fundamentals for ticker_id: {ticker_id}, symbol: {symbol}")
            
            # 2. KIS API 호출
            logger.info(f"📡 DEBUG: Calling KIS stock_basic_info API for {symbol}")
            stock_basic_result = self.kis_client.stock_basic_info(symbol, "300")
            logger.info(f"📡 DEBUG: Stock basic info response: rt_cd={stock_basic_result.get('rt_cd')}, msg1={stock_basic_result.get('msg1', 'N/A')}")
            
            logger.info(f"📡 DEBUG: Calling KIS financial_ratio API for {symbol}")
            financial_ratio_result = self.kis_client.financial_ratio(symbol, "0")  # 년간
            logger.info(f"📡 DEBUG: Financial ratio response: rt_cd={financial_ratio_result.get('rt_cd')}, msg1={financial_ratio_result.get('msg1', 'N/A')}")
            
            # 배당일정 조회 (최근 2년)
            two_years_ago = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
            today = datetime.now().strftime("%Y%m%d")
            logger.info(f"📡 DEBUG: Calling KIS dividend_schedule API for {symbol} from {two_years_ago} to {today}")
            dividend_result = self.kis_client.dividend_schedule("0", two_years_ago, today, symbol)
            logger.info(f"📡 DEBUG: Dividend schedule response: rt_cd={dividend_result.get('rt_cd')}, msg1={dividend_result.get('msg1', 'N/A')}")
            logger.info(f"📡 DEBUG: Dividend data count: {len(dividend_result.get('output1', []))}")
            
            # 3. API 응답 검증
            if stock_basic_result.get("rt_cd") != "0":
                raise ValueError(f"Stock basic info API failed: {stock_basic_result.get('msg1', 'Unknown error')}")
            
            if financial_ratio_result.get("rt_cd") != "0":
                raise ValueError(f"Financial ratio API failed: {financial_ratio_result.get('msg1', 'Unknown error')}")
            
            if dividend_result.get("rt_cd") != "0":
                logger.warning(f"Dividend schedule API failed: {dividend_result.get('msg1', 'Unknown error')}")
                dividend_result = {"output1": []}  # 빈 배열로 처리
            
            # 4. 데이터 파싱 및 저장
            logger.info(f"💾 DEBUG: Parsing and saving fundamental snapshot for ticker_id: {ticker_id}")
            fundamental_data = self._parse_and_save_fundamental_snapshot(
                ticker_id, stock_basic_result, financial_ratio_result
            )
            logger.info(f"💾 DEBUG: Fundamental snapshot result: {fundamental_data is not None}")
            
            logger.info(f"💾 DEBUG: Parsing and saving dividend history for ticker_id: {ticker_id}")
            dividend_count = self._parse_and_save_dividend_history(
                ticker_id, dividend_result
            )
            logger.info(f"💾 DEBUG: Dividend records processed: {dividend_count}")
            
            return {
                "success": True,
                "ticker_id": ticker_id,
                "symbol": symbol,
                "fundamental_updated": fundamental_data is not None,
                "dividend_records_processed": dividend_count,
                "api_status": {
                    "stock_basic": stock_basic_result.get("rt_cd"),
                    "financial_ratio": financial_ratio_result.get("rt_cd"),
                    "dividend_schedule": dividend_result.get("rt_cd")
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to collect fundamentals for ticker_id {ticker_id}: {str(e)}")
            raise
    
    def _parse_and_save_fundamental_snapshot(
        self, 
        ticker_id: int, 
        stock_basic_result: Dict[str, Any], 
        financial_ratio_result: Dict[str, Any]
    ) -> Optional[FundamentalSnapshot]:
        """펀더멘털 스냅샷 데이터 파싱 및 저장"""
        
        try:
            # 주식기본조회 데이터 파싱
            stock_basic_output = stock_basic_result.get("output", {})
            lstg_stqt = self._safe_float(stock_basic_output.get("lstg_stqt"))  # 상장 주수
            bfdy_clpr = self._safe_float(stock_basic_output.get("bfdy_clpr"))  # 전일종가
            
            # 재무비율 데이터 파싱 (첫 번째 년도 데이터 사용)
            financial_output = financial_ratio_result.get("output", [])
            if not financial_output:
                raise ValueError("No financial ratio data available")
            
            first_year_data = financial_output[0]
            eps = self._safe_float(first_year_data.get("eps"))  # EPS
            bps = self._safe_float(first_year_data.get("bps"))  # BPS
            lblt_rate = self._safe_float(first_year_data.get("lblt_rate"))  # 부채비율
            
            # 계산된 지표
            market_cap = None
            per = None
            pbr = None
            
            if lstg_stqt and bfdy_clpr:
                market_cap = lstg_stqt * bfdy_clpr
            
            if bfdy_clpr and eps and eps > 0:
                per = round(bfdy_clpr / eps, 4)
            
            if bfdy_clpr and bps and bps > 0:
                pbr = round(bfdy_clpr / bps, 4)
            
            # 기존 스냅샷 조회 또는 새로 생성
            snapshot = self.db.query(FundamentalSnapshot).filter(
                FundamentalSnapshot.ticker_id == ticker_id
            ).first()
            
            if snapshot:
                # 기존 데이터 업데이트 (null이 아닌 값만)
                if market_cap is not None:
                    snapshot.market_cap = market_cap
                if per is not None:
                    snapshot.per = per
                if pbr is not None:
                    snapshot.pbr = pbr
                if lblt_rate is not None:
                    snapshot.debt_ratio = lblt_rate
                snapshot.updated_at = datetime.utcnow()
            else:
                # 새로 생성
                snapshot = FundamentalSnapshot(
                    ticker_id=ticker_id,
                    market_cap=market_cap,
                    per=per,
                    pbr=pbr,
                    debt_ratio=lblt_rate,
                    updated_at=datetime.utcnow()
                )
                self.db.add(snapshot)
            
            self.db.commit()
            logger.info(f"Fundamental snapshot updated for ticker_id: {ticker_id}")
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to parse and save fundamental snapshot for ticker_id {ticker_id}: {str(e)}")
            self.db.rollback()
            return None
    
    def _parse_and_save_dividend_history(
        self, 
        ticker_id: int, 
        dividend_result: Dict[str, Any]
    ) -> int:
        """배당이력 데이터 파싱 및 저장"""
        
        try:
            dividend_output = dividend_result.get("output1", [])
            processed_count = 0
            
            logger.info(f"🔍 DEBUG: Processing dividend data for ticker_id: {ticker_id}")
            logger.info(f"🔍 DEBUG: Raw dividend output count: {len(dividend_output)}")
            
            if len(dividend_output) > 0:
                logger.info(f"🔍 DEBUG: Sample dividend data: {dividend_output[0] if dividend_output else 'None'}")
            
            for i, dividend_data in enumerate(dividend_output):
                logger.info(f"🔍 DEBUG: Processing dividend record {i+1}/{len(dividend_output)}: {dividend_data}")
                # 배당금지급일이 있는 경우만 처리
                divi_pay_dt = dividend_data.get("divi_pay_dt")
                logger.info(f"🔍 DEBUG: Dividend payment date: '{divi_pay_dt}'")
                
                if not divi_pay_dt or divi_pay_dt.strip() == "":
                    logger.info(f"⚠️ DEBUG: Skipping dividend record {i+1} - no payment date")
                    continue
                
                try:
                    # 날짜 파싱 (YYYY/MM/DD 또는 YYYY-MM-DD 형식 지원)
                    if "/" in divi_pay_dt:
                        payment_date = datetime.strptime(divi_pay_dt, "%Y/%m/%d").date()
                    else:
                        payment_date = datetime.strptime(divi_pay_dt, "%Y-%m-%d").date()
                    logger.info(f"🔍 DEBUG: Parsed payment date: {payment_date}")
                except ValueError:
                    logger.warning(f"⚠️ DEBUG: Invalid dividend payment date format: {divi_pay_dt}")
                    continue
                
                # 배당 데이터 파싱
                per_sto_divi_amt = self._safe_float(dividend_data.get("per_sto_divi_amt"))  # 현금배당금
                divi_rate = self._safe_float(dividend_data.get("divi_rate")) / 100
                
                logger.info(f"🔍 DEBUG: Dividend amount: {per_sto_divi_amt}, rate: {divi_rate}")

                # 배당금이 있는 경우만 저장
                if per_sto_divi_amt is None or per_sto_divi_amt <= 0:
                    logger.info(f"⚠️ DEBUG: Skipping dividend record {i+1} - no valid dividend amount")
                    continue
                
                # 기존 배당이력 조회
                logger.info(f"🔍 DEBUG: Checking existing dividend record for ticker_id: {ticker_id}, payment_date: {payment_date}")
                existing_dividend = self.db.query(DividendHistory).filter(
                    and_(
                        DividendHistory.ticker_id == ticker_id,
                        DividendHistory.payment_date == payment_date
                    )
                ).first()
                
                if existing_dividend:
                    # 기존 데이터 업데이트
                    logger.info(f"💾 DEBUG: Updating existing dividend record: {existing_dividend.id}")
                    existing_dividend.dividend_per_share = per_sto_divi_amt
                    existing_dividend.dividend_yield = divi_rate
                    existing_dividend.currency = "KRW"
                else:
                    # 새로 생성
                    logger.info(f"💾 DEBUG: Creating new dividend record for ticker_id: {ticker_id}")
                    new_dividend = DividendHistory(
                        ticker_id=ticker_id,
                        dividend_per_share=per_sto_divi_amt,
                        dividend_yield=divi_rate,
                        payment_date=payment_date,
                        currency="KRW",
                        created_at=datetime.utcnow()
                    )
                    self.db.add(new_dividend)
                
                processed_count += 1
                logger.info(f"✅ DEBUG: Successfully processed dividend record {i+1}, total processed: {processed_count}")
            
            self.db.commit()
            logger.info(f"Processed {processed_count} dividend records for ticker_id: {ticker_id}")
            return processed_count
            
        except Exception as e:
            logger.error(f"Failed to parse and save dividend history for ticker_id {ticker_id}: {str(e)}")
            self.db.rollback()
            return 0
    
    def sync_all_korean_fundamentals(self) -> Dict[str, Any]:
        """
        모든 한국 주식의 펀더멘털 데이터를 동기화
        
        Returns:
            Dict[str, Any]: 동기화 결과 요약
        """
        from datetime import datetime
        
        start_time = datetime.utcnow()
        logger.info("Starting Korean fundamentals sync for all tickers")
        
        # 한국 주식 티커 조회 (KOE)
        logger.info("🔍 DEBUG: Querying Korean tickers from database...")
        korean_tickers = self.db.query(Ticker).filter(
            and_(
                Ticker.country == "KR",
                Ticker.exchange == "KOE"
            )
        ).all()
        
        total_tickers = len(korean_tickers)
        processed = 0
        errors = []
        results = []
        
        logger.info(f"🔍 DEBUG: Found {total_tickers} Korean tickers to sync")
        
        # 디버깅: 티커 샘플 출력
        if total_tickers > 0:
            sample_tickers = korean_tickers[:5]  # 처음 5개만 샘플로 출력
            logger.info(f"🔍 DEBUG: Sample tickers: {[(t.id, t.symbol, t.exchange, t.country) for t in sample_tickers]}")
        else:
            logger.warning("⚠️ DEBUG: No Korean tickers found! Checking database...")
            # 전체 티커 수 확인
            all_tickers = self.db.query(Ticker).all()
            logger.info(f"🔍 DEBUG: Total tickers in database: {len(all_tickers)}")
            
            # 국가별 티커 수 확인
            country_counts = self.db.query(Ticker.country, func.count(Ticker.id)).group_by(Ticker.country).all()
            logger.info(f"🔍 DEBUG: Tickers by country: {country_counts}")
            
            # 거래소별 티커 수 확인
            exchange_counts = self.db.query(Ticker.exchange, func.count(Ticker.id)).group_by(Ticker.exchange).all()
            logger.info(f"🔍 DEBUG: Tickers by exchange: {exchange_counts}")
        
        for ticker in korean_tickers:
            try:
                logger.info(f"🔄 DEBUG: Processing ticker {ticker.id} ({ticker.symbol}) from {ticker.exchange}")
                result = self.collect_fundamentals(ticker.id)
                logger.info(f"✅ DEBUG: Successfully processed ticker {ticker.id} - fundamental: {result.get('fundamental_updated', False)}, dividends: {result.get('dividend_records_processed', 0)}")
                
                results.append({
                    "ticker_id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "success": result["success"],
                    "fundamental_updated": result.get("fundamental_updated", False),
                    "dividend_records_processed": result.get("dividend_records_processed", 0)
                })
                processed += 1
                
                if processed % 10 == 0:
                    logger.info(f"📊 DEBUG: Processed {processed}/{total_tickers} tickers")
                    
            except Exception as e:
                error_msg = f"Failed to sync ticker {ticker.id} ({ticker.symbol}): {str(e)}"
                errors.append(error_msg)
                logger.error(f"❌ DEBUG: {error_msg}")
                
                results.append({
                    "ticker_id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "success": False,
                    "error": str(e)
                })
        
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        # 통계 계산
        successful_syncs = len([r for r in results if r["success"]])
        total_dividend_records = sum([r.get("dividend_records_processed", 0) for r in results])
        
        logger.info(f"Korean fundamentals sync completed: {successful_syncs}/{total_tickers} successful, {total_dividend_records} dividend records processed in {duration_seconds:.2f}s")
        
        return {
            "success": True,
            "total_tickers": total_tickers,
            "processed": processed,
            "successful": successful_syncs,
            "failed": len(errors),
            "total_dividend_records": total_dividend_records,
            "errors": errors,
            "results": results,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_seconds
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        """안전하게 float로 변환"""
        if value is None or value == "" or value == "N/A":
            return None
        try:
            return float(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return None
