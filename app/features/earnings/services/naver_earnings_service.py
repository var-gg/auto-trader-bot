# app/features/earnings/services/naver_earnings_service.py

from datetime import datetime, date, timedelta
import requests
import logging
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from app.features.earnings.repositories.earnings_repository import EarningsRepository
from app.shared.models.ticker import Ticker
from app.features.earnings.services.kind_crawling_service import KindCrawlingService

logger = logging.getLogger(__name__)

class NaverEarningsService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = EarningsRepository(db)
        self.base_url = "https://navercomp.wisereport.co.kr/company/ajax/c1050001_data.aspx"
        self.kind_service = KindCrawlingService()
    
    def sync_earnings_by_ticker_id(self, ticker_id: int) -> Dict[str, Any]:
        """
        티커 ID로 네이버 실적/예상 데이터 동기화
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            동기화 결과 정보
        """
        # 티커 정보 조회
        ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
        
        # 한국 주식만 처리
        if ticker.country != "KR":
            raise ValueError(f"티커 {ticker.symbol}은 한국 주식이 아닙니다. (country: {ticker.country})")
        
        logger.info(f"네이버 실적 데이터 동기화 시작: {ticker.symbol} (ID: {ticker_id})")
        
        # 네이버 API 호출
        naver_data = self._fetch_naver_data(ticker.symbol)
        if not naver_data or not naver_data.get("JsonData"):
            logger.warning(f"{ticker.symbol}: 네이버에서 데이터를 가져올 수 없습니다.")
            return {
                "status": "error",
                "ticker_symbol": ticker.symbol,
                "message": "네이버에서 데이터를 가져올 수 없습니다."
            }
        
        # 데이터 변환 및 저장
        processed_count = 0
        for item in naver_data["JsonData"]:
            try:
                event_data = self._map_naver_item_to_earnings_event(item, ticker.symbol)
                if event_data:
                    self.repo.upsert_event(event_data)
                    processed_count += 1
            except Exception as e:
                logger.error(f"{ticker.symbol}: 데이터 변환 오류 - {e}")
                continue
        
        logger.info(f"{ticker.symbol}: {processed_count}개 데이터 동기화 완료")
        
        return {
            "status": "success",
            "ticker_symbol": ticker.symbol,
            "processed_count": processed_count,
            "source": "naver"
        }
    
    def _fetch_naver_data(self, ticker_symbol: str) -> Optional[Dict[str, Any]]:
        """
        네이버 API에서 실적/예상 데이터 조회
        
        Args:
            ticker_symbol: 티커 심볼 (예: "005930")
            
        Returns:
            네이버 API 응답 데이터
        """
        today = datetime.now().strftime("%Y%m%d")
        
        params = {
            "flag": "2",
            "cmp_cd": ticker_symbol,
            "finGubun": "MAIN", 
            "frq": "1",
            "sDT": today,
            "chartType": "svg"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"{ticker_symbol}: 네이버 API 응답 수신 완료")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"{ticker_symbol}: 네이버 API 호출 실패 - {e}")
            raise
        except Exception as e:
            logger.error(f"{ticker_symbol}: 네이버 API 응답 파싱 실패 - {e}")
            raise
    
    def _map_naver_item_to_earnings_event(self, item: Dict[str, Any], ticker_symbol: str) -> Optional[Dict[str, Any]]:
        """
        네이버 API 응답 항목을 earnings_event 데이터로 변환
        
        Args:
            item: 네이버 API 응답의 개별 항목
            ticker_symbol: 티커 심볼
            
        Returns:
            earnings_event 데이터 딕셔너리
        """
        try:
            # YYMM 파싱 (예: "2024.09(A)" 또는 "2025.09(E)")
            yymm = item.get("YYMM", "")
            if not yymm:
                return None
            
            # 년월과 실적/예상 구분 파싱
            fiscal_year, fiscal_quarter, is_actual = self._parse_yymm(yymm)
            if not fiscal_year or not fiscal_quarter:
                logger.warning(f"{ticker_symbol}: YYMM 파싱 실패 - {yymm}")
                return None
            
            # 기본 데이터 구성
            event_data = {
                "ticker_symbol": ticker_symbol,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "source": "naver"
            }
            
            if is_actual:
                # 실적 데이터 (A)
                event_data.update({
                    "status": "reported",
                    "period_end_date": self._get_quarter_end_date(fiscal_year, fiscal_quarter),
                    "actual_eps": self._safe_float(item.get("EPS")),
                    "actual_revenue": self._safe_float(item.get("SALES"))
                })
                
                # 예상 EPS가 있으면 서프라이즈 계산
                estimate_eps = self._get_estimate_eps_for_surprise(ticker_symbol, fiscal_year, fiscal_quarter)
                if estimate_eps and event_data["actual_eps"]:
                    surprise_eps = ((event_data["actual_eps"] - estimate_eps) / estimate_eps * 100)
                    event_data["surprise_eps"] = round(surprise_eps, 4)
                
            else:
                # 예상 데이터 (E)
                event_data.update({
                    "status": "scheduled",
                    "estimate_eps": self._safe_float(item.get("EPS")),
                    "estimate_revenue": self._safe_float(item.get("SALES"))
                })
            
            return event_data
            
        except Exception as e:
            logger.error(f"{ticker_symbol}: 데이터 변환 중 오류 - {e}")
            return None
    
    def _parse_yymm(self, yymm: str) -> tuple[Optional[int], Optional[int], Optional[bool]]:
        """
        YYMM 문자열 파싱 (예: "2024.09(A)" -> 2024, 3, True)
        
        Args:
            yymm: YYMM 문자열
            
        Returns:
            (fiscal_year, fiscal_quarter, is_actual)
        """
        try:
            # "(A)" 또는 "(E)" 제거
            clean_yymm = yymm.replace("(A)", "").replace("(E)", "")
            is_actual = "(A)" in yymm
            
            # 년월 분리
            parts = clean_yymm.split(".")
            if len(parts) != 2:
                return None, None, None
            
            year = int(parts[0])
            month = int(parts[1])
            
            # 분기 계산
            quarter = (month - 1) // 3 + 1
            
            return year, quarter, is_actual
            
        except Exception as e:
            logger.error(f"YYMM 파싱 오류: {yymm} - {e}")
            return None, None, None
    
    def _get_quarter_end_date(self, year: int, quarter: int) -> date:
        """
        분기 종료일 계산
        
        Args:
            year: 년도
            quarter: 분기 (1-4)
            
        Returns:
            분기 종료일
        """
        if quarter == 1:
            return date(year, 3, 31)
        elif quarter == 2:
            return date(year, 6, 30)
        elif quarter == 3:
            return date(year, 9, 30)
        else:  # quarter == 4
            return date(year, 12, 31)
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """
        안전한 float 변환
        
        Args:
            value: 변환할 값
            
        Returns:
            float 값 또는 None
        """
        if value is None or value == "":
            return None
        
        try:
            # 쉼표 제거 후 변환
            if isinstance(value, str):
                value = value.replace(",", "")
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _get_estimate_eps_for_surprise(self, ticker_symbol: str, fiscal_year: int, fiscal_quarter: int) -> Optional[float]:
        """
        서프라이즈 계산을 위한 예상 EPS 조회
        (실제 데이터가 나오기 전에 저장된 예상 데이터에서 조회)
        
        Args:
            ticker_symbol: 티커 심볼
            fiscal_year: 회계연도
            fiscal_quarter: 회계분기
            
        Returns:
            예상 EPS 값
        """
        try:
            from app.features.earnings.models.earnings_event import EarningsEvent
            
            # 해당 분기의 예상 EPS 조회
            existing_event = (
                self.db.query(EarningsEvent)
                .filter(EarningsEvent.ticker_symbol == ticker_symbol)
                .filter(EarningsEvent.fiscal_year == fiscal_year)
                .filter(EarningsEvent.fiscal_quarter == fiscal_quarter)
                .filter(EarningsEvent.estimate_eps.isnot(None))
                .first()
            )
            
            return existing_event.estimate_eps if existing_event else None
            
        except Exception as e:
            logger.error(f"예상 EPS 조회 오류: {e}")
            return None
    
    def sync_all_korean_earnings(self) -> Dict[str, Any]:
        """
        KOE 거래소의 모든 국내주식 실적/예상 데이터 동기화
        
        Returns:
            동기화 결과 정보
        """
        # KOE 거래소 티커들 조회
        korean_tickers = (
            self.db.query(Ticker)
            .filter(Ticker.country == "KR")
            .filter(Ticker.exchange == "KOE")
            .all()
        )
        
        if not korean_tickers:
            logger.warning("KOE 거래소 티커가 없습니다.")
            return {
                "status": "error",
                "message": "KOE 거래소 티커가 없습니다.",
                "processed_count": 0,
                "total_count": 0
            }
        
        logger.info(f"총 {len(korean_tickers)}개 국내주식의 실적 데이터 동기화를 시작합니다.")
        
        processed_count = 0
        error_count = 0
        results = []
        
        for idx, ticker in enumerate(korean_tickers, start=1):
            try:
                logger.info(f"[{idx}/{len(korean_tickers)}] {ticker.symbol} 처리 중...")
                
                result = self.sync_earnings_by_ticker_id(ticker.id)
                if result.get("status") == "success":
                    processed_count += result.get("processed_count", 0)
                    results.append({
                        "ticker_symbol": ticker.symbol,
                        "status": "success",
                        "processed_count": result.get("processed_count", 0)
                    })
                else:
                    error_count += 1
                    results.append({
                        "ticker_symbol": ticker.symbol,
                        "status": "error",
                        "message": result.get("message", "Unknown error")
                    })
                    
            except Exception as e:
                error_count += 1
                logger.error(f"{ticker.symbol} 동기화 중 오류 발생: {e}")
                results.append({
                    "ticker_symbol": ticker.symbol,
                    "status": "error",
                    "message": str(e)
                })
                continue
        
        logger.info(f"국내주식 실적 동기화 완료: 성공 {len(korean_tickers) - error_count}개, 실패 {error_count}개")
        
        return {
            "status": "completed",
            "total_tickers": len(korean_tickers),
            "processed_count": processed_count,
            "success_count": len(korean_tickers) - error_count,
            "error_count": error_count,
            "source": "naver",
            "results": results
        }
    
    def _infer_fiscal_period_from_report_date(self, report_date: date) -> tuple[int, int]:
        """실적 발표일로부터 가장 가능성 높은 회계 분기 추정."""
        month = report_date.month
        year = report_date.year

        if month <= 3:
            return year - 1, 4
        if month <= 6:
            return year, 1
        if month <= 9:
            return year, 2
        return year, 3

    def _select_best_scheduled_event(self, company_code: str, report_date: date):
        """KIND에서 찾은 발표일에 가장 plausbile한 scheduled row 선택."""
        from app.features.earnings.models.earnings_event import EarningsEvent

        scheduled_events = (
            self.db.query(EarningsEvent)
            .filter(EarningsEvent.ticker_symbol == company_code)
            .filter(EarningsEvent.status == "scheduled")
            .all()
        )

        if not scheduled_events:
            return None

        target_year, target_quarter = self._infer_fiscal_period_from_report_date(report_date)

        exact_match = [
            e for e in scheduled_events
            if e.fiscal_year == target_year and e.fiscal_quarter == target_quarter
        ]
        if exact_match:
            exact_match.sort(key=lambda e: (e.preferred_report_date is not None, e.preferred_report_date or date.max))
            return exact_match[0]

        def quarter_distance(event) -> int:
            return abs(((int(event.fiscal_year) * 4) + int(event.fiscal_quarter)) - ((target_year * 4) + target_quarter))

        scheduled_events.sort(
            key=lambda e: (
                quarter_distance(e),
                e.preferred_report_date is not None,
                -(int(e.fiscal_year or 0)),
                -(int(e.fiscal_quarter or 0)),
            )
        )
        return scheduled_events[0]

    async def sync_earnings_schedule(self) -> Dict[str, Any]:
        """
        KIND에서 실적발표일자 크롤링하여 earnings_event에 통합
        
        Returns:
            동기화 결과 정보
        """
        logger.info("실적발표일자 크롤링 및 통합을 시작합니다.")
        
        # 오늘 날짜부터 3개월 후까지
        today = datetime.now().date()
        from_date = today.strftime("%Y-%m-%d")
        to_date = (today + timedelta(days=90)).strftime("%Y-%m-%d")
        
        processed_count = 0
        total_found = 0
        
        # "실적발표"와 "실적 발표" 두 번 검색
        search_titles = ["실적발표", "실적 발표"]
        
        for title in search_titles:
            try:
                logger.info(f'"{title}" 검색 시작...')
                
                # KIND 크롤링 실행
                result = await self.kind_service.crawl_ir_schedule_advanced(
                    title=title,
                    from_date=from_date,
                    to_date=to_date,
                    current_page_size=100
                )
                
                if not result.get("success"):
                    logger.error(f'"{title}" 검색 실패: {result.get("error")}')
                    continue
                
                found_count = result.get("extracted_count", 0)
                total_found += found_count
                logger.info(f'"{title}" 검색 완료: {found_count}건 발견')
                
                # 결과 데이터 처리
                for company_data in result.get("results", []):
                    try:
                        company_code = company_data["company_code"]
                        report_date_str = company_data["date"]
                        company_name = company_data["company_name"]
                        
                        # 해당 종목이 ticker 테이블에 존재하는지 확인
                        ticker = (
                            self.db.query(Ticker)
                            .filter(Ticker.symbol == company_code)
                            .filter(Ticker.country == "KR")
                            .first()
                        )
                        
                        if not ticker:
                            logger.debug(f"종목코드 {company_code}는 ticker 테이블에 없습니다.")
                            continue
                        
                        # 날짜 파싱
                        report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
                        
                        closest_scheduled = self._select_best_scheduled_event(company_code, report_date)

                        if not closest_scheduled:
                            logger.debug(f"{company_code} ({company_name}): scheduled 데이터가 없어서 건너뜁니다.")
                            continue

                        closest_scheduled.report_date = report_date
                        closest_scheduled.confirmed_report_date = report_date
                        closest_scheduled.expected_report_date_start = None
                        closest_scheduled.expected_report_date_end = None
                        closest_scheduled.report_date_confidence = 0.95
                        closest_scheduled.report_date_kind = "confirmed"
                        closest_scheduled.source = "kind"
                        
                        self.db.commit()
                        processed_count += 1
                        
                        logger.debug(f"{company_code} ({company_name}): {report_date_str} 실적발표일 업데이트 (분기: {closest_scheduled.fiscal_year}Q{closest_scheduled.fiscal_quarter})")
                        
                    except Exception as e:
                        logger.error(f"종목 {company_data} 처리 중 오류: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f'"{title}" 검색 중 오류: {e}')
                continue
        
        logger.info(f"실적발표일자 동기화 완료: {processed_count}건 처리 (총 발견: {total_found}건)")
        
        return {
            "status": "completed",
            "total_found": total_found,
            "processed_count": processed_count,
            "search_period": f"{from_date} ~ {to_date}",
            "search_titles": search_titles,
            "source": "kind"
        }
    
    
    async def sync_all_korean_earnings_with_schedule(self) -> Dict[str, Any]:
        """
        국내주식 전체 실적 데이터 동기화 + 실적발표일자 크롤링 통합 실행
        
        Returns:
            통합 동기화 결과 정보
        """
        logger.info("국내주식 실적 데이터 통합 동기화를 시작합니다.")
        
        # Step 1: 네이버 실적 데이터 동기화
        logger.info("Step 1: 네이버 실적 데이터 동기화 시작")
        naver_result = self.sync_all_korean_earnings()
        
        # Step 2: KIND 실적발표일자 크롤링
        logger.info("Step 2: KIND 실적발표일자 크롤링 시작")
        schedule_result = await self.sync_earnings_schedule()
        
        logger.info("국내주식 실적 데이터 통합 동기화 완료")
        
        return {
            "status": "completed",
            "summary": {
                "naver_sync": {
                    "status": naver_result.get("status"),
                    "total_tickers": naver_result.get("total_tickers", 0),
                    "success_count": naver_result.get("success_count", 0),
                    "error_count": naver_result.get("error_count", 0),
                    "processed_count": naver_result.get("processed_count", 0)
                },
                "schedule_crawl": {
                    "status": schedule_result.get("status"),
                    "total_found": schedule_result.get("total_found", 0),
                    "processed_count": schedule_result.get("processed_count", 0),
                    "search_period": schedule_result.get("search_period", ""),
                    "search_titles": schedule_result.get("search_titles", [])
                }
            },
            "total_processed": naver_result.get("processed_count", 0) + schedule_result.get("processed_count", 0),
            "sources": ["naver", "kind"],
            "execution_order": [
                "1. 네이버 실적 데이터 동기화 (KOE 거래소 전체)",
                "2. KIND 실적발표일자 크롤링 (3개월 후까지)"
            ]
        }
