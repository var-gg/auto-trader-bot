# app/features/marketdata/services/kr_market_holiday_service.py
from __future__ import annotations
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Any
from sqlalchemy.orm import Session

from app.core.kis_client import KISClient
from app.features.marketdata.repositories.us_market_holiday_repository import USMarketHolidayRepository
from app.shared.models.market_holiday import MarketHoliday

class KRMarketHolidayService:
    """
    🇰🇷 국내주식 마켓휴일 정보 관리 서비스
    - KIS 국내휴장일조회 API를 통해 국내주식 휴일 정보 수집
    - 기존 market_holiday 테이블에 저장 (exchange='KR')
    """

    def __init__(self, db: Session):
        import logging
        logger = logging.getLogger(__name__)
        
        logger.debug("KRMarketHolidayService.__init__ 시작")
        self.db = db
        self._client = None  # 지연 초기화
        logger.debug("USMarketHolidayRepository 초기화 시작")
        self.repo = USMarketHolidayRepository(db)
        logger.debug("KRMarketHolidayService.__init__ 완료")
    
    @property
    def client(self):
        """KISClient 지연 초기화 (필요할 때만 생성)"""
        import logging
        logger = logging.getLogger(__name__)
        
        if self._client is None:
            logger.debug("KISClient 지연 초기화 시작")
            self._client = KISClient(self.db)
            logger.debug("KISClient 지연 초기화 완료")
        return self._client

    def sync_holidays_for_date_range(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        🇰🇷 지정된 날짜 범위의 국내주식 휴일 정보를 동기화
        
        Args:
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            
        Returns:
            Dict[str, Any]: 동기화 결과
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Starting KR market holiday sync from {start_date} to {end_date}")
        
        try:
            # KIS API 호출
            result = self.client.domestic_holiday_check(start_date)
            
            if result.get("rt_cd") != "0":
                logger.error(f"KIS API error: {result.get('msg1', 'Unknown error')}")
                return {
                    "status": "error",
                    "message": f"KIS API error: {result.get('msg1', 'Unknown error')}",
                    "upserted": 0
                }
            
            # 응답 데이터 파싱
            output_data = result.get("output", [])
            if not output_data:
                logger.warning("No holiday data received from KIS API")
                return {
                    "status": "warning",
                    "message": "No holiday data received",
                    "upserted": 0
                }
            
            logger.info(f"Received {len(output_data)} holiday records from KIS API")
            
            # 데이터베이스에 저장
            upserted_count = 0
            for item in output_data:
                try:
                    # 날짜 파싱
                    bass_dt = item.get("bass_dt", "")
                    if len(bass_dt) != 8:
                        logger.warning(f"Invalid date format: {bass_dt}")
                        continue
                    
                    holiday_date = date(
                        int(bass_dt[:4]),  # year
                        int(bass_dt[4:6]), # month
                        int(bass_dt[6:8])  # day
                    )
                    
                    # 날짜 범위 체크
                    start_dt = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
                    end_dt = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
                    
                    if holiday_date < start_dt or holiday_date > end_dt:
                        continue
                    
                    # 개장일 여부 (opnd_yn이 Y면 True)
                    is_open = item.get("opnd_yn") == "Y"
                    
                    # 요일 코드를 요일명으로 변환
                    wday_codes = {
                        "01": "일요일", "02": "월요일", "03": "화요일", 
                        "04": "수요일", "05": "목요일", "06": "금요일", "07": "토요일"
                    }
                    wday_dvsn_cd = item.get("wday_dvsn_cd", "")
                    event_name = wday_codes.get(wday_dvsn_cd, f"요일코드_{wday_dvsn_cd}")
                    
                    # 휴일인 경우 특별한 이벤트명 설정
                    if not is_open:
                        if wday_dvsn_cd in ["01", "07"]:  # 일요일, 토요일
                            event_name = f"{wday_codes.get(wday_dvsn_cd, '주말')}"
                        else:
                            event_name = "국내주식 휴일"
                    
                    # 기존 데이터 확인
                    existing = self.db.query(MarketHoliday).filter(
                        MarketHoliday.exchange == "KR",
                        MarketHoliday.at_date == holiday_date
                    ).first()
                    
                    if existing:
                        # 기존 데이터 업데이트
                        existing.is_open = is_open
                        existing.event_name = event_name
                        logger.debug(f"Updated existing holiday record: {holiday_date}")
                    else:
                        # 새 데이터 생성
                        new_holiday = MarketHoliday(
                            exchange="KR",
                            timezone="Asia/Seoul",
                            at_date=holiday_date,
                            event_name=event_name,
                            trading_hour=None,  # 국내주식 API에서는 거래시간 정보 없음
                            is_open=is_open
                        )
                        self.db.add(new_holiday)
                        logger.debug(f"Added new holiday record: {holiday_date}")
                    
                    upserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing holiday record {item}: {e}")
                    continue
            
            # 커밋
            self.db.commit()
            
            logger.info(f"KR market holiday sync completed: {upserted_count} records upserted")
            
            return {
                "status": "success",
                "message": f"Successfully synced {upserted_count} KR market holiday records",
                "upserted": upserted_count,
                "date_range": f"{start_date} to {end_date}",
                "exchange": "KR"
            }
            
        except Exception as e:
            logger.error(f"Error syncing KR market holidays: {e}")
            self.db.rollback()
            return {
                "status": "error",
                "message": f"Error syncing KR market holidays: {str(e)}",
                "upserted": 0
            }

    def sync_holidays_for_today(self) -> Dict[str, Any]:
        """
        🇰🇷 오늘 날짜 기준의 국내주식 휴일 정보를 동기화
        
        Returns:
            Dict[str, Any]: 동기화 결과
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 오늘 날짜를 KST 기준 YYYYMMDD 형식으로 변환
        today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
        
        logger.info(f"Syncing KR market holidays for today: {today}")
        
        try:
            # KIS API 호출 (오늘 날짜 기준)
            result = self.client.domestic_holiday_check(today)
            
            if result.get("rt_cd") != "0":
                logger.error(f"KIS API error: {result.get('msg1', 'Unknown error')}")
                return {
                    "status": "error",
                    "message": f"KIS API error: {result.get('msg1', 'Unknown error')}",
                    "upserted": 0
                }
            
            # 응답 데이터 파싱
            output_data = result.get("output", [])
            if not output_data:
                logger.warning("No holiday data received from KIS API")
                return {
                    "status": "warning",
                    "message": "No holiday data received",
                    "upserted": 0
                }
            
            logger.info(f"Received {len(output_data)} holiday records from KIS API")
            
            # 데이터베이스에 저장
            upserted_count = 0
            for item in output_data:
                try:
                    # 날짜 파싱
                    bass_dt = item.get("bass_dt", "")
                    if len(bass_dt) != 8:
                        logger.warning(f"Invalid date format: {bass_dt}")
                        continue
                    
                    holiday_date = date(
                        int(bass_dt[:4]),  # year
                        int(bass_dt[4:6]), # month
                        int(bass_dt[6:8])  # day
                    )
                    
                    # 개장일 여부 (opnd_yn이 Y면 True)
                    is_open = item.get("opnd_yn") == "Y"
                    
                    # 요일 코드를 요일명으로 변환
                    wday_codes = {
                        "01": "일요일", "02": "월요일", "03": "화요일", 
                        "04": "수요일", "05": "목요일", "06": "금요일", "07": "토요일"
                    }
                    wday_dvsn_cd = item.get("wday_dvsn_cd", "")
                    event_name = wday_codes.get(wday_dvsn_cd, f"요일코드_{wday_dvsn_cd}")
                    
                    # 휴일인 경우 특별한 이벤트명 설정
                    if not is_open:
                        if wday_dvsn_cd in ["01", "07"]:  # 일요일, 토요일
                            event_name = f"{wday_codes.get(wday_dvsn_cd, '주말')}"
                        else:
                            event_name = "국내주식 휴일"
                    
                    # 기존 데이터 확인
                    existing = self.db.query(MarketHoliday).filter(
                        MarketHoliday.exchange == "KR",
                        MarketHoliday.at_date == holiday_date
                    ).first()
                    
                    if existing:
                        # 기존 데이터 업데이트
                        existing.is_open = is_open
                        existing.event_name = event_name
                        logger.debug(f"Updated existing holiday record: {holiday_date}")
                    else:
                        # 새 데이터 생성
                        new_holiday = MarketHoliday(
                            exchange="KR",
                            timezone="Asia/Seoul",
                            at_date=holiday_date,
                            event_name=event_name,
                            trading_hour=None,  # 국내주식 API에서는 거래시간 정보 없음
                            is_open=is_open
                        )
                        self.db.add(new_holiday)
                        logger.debug(f"Added new holiday record: {holiday_date}")
                    
                    upserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing holiday record {item}: {e}")
                    continue
            
            # 커밋
            self.db.commit()
            
            logger.info(f"KR market holiday sync completed: {upserted_count} records upserted")
            
            return {
                "status": "success",
                "message": f"Successfully synced {upserted_count} KR market holiday records",
                "upserted": upserted_count,
                "base_date": today,
                "exchange": "KR"
            }
            
        except Exception as e:
            logger.error(f"Error syncing KR market holidays: {e}")
            self.db.rollback()
            return {
                "status": "error",
                "message": f"Error syncing KR market holidays: {str(e)}",
                "upserted": 0
            }

    def is_market_closed_now(self) -> bool:
        """
        🇰🇷 현재 날짜가 국내주식 휴장인지 판별 (DB 기반)
        - 주말 (토요일, 일요일) 체크
        - 국내주식 완전휴장 (부분개장 제외) 체크
        - exchange='KR' 데이터 기준으로 판별
        
        주의: 이 메서드는 KIS API를 호출하지 않습니다. DB에서만 조회합니다.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        from datetime import date
        
        logger.info("is_market_closed_now 시작")
        try:
            today = datetime.now(ZoneInfo("Asia/Seoul")).date()
            logger.info(f"오늘 날짜(KST): {today}, 요일: {today.weekday()}")
            
            # 1. 주말 체크 (토요일=5, 일요일=6)
            if today.weekday() >= 5:  # 토요일(5) 또는 일요일(6)
                logger.info("주말이므로 휴장")
                return True
            
            # 2. 국내주식 완전휴장 체크 (부분개장 제외)
            logger.info("DB에서 휴일 정보 조회 시작")
            holidays = self.repo.get_holidays_by_exchange("KR", today, today)
            logger.info(f"조회된 휴일 수: {len(holidays)}")
            
            for holiday in holidays:
                # 완전휴장인 경우 (is_open = False)
                if holiday.at_date == today and not holiday.is_open:
                    logger.info(f"완전휴장일 발견: {holiday.at_date}, 이벤트: {holiday.event_name}")
                    return True
            
            # 주말도 아니고 완전휴장도 아니면 거래일
            logger.info("거래일로 판별")
            return False
            
        except Exception as e:
            logger.error(f"is_market_closed_now 오류: {e}", exc_info=True)
            raise

    def is_market_open_now_kis(self) -> bool:
        """
        🇰🇷 현재 날짜가 국내주식 개장일인지 KIS API로 확인
        
        KIS API의 domestic_holiday_check를 사용하여 opnd_yn (개장일여부)를 확인합니다.
        이 메서드는 실제 장 오픈 여부를 정확하게 판별합니다.
        
        Returns:
            True: 개장일 (opnd_yn == "Y")
            False: 휴장일 (opnd_yn == "N" 또는 API 오류)
        
        Note:
            - 주문을 넣을 수 있는지 확인하고자 할 경우 개장일여부(opnd_yn)을 사용
            - 모의투자에서는 지원하지 않습니다 (실전만 지원)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        from datetime import date, datetime
        
        try:
            today = datetime.now(ZoneInfo("Asia/Seoul")).date()
            today_str = today.strftime("%Y%m%d")
            
            logger.info(f"KIS API로 개장 여부 확인 시작 - 기준일자: {today_str}")
            
            # KIS API 호출
            result = self.client.domestic_holiday_check(bass_dt=today_str)
            
            # API 응답 확인
            rt_cd = result.get("rt_cd", "")
            if rt_cd != "0":
                error_msg = result.get("msg1", "알 수 없는 오류")
                logger.error(f"KIS API 오류 - rt_cd: {rt_cd}, msg1: {error_msg}")
                # API 오류 시 안전을 위해 False 반환 (거래 불가)
                return False
            
            # output 확인
            output = result.get("output")
            if not output:
                logger.error("KIS API 응답에 output이 없습니다")
                return False
            
            # output이 리스트인 경우 첫 번째 요소 사용
            if isinstance(output, list):
                if len(output) == 0:
                    logger.error("KIS API 응답의 output 리스트가 비어있습니다")
                    return False
                output = output[0]
                logger.debug(f"output이 리스트였으므로 첫 번째 요소 사용: {output}")
            
            # output이 딕셔너리가 아니면 오류
            if not isinstance(output, dict):
                logger.error(f"KIS API 응답의 output이 예상과 다른 형식입니다: {type(output)}")
                return False
            
            # opnd_yn (개장일여부) 확인
            opnd_yn = output.get("opnd_yn", "")
            
            if opnd_yn == "Y":
                logger.info(f"✅ KIS API 기준 개장일 확인 - 기준일자: {today_str}, opnd_yn: {opnd_yn}")
                return True
            else:
                logger.warning(f"🚫 KIS API 기준 휴장일 확인 - 기준일자: {today_str}, opnd_yn: {opnd_yn}")
                return False
                
        except Exception as e:
            logger.error(f"KIS API 개장 여부 확인 오류: {e}", exc_info=True)
            # 오류 시 안전을 위해 False 반환 (거래 불가)
            return False

    def get_holidays_by_date_range(self, start_date: date, end_date: date) -> List[MarketHoliday]:
        """국내주식 휴일 조회 (Repository 메서드 래핑)"""
        return self.repo.get_holidays_by_exchange("KR", start_date, end_date)
