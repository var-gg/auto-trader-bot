from __future__ import annotations
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
import hashlib
import httpx
from sqlalchemy.orm import Session
import logging

from app.core import config as settings
from app.core.repositories.kis_token_repository import KISTokenRepository
from app.core.models.kis_token import KISToken
from app.core.symbol_normalizer import to_kis_symbol

def _appkey_hash(appkey: str, appsecret: str) -> str:
    # 민감정보 저장 방지: appkey+secret 해시
    src = f"{appkey}::{appsecret}".encode("utf-8")
    return hashlib.sha256(src).hexdigest()

class KISAuth:
    """
    DB 기반 토큰 관리 (단순화)
    - 매번 DB에서 유효 토큰 조회
    - 없거나 만료 임박이면 새로 발급 → DB upsert
    """
    _lock = threading.Lock()

    @classmethod
    def token(cls, db: Session, tr_id: str) -> str:
        logger = logging.getLogger(__name__)
        
        with cls._lock:
            logger.info(f"🔑 KIS 토큰 요청 시작 - tr_id: {tr_id}")
            
            repo = KISTokenRepository(db)
            # 가상환경에 따른 APPKEY/APPSECRET 선택
            appkey = settings.KIS_VIRTUAL_APPKEY if settings.KIS_VIRTUAL else settings.KIS_APPKEY
            appsecret = settings.KIS_VIRTUAL_APPSECRET if settings.KIS_VIRTUAL else settings.KIS_APPSECRET
            app_hash = _appkey_hash(appkey, appsecret)
            
            # 환경 정보 로깅
            provider = "KIS_VIRTUAL" if settings.KIS_VIRTUAL else "KIS"
            base_url = settings.KIS_VIRTUAL_BASE_URL if settings.KIS_VIRTUAL else settings.KIS_BASE_URL
            logger.debug(f"📋 환경 설정 - provider: {provider}, base_url: {base_url}, app_hash: {app_hash[:8]}...")

            # DB 유효 토큰 조회 (환경별 provider 구분)
            logger.debug(f"🔍 DB에서 유효 토큰 조회 중 - tr_id: {tr_id}")
            db_token = repo.get_valid_token(app_hash, tr_id, base_url, provider)
            
            if db_token:
                logger.info(f"✅ DB에서 유효 토큰 발견 - tr_id: {tr_id}, 만료일시: {db_token.expires_at}")
                return db_token.access_token

            # DB에도 없으면 새로 발급 (실패 시 재시도)
            logger.info(f"🆕 새 토큰 발급 시작 - tr_id: {tr_id}")
            url = f"{base_url}/oauth2/tokenP"
            payload = {
                "grant_type": "client_credentials",
                "appkey": appkey,
                "appsecret": appsecret,
            }
            
            max_retries = 3
            retry_delay = 60  # 1분
            last_error = None
            access_token = None
            data = {}
            
            for attempt in range(max_retries):
                try:
                    with httpx.Client(timeout=15) as client:
                        logger.debug(f"🌐 KIS 토큰 발급 API 호출 (시도 {attempt + 1}/{max_retries}) - URL: {url}")
                        r = client.post(url, json=payload)
                        r.raise_for_status()
                        data = r.json()
                        logger.debug(f"📥 토큰 발급 응답 수신 - 상태코드: {r.status_code}")
                        
                        # 토큰 발급 성공
                        access_token = data.get("access_token")
                        if not access_token:
                            raise ValueError("토큰 발급 응답에 access_token이 없습니다")
                        
                        # 성공하면 루프 탈출
                        break
                        
                except Exception as e:
                    last_error = e
                    logger.error(f"❌ 토큰 발급 실패 (시도 {attempt + 1}/{max_retries}) - tr_id: {tr_id}, 오류: {str(e)}")
                    
                    # 마지막 시도가 아니면 대기 후 재시도
                    if attempt < max_retries - 1:
                        logger.info(f"⏰ {retry_delay}초 후 재시도합니다...")
                        time.sleep(retry_delay)
                    else:
                        # 모든 재시도 실패
                        logger.error(f"❌ 모든 토큰 발급 시도 실패 - tr_id: {tr_id}")
                        raise
            
            # access_token이 설정되지 않았다면 에러
            if not access_token:
                logger.error(f"❌ 토큰 발급 응답에 access_token 없음 - tr_id: {tr_id}, 응답: {data}")
                raise ValueError("토큰 발급 응답에 access_token이 없습니다")
            
            # 문서/환경마다 expires_in 미노출 가능 → 기본 TTL 사용
            expires_in = int(data.get("expires_in", settings.KIS_TOKEN_TTL))
            
            # 보수적인 만료 시간 설정 (KIS 서버와의 시간 차이 고려)
            # 기본적으로 2시간(7200초) 전에 만료 처리
            conservative_skew = 7200  # 2시간
            expires_in_adj = max(300, expires_in - conservative_skew)  # 최소 5분은 보장

            expires_at_utc = datetime.now(timezone.utc) + timedelta(seconds=expires_in_adj)
            
            # KST 시간으로도 로깅
            kst = timezone(timedelta(hours=9))
            expires_at_kst = expires_at_utc.astimezone(kst)
            
            logger.info(f"⏰ 토큰 만료 정보 - 원본TTL: {expires_in}초, 보수적스큐: {conservative_skew}초, 조정TTL: {expires_in_adj}초")
            logger.info(f"⏰ 만료일시(UTC): {expires_at_utc}, 만료일시(KST): {expires_at_kst}")

            # DB upsert (환경별 provider 구분)
            try:
                repo.upsert_token(
                    appkey_hash=app_hash,
                    tr_id=tr_id,
                    base_url=base_url,
                    access_token=access_token,
                    expires_at_utc=expires_at_utc,
                    provider=provider,
                )
                logger.info(f"💾 토큰 DB 저장 완료 - tr_id: {tr_id}, 만료일시: {expires_at_utc}")
            except Exception as e:
                logger.error(f"❌ 토큰 DB 저장 실패 - tr_id: {tr_id}, 오류: {str(e)}")
                raise

            logger.info(f"✅ 토큰 발급 및 저장 완료 - tr_id: {tr_id}")
            return access_token


class KISClient:
    """
    한국투자증권 해외 API 통합 클라이언트 (DB 캐시 토큰 사용)
    - 다양한 TR ID와 엔드포인트를 지원하는 범용 클라이언트
    """

    def __init__(self, db: Session):
        self.db = db
        # 가상환경일 때는 모의투자 URL 사용
        self.base_url = settings.KIS_VIRTUAL_BASE_URL if settings.KIS_VIRTUAL else settings.KIS_BASE_URL
        self.exchange_map = settings.KIS_OVERSEAS_EXCHANGE_MAP
        
        # 디버그 로그
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"KIS Client initialized - VIRTUAL: {settings.KIS_VIRTUAL}, URL: {self.base_url}")

    def _headers(self, tr_id: str) -> Dict[str, str]:
        # 가상환경에 따른 APPKEY/APPSECRET 선택
        appkey = settings.KIS_VIRTUAL_APPKEY if settings.KIS_VIRTUAL else settings.KIS_APPKEY
        appsecret = settings.KIS_VIRTUAL_APPSECRET if settings.KIS_VIRTUAL else settings.KIS_APPSECRET
        
        # 디버그 로그
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Using APPKEY: {appkey[:10]}..., VIRTUAL: {settings.KIS_VIRTUAL}")
        
        return {
            "authorization": f"Bearer {KISAuth.token(self.db, tr_id)}",
            "appkey": appkey,
            "appsecret": appsecret,
            "tr_id": tr_id,
            "content-type": "application/json; charset=utf-8",
        }

    def _make_request(self, url: str, tr_id: str, params: Dict[str, Any], retry_count: int = 0, extra_headers: Dict[str, str] = None) -> Dict[str, Any]:
        """공통 API 요청 메서드 (토큰 만료시 자동 재시도)"""
        logger = logging.getLogger(__name__)
        
        time.sleep(settings.KIS_REQUEST_INTERVAL_MS / 1000.0)
        
        try:
            with httpx.Client(timeout=20) as client:
                # 기본 헤더에 추가 헤더 병합 (연속조회 tr_cont 등)
                headers = self._headers(tr_id)
                if extra_headers:
                    headers.update(extra_headers)
                
                logger.debug(f"🌐 KIS API 호출 - URL: {url}, tr_id: {tr_id}, extra_headers: {extra_headers}")
                r = client.get(url, headers=headers, params=params)
                logger.debug(f"📥 KIS API 응답 - 상태코드: {r.status_code}")
                
                # 응답 데이터 파싱 (HTTP 500도 토큰 만료일 수 있음)
                try:
                    response_data = r.json()
                except:
                    response_data = {}
                
                # 토큰 만료 에러 감지 (EGW00123) - HTTP 500도 포함
                if (response_data.get("rt_cd") == "1" and response_data.get("msg_cd") == "EGW00123") or \
                   (r.status_code == 500 and response_data.get("msg1") == "기간이 만료된 token 입니다."):
                    logger.warning(f"⚠️ 토큰 만료 감지 - tr_id: {tr_id}, 재시도 횟수: {retry_count}")
                    
                    if retry_count == 0:  # 1회만 재시도
                        # 토큰 강제 갱신 (DB에서 기존 토큰 삭제)
                        repo = KISTokenRepository(self.db)
                        appkey = settings.KIS_VIRTUAL_APPKEY if settings.KIS_VIRTUAL else settings.KIS_APPKEY
                        appsecret = settings.KIS_VIRTUAL_APPSECRET if settings.KIS_VIRTUAL else settings.KIS_APPSECRET
                        app_hash = _appkey_hash(appkey, appsecret)
                        base_url = settings.KIS_VIRTUAL_BASE_URL if settings.KIS_VIRTUAL else settings.KIS_BASE_URL
                        provider = "KIS_VIRTUAL" if settings.KIS_VIRTUAL else "KIS"
                        
                        # 기존 토큰 삭제하여 강제 갱신 유도
                        repo.db.query(KISToken).filter(
                            KISToken.provider == provider,
                            KISToken.appkey_hash == app_hash,
                            KISToken.tr_id == tr_id,
                            KISToken.base_url == base_url,
                        ).delete()
                        repo.db.commit()
                        
                        logger.info(f"🔄 토큰 강제 갱신 후 재시도 - tr_id: {tr_id}")
                        return self._make_request(url, tr_id, params, retry_count + 1, extra_headers)
                    else:
                        logger.error(f"❌ 토큰 갱신 후에도 만료 에러 지속 - tr_id: {tr_id}")
                        raise Exception(f"토큰 만료 에러가 지속됩니다: {response_data.get('msg1', '')}")
                
                # 정상 응답이 아닌 경우에만 HTTP 에러 체크
                if r.status_code >= 400:
                    r.raise_for_status()
                
                # 응답에 헤더 정보 추가 (페이지네이션 등에 필요)
                if isinstance(response_data, dict):
                    response_data["_headers"] = {
                        "tr_id": r.headers.get("tr_id", ""),
                        "tr_cont": r.headers.get("tr_cont", ""),
                        "gt_uid": r.headers.get("gt_uid", ""),
                    }
                
                return response_data
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ KIS API 호출 실패 - 상태코드: {e.response.status_code}, 응답: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"❌ KIS API 호출 중 오류 - tr_id: {tr_id}, 오류: {str(e)}")
            raise

    def daily_price(self, symbol: str, exchange: str, bymd: str = "") -> Dict[str, Any]:
        """
        해외 기간별 시세(일봉) 조회
        - symbol: 예) AAPL
        - exchange: 예) NASDAQ / NYSE / AMEX
        - bymd: 기준일(YYYYMMDD, 빈값=오늘)
        """
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = to_kis_symbol(symbol)
        
        kis_ex = self.exchange_map.get((exchange or "").upper(), "NAS")
        params = {
            "EXCD": kis_ex,
            "SYMB": kis_symbol,
            "GUBN": "0",  # 일봉
            "BYMD": bymd,
            "MODP": "1",  # 수정주가
        }
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        return self._make_request(url, settings.KIS_TR_ID_DAILYPRICE, params)

    def price_detail(self, symbol: str, exchange: str) -> Dict[str, Any]:
        """
        해외주식현재가상세 조회
        - symbol: 예) AAPL
        - exchange: 예) NMS / NYQ / ASE (Yahoo Finance 코드)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = to_kis_symbol(symbol)
        
        kis_ex = self.exchange_map.get((exchange or "").upper(), "NAS")
        logger.debug(f"Converting exchange {exchange} -> {kis_ex} for symbol {symbol} -> {kis_symbol}")
        
        params = {
            "EXCD": kis_ex,
            "SYMB": kis_symbol,
        }
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price-detail"
        
        logger.info(f"Making KIS price detail request: {symbol} -> {kis_symbol} on {exchange} (KIS: {kis_ex})")
        result = self._make_request(url, settings.KIS_TR_ID_PRICE_DETAIL, params)
        logger.debug(f"KIS API response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def present_balance(self, wcrc_frcr_dvsn_cd: str = "01", natn_cd: str = "", tr_mket_cd: str = "", inqr_dvsn_cd: str = "02") -> Dict[str, Any]:
        """
        체결기준현재 잔고 조회
        - 해외주식 체결기준 잔고 조회
        
        Parameters:
        - wcrc_frcr_dvsn_cd: 원화외화구분코드 (01: 원화, 02: 외화)
        - natn_cd: 국가코드 (빈값: 전체)
        - tr_mket_cd: 거래시장코드 (빈값: 전체)
        - inqr_dvsn_cd: 조회구분코드 (02: 체결기준)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": settings.KIS_CANO,  # 종합계좌번호
            "ACNT_PRDT_CD": settings.KIS_ACNT_PRDT_CD,  # 계좌상품코드
            "WCRC_FRCR_DVSN_CD": wcrc_frcr_dvsn_cd,  # 원화외화구분코드
            "NATN_CD": natn_cd,  # 국가코드
            "TR_MKET_CD": tr_mket_cd,  # 거래시장코드
            "INQR_DVSN_CD": inqr_dvsn_cd,  # 조회구분코드
        }
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-present-balance"
        
        logger.info(f"Making KIS present balance request with params: {params}")
        result = self._make_request(url, settings.KIS_TR_ID_PRESENT_BALANCE, params)
        logger.debug(f"KIS API response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def inquire_balance(self, CANO: str, ACNT_PRDT_CD: str, AFHR_FLPR_YN: str = "N", 
                       OFL_YN: str = "", INQR_DVSN: str = "02", UNPR_DVSN: str = "01", 
                       FUND_STTL_ICLD_YN: str = "N", FNCG_AMT_AUTO_RDPT_YN: str = "N", 
                       PRCS_DVSN: str = "00", CTX_AREA_FK100: str = "", CTX_AREA_NK100: str = "") -> Dict[str, Any]:
        """
        국내 잔고조회 (v1_국내주식-006)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - AFHR_FLPR_YN: 시간외단일가, 거래소여부 (N: 기본값, Y: 시간외단일가, X: NXT 정규장)
        - OFL_YN: 오프라인여부
        - INQR_DVSN: 조회구분 (01: 대출일별, 02: 종목별)
        - UNPR_DVSN: 단가구분 (01: 기본값)
        - FUND_STTL_ICLD_YN: 펀드결제분포함여부 (N: 포함하지 않음, Y: 포함)
        - FNCG_AMT_AUTO_RDPT_YN: 융자금액자동상환여부 (N: 기본값)
        - PRCS_DVSN: 처리구분 (00: 전일매매포함, 01: 전일매매미포함)
        - CTX_AREA_FK100: 연속조회검색조건100
        - CTX_AREA_NK100: 연속조회키100
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "AFHR_FLPR_YN": AFHR_FLPR_YN,
            "OFL_YN": OFL_YN,
            "INQR_DVSN": INQR_DVSN,
            "UNPR_DVSN": UNPR_DVSN,
            "FUND_STTL_ICLD_YN": FUND_STTL_ICLD_YN,
            "FNCG_AMT_AUTO_RDPT_YN": FNCG_AMT_AUTO_RDPT_YN,
            "PRCS_DVSN": PRCS_DVSN,
            "CTX_AREA_FK100": CTX_AREA_FK100,
            "CTX_AREA_NK100": CTX_AREA_NK100,
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        
        logger.info(f"Making KIS inquire balance request with params: {params}")
        result = self._make_request(url, "TTTC8434R", params)
        logger.debug(f"KIS API response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def inquire_psbl_order(self, CANO: str, ACNT_PRDT_CD: str, PDNO: str = "", ORD_UNPR: str = "", 
                          ORD_DVSN: str = "00", CMA_EVLU_AMT_ICLD_YN: str = "Y", 
                          OVRS_ICLD_YN: str = "N") -> Dict[str, Any]:
        """
        매수가능조회 (v1_국내주식-007)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - PDNO: 상품번호
        - ORD_UNPR: 주문단가
        - ORD_DVSN: 주문구분
        - CMA_EVLU_AMT_ICLD_YN: CMA평가금액포함여부
        - OVRS_ICLD_YN: 해외포함여부
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "ORD_UNPR": ORD_UNPR,
            "ORD_DVSN": ORD_DVSN,
            "CMA_EVLU_AMT_ICLD_YN": CMA_EVLU_AMT_ICLD_YN,
            "OVRS_ICLD_YN": OVRS_ICLD_YN,
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        
        logger.info(f"Making KIS inquire psbl order request with params: {params}")
        result = self._make_request(url, "TTTC8908R", params)
        logger.debug(f"KIS API response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def estimate_perform(self, symbol: str) -> Dict[str, Any]:
        """
        국내주식 실적추정 조회
        - symbol: 종목코드 (예: 005930)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "SHT_CD": symbol,  # 종목코드
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/estimate-perform"
        
        logger.info(f"Making KIS estimate perform request for symbol: {symbol}")
        result = self._make_request(url, settings.KIS_TR_ID_ESTIMATE_PERFORM, params)
        logger.debug(f"KIS API response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def financial_ratio(self, stock_code: str, div_cls_code: str = "0") -> Dict[str, Any]:
        """
        국내주식 재무비율 조회 (v1_국내주식-080)
        - stock_code: 종목코드 (예: 000660)
        - div_cls_code: 분류구분코드 (0: 년, 1: 분기)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_DIV_CLS_CODE": div_cls_code,  # 분류구분코드
            "fid_cond_mrkt_div_code": "J",  # 건 시장 분류 코드 (J)
            "fid_input_iscd": stock_code,  # 입력 종목코드
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/finance/financial-ratio"
        
        logger.info(f"Making KIS financial ratio request for stock: {stock_code}")
        result = self._make_request(url, settings.KIS_TR_ID_FINANCIAL_RATIO, params)
        logger.debug(f"KIS financial ratio response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def dividend_schedule(self, query_type: str = "0", from_date: str = "", to_date: str = "", stock_code: str = "") -> Dict[str, Any]:
        """
        예탁원정보(배당일정) 조회 (국내주식-145)
        - query_type: 조회구분 (0: 배당전체, 1: 결산배당, 2: 중간배당)
        - from_date: 조회일자From (YYYYMMDD)
        - to_date: 조회일자To (YYYYMMDD)
        - stock_code: 종목코드 (빈값: 전체)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CTS": "",  # 공백
            "GB1": query_type,  # 조회구분 (0: 배당전체, 1: 결산배당, 2: 중간배당)
            "F_DT": from_date,  # 조회일자From (YYYYMMDD)
            "T_DT": to_date,  # 조회일자To (YYYYMMDD)
            "SHT_CD": stock_code,  # 종목코드 (빈값: 전체)
            "HIGH_GB": "",  # 고배당여부 (공백)
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/ksdinfo/dividend"
        
        logger.info(f"📡 DEBUG: Making KIS dividend schedule request: type={query_type}, from={from_date}, to={to_date}, stock={stock_code}")
        logger.info(f"📡 DEBUG: Request URL: {url}")
        logger.info(f"📡 DEBUG: Request params: {params}")
        
        result = self._make_request(url, settings.KIS_TR_ID_DIVIDEND_SCHEDULE, params)
        
        logger.info(f"📡 DEBUG: KIS dividend schedule response status: {result.get('rt_cd', 'unknown')}")
        logger.info(f"📡 DEBUG: KIS dividend schedule response msg1: {result.get('msg1', 'N/A')}")
        logger.info(f"📡 DEBUG: KIS dividend schedule response msg_cd: {result.get('msg_cd', 'N/A')}")
        
        # 응답 데이터 구조 확인
        if 'output1' in result:
            logger.info(f"📡 DEBUG: Dividend output1 count: {len(result['output1'])}")
            if len(result['output1']) > 0:
                logger.info(f"📡 DEBUG: Sample dividend output1: {result['output1'][0]}")
        else:
            logger.warning(f"⚠️ DEBUG: No 'output1' field in dividend response")
        
        return result

    def stock_basic_info(self, stock_code: str, product_type: str = "300") -> Dict[str, Any]:
        """
        주식기본조회 (v1_국내주식-067)
        - stock_code: 종목번호 (6자리, 예: 005930)
        - product_type: 상품유형코드 (300: 주식/ETF/ETN/ELW, 301: 선물옵션, 302: 채권, 306: ELS)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "PRDT_TYPE_CD": product_type,  # 상품유형코드
            "PDNO": stock_code,  # 상품번호 (종목번호)
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/search-stock-info"
        
        logger.info(f"Making KIS stock basic info request for stock: {stock_code}, type: {product_type}")
        result = self._make_request(url, settings.KIS_TR_ID_STOCK_BASIC_INFO, params)
        logger.debug(f"KIS stock basic info response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def domestic_daily_price(self, stock_code: str, period_div_code: str = "D", org_adj_prc: str = "0", 
                             start_date: str = "", end_date: str = "") -> Dict[str, Any]:
        """
        국내주식 기간별시세 조회 (v1_국내주식-016)
        - stock_code: 종목코드 (예: 005930)
        - period_div_code: 기간 분류 코드 (D: 일봉, W: 주봉, M: 월봉, Y: 년봉)
        - org_adj_prc: 수정주가 원주가 가격 여부 (0: 수정주가, 1: 원주가)
        - start_date: 조회 시작일자 (YYYYMMDD, 빈값=미지정)
        - end_date: 조회 종료일자 (YYYYMMDD, 빈값=오늘, 최대 100개)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 조건 시장 분류 코드 (J: KRX)
            "FID_INPUT_ISCD": stock_code,  # 입력 종목코드
            "FID_INPUT_DATE_1": start_date,  # 조회 시작일자
            "FID_INPUT_DATE_2": end_date,  # 조회 종료일자 (최대 100개)
            "FID_PERIOD_DIV_CODE": period_div_code,  # 기간분류코드
            "FID_ORG_ADJ_PRC": org_adj_prc,  # 수정주가 원주가 가격 여부
        }
        
        # 가상환경에 따른 TR ID 선택 (실전/모의투자 동일)
        tr_id = "FHKST03010100"
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        
        logger.info(f"Making KIS domestic period price request for stock: {stock_code}, period: {period_div_code}, dates: {start_date} ~ {end_date}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS domestic period price response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def daily_price_test(self, FID_COND_MRKT_DIV_CODE: str = "J", FID_INPUT_ISCD: str = "005930",
                        FID_PERIOD_DIV_CODE: str = "D", FID_ORG_ADJ_PRC: str = "1") -> Dict[str, Any]:
        """
        주식현재가 일자별 API 테스트 (v1_국내주식-010)
        
        Parameters:
        - FID_COND_MRKT_DIV_CODE: 조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)
        - FID_INPUT_ISCD: 입력 종목코드 (예: 005930 삼성전자)
        - FID_PERIOD_DIV_CODE: 기간 분류 코드 (D: 일/최근 30거래일, W: 주/최근 30주, M: 월/최근 30개월)
        - FID_ORG_ADJ_PRC: 수정주가 원주가 가격 (0: 수정주가미반영, 1: 수정주가반영)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_PERIOD_DIV_CODE": FID_PERIOD_DIV_CODE,
            "FID_ORG_ADJ_PRC": FID_ORG_ADJ_PRC,
        }
        
        # 가상환경에 따른 TR ID 선택 (실전/모의투자 동일)
        tr_id = "FHKST01010400"
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        
        logger.info(f"Making KIS daily price test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS daily price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def domestic_holiday_check(self, bass_dt: str) -> Dict[str, Any]:
        """
        국내휴장일조회 (v1_국내주식-040)
        - bass_dt: 기준일자 (YYYYMMDD 형식)
        - 모의투자 미지원 (실전만 지원)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 모의투자 환경에서는 지원하지 않음
        if settings.KIS_VIRTUAL:
            logger.warning("⚠️ 국내휴장일조회 API는 모의투자에서 지원하지 않습니다.")
            return {
                "rt_cd": "1",
                "msg_cd": "NOT_SUPPORTED",
                "msg1": "모의투자에서는 국내휴장일조회 API를 지원하지 않습니다.",
                "output": None
            }
        
        params = {
            "BASS_DT": bass_dt,  # 기준일자
            "CTX_AREA_NK": "",   # 연속조회키 (공백)
            "CTX_AREA_FK": "",   # 연속조회검색조건 (공백)
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/chk-holiday"
        
        logger.info(f"Making KIS domestic holiday check request for date: {bass_dt}")
        result = self._make_request(url, settings.KIS_TR_ID_DOMESTIC_HOLIDAY_CHECK, params)
        logger.debug(f"KIS domestic holiday check response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def kr_current_price(self, stock_code: str) -> Dict[str, Any]:
        """
        국내주식 현재가 시세 조회 (v1_국내주식-008)
        - stock_code: 종목코드 (ex: 005930 삼성전자)
        - 실전/모의투자 모두 지원
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Requesting KR current price for stock_code: {stock_code}")
        
        # URL 구성
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        
        # Query Parameters
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # J:KRX (조건 시장 분류 코드)
            "FID_INPUT_ISCD": stock_code    # 입력 종목코드
        }
        
        # TR ID 설정 (실전/모의투자 동일)
        tr_id = "FHKST01010100"
        
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS KR current price response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def current_price_test(self, FID_COND_MRKT_DIV_CODE: str = "J", 
                          FID_INPUT_ISCD: str = "005930") -> Dict[str, Any]:
        """
        주식현재가 시세 API 테스트 (v1_국내주식-008)
        
        Parameters:
        - FID_COND_MRKT_DIV_CODE: 조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)
        - FID_INPUT_ISCD: 입력 종목코드 (예: 005930 삼성전자, ETN은 종목코드 6자리 앞에 Q 입력 필수)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
        }
        
        # 가상환경에 따른 TR ID 선택 (실전/모의투자 동일)
        tr_id = "FHKST01010100"
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        
        logger.info(f"Making KIS current price test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS current price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def _make_post_request(self, url: str, tr_id: str, data: Dict[str, Any], retry_count: int = 0) -> Dict[str, Any]:
        """공통 POST API 요청 메서드 (토큰 만료시 자동 재시도)"""
        logger = logging.getLogger(__name__)
        
        time.sleep(settings.KIS_REQUEST_INTERVAL_MS / 1000.0)
        
        try:
            with httpx.Client(timeout=20) as client:
                logger.debug(f"🌐 KIS POST API 호출 - URL: {url}, tr_id: {tr_id}")
                r = client.post(url, headers=self._headers(tr_id), json=data)
                logger.debug(f"📥 KIS POST API 응답 - 상태코드: {r.status_code}")
                
                # 응답 데이터 파싱 (HTTP 500도 토큰 만료일 수 있음)
                try:
                    response_data = r.json()
                except:
                    response_data = {}
                
                # 토큰 만료 에러 감지 (EGW00123) - HTTP 500도 포함
                if (response_data.get("rt_cd") == "1" and response_data.get("msg_cd") == "EGW00123") or \
                   (r.status_code == 500 and response_data.get("msg1") == "기간이 만료된 token 입니다."):
                    logger.warning(f"⚠️ 토큰 만료 감지 (POST) - tr_id: {tr_id}, 재시도 횟수: {retry_count}")
                    
                    if retry_count == 0:  # 1회만 재시도
                        # 토큰 강제 갱신 (DB에서 기존 토큰 삭제)
                        repo = KISTokenRepository(self.db)
                        appkey = settings.KIS_VIRTUAL_APPKEY if settings.KIS_VIRTUAL else settings.KIS_APPKEY
                        appsecret = settings.KIS_VIRTUAL_APPSECRET if settings.KIS_VIRTUAL else settings.KIS_APPSECRET
                        app_hash = _appkey_hash(appkey, appsecret)
                        base_url = settings.KIS_VIRTUAL_BASE_URL if settings.KIS_VIRTUAL else settings.KIS_BASE_URL
                        provider = "KIS_VIRTUAL" if settings.KIS_VIRTUAL else "KIS"
                        
                        # 기존 토큰 삭제하여 강제 갱신 유도
                        repo.db.query(KISToken).filter(
                            KISToken.provider == provider,
                            KISToken.appkey_hash == app_hash,
                            KISToken.tr_id == tr_id,
                            KISToken.base_url == base_url,
                        ).delete()
                        repo.db.commit()
                        
                        logger.info(f"🔄 토큰 강제 갱신 후 재시도 (POST) - tr_id: {tr_id}")
                        return self._make_post_request(url, tr_id, data, retry_count + 1)
                    else:
                        logger.error(f"❌ 토큰 갱신 후에도 만료 에러 지속 (POST) - tr_id: {tr_id}")
                        raise Exception(f"토큰 만료 에러가 지속됩니다: {response_data.get('msg1', '')}")
                
                # 정상 응답이 아닌 경우에만 HTTP 에러 체크
                if r.status_code >= 400:
                    r.raise_for_status()
                
                return response_data
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ KIS POST API 호출 실패 - 상태코드: {e.response.status_code}, 응답: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"❌ KIS POST API 호출 중 오류 - tr_id: {tr_id}, 오류: {str(e)}")
            raise

    def order_stock(self, order_type: str, symbol: str, quantity: str, price: str = None, order_method: str = "LIMIT", exchange: str = "NASD") -> Dict[str, Any]:
        """
        미국 주식 주문 (매수/매도)
        - order_type: "buy" 또는 "sell"
        - symbol: 종목코드 (예: AAPL)
        - quantity: 주문수량
        - price: 주문단가 (LIMIT/LOC일 때 필수, MARKET일 때 None)
        - order_method: "LIMIT", "MARKET", "LOC" 중 하나
        - exchange: 거래소코드 (기본값: NASD)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = to_kis_symbol(symbol)
        
        # 주문 방식별 ORD_DVSN 매핑 (KIS API 문서 기준)
        if order_method == "LIMIT":
            ord_dvsn = "00"  # 지정가
        elif order_method == "MARKET":
            ord_dvsn = "00"  # 시장가도 지정가 코드 사용, 단가는 "0"
            price = "0"      # 시장가는 반드시 "0"으로 설정
        elif order_method == "LOC":
            # 모의투자에서는 LOC 지원하지 않음
            if settings.KIS_VIRTUAL:
                logger.warning("⚠️ 모의투자에서는 LOC 주문을 지원하지 않습니다. LIMIT으로 변경합니다.")
                ord_dvsn = "00"  # 지정가로 대체
            else:
                ord_dvsn = "34"  # 장마감지정가 (LOC: Limit on Close)
        else:
            raise ValueError(f"지원하지 않는 주문 방식: {order_method}. 지원 방식: LIMIT, MARKET, LOC")
        
        # 가격 검증
        if order_method == "MARKET":
            # 시장가는 항상 "0"
            price = "0"
        elif order_method in ["LIMIT", "LOC"] and price is None:
            raise ValueError(f"{order_method} 주문에는 price가 필수입니다")
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = settings.KIS_TR_ID_ORDER_BUY_US_VIRTUAL if order_type == "buy" else settings.KIS_TR_ID_ORDER_SELL_US_VIRTUAL
            cano = settings.KIS_VIRTUAL_CANO
        else:
            tr_id = settings.KIS_TR_ID_ORDER_BUY_US if order_type == "buy" else settings.KIS_TR_ID_ORDER_SELL_US
            cano = settings.KIS_CANO
        
        # 주문 데이터 구성
        order_data = {
            "CANO": cano,  # 종합계좌번호
            "ACNT_PRDT_CD": settings.KIS_ACNT_PRDT_CD,  # 계좌상품코드
            "OVRS_EXCG_CD": {"NYS": "NYSE", "NYQ": "NYSE", "NMS": "NASD", "NAS": "NASD"}.get(exchange, exchange),  # 해외거래소코드
            "PDNO": kis_symbol,  # 상품번호
            "ORD_QTY": quantity,  # 주문수량
            "OVRS_ORD_UNPR": price or "0",  # 해외주문단가 (시장가일 때는 0)
            "ORD_SVR_DVSN_CD": "0",  # 주문서버구분코드
            "ORD_DVSN": ord_dvsn,  # 주문구분 (동적 설정)
        }
        
        # 매도 주문인 경우 SLL_TYPE 추가
        if order_type == "sell":
            order_data["SLL_TYPE"] = "00"  # 매도
        
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        
        logger.info(f"Making KIS order request: {order_type} {symbol} -> {kis_symbol} {quantity} {order_method}@{price} on {exchange}")
        logger.info(f"Using TR ID: {tr_id}, CANO: {cano}, ORD_DVSN: {ord_dvsn}")
        logger.debug(f"Final order data: OVRS_ORD_UNPR={price}, ORD_DVSN={ord_dvsn}")
        
        result = self._make_post_request(url, tr_id, order_data)
        logger.debug(f"KIS order response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def order_cash_buy(
        self, 
        CANO: str, 
        ACNT_PRDT_CD: str, 
        PDNO: str, 
        ORD_DVSN: str, 
        ORD_QTY: str, 
        ORD_UNPR: str,
        EXCG_ID_DVSN_CD: str = "KRX",
        **kwargs
    ) -> Dict[str, Any]:
        """
        국내주식 현금매수 주문 (v1_국내주식-001)
        
        Args:
            CANO: 종합계좌번호
            ACNT_PRDT_CD: 계좌상품코드
            PDNO: 상품번호 (종목코드 6자리)
            ORD_DVSN: 주문구분 (00:지정가, 01:시장가)
            ORD_QTY: 주문수량
            ORD_UNPR: 주문단가 (시장가시 "0")
            EXCG_ID_DVSN_CD: 거래소ID구분코드 (기본: KRX)
            
        Returns:
            주문 결과
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTTC0012U"  # 모의투자 매수
        else:
            tr_id = "TTTC0012U"  # 실전투자 매수
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": ORD_QTY,
            "ORD_UNPR": ORD_UNPR,
            "EXCG_ID_DVSN_CD": EXCG_ID_DVSN_CD,
            **kwargs
        }
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        logger.info(f"Making KIS domestic buy order request with params: {params}")
        result = self._make_post_request(url, tr_id, params)
        logger.debug(f"KIS domestic buy order response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def order_cash_sell(
        self, 
        CANO: str, 
        ACNT_PRDT_CD: str, 
        PDNO: str, 
        ORD_DVSN: str, 
        ORD_QTY: str, 
        ORD_UNPR: str,
        SLL_TYPE: str = "01",
        EXCG_ID_DVSN_CD: str = "KRX",
        **kwargs
    ) -> Dict[str, Any]:
        """
        국내주식 현금매도 주문 (v1_국내주식-001)
        
        Args:
            CANO: 종합계좌번호
            ACNT_PRDT_CD: 계좌상품코드
            PDNO: 상품번호 (종목코드 6자리)
            ORD_DVSN: 주문구분 (00:지정가, 01:시장가)
            ORD_QTY: 주문수량
            ORD_UNPR: 주문단가 (시장가시 "0")
            SLL_TYPE: 매도유형 (01:일반매도)
            EXCG_ID_DVSN_CD: 거래소ID구분코드 (기본: KRX)
            
        Returns:
            주문 결과
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTTC0011U"  # 모의투자 매도
        else:
            tr_id = "TTTC0011U"  # 실전투자 매도
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": ORD_QTY,
            "ORD_UNPR": ORD_UNPR,
            "SLL_TYPE": SLL_TYPE,
            "EXCG_ID_DVSN_CD": EXCG_ID_DVSN_CD,
            **kwargs
        }
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        
        logger.info(f"Making KIS domestic sell order request with params: {params}")
        result = self._make_post_request(url, tr_id, params)
        logger.debug(f"KIS domestic sell order response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def overseas_present_balance_test(self, CANO: str, ACNT_PRDT_CD: str, WCRC_FRCR_DVSN_CD: str = "01", 
                                     NATN_CD: str = "000", TR_MKET_CD: str = "00", INQR_DVSN_CD: str = "00") -> Dict[str, Any]:
        """
        해외주식 체결기준현재잔고 API 테스트 (v1_해외주식-008)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - WCRC_FRCR_DVSN_CD: 원화외화구분코드 (01: 원화, 02: 외화)
        - NATN_CD: 국가코드 (000: 전체, 840: 미국, 344: 홍콩, 156: 중국, 392: 일본, 704: 베트남)
        - TR_MKET_CD: 거래시장코드 (00: 전체, 국가별 세부 코드 참조)
        - INQR_DVSN_CD: 조회구분코드 (00: 전체, 01: 일반해외주식, 02: 미니스탁)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "WCRC_FRCR_DVSN_CD": WCRC_FRCR_DVSN_CD,
            "NATN_CD": NATN_CD,
            "TR_MKET_CD": TR_MKET_CD,
            "INQR_DVSN_CD": INQR_DVSN_CD,
        }
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTRP6504R"  # 모의투자
        else:
            tr_id = "CTRP6504R"  # 실전투자
            
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-present-balance"
        
        logger.info(f"Making KIS overseas present balance test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas present balance test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_order_history_test(self, CANO: str, ACNT_PRDT_CD: str, PDNO: str = "%", 
                                   ORD_STRT_DT: str = "", ORD_END_DT: str = "", 
                                   SLL_BUY_DVSN: str = "00", CCLD_NCCS_DVSN: str = "00", 
                                   OVRS_EXCG_CD: str = "%", SORT_SQN: str = "DS", 
                                   ORD_DT: str = "", ORD_GNO_BRNO: str = "", ODNO: str = "",
                                   CTX_AREA_NK200: str = "", CTX_AREA_FK200: str = "",
                                   extra_headers: Dict[str, str] = None) -> Dict[str, Any]:
        """
        해외주식 주문체결내역 API 테스트 (v1_해외주식-007)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - PDNO: 상품번호 (전종목일 경우 "%" 입력)
        - ORD_STRT_DT: 주문시작일자 (YYYYMMDD 형식)
        - ORD_END_DT: 주문종료일자 (YYYYMMDD 형식)
        - SLL_BUY_DVSN: 매도매수구분 (00: 전체, 01: 매도, 02: 매수)
        - CCLD_NCCS_DVSN: 체결미체결구분 (00: 전체, 01: 체결, 02: 미체결)
        - OVRS_EXCG_CD: 해외거래소코드 (전종목일 경우 "%" 입력)
        - SORT_SQN: 정렬순서 (DS: 정순, AS: 역순)
        - ORD_DT: 주문일자 (반드시 ""(Null 값 설정))
        - ORD_GNO_BRNO: 주문채번지점번호 (반드시 ""(Null 값 설정))
        - ODNO: 주문번호 (반드시 ""(Null 값 설정))
        - CTX_AREA_NK200: 연속조회키200
        - CTX_AREA_FK200: 연속조회검색조건200
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환 (PDNO가 "%"가 아닐 때만)
        kis_pdno = to_kis_symbol(PDNO) if PDNO != "%" else PDNO
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": kis_pdno,
            "ORD_STRT_DT": ORD_STRT_DT,
            "ORD_END_DT": ORD_END_DT,
            "SLL_BUY_DVSN": SLL_BUY_DVSN,
            "CCLD_NCCS_DVSN": CCLD_NCCS_DVSN,
            "OVRS_EXCG_CD": OVRS_EXCG_CD,
            "SORT_SQN": SORT_SQN,
            "ORD_DT": ORD_DT,
            "ORD_GNO_BRNO": ORD_GNO_BRNO,
            "ODNO": ODNO,
            "CTX_AREA_NK200": CTX_AREA_NK200,
            "CTX_AREA_FK200": CTX_AREA_FK200,
        }
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTTS3035R"  # 모의투자
        else:
            tr_id = "TTTS3035R"  # 실전투자
            
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl"
        
        logger.debug(f"해외주식 주문내역 조회 - 날짜: {ORD_STRT_DT}~{ORD_END_DT}, 체결구분: {CCLD_NCCS_DVSN}")
        
        result = self._make_request(url, tr_id, params, extra_headers=extra_headers)
        
        logger.debug(f"응답: rt_cd={result.get('rt_cd')}, 데이터 {len(result.get('output', []))}건")
        
        return result
    
    def overseas_profit_test(self, CANO: str, ACNT_PRDT_CD: str, OVRS_EXCG_CD: str = "", 
                            NATN_CD: str = "", CRCY_CD: str = "", PDNO: str = "", 
                            INQR_STRT_DT: str = "", INQR_END_DT: str = "", 
                            WCRC_FRCR_DVSN_CD: str = "01", CTX_AREA_FK200: str = "", 
                            CTX_AREA_NK200: str = "") -> Dict[str, Any]:
        """
        해외주식 기간손익 API 테스트 (v1_해외주식-032)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - OVRS_EXCG_CD: 해외거래소코드 (공란: 전체, NASD: 미국, SEHK: 홍콩, SHAA: 중국, TKSE: 일본, HASE: 베트남)
        - NATN_CD: 국가코드 (공란: Default)
        - CRCY_CD: 통화코드 (공란: 전체, USD: 미국달러, HKD: 홍콩달러, CNY: 중국위안화, JPY: 일본엔화, VND: 베트남동)
        - PDNO: 상품번호 (공란: 전체)
        - INQR_STRT_DT: 조회시작일자 (YYYYMMDD 형식)
        - INQR_END_DT: 조회종료일자 (YYYYMMDD 형식)
        - WCRC_FRCR_DVSN_CD: 원화외화구분코드 (01: 외화, 02: 원화)
        - CTX_AREA_FK200: 연속조회검색조건200
        - CTX_AREA_NK200: 연속조회키200
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환 (PDNO가 빈 문자열이 아닐 때만)
        kis_pdno = to_kis_symbol(PDNO) if PDNO else PDNO
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": OVRS_EXCG_CD,
            "NATN_CD": NATN_CD,
            "CRCY_CD": CRCY_CD,
            "PDNO": kis_pdno,
            "INQR_STRT_DT": INQR_STRT_DT,
            "INQR_END_DT": INQR_END_DT,
            "WCRC_FRCR_DVSN_CD": WCRC_FRCR_DVSN_CD,
            "CTX_AREA_FK200": CTX_AREA_FK200,
            "CTX_AREA_NK200": CTX_AREA_NK200,
        }
        
        # 가상환경에 따른 TR ID 선택 (모의투자 미지원)
        if settings.KIS_VIRTUAL:
            tr_id = "TTTS3039R"  # 모의투자 미지원이지만 일단 설정
        else:
            tr_id = "TTTS3039R"  # 실전투자
            
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-period-profit"
        
        logger.info(f"Making KIS overseas profit test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas profit test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def domestic_profit_test(self, CANO: str, SORT_DVSN: str = "00", ACNT_PRDT_CD: str = "01", 
                           PDNO: str = "", INQR_STRT_DT: str = "", INQR_END_DT: str = "", 
                           CTX_AREA_NK100: str = "", CBLC_DVSN: str = "00", 
                           CTX_AREA_FK100: str = "") -> Dict[str, Any]:
        """
        기간별매매손익현황조회 API 테스트 (v1_국내주식-060)
        
        Parameters:
        - CANO: 종합계좌번호
        - SORT_DVSN: 정렬구분 (00: 최근 순, 01: 과거 순, 02: 최근 순)
        - ACNT_PRDT_CD: 계좌상품코드
        - PDNO: 상품번호 (공란 입력 시 전체)
        - INQR_STRT_DT: 조회시작일자 (YYYYMMDD 형식)
        - INQR_END_DT: 조회종료일자 (YYYYMMDD 형식)
        - CTX_AREA_NK100: 연속조회키100
        - CBLC_DVSN: 잔고구분 (00: 전체)
        - CTX_AREA_FK100: 연속조회검색조건100
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "SORT_DVSN": SORT_DVSN,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "INQR_STRT_DT": INQR_STRT_DT,
            "INQR_END_DT": INQR_END_DT,
            "CTX_AREA_NK100": CTX_AREA_NK100,
            "CBLC_DVSN": CBLC_DVSN,
            "CTX_AREA_FK100": CTX_AREA_FK100,
        }
        
        # 가상환경에 따른 TR ID 선택 (모의투자 미지원)
        if settings.KIS_VIRTUAL:
            tr_id = "TTTC8715R"  # 모의투자 미지원이지만 일단 설정
        else:
            tr_id = "TTTC8715R"  # 실전투자
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
        
        logger.info(f"Making KIS domestic profit test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS domestic profit test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def buy_possible_test(self, CANO: str, ACNT_PRDT_CD: str = "01", PDNO: str = "", 
                         ORD_UNPR: str = "", ORD_DVSN: str = "01", 
                         CMA_EVLU_AMT_ICLD_YN: str = "N", OVRS_ICLD_YN: str = "N") -> Dict[str, Any]:
        """
        매수가능조회 API 테스트 (v1_국내주식-007)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - PDNO: 상품번호 (종목번호 6자리, 공란 입력 시 매수수량 없이 매수금액만 조회)
        - ORD_UNPR: 주문단가 (1주당 가격, 시장가 조회 시 공란)
        - ORD_DVSN: 주문구분 (00: 지정가, 01: 시장가, 02: 조건부지정가 등)
        - CMA_EVLU_AMT_ICLD_YN: CMA평가금액포함여부 (Y: 포함, N: 포함하지 않음)
        - OVRS_ICLD_YN: 해외포함여부 (Y: 포함, N: 포함하지 않음)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "ORD_UNPR": ORD_UNPR,
            "ORD_DVSN": ORD_DVSN,
            "CMA_EVLU_AMT_ICLD_YN": CMA_EVLU_AMT_ICLD_YN,
            "OVRS_ICLD_YN": OVRS_ICLD_YN,
        }
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTTC8908R"  # 모의투자
        else:
            tr_id = "TTTC8908R"  # 실전투자
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        
        logger.info(f"Making KIS buy possible test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS buy possible test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def period_price_test(self, FID_COND_MRKT_DIV_CODE: str = "J", FID_INPUT_ISCD: str = "005930",
                         FID_INPUT_DATE_1: str = "", FID_INPUT_DATE_2: str = "",
                         FID_PERIOD_DIV_CODE: str = "D", FID_ORG_ADJ_PRC: str = "0") -> Dict[str, Any]:
        """
        국내주식기간별시세 API 테스트 (v1_국내주식-016)
        
        Parameters:
        - FID_COND_MRKT_DIV_CODE: 조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)
        - FID_INPUT_ISCD: 입력 종목코드 (예: 005930 삼성전자)
        - FID_INPUT_DATE_1: 입력 날짜 1 (조회 시작일자, YYYYMMDD 형식)
        - FID_INPUT_DATE_2: 입력 날짜 2 (조회 종료일자, YYYYMMDD 형식, 최대 100개)
        - FID_PERIOD_DIV_CODE: 기간분류코드 (D: 일봉, W: 주봉, M: 월봉, Y: 년봉)
        - FID_ORG_ADJ_PRC: 수정주가 원주가 가격 여부 (0: 수정주가, 1: 원주가)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_INPUT_DATE_1": FID_INPUT_DATE_1,
            "FID_INPUT_DATE_2": FID_INPUT_DATE_2,
            "FID_PERIOD_DIV_CODE": FID_PERIOD_DIV_CODE,
            "FID_ORG_ADJ_PRC": FID_ORG_ADJ_PRC,
        }
        
        # 가상환경에 따른 TR ID 선택 (실전/모의투자 동일)
        tr_id = "FHKST03010100"
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        
        logger.info(f"Making KIS period price test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS period price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_minute_price_test(self, AUTH: str = "", EXCD: str = "NAS", SYMB: str = "AAPL",
                                   NMIN: str = "1", PINC: str = "0", NEXT: str = "",
                                   NREC: str = "120", FILL: str = "", KEYB: str = "") -> Dict[str, Any]:
        """
        해외주식분봉조회 API 테스트 (v1_해외주식-030)
        
        Parameters:
        - AUTH: 사용자권한정보 (공백으로 입력)
        - EXCD: 거래소코드 (NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, HKS: 홍콩, SHS: 상해, SZS: 심천, HSX: 호치민, HNX: 하노이, TSE: 도쿄, BAY: 뉴욕(주간), BAQ: 나스닥(주간), BAA: 아멕스(주간))
        - SYMB: 종목코드 (예: AAPL, TSLA)
        - NMIN: 분갭 (분단위: 1: 1분봉, 2: 2분봉 등)
        - PINC: 전일포함여부 (0: 당일, 1: 전일포함)
        - NEXT: 다음여부 (처음조회 시 공백, 다음조회 시 1)
        - NREC: 요청갯수 (레코드요청갯수, 최대 120)
        - FILL: 미체결채움구분 (공백으로 입력)
        - KEYB: NEXT KEY BUFF (처음조회 시 공백, 다음조회 시 YYYYMMDDHHMMSS 형식)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = SYMB.replace("-", "/")
        
        params = {
            "AUTH": AUTH,
            "EXCD": EXCD,
            "SYMB": kis_symbol,
            "NMIN": NMIN,
            "PINC": PINC,
            "NEXT": NEXT,
            "NREC": NREC,
            "FILL": FILL,
            "KEYB": KEYB,
        }
        
        # 가상환경에 따른 TR ID 선택 (모의투자 미지원)
        if settings.KIS_VIRTUAL:
            tr_id = "HHDFS76950200"  # 모의투자 미지원이지만 일단 설정
        else:
            tr_id = "HHDFS76950200"  # 실전투자
            
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
        
        logger.info(f"Making KIS overseas minute price test request: {SYMB} -> {kis_symbol} with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas minute price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def domestic_minute_price_test(self, FID_COND_MRKT_DIV_CODE: str = "J", FID_INPUT_ISCD: str = "005930",
                                   FID_INPUT_HOUR_1: str = "", FID_INPUT_DATE_1: str = "",
                                   FID_PW_DATA_INCU_YN: str = "Y", FID_FAKE_TICK_INCU_YN: str = "") -> Dict[str, Any]:
        """
        주식일별분봉조회 API 테스트 (국내주식-213)
        
        Parameters:
        - FID_COND_MRKT_DIV_CODE: 조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)
        - FID_INPUT_ISCD: 입력 종목코드 (예: 005930 삼성전자)
        - FID_INPUT_HOUR_1: 입력 시간1 (예: 13시 → 130000, 공백: 현재시간)
        - FID_INPUT_DATE_1: 입력 날짜1 (YYYYMMDD 형식)
        - FID_PW_DATA_INCU_YN: 과거 데이터 포함 여부
        - FID_FAKE_TICK_INCU_YN: 허봉 포함 여부 (공백 필수 입력)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_INPUT_HOUR_1": FID_INPUT_HOUR_1,
            "FID_INPUT_DATE_1": FID_INPUT_DATE_1,
            "FID_PW_DATA_INCU_YN": FID_PW_DATA_INCU_YN,
            "FID_FAKE_TICK_INCU_YN": FID_FAKE_TICK_INCU_YN,
        }
        
        # 가상환경에 따른 TR ID 선택 (모의투자 미지원)
        if settings.KIS_VIRTUAL:
            tr_id = "FHKST03010230"  # 모의투자 미지원이지만 일단 설정
        else:
            tr_id = "FHKST03010230"  # 실전투자
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
        
        logger.info(f"Making KIS domestic minute price test request with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS domestic minute price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def domestic_order_test(self, CANO: str, ACNT_PRDT_CD: str = "01", INQR_STRT_DT: str = "", 
                           INQR_END_DT: str = "", SLL_BUY_DVSN_CD: str = "00", PDNO: str = "",
                           ORD_GNO_BRNO: str = "", ODNO: str = "", CCLD_DVSN: str = "00",
                           INQR_DVSN: str = "00", INQR_DVSN_1: str = "", INQR_DVSN_3: str = "00",
                           EXCG_ID_DVSN_CD: str = "KRX", CTX_AREA_FK100: str = "", 
                           CTX_AREA_NK100: str = "", extra_headers: Dict[str, str] = None) -> Dict[str, Any]:
        """
        주식일별주문체결조회 API 테스트 (v1_국내주식-005)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - INQR_STRT_DT: 조회시작일자 (YYYYMMDD 형식)
        - INQR_END_DT: 조회종료일자 (YYYYMMDD 형식)
        - SLL_BUY_DVSN_CD: 매도매수구분코드 (00: 전체, 01: 매도, 02: 매수)
        - PDNO: 상품번호 (종목번호 6자리, 공란: 전체)
        - ORD_GNO_BRNO: 주문채번지점번호
        - ODNO: 주문번호
        - CCLD_DVSN: 체결구분 (00: 전체, 01: 체결, 02: 미체결)
        - INQR_DVSN: 조회구분 (00: 역순, 01: 정순)
        - INQR_DVSN_1: 조회구분1 (공란: 전체, 1: ELW, 2: 프리보드)
        - INQR_DVSN_3: 조회구분3 (00: 전체, 01: 현금, 02: 신용 등)
        - EXCG_ID_DVSN_CD: 거래소ID구분코드 (KRX: 한국거래소, NXT: 대체거래소, SOR: Smart Order Routing, ALL: 전체)
        - CTX_AREA_FK100: 연속조회검색조건100
        - CTX_AREA_NK100: 연속조회키100
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "INQR_STRT_DT": INQR_STRT_DT,
            "INQR_END_DT": INQR_END_DT,
            "SLL_BUY_DVSN_CD": SLL_BUY_DVSN_CD,
            "PDNO": PDNO,
            "ORD_GNO_BRNO": ORD_GNO_BRNO,
            "ODNO": ODNO,
            "CCLD_DVSN": CCLD_DVSN,
            "INQR_DVSN": INQR_DVSN,
            "INQR_DVSN_1": INQR_DVSN_1,
            "INQR_DVSN_3": INQR_DVSN_3,
            "EXCG_ID_DVSN_CD": EXCG_ID_DVSN_CD,
            "CTX_AREA_FK100": CTX_AREA_FK100,
            "CTX_AREA_NK100": CTX_AREA_NK100,
        }
        
        # 가상환경에 따른 TR ID 선택 (3개월이내 기준)
        if settings.KIS_VIRTUAL:
            tr_id = "VTTC0081R"  # 모의투자 (3개월이내)
        else:
            tr_id = "TTTC0081R"  # 실전투자 (3개월이내)
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        
        logger.debug(f"국내주식 주문내역 조회 - 날짜: {INQR_STRT_DT}~{INQR_END_DT}, 체결구분: {CCLD_DVSN}")
        
        result = self._make_request(url, tr_id, params, extra_headers=extra_headers)
        
        logger.debug(f"응답: rt_cd={result.get('rt_cd')}, 데이터 {len(result.get('output1', []))}건")
        
        return result
    
    def domestic_order_revise_test(self, CANO: str, ACNT_PRDT_CD: str = "01", 
                                   KRX_FWDG_ORD_ORGNO: str = "", ORGN_ODNO: str = "",
                                   ORD_DVSN: str = "00", RVSE_CNCL_DVSN_CD: str = "02",
                                   ORD_QTY: str = "0", ORD_UNPR: str = "0",
                                   QTY_ALL_ORD_YN: str = "Y", CNDT_PRIC: str = "",
                                   EXCG_ID_DVSN_CD: str = "KRX") -> Dict[str, Any]:
        """
        주식주문(정정취소) API 테스트 (v1_국내주식-003)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - KRX_FWDG_ORD_ORGNO: 한국거래소전송주문조직번호
        - ORGN_ODNO: 원주문번호
        - ORD_DVSN: 주문구분 (00: 지정가, 01: 시장가 등)
        - RVSE_CNCL_DVSN_CD: 정정취소구분코드 (01: 정정, 02: 취소)
        - ORD_QTY: 주문수량
        - ORD_UNPR: 주문단가
        - QTY_ALL_ORD_YN: 잔량전부주문여부 (Y: 전량, N: 일부)
        - CNDT_PRIC: 조건가격 (스탑지정가호가에서 사용)
        - EXCG_ID_DVSN_CD: 거래소ID구분코드 (KRX: 한국거래소, NXT: 대체거래소, SOR: Smart Order Routing)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "KRX_FWDG_ORD_ORGNO": KRX_FWDG_ORD_ORGNO,
            "ORGN_ODNO": ORGN_ODNO,
            "ORD_DVSN": ORD_DVSN,
            "RVSE_CNCL_DVSN_CD": RVSE_CNCL_DVSN_CD,
            "ORD_QTY": ORD_QTY,
            "ORD_UNPR": ORD_UNPR,
            "QTY_ALL_ORD_YN": QTY_ALL_ORD_YN,
            "CNDT_PRIC": CNDT_PRIC,
            "EXCG_ID_DVSN_CD": EXCG_ID_DVSN_CD,
        }
        
        # 가상환경에 따른 TR ID 선택
        if settings.KIS_VIRTUAL:
            tr_id = "VTTC0013U"  # 모의투자
        else:
            tr_id = "TTTC0013U"  # 실전투자
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
        
        logger.info(f"Making KIS domestic order revise test request with data: {data}")
        result = self._make_post_request(url, tr_id, data)
        logger.debug(f"KIS domestic order revise test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_order_revise_test(self, CANO: str, ACNT_PRDT_CD: str = "01", 
                                   OVRS_EXCG_CD: str = "NASD", PDNO: str = "",
                                   ORGN_ODNO: str = "", RVSE_CNCL_DVSN_CD: str = "02",
                                   ORD_QTY: str = "0", OVRS_ORD_UNPR: str = "0",
                                   MGCO_APTM_ODNO: str = "", ORD_SVR_DVSN_CD: str = "0") -> Dict[str, Any]:
        """
        해외주식 정정취소주문 API 테스트 (v1_해외주식-003)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - OVRS_EXCG_CD: 해외거래소코드 (NASD: 나스닥, NYSE: 뉴욕, AMEX: 아멕스, SEHK: 홍콩 등)
        - PDNO: 상품번호 (종목코드)
        - ORGN_ODNO: 원주문번호 (정정 또는 취소할 원주문번호)
        - RVSE_CNCL_DVSN_CD: 정정취소구분코드 (01: 정정, 02: 취소)
        - ORD_QTY: 주문수량
        - OVRS_ORD_UNPR: 해외주문단가 (취소주문 시 0 입력)
        - MGCO_APTM_ODNO: 운용사지정주문번호
        - ORD_SVR_DVSN_CD: 주문서버구분코드 (기본값: 0)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환 (PDNO가 빈 문자열이 아닐 때만)
        kis_pdno = to_kis_symbol(PDNO) if PDNO else PDNO
        
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": {"NYS": "NYSE", "NYQ": "NYSE", "NMS": "NASD", "NAS": "NASD"}.get(OVRS_EXCG_CD, OVRS_EXCG_CD),
            "PDNO": kis_pdno,
            "ORGN_ODNO": ORGN_ODNO,
            "RVSE_CNCL_DVSN_CD": RVSE_CNCL_DVSN_CD,
            "ORD_QTY": ORD_QTY,
            "OVRS_ORD_UNPR": OVRS_ORD_UNPR,
            "MGCO_APTM_ODNO": MGCO_APTM_ODNO,
            "ORD_SVR_DVSN_CD": ORD_SVR_DVSN_CD,
        }
        
        # 가상환경에 따른 TR ID 선택 (미국 기준)
        if settings.KIS_VIRTUAL:
            tr_id = "VTTT1004U"  # 모의투자 (미국)
        else:
            tr_id = "TTTT1004U"  # 실전투자 (미국)
            
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order-rvsecncl"
        
        logger.info(f"Making KIS overseas order revise test request with data: {data}")
        result = self._make_post_request(url, tr_id, data)
        logger.debug(f"KIS overseas order revise test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_period_price_test(self, AUTH: str = "", EXCD: str = "NAS", 
                                   SYMB: str = "AAPL", GUBN: str = "0",
                                   BYMD: str = "", MODP: str = "0",
                                   KEYB: str = "") -> Dict[str, Any]:
        """
        해외주식 기간별시세 API 테스트 (v1_해외주식-010)
        
        Parameters:
        - AUTH: 사용자권한정보 (기본값: 공백)
        - EXCD: 거래소코드 (HKS: 홍콩, NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, TSE: 도쿄 등)
        - SYMB: 종목코드 (예: AAPL)
        - GUBN: 일/주/월구분 (0: 일, 1: 주, 2: 월)
        - BYMD: 조회기준일자 (YYYYMMDD, 공란 시 오늘 날짜)
        - MODP: 수정주가반영여부 (0: 미반영, 1: 반영)
        - KEYB: NEXT KEY BUFF (다음 조회시 응답값 그대로 설정)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = SYMB.replace("-", "/")
        
        params = {
            "AUTH": AUTH,
            "EXCD": EXCD,
            "SYMB": kis_symbol,
            "GUBN": GUBN,
            "BYMD": BYMD,
            "MODP": MODP,
            "KEYB": KEYB,
        }
        
        # 실전/모의투자 동일한 TR ID
        tr_id = "HHDFS76240000"
            
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        
        logger.info(f"Making KIS overseas period price test request: {SYMB} -> {kis_symbol} with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas period price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_current_price_test(self, AUTH: str = "", EXCD: str = "NAS", 
                                    SYMB: str = "AAPL") -> Dict[str, Any]:
        """
        해외주식 현재가상세 API 테스트 (v1_해외주식-029)
        
        Parameters:
        - AUTH: 사용자권한정보
        - EXCD: 거래소명 (HKS: 홍콩, NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, TSE: 도쿄 등)
        - SYMB: 종목코드 (예: AAPL)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환
        kis_symbol = SYMB.replace("-", "/")
        
        params = {
            "AUTH": AUTH,
            "EXCD": EXCD,
            "SYMB": kis_symbol,
        }
        
        # 실전투자만 지원 (모의투자 미지원)
        tr_id = "HHDFS76200200"
            
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price-detail"
        
        logger.info(f"Making KIS overseas current price test request: {SYMB} -> {kis_symbol} with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas current price test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def overseas_news_test(self, INFO_GB: str = "", CLASS_CD: str = "", 
                          NATION_CD: str = "", EXCHANGE_CD: str = "",
                          SYMB: str = "", DATA_DT: str = "",
                          DATA_TM: str = "", CTS: str = "") -> Dict[str, Any]:
        """
        해외뉴스종합(제목) API 테스트 (해외주식-053)
        
        Parameters:
        - INFO_GB: 뉴스구분 (전체: 공백)
        - CLASS_CD: 중분류 (전체: 공백)
        - NATION_CD: 국가코드 (전체: 공백, CN: 중국, HK: 홍콩, US: 미국)
        - EXCHANGE_CD: 거래소코드 (전체: 공백)
        - SYMB: 종목코드 (전체: 공백)
        - DATA_DT: 조회일자 (전체: 공백, 특정일자: YYYYMMDD)
        - DATA_TM: 조회시간 (전체: 공백, 특정시간: HHMMSS)
        - CTS: 다음키 (공백 입력)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # KIS API용 심볼 변환: 하이픈(-)을 슬래시(/)로 변환 (SYMB가 빈 문자열이 아닐 때만)
        kis_symb = SYMB.replace("-", "/") if SYMB else SYMB
        
        params = {
            "INFO_GB": INFO_GB,
            "CLASS_CD": CLASS_CD,
            "NATION_CD": NATION_CD,
            "EXCHANGE_CD": EXCHANGE_CD,
            "SYMB": kis_symb,
            "DATA_DT": DATA_DT,
            "DATA_TM": DATA_TM,
            "CTS": CTS,
        }
        
        # 실전투자만 지원 (모의투자 미지원)
        tr_id = "HHPSTH60100C1"
            
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/news-title"
        
        logger.info(f"Making KIS overseas news test request: SYMB={SYMB} -> {kis_symb}, NATION_CD={NATION_CD} with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS overseas news test response status: {result.get('rt_cd', 'unknown')}")
        
        return result
    
    def domestic_news_test(self, FID_NEWS_OFER_ENTP_CODE: str = "", 
                          FID_COND_MRKT_CLS_CODE: str = "", FID_INPUT_ISCD: str = "",
                          FID_TITL_CNTT: str = "", FID_INPUT_DATE_1: str = "",
                          FID_INPUT_HOUR_1: str = "", FID_RANK_SORT_CLS_CODE: str = "",
                          FID_INPUT_SRNO: str = "") -> Dict[str, Any]:
        """
        종합 시황/공시(제목) API 테스트 (국내주식-141)
        
        Parameters:
        - FID_NEWS_OFER_ENTP_CODE: 뉴스 제공 업체 코드 (공백 필수 입력)
        - FID_COND_MRKT_CLS_CODE: 조건 시장 구분 코드 (공백 필수 입력)
        - FID_INPUT_ISCD: 입력 종목코드 (공백: 전체, 종목코드: 해당코드가 등록된 뉴스)
        - FID_TITL_CNTT: 제목 내용 (공백 필수 입력)
        - FID_INPUT_DATE_1: 입력 날짜 (공백: 현재기준, 조회일자: 00YYYYMMDD)
        - FID_INPUT_HOUR_1: 입력 시간 (공백: 현재기준, 조회시간: 0000HHMMSS)
        - FID_RANK_SORT_CLS_CODE: 순위 정렬 구분 코드 (공백 필수 입력)
        - FID_INPUT_SRNO: 입력 일련번호 (공백 필수 입력)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        params = {
            "FID_NEWS_OFER_ENTP_CODE": FID_NEWS_OFER_ENTP_CODE,
            "FID_COND_MRKT_CLS_CODE": FID_COND_MRKT_CLS_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_TITL_CNTT": FID_TITL_CNTT,
            "FID_INPUT_DATE_1": FID_INPUT_DATE_1,
            "FID_INPUT_HOUR_1": FID_INPUT_HOUR_1,
            "FID_RANK_SORT_CLS_CODE": FID_RANK_SORT_CLS_CODE,
            "FID_INPUT_SRNO": FID_INPUT_SRNO,
        }
        
        # 실전투자만 지원 (모의투자 미지원)
        tr_id = "FHKST01011800"
            
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/news-title"
        
        logger.info(f"Making KIS domestic news test request: FID_INPUT_ISCD={FID_INPUT_ISCD} with params: {params}")
        result = self._make_request(url, tr_id, params)
        logger.debug(f"KIS domestic news test response status: {result.get('rt_cd', 'unknown')}")
        
        return result

    def inquire_balance_rlz_pl(self, CANO: str, ACNT_PRDT_CD: str, AFHR_FLPR_YN: str = "N", 
                              OFL_YN: str = "", INQR_DVSN: str = "00", UNPR_DVSN: str = "01", 
                              FUND_STTL_ICLD_YN: str = "N", FNCG_AMT_AUTO_RDPT_YN: str = "N", 
                              PRCS_DVSN: str = "00", COST_ICLD_YN: str = "", 
                              CTX_AREA_FK100: str = "", CTX_AREA_NK100: str = "") -> Dict[str, Any]:
        """
        주식잔고조회_실현손익 (v1_국내주식-041)
        
        Parameters:
        - CANO: 종합계좌번호
        - ACNT_PRDT_CD: 계좌상품코드
        - AFHR_FLPR_YN: 시간외단일가여부 (N: 기본값, Y: 시간외단일가)
        - OFL_YN: 오프라인여부 (공란)
        - INQR_DVSN: 조회구분 (00: 전체)
        - UNPR_DVSN: 단가구분 (01: 기본값)
        - FUND_STTL_ICLD_YN: 펀드결제포함여부 (N: 포함하지 않음, Y: 포함)
        - FNCG_AMT_AUTO_RDPT_YN: 융자금액자동상환여부 (N: 기본값)
        - PRCS_DVSN: PRCS_DVSN (00: 전일매매포함, 01: 전일매매미포함)
        - COST_ICLD_YN: 비용포함여부 (공란)
        - CTX_AREA_FK100: 연속조회검색조건100 (공란: 최초 조회시)
        - CTX_AREA_NK100: 연속조회키100 (공란: 최초 조회시)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 모의투자 환경에서는 지원하지 않음
        if settings.KIS_VIRTUAL:
            logger.warning("⚠️ 주식잔고조회_실현손익 API는 모의투자에서 지원하지 않습니다.")
            return {
                "rt_cd": "1",
                "msg_cd": "NOT_SUPPORTED",
                "msg1": "모의투자에서는 주식잔고조회_실현손익 API를 지원하지 않습니다.",
                "output1": None,
                "output2": None
            }
        
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "AFHR_FLPR_YN": AFHR_FLPR_YN,
            "OFL_YN": OFL_YN,
            "INQR_DVSN": INQR_DVSN,
            "UNPR_DVSN": UNPR_DVSN,
            "FUND_STTL_ICLD_YN": FUND_STTL_ICLD_YN,
            "FNCG_AMT_AUTO_RDPT_YN": FNCG_AMT_AUTO_RDPT_YN,
            "PRCS_DVSN": PRCS_DVSN,
            "COST_ICLD_YN": COST_ICLD_YN,
            "CTX_AREA_FK100": CTX_AREA_FK100,
            "CTX_AREA_NK100": CTX_AREA_NK100,
        }
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance-rlz-pl"
        
        logger.info(f"Making KIS inquire balance rlz pl request with params: {params}")
        result = self._make_request(url, "TTTC8494R", params)
        logger.debug(f"KIS inquire balance rlz pl response status: {result.get('rt_cd', 'unknown')}")
        
        return result