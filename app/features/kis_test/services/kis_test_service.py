# app/features/kis_test/services/kis_test_service.py

from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from app.core.kis_client import KISClient
from app.features.kis_test.models.kis_test_models import (
    KISBalanceInquiryRequest, 
    KISBalanceInquiryResponse,
    KISOverseasBalanceRequest,
    KISOverseasBalanceResponse,
    KISOverseasOrderRequest,
    KISOverseasOrderResponse,
    KISOverseasProfitRequest,
    KISOverseasProfitResponse,
    KISDomesticProfitRequest,
    KISDomesticProfitResponse,
    KISBuyPossibleRequest,
    KISBuyPossibleResponse,
    KISDailyPriceRequest,
    KISDailyPriceResponse,
    KISPeriodPriceRequest,
    KISPeriodPriceResponse,
    KISOverseasMinuteRequest,
    KISOverseasMinuteResponse,
    KISDomesticMinuteRequest,
    KISDomesticMinuteResponse,
    KISCurrentPriceRequest,
    KISCurrentPriceResponse,
    KISDomesticOrderRequest,
    KISDomesticOrderResponse,
    KISDomesticOrderReviseRequest,
    KISDomesticOrderReviseResponse,
    KISOverseasOrderReviseRequest,
    KISOverseasOrderReviseResponse,
    KISOverseasPeriodPriceRequest,
    KISOverseasPeriodPriceResponse,
    KISOverseasCurrentPriceRequest,
    KISOverseasCurrentPriceResponse,
    KISOverseasNewsRequest,
    KISOverseasNewsResponse,
    KISDomesticNewsRequest,
    KISDomesticNewsResponse,
    KISDomesticHolidayRequest,
    KISDomesticHolidayResponse,
    KISTestResponse,
    KISAPITestRequest,
    KISAPITestResponse,
    KISEnvironment
)
import logging

logger = logging.getLogger(__name__)


class KISTestService:
    """KIS API 테스트 서비스
    
    한국투자증권 KIS API를 테스트하기 위한 서비스입니다.
    각종 KIS API를 안전하게 테스트하고 결과를 반환합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
    
    async def test_balance_inquiry_rlz_pl(self, request: KISBalanceInquiryRequest) -> KISBalanceInquiryResponse:
        """주식잔고조회_실현손익 API 테스트
        
        한국투자증권의 주식잔고조회_실현손익 API (v1_국내주식-041)를 테스트합니다.
        계좌의 주식 잔고와 실현손익 정보를 조회합니다.
        
        Args:
            request: 주식잔고조회_실현손익 요청 파라미터
            
        Returns:
            KISBalanceInquiryResponse: 주식잔고조회_실현손익 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식잔고조회_실현손익 테스트 요청 - CANO: {request.CANO}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.inquire_balance_rlz_pl(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                AFHR_FLPR_YN=request.AFHR_FLPR_YN,
                OFL_YN=request.OFL_YN,
                INQR_DVSN=request.INQR_DVSN,
                UNPR_DVSN=request.UNPR_DVSN,
                FUND_STTL_ICLD_YN=request.FUND_STTL_ICLD_YN,
                FNCG_AMT_AUTO_RDPT_YN=request.FNCG_AMT_AUTO_RDPT_YN,
                PRCS_DVSN=request.PRCS_DVSN,
                COST_ICLD_YN=request.COST_ICLD_YN,
                CTX_AREA_FK100=request.CTX_AREA_FK100,
                CTX_AREA_NK100=request.CTX_AREA_NK100
            )
            
            logger.info(f"KIS 주식잔고조회_실현손익 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISBalanceInquiryResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                ctx_area_fk100=result.get("ctx_area_fk100"),
                ctx_area_nk100=result.get("ctx_area_nk100"),
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식잔고조회_실현손익 API 테스트 중 오류 발생: {str(e)}")
            raise
    
    async def test_overseas_present_balance(self, request: KISOverseasBalanceRequest) -> KISOverseasBalanceResponse:
        """해외주식 체결기준현재잔고 API 테스트
        
        한국투자증권의 해외주식 체결기준현재잔고 API (v1_해외주식-008)를 테스트합니다.
        해외주식 계좌의 체결기준 현재 잔고 정보를 조회합니다.
        
        Args:
            request: 해외주식 체결기준현재잔고 요청 파라미터
            
        Returns:
            KISOverseasBalanceResponse: 해외주식 체결기준현재잔고 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 체결기준현재잔고 테스트 요청 - CANO: {request.CANO}, NATN_CD: {request.NATN_CD}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_present_balance_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                WCRC_FRCR_DVSN_CD=request.WCRC_FRCR_DVSN_CD,
                NATN_CD=request.NATN_CD,
                TR_MKET_CD=request.TR_MKET_CD,
                INQR_DVSN_CD=request.INQR_DVSN_CD
            )
            
            logger.info(f"KIS 해외주식 체결기준현재잔고 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasBalanceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                output3=result.get("output3"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 체결기준현재잔고 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_order_history(self, request: KISOverseasOrderRequest, tr_count: Optional[str] = None, gt_uid: Optional[str] = None) -> KISOverseasOrderResponse:
        """해외주식 주문체결내역 API 테스트
        
        한국투자증권의 해외주식 주문체결내역 API (v1_해외주식-007)를 테스트합니다.
        해외주식 계좌의 주문체결내역을 조회합니다.
        
        Args:
            request: 해외주식 주문체결내역 요청 파라미터
            tr_count: tr_count 헤더 값 (연속조회 시 사용)
            gt_uid: Global UID 헤더 값 (법인 전용, 거래고유번호)
            
        Returns:
            KISOverseasOrderResponse: 해외주식 주문체결내역 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 주문체결내역 테스트 요청 - CANO: {request.CANO}, ORD_STRT_DT: {request.ORD_STRT_DT}, ORD_END_DT: {request.ORD_END_DT}, tr_count: {tr_count}, gt_uid: {gt_uid}")
            
            # extra_headers 구성
            extra_headers = {}
            if tr_count is not None:
                extra_headers["tr_count"] = tr_count
            if gt_uid is not None:
                extra_headers["gt_uid"] = gt_uid
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_order_history_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                PDNO=request.PDNO,
                ORD_STRT_DT=request.ORD_STRT_DT,
                ORD_END_DT=request.ORD_END_DT,
                SLL_BUY_DVSN=request.SLL_BUY_DVSN,
                CCLD_NCCS_DVSN=request.CCLD_NCCS_DVSN,
                OVRS_EXCG_CD=request.OVRS_EXCG_CD,
                SORT_SQN=request.SORT_SQN,
                ORD_DT=request.ORD_DT,
                ORD_GNO_BRNO=request.ORD_GNO_BRNO,
                ODNO=request.ODNO,
                CTX_AREA_NK200=request.CTX_AREA_NK200,
                CTX_AREA_FK200=request.CTX_AREA_FK200,
                extra_headers=extra_headers if extra_headers else None
            )
            
            logger.info(f"KIS 해외주식 주문체결내역 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasOrderResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                ctx_area_fk200=result.get("ctx_area_fk200", ""),
                ctx_area_nk200=result.get("ctx_area_nk200", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 주문체결내역 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_profit(self, request: KISOverseasProfitRequest) -> KISOverseasProfitResponse:
        """해외주식 기간손익 API 테스트
        
        한국투자증권의 해외주식 기간손익 API (v1_해외주식-032)를 테스트합니다.
        해외주식 계좌의 기간별 손익 정보를 조회합니다.
        
        Args:
            request: 해외주식 기간손익 요청 파라미터
            
        Returns:
            KISOverseasProfitResponse: 해외주식 기간손익 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 기간손익 테스트 요청 - CANO: {request.CANO}, INQR_STRT_DT: {request.INQR_STRT_DT}, INQR_END_DT: {request.INQR_END_DT}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_profit_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                OVRS_EXCG_CD=request.OVRS_EXCG_CD,
                NATN_CD=request.NATN_CD,
                CRCY_CD=request.CRCY_CD,
                PDNO=request.PDNO,
                INQR_STRT_DT=request.INQR_STRT_DT,
                INQR_END_DT=request.INQR_END_DT,
                WCRC_FRCR_DVSN_CD=request.WCRC_FRCR_DVSN_CD,
                CTX_AREA_FK200=request.CTX_AREA_FK200,
                CTX_AREA_NK200=request.CTX_AREA_NK200
            )
            
            logger.info(f"KIS 해외주식 기간손익 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasProfitResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 기간손익 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_profit(self, request: KISDomesticProfitRequest) -> KISDomesticProfitResponse:
        """기간별매매손익현황조회 API 테스트
        
        한국투자증권의 기간별매매손익현황조회 API (v1_국내주식-060)를 테스트합니다.
        국내주식 계좌의 기간별 매매손익 현황을 조회합니다.
        
        Args:
            request: 기간별매매손익현황조회 요청 파라미터
            
        Returns:
            KISDomesticProfitResponse: 기간별매매손익현황조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 기간별매매손익현황조회 테스트 요청 - CANO: {request.CANO}, INQR_STRT_DT: {request.INQR_STRT_DT}, INQR_END_DT: {request.INQR_END_DT}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_profit_test(
                CANO=request.CANO,
                SORT_DVSN=request.SORT_DVSN,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                PDNO=request.PDNO,
                INQR_STRT_DT=request.INQR_STRT_DT,
                INQR_END_DT=request.INQR_END_DT,
                CTX_AREA_NK100=request.CTX_AREA_NK100,
                CBLC_DVSN=request.CBLC_DVSN,
                CTX_AREA_FK100=request.CTX_AREA_FK100
            )
            
            logger.info(f"KIS 기간별매매손익현황조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticProfitResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                ctx_area_nk100=result.get("ctx_area_nk100", ""),
                ctx_area_fk100=result.get("ctx_area_fk100", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 기간별매매손익현황조회 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_buy_possible(self, request: KISBuyPossibleRequest) -> KISBuyPossibleResponse:
        """매수가능조회 API 테스트
        
        한국투자증권의 매수가능조회 API (v1_국내주식-007)를 테스트합니다.
        국내주식 계좌의 매수 가능 금액과 수량을 조회합니다.
        
        Args:
            request: 매수가능조회 요청 파라미터
            
        Returns:
            KISBuyPossibleResponse: 매수가능조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 매수가능조회 테스트 요청 - CANO: {request.CANO}, PDNO: {request.PDNO}, ORD_DVSN: {request.ORD_DVSN}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.buy_possible_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                PDNO=request.PDNO,
                ORD_UNPR=request.ORD_UNPR,
                ORD_DVSN=request.ORD_DVSN,
                CMA_EVLU_AMT_ICLD_YN=request.CMA_EVLU_AMT_ICLD_YN,
                OVRS_ICLD_YN=request.OVRS_ICLD_YN
            )
            
            logger.info(f"KIS 매수가능조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISBuyPossibleResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 매수가능조회 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_daily_price(self, request: KISDailyPriceRequest) -> KISDailyPriceResponse:
        """주식현재가 일자별 API 테스트
        
        한국투자증권의 주식현재가 일자별 API (v1_국내주식-010)를 테스트합니다.
        국내주식의 일별/주별/월별 시세 정보를 조회합니다.
        
        Args:
            request: 주식현재가 일자별 요청 파라미터
            
        Returns:
            KISDailyPriceResponse: 주식현재가 일자별 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식현재가 일자별 테스트 요청 - 종목코드: {request.FID_INPUT_ISCD}, 기간: {request.FID_PERIOD_DIV_CODE}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.daily_price_test(
                FID_COND_MRKT_DIV_CODE=request.FID_COND_MRKT_DIV_CODE,
                FID_INPUT_ISCD=request.FID_INPUT_ISCD,
                FID_PERIOD_DIV_CODE=request.FID_PERIOD_DIV_CODE,
                FID_ORG_ADJ_PRC=request.FID_ORG_ADJ_PRC
            )
            
            logger.info(f"KIS 주식현재가 일자별 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDailyPriceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식현재가 일자별 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_period_price(self, request: KISPeriodPriceRequest) -> KISPeriodPriceResponse:
        """국내주식기간별시세 API 테스트
        
        한국투자증권의 국내주식기간별시세 API (v1_국내주식-016)를 테스트합니다.
        국내주식의 일/주/월/년 단위 기간별 시세 정보를 조회합니다.
        
        Args:
            request: 국내주식기간별시세 요청 파라미터
            
        Returns:
            KISPeriodPriceResponse: 국내주식기간별시세 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 국내주식기간별시세 테스트 요청 - 종목코드: {request.FID_INPUT_ISCD}, 기간: {request.FID_PERIOD_DIV_CODE}, 날짜: {request.FID_INPUT_DATE_1} ~ {request.FID_INPUT_DATE_2}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.period_price_test(
                FID_COND_MRKT_DIV_CODE=request.FID_COND_MRKT_DIV_CODE,
                FID_INPUT_ISCD=request.FID_INPUT_ISCD,
                FID_INPUT_DATE_1=request.FID_INPUT_DATE_1,
                FID_INPUT_DATE_2=request.FID_INPUT_DATE_2,
                FID_PERIOD_DIV_CODE=request.FID_PERIOD_DIV_CODE,
                FID_ORG_ADJ_PRC=request.FID_ORG_ADJ_PRC
            )
            
            logger.info(f"KIS 국내주식기간별시세 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISPeriodPriceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 국내주식기간별시세 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_minute_price(self, request: KISOverseasMinuteRequest) -> KISOverseasMinuteResponse:
        """해외주식분봉조회 API 테스트
        
        한국투자증권의 해외주식분봉조회 API (v1_해외주식-030)를 테스트합니다.
        해외주식의 분 단위 시세 정보를 조회합니다.
        
        Args:
            request: 해외주식분봉조회 요청 파라미터
            
        Returns:
            KISOverseasMinuteResponse: 해외주식분봉조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식분봉조회 테스트 요청 - 종목코드: {request.SYMB}, 거래소: {request.EXCD}, 분갭: {request.NMIN}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_minute_price_test(
                AUTH=request.AUTH,
                EXCD=request.EXCD,
                SYMB=request.SYMB,
                NMIN=request.NMIN,
                PINC=request.PINC,
                NEXT=request.NEXT,
                NREC=request.NREC,
                FILL=request.FILL,
                KEYB=request.KEYB
            )
            
            logger.info(f"KIS 해외주식분봉조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasMinuteResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식분봉조회 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_minute_price(self, request: KISDomesticMinuteRequest) -> KISDomesticMinuteResponse:
        """주식일별분봉조회 API 테스트
        
        한국투자증권의 주식일별분봉조회 API (국내주식-213)를 테스트합니다.
        국내주식의 분 단위 시세 정보를 조회합니다.
        
        Args:
            request: 주식일별분봉조회 요청 파라미터
            
        Returns:
            KISDomesticMinuteResponse: 주식일별분봉조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식일별분봉조회 테스트 요청 - 종목코드: {request.FID_INPUT_ISCD}, 날짜: {request.FID_INPUT_DATE_1}, 시간: {request.FID_INPUT_HOUR_1}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_minute_price_test(
                FID_COND_MRKT_DIV_CODE=request.FID_COND_MRKT_DIV_CODE,
                FID_INPUT_ISCD=request.FID_INPUT_ISCD,
                FID_INPUT_HOUR_1=request.FID_INPUT_HOUR_1,
                FID_INPUT_DATE_1=request.FID_INPUT_DATE_1,
                FID_PW_DATA_INCU_YN=request.FID_PW_DATA_INCU_YN,
                FID_FAKE_TICK_INCU_YN=request.FID_FAKE_TICK_INCU_YN
            )
            
            logger.info(f"KIS 주식일별분봉조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticMinuteResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식일별분봉조회 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_current_price(self, request: KISCurrentPriceRequest) -> KISCurrentPriceResponse:
        """주식현재가 시세 API 테스트
        
        한국투자증권의 주식현재가 시세 API (v1_국내주식-008)를 테스트합니다.
        국내주식의 실시간 현재가와 상세 시세 정보를 조회합니다.
        
        Args:
            request: 주식현재가 시세 요청 파라미터
            
        Returns:
            KISCurrentPriceResponse: 주식현재가 시세 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식현재가 시세 테스트 요청 - 종목코드: {request.FID_INPUT_ISCD}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.current_price_test(
                FID_COND_MRKT_DIV_CODE=request.FID_COND_MRKT_DIV_CODE,
                FID_INPUT_ISCD=request.FID_INPUT_ISCD
            )
            
            logger.info(f"KIS 주식현재가 시세 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISCurrentPriceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식현재가 시세 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_order(self, request: KISDomesticOrderRequest, tr_count: Optional[str] = None, gt_uid: Optional[str] = None) -> KISDomesticOrderResponse:
        """주식일별주문체결조회 API 테스트
        
        한국투자증권의 주식일별주문체결조회 API (v1_국내주식-005)를 테스트합니다.
        국내주식 계좌의 일별 주문체결 내역을 조회합니다.
        
        Args:
            request: 주식일별주문체결조회 요청 파라미터
            tr_count: tr_count 헤더 값 (연속조회 시 사용)
            gt_uid: Global UID 헤더 값 (법인 전용, 거래고유번호)
            
        Returns:
            KISDomesticOrderResponse: 주식일별주문체결조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식일별주문체결조회 테스트 요청 - CANO: {request.CANO}, 조회기간: {request.INQR_STRT_DT} ~ {request.INQR_END_DT}, tr_count: {tr_count}, gt_uid: {gt_uid}")
            
            # extra_headers 구성
            extra_headers = {}
            if tr_count is not None:
                extra_headers["tr_count"] = tr_count
            if gt_uid is not None:
                extra_headers["gt_uid"] = gt_uid
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_order_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                INQR_STRT_DT=request.INQR_STRT_DT,
                INQR_END_DT=request.INQR_END_DT,
                SLL_BUY_DVSN_CD=request.SLL_BUY_DVSN_CD,
                PDNO=request.PDNO,
                ORD_GNO_BRNO=request.ORD_GNO_BRNO,
                ODNO=request.ODNO,
                CCLD_DVSN=request.CCLD_DVSN,
                INQR_DVSN=request.INQR_DVSN,
                INQR_DVSN_1=request.INQR_DVSN_1,
                INQR_DVSN_3=request.INQR_DVSN_3,
                EXCG_ID_DVSN_CD=request.EXCG_ID_DVSN_CD,
                CTX_AREA_FK100=request.CTX_AREA_FK100,
                CTX_AREA_NK100=request.CTX_AREA_NK100,
                extra_headers=extra_headers if extra_headers else None
            )
            
            logger.info(f"KIS 주식일별주문체결조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticOrderResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                ctx_area_fk100=result.get("ctx_area_fk100", ""),
                ctx_area_nk100=result.get("ctx_area_nk100", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식일별주문체결조회 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_order_revise(self, request: KISDomesticOrderReviseRequest) -> KISDomesticOrderReviseResponse:
        """주식주문(정정취소) API 테스트
        
        한국투자증권의 주식주문(정정취소) API (v1_국내주식-003)를 테스트합니다.
        기존 주문을 정정하거나 취소합니다.
        
        Args:
            request: 주식주문(정정취소) 요청 파라미터
            
        Returns:
            KISDomesticOrderReviseResponse: 주식주문(정정취소) 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 주식주문(정정취소) 테스트 요청 - CANO: {request.CANO}, 원주문번호: {request.ORGN_ODNO}, 구분: {request.RVSE_CNCL_DVSN_CD}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_order_revise_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                KRX_FWDG_ORD_ORGNO=request.KRX_FWDG_ORD_ORGNO,
                ORGN_ODNO=request.ORGN_ODNO,
                ORD_DVSN=request.ORD_DVSN,
                RVSE_CNCL_DVSN_CD=request.RVSE_CNCL_DVSN_CD,
                ORD_QTY=request.ORD_QTY,
                ORD_UNPR=request.ORD_UNPR,
                QTY_ALL_ORD_YN=request.QTY_ALL_ORD_YN,
                CNDT_PRIC=request.CNDT_PRIC,
                EXCG_ID_DVSN_CD=request.EXCG_ID_DVSN_CD
            )
            
            logger.info(f"KIS 주식주문(정정취소) 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticOrderReviseResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 주식주문(정정취소) API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_order_revise(self, request: KISOverseasOrderReviseRequest) -> KISOverseasOrderReviseResponse:
        """해외주식 정정취소주문 API 테스트
        
        한국투자증권의 해외주식 정정취소주문 API (v1_해외주식-003)를 테스트합니다.
        기존 해외주식 주문을 정정하거나 취소합니다.
        
        Args:
            request: 해외주식 정정취소주문 요청 파라미터
            
        Returns:
            KISOverseasOrderReviseResponse: 해외주식 정정취소주문 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 정정취소주문 테스트 요청 - CANO: {request.CANO}, 거래소: {request.OVRS_EXCG_CD}, 원주문번호: {request.ORGN_ODNO}, 구분: {request.RVSE_CNCL_DVSN_CD}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_order_revise_test(
                CANO=request.CANO,
                ACNT_PRDT_CD=request.ACNT_PRDT_CD,
                OVRS_EXCG_CD=request.OVRS_EXCG_CD,
                PDNO=request.PDNO,
                ORGN_ODNO=request.ORGN_ODNO,
                RVSE_CNCL_DVSN_CD=request.RVSE_CNCL_DVSN_CD,
                ORD_QTY=request.ORD_QTY,
                OVRS_ORD_UNPR=request.OVRS_ORD_UNPR,
                MGCO_APTM_ODNO=request.MGCO_APTM_ODNO,
                ORD_SVR_DVSN_CD=request.ORD_SVR_DVSN_CD
            )
            
            logger.info(f"KIS 해외주식 정정취소주문 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasOrderReviseResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 정정취소주문 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_period_price(self, request: KISOverseasPeriodPriceRequest) -> KISOverseasPeriodPriceResponse:
        """해외주식 기간별시세 API 테스트
        
        한국투자증권의 해외주식 기간별시세 API (v1_해외주식-010)를 테스트합니다.
        해외주식의 일/주/월 단위 시세 정보를 조회합니다.
        
        Args:
            request: 해외주식 기간별시세 요청 파라미터
            
        Returns:
            KISOverseasPeriodPriceResponse: 해외주식 기간별시세 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 기간별시세 테스트 요청 - 종목코드: {request.SYMB}, 거래소: {request.EXCD}, 구분: {request.GUBN}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_period_price_test(
                AUTH=request.AUTH,
                EXCD=request.EXCD,
                SYMB=request.SYMB,
                GUBN=request.GUBN,
                BYMD=request.BYMD,
                MODP=request.MODP,
                KEYB=request.KEYB
            )
            
            logger.info(f"KIS 해외주식 기간별시세 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasPeriodPriceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output1=result.get("output1"),  # KIS API 원본 데이터 그대로 전달
                output2=result.get("output2"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 기간별시세 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_current_price(self, request: KISOverseasCurrentPriceRequest) -> KISOverseasCurrentPriceResponse:
        """해외주식 현재가상세 API 테스트
        
        한국투자증권의 해외주식 현재가상세 API (v1_해외주식-029)를 테스트합니다.
        해외주식의 실시간 현재가와 상세 정보를 조회합니다.
        
        Args:
            request: 해외주식 현재가상세 요청 파라미터
            
        Returns:
            KISOverseasCurrentPriceResponse: 해외주식 현재가상세 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외주식 현재가상세 테스트 요청 - 종목코드: {request.SYMB}, 거래소: {request.EXCD}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_current_price_test(
                AUTH=request.AUTH,
                EXCD=request.EXCD,
                SYMB=request.SYMB
            )
            
            logger.info(f"KIS 해외주식 현재가상세 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasCurrentPriceResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외주식 현재가상세 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_overseas_news(self, request: KISOverseasNewsRequest) -> KISOverseasNewsResponse:
        """해외뉴스종합(제목) API 테스트
        
        한국투자증권의 해외뉴스종합(제목) API (해외주식-053)를 테스트합니다.
        해외주식 관련 뉴스 제목을 조회합니다.
        
        Args:
            request: 해외뉴스종합 요청 파라미터
            
        Returns:
            KISOverseasNewsResponse: 해외뉴스종합 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 해외뉴스종합 테스트 요청 - 종목코드: {request.SYMB}, 국가코드: {request.NATION_CD}, 조회일자: {request.DATA_DT}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.overseas_news_test(
                INFO_GB=request.INFO_GB,
                CLASS_CD=request.CLASS_CD,
                NATION_CD=request.NATION_CD,
                EXCHANGE_CD=request.EXCHANGE_CD,
                SYMB=request.SYMB,
                DATA_DT=request.DATA_DT,
                DATA_TM=request.DATA_TM,
                CTS=request.CTS
            )
            
            logger.info(f"KIS 해외뉴스종합 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISOverseasNewsResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                outblock1=result.get("outblock1"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 해외뉴스종합 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_news(self, request: KISDomesticNewsRequest) -> KISDomesticNewsResponse:
        """종합 시황/공시(제목) API 테스트
        
        한국투자증권의 종합 시황/공시(제목) API (국내주식-141)를 테스트합니다.
        국내주식 관련 뉴스 및 공시 제목을 조회합니다.
        
        Args:
            request: 종합 시황/공시 요청 파라미터
            
        Returns:
            KISDomesticNewsResponse: 종합 시황/공시 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 종합 시황/공시 테스트 요청 - 종목코드: {request.FID_INPUT_ISCD}, 조회일자: {request.FID_INPUT_DATE_1}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_news_test(
                FID_NEWS_OFER_ENTP_CODE=request.FID_NEWS_OFER_ENTP_CODE,
                FID_COND_MRKT_CLS_CODE=request.FID_COND_MRKT_CLS_CODE,
                FID_INPUT_ISCD=request.FID_INPUT_ISCD,
                FID_TITL_CNTT=request.FID_TITL_CNTT,
                FID_INPUT_DATE_1=request.FID_INPUT_DATE_1,
                FID_INPUT_HOUR_1=request.FID_INPUT_HOUR_1,
                FID_RANK_SORT_CLS_CODE=request.FID_RANK_SORT_CLS_CODE,
                FID_INPUT_SRNO=request.FID_INPUT_SRNO
            )
            
            logger.info(f"KIS 종합 시황/공시 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticNewsResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 종합 시황/공시 API 테스트 중 오류 발생: {str(e)}")
            raise

    async def test_domestic_holiday(self, request: KISDomesticHolidayRequest) -> KISDomesticHolidayResponse:
        """국내휴장일조회 API 테스트
        
        한국투자증권의 국내휴장일조회 API (국내주식-040)를 테스트합니다.
        영업일, 거래일, 개장일, 결제일 여부를 조회할 수 있습니다.
        
        Args:
            request: 국내휴장일조회 요청 파라미터
            
        Returns:
            KISDomesticHolidayResponse: 국내휴장일조회 응답 데이터
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS 국내휴장일조회 테스트 요청 - 기준일자: {request.BASS_DT}")
            
            # KIS 클라이언트를 통해 API 호출
            result = self.kis_client.domestic_holiday_check(
                bass_dt=request.BASS_DT
            )
            
            logger.info(f"KIS 국내휴장일조회 응답 완료 - rt_cd: {result.get('rt_cd', 'unknown')}")
            
            return KISDomesticHolidayResponse(
                rt_cd=result.get("rt_cd", ""),
                msg_cd=result.get("msg_cd", ""),
                msg1=result.get("msg1", ""),
                output=result.get("output"),  # KIS API 원본 데이터 그대로 전달
                raw_response=result
            )
            
        except Exception as e:
            logger.error(f"KIS 국내휴장일조회 API 테스트 중 오류 발생: {str(e)}")
            raise
    
    async def test_kis_api(self, request: KISAPITestRequest) -> KISAPITestResponse:
        """범용 KIS API 테스트
        
        지원하는 KIS API를 범용적으로 테스트할 수 있는 메서드입니다.
        API 이름과 파라미터를 지정하여 다양한 KIS API를 테스트할 수 있습니다.
        
        Args:
            request: KIS API 테스트 요청 (API 이름, 환경, 파라미터)
            
        Returns:
            KISAPITestResponse: API 테스트 결과
            
        Raises:
            ValueError: 지원하지 않는 API 요청시
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"KIS API 테스트 요청 - API: {request.api_name}, 환경: {request.environment}")
            
            # API별 분기 처리
            if request.api_name == "balance_inquiry_rlz_pl":
                # 주식잔고조회_실현손익 테스트
                balance_request = KISBalanceInquiryRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    AFHR_FLPR_YN=request.parameters.get("AFHR_FLPR_YN", "N"),
                    OFL_YN=request.parameters.get("OFL_YN", ""),
                    INQR_DVSN=request.parameters.get("INQR_DVSN", "00"),
                    UNPR_DVSN=request.parameters.get("UNPR_DVSN", "01"),
                    FUND_STTL_ICLD_YN=request.parameters.get("FUND_STTL_ICLD_YN", "N"),
                    FNCG_AMT_AUTO_RDPT_YN=request.parameters.get("FNCG_AMT_AUTO_RDPT_YN", "N"),
                    PRCS_DVSN=request.parameters.get("PRCS_DVSN", "00"),
                    COST_ICLD_YN=request.parameters.get("COST_ICLD_YN", ""),
                    CTX_AREA_FK100=request.parameters.get("CTX_AREA_FK100", ""),
                    CTX_AREA_NK100=request.parameters.get("CTX_AREA_NK100", "")
                )
                
                response = await self.test_balance_inquiry_rlz_pl(balance_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,
                        "output2": response.output2,  # KIS API가 list 또는 dict를 반환할 수 있음
                        "ctx_area_fk100": response.ctx_area_fk100,
                        "ctx_area_nk100": response.ctx_area_nk100
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_present_balance":
                # 해외주식 체결기준현재잔고 테스트
                overseas_request = KISOverseasBalanceRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    WCRC_FRCR_DVSN_CD=request.parameters.get("WCRC_FRCR_DVSN_CD", "01"),
                    NATN_CD=request.parameters.get("NATN_CD", "000"),
                    TR_MKET_CD=request.parameters.get("TR_MKET_CD", "00"),
                    INQR_DVSN_CD=request.parameters.get("INQR_DVSN_CD", "00")
                )
                
                response = await self.test_overseas_present_balance(overseas_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "output3": response.output3,  # KIS API 원본 데이터 그대로 전달
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_order_history":
                # 해외주식 주문체결내역 테스트
                overseas_order_request = KISOverseasOrderRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    PDNO=request.parameters.get("PDNO", "%"),
                    ORD_STRT_DT=request.parameters.get("ORD_STRT_DT", ""),
                    ORD_END_DT=request.parameters.get("ORD_END_DT", ""),
                    SLL_BUY_DVSN=request.parameters.get("SLL_BUY_DVSN", "00"),
                    CCLD_NCCS_DVSN=request.parameters.get("CCLD_NCCS_DVSN", "00"),
                    OVRS_EXCG_CD=request.parameters.get("OVRS_EXCG_CD", "%"),
                    SORT_SQN=request.parameters.get("SORT_SQN", "DS"),
                    ORD_DT=request.parameters.get("ORD_DT", ""),
                    ORD_GNO_BRNO=request.parameters.get("ORD_GNO_BRNO", ""),
                    ODNO=request.parameters.get("ODNO", ""),
                    CTX_AREA_NK200=request.parameters.get("CTX_AREA_NK200", ""),
                    CTX_AREA_FK200=request.parameters.get("CTX_AREA_FK200", "")
                )
                
                response = await self.test_overseas_order_history(overseas_order_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "ctx_area_fk200": response.ctx_area_fk200,
                        "ctx_area_nk200": response.ctx_area_nk200,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_profit":
                # 해외주식 기간손익 테스트
                overseas_profit_request = KISOverseasProfitRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    OVRS_EXCG_CD=request.parameters.get("OVRS_EXCG_CD", ""),
                    NATN_CD=request.parameters.get("NATN_CD", ""),
                    CRCY_CD=request.parameters.get("CRCY_CD", ""),
                    PDNO=request.parameters.get("PDNO", ""),
                    INQR_STRT_DT=request.parameters.get("INQR_STRT_DT", ""),
                    INQR_END_DT=request.parameters.get("INQR_END_DT", ""),
                    WCRC_FRCR_DVSN_CD=request.parameters.get("WCRC_FRCR_DVSN_CD", "01"),
                    CTX_AREA_FK200=request.parameters.get("CTX_AREA_FK200", ""),
                    CTX_AREA_NK200=request.parameters.get("CTX_AREA_NK200", "")
                )
                
                response = await self.test_overseas_profit(overseas_profit_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "domestic_profit":
                # 기간별매매손익현황조회 테스트
                domestic_profit_request = KISDomesticProfitRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    SORT_DVSN=request.parameters.get("SORT_DVSN", "00"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    PDNO=request.parameters.get("PDNO", ""),
                    INQR_STRT_DT=request.parameters.get("INQR_STRT_DT", ""),
                    INQR_END_DT=request.parameters.get("INQR_END_DT", ""),
                    CTX_AREA_NK100=request.parameters.get("CTX_AREA_NK100", ""),
                    CBLC_DVSN=request.parameters.get("CBLC_DVSN", "00"),
                    CTX_AREA_FK100=request.parameters.get("CTX_AREA_FK100", "")
                )
                
                response = await self.test_domestic_profit(domestic_profit_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "ctx_area_nk100": response.ctx_area_nk100,
                        "ctx_area_fk100": response.ctx_area_fk100,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "buy_possible":
                # 매수가능조회 테스트
                buy_possible_request = KISBuyPossibleRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    PDNO=request.parameters.get("PDNO", ""),
                    ORD_UNPR=request.parameters.get("ORD_UNPR", ""),
                    ORD_DVSN=request.parameters.get("ORD_DVSN", "01"),
                    CMA_EVLU_AMT_ICLD_YN=request.parameters.get("CMA_EVLU_AMT_ICLD_YN", "N"),
                    OVRS_ICLD_YN=request.parameters.get("OVRS_ICLD_YN", "N")
                )
                
                response = await self.test_buy_possible(buy_possible_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "daily_price":
                # 주식현재가 일자별 테스트
                daily_price_request = KISDailyPriceRequest(
                    FID_COND_MRKT_DIV_CODE=request.parameters.get("FID_COND_MRKT_DIV_CODE", "J"),
                    FID_INPUT_ISCD=request.parameters.get("FID_INPUT_ISCD", "005930"),
                    FID_PERIOD_DIV_CODE=request.parameters.get("FID_PERIOD_DIV_CODE", "D"),
                    FID_ORG_ADJ_PRC=request.parameters.get("FID_ORG_ADJ_PRC", "1")
                )
                
                response = await self.test_daily_price(daily_price_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "period_price":
                # 국내주식기간별시세 테스트
                period_price_request = KISPeriodPriceRequest(
                    FID_COND_MRKT_DIV_CODE=request.parameters.get("FID_COND_MRKT_DIV_CODE", "J"),
                    FID_INPUT_ISCD=request.parameters.get("FID_INPUT_ISCD", "005930"),
                    FID_INPUT_DATE_1=request.parameters.get("FID_INPUT_DATE_1", ""),
                    FID_INPUT_DATE_2=request.parameters.get("FID_INPUT_DATE_2", ""),
                    FID_PERIOD_DIV_CODE=request.parameters.get("FID_PERIOD_DIV_CODE", "D"),
                    FID_ORG_ADJ_PRC=request.parameters.get("FID_ORG_ADJ_PRC", "0")
                )
                
                response = await self.test_period_price(period_price_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_minute_price":
                # 해외주식분봉조회 테스트
                overseas_minute_request = KISOverseasMinuteRequest(
                    AUTH=request.parameters.get("AUTH", ""),
                    EXCD=request.parameters.get("EXCD", "NAS"),
                    SYMB=request.parameters.get("SYMB", "AAPL"),
                    NMIN=request.parameters.get("NMIN", "1"),
                    PINC=request.parameters.get("PINC", "0"),
                    NEXT=request.parameters.get("NEXT", ""),
                    NREC=request.parameters.get("NREC", "120"),
                    FILL=request.parameters.get("FILL", ""),
                    KEYB=request.parameters.get("KEYB", "")
                )
                
                response = await self.test_overseas_minute_price(overseas_minute_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "domestic_minute_price":
                # 주식일별분봉조회 테스트
                domestic_minute_request = KISDomesticMinuteRequest(
                    FID_COND_MRKT_DIV_CODE=request.parameters.get("FID_COND_MRKT_DIV_CODE", "J"),
                    FID_INPUT_ISCD=request.parameters.get("FID_INPUT_ISCD", "005930"),
                    FID_INPUT_HOUR_1=request.parameters.get("FID_INPUT_HOUR_1", ""),
                    FID_INPUT_DATE_1=request.parameters.get("FID_INPUT_DATE_1", ""),
                    FID_PW_DATA_INCU_YN=request.parameters.get("FID_PW_DATA_INCU_YN", "Y"),
                    FID_FAKE_TICK_INCU_YN=request.parameters.get("FID_FAKE_TICK_INCU_YN", "")
                )
                
                response = await self.test_domestic_minute_price(domestic_minute_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "current_price":
                # 주식현재가 시세 테스트
                current_price_request = KISCurrentPriceRequest(
                    FID_COND_MRKT_DIV_CODE=request.parameters.get("FID_COND_MRKT_DIV_CODE", "J"),
                    FID_INPUT_ISCD=request.parameters.get("FID_INPUT_ISCD", "005930")
                )
                
                response = await self.test_current_price(current_price_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "domestic_order":
                # 주식일별주문체결조회 테스트
                domestic_order_request = KISDomesticOrderRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    INQR_STRT_DT=request.parameters.get("INQR_STRT_DT", ""),
                    INQR_END_DT=request.parameters.get("INQR_END_DT", ""),
                    SLL_BUY_DVSN_CD=request.parameters.get("SLL_BUY_DVSN_CD", "00"),
                    PDNO=request.parameters.get("PDNO", ""),
                    ORD_GNO_BRNO=request.parameters.get("ORD_GNO_BRNO", ""),
                    ODNO=request.parameters.get("ODNO", ""),
                    CCLD_DVSN=request.parameters.get("CCLD_DVSN", "00"),
                    INQR_DVSN=request.parameters.get("INQR_DVSN", "00"),
                    INQR_DVSN_1=request.parameters.get("INQR_DVSN_1", ""),
                    INQR_DVSN_3=request.parameters.get("INQR_DVSN_3", "00"),
                    EXCG_ID_DVSN_CD=request.parameters.get("EXCG_ID_DVSN_CD", "KRX"),
                    CTX_AREA_FK100=request.parameters.get("CTX_AREA_FK100", ""),
                    CTX_AREA_NK100=request.parameters.get("CTX_AREA_NK100", "")
                )
                
                response = await self.test_domestic_order(domestic_order_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "ctx_area_fk100": response.ctx_area_fk100,
                        "ctx_area_nk100": response.ctx_area_nk100,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "domestic_order_revise":
                # 주식주문(정정취소) 테스트
                domestic_order_revise_request = KISDomesticOrderReviseRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    KRX_FWDG_ORD_ORGNO=request.parameters.get("KRX_FWDG_ORD_ORGNO", ""),
                    ORGN_ODNO=request.parameters.get("ORGN_ODNO", ""),
                    ORD_DVSN=request.parameters.get("ORD_DVSN", "00"),
                    RVSE_CNCL_DVSN_CD=request.parameters.get("RVSE_CNCL_DVSN_CD", "02"),
                    ORD_QTY=request.parameters.get("ORD_QTY", "0"),
                    ORD_UNPR=request.parameters.get("ORD_UNPR", "0"),
                    QTY_ALL_ORD_YN=request.parameters.get("QTY_ALL_ORD_YN", "Y"),
                    CNDT_PRIC=request.parameters.get("CNDT_PRIC", ""),
                    EXCG_ID_DVSN_CD=request.parameters.get("EXCG_ID_DVSN_CD", "KRX")
                )
                
                response = await self.test_domestic_order_revise(domestic_order_revise_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_order_revise":
                # 해외주식 정정취소주문 테스트
                overseas_order_revise_request = KISOverseasOrderReviseRequest(
                    CANO=request.parameters.get("CANO", "00000000"),
                    ACNT_PRDT_CD=request.parameters.get("ACNT_PRDT_CD", "01"),
                    OVRS_EXCG_CD=request.parameters.get("OVRS_EXCG_CD", "NASD"),
                    PDNO=request.parameters.get("PDNO", ""),
                    ORGN_ODNO=request.parameters.get("ORGN_ODNO", ""),
                    RVSE_CNCL_DVSN_CD=request.parameters.get("RVSE_CNCL_DVSN_CD", "02"),
                    ORD_QTY=request.parameters.get("ORD_QTY", "0"),
                    OVRS_ORD_UNPR=request.parameters.get("OVRS_ORD_UNPR", "0"),
                    MGCO_APTM_ODNO=request.parameters.get("MGCO_APTM_ODNO", ""),
                    ORD_SVR_DVSN_CD=request.parameters.get("ORD_SVR_DVSN_CD", "0")
                )
                
                response = await self.test_overseas_order_revise(overseas_order_revise_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_period_price":
                # 해외주식 기간별시세 테스트
                overseas_period_price_request = KISOverseasPeriodPriceRequest(
                    AUTH=request.parameters.get("AUTH", ""),
                    EXCD=request.parameters.get("EXCD", "NAS"),
                    SYMB=request.parameters.get("SYMB", "AAPL"),
                    GUBN=request.parameters.get("GUBN", "0"),
                    BYMD=request.parameters.get("BYMD", ""),
                    MODP=request.parameters.get("MODP", "0"),
                    KEYB=request.parameters.get("KEYB", "")
                )
                
                response = await self.test_overseas_period_price(overseas_period_price_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output1": response.output1,  # KIS API 원본 데이터 그대로 전달
                        "output2": response.output2,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_current_price":
                # 해외주식 현재가상세 테스트
                overseas_current_price_request = KISOverseasCurrentPriceRequest(
                    AUTH=request.parameters.get("AUTH", ""),
                    EXCD=request.parameters.get("EXCD", "NAS"),
                    SYMB=request.parameters.get("SYMB", "AAPL")
                )
                
                response = await self.test_overseas_current_price(overseas_current_price_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "overseas_news":
                # 해외뉴스종합 테스트
                overseas_news_request = KISOverseasNewsRequest(
                    INFO_GB=request.parameters.get("INFO_GB", ""),
                    CLASS_CD=request.parameters.get("CLASS_CD", ""),
                    NATION_CD=request.parameters.get("NATION_CD", ""),
                    EXCHANGE_CD=request.parameters.get("EXCHANGE_CD", ""),
                    SYMB=request.parameters.get("SYMB", ""),
                    DATA_DT=request.parameters.get("DATA_DT", ""),
                    DATA_TM=request.parameters.get("DATA_TM", ""),
                    CTS=request.parameters.get("CTS", "")
                )
                
                response = await self.test_overseas_news(overseas_news_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "outblock1": response.outblock1,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            elif request.api_name == "domestic_news":
                # 종합 시황/공시 테스트
                domestic_news_request = KISDomesticNewsRequest(
                    FID_NEWS_OFER_ENTP_CODE=request.parameters.get("FID_NEWS_OFER_ENTP_CODE", ""),
                    FID_COND_MRKT_CLS_CODE=request.parameters.get("FID_COND_MRKT_CLS_CODE", ""),
                    FID_INPUT_ISCD=request.parameters.get("FID_INPUT_ISCD", ""),
                    FID_TITL_CNTT=request.parameters.get("FID_TITL_CNTT", ""),
                    FID_INPUT_DATE_1=request.parameters.get("FID_INPUT_DATE_1", ""),
                    FID_INPUT_HOUR_1=request.parameters.get("FID_INPUT_HOUR_1", ""),
                    FID_RANK_SORT_CLS_CODE=request.parameters.get("FID_RANK_SORT_CLS_CODE", ""),
                    FID_INPUT_SRNO=request.parameters.get("FID_INPUT_SRNO", "")
                )
                
                response = await self.test_domestic_news(domestic_news_request)
                
                return KISAPITestResponse(
                    api_name=request.api_name,
                    environment=request.environment,
                    success=response.rt_cd == "0",
                    response_data={
                        "rt_cd": response.rt_cd,
                        "msg_cd": response.msg_cd,
                        "msg1": response.msg1,
                        "output": response.output,  # KIS API 원본 데이터 그대로 전달
                        "raw_response": response.raw_response
                    },
                    error=None if response.rt_cd == "0" else response.msg1,
                    raw_response=response.raw_response
                )
            
            else:
                raise ValueError(f"지원하지 않는 API입니다: {request.api_name}")
            
        except Exception as e:
            logger.error(f"KIS API 테스트 중 오류 발생: {str(e)}")
            return KISAPITestResponse(
                api_name=request.api_name,
                environment=request.environment,
                success=False,
                response_data=None,
                error=str(e),
                raw_response=None
            )
    
    def get_supported_apis(self) -> Dict[str, Any]:
        """지원하는 KIS API 목록 반환
        
        현재 지원하는 모든 KIS API의 정보를 반환합니다.
        각 API의 이름, 설명, 지원 환경, 파라미터 정보를 포함합니다.
        
        Returns:
            Dict[str, Any]: 지원 API 목록과 상세 정보
        """
        return {
            "apis": [
                {
                    "name": "balance_inquiry_rlz_pl",
                    "display_name": "주식잔고조회_실현손익",
                    "description": "주식잔고조회_실현손익 API (v1_국내주식-041)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "AFHR_FLPR_YN": "시간외단일가여부 (기본값: N)",
                        "OFL_YN": "오프라인여부 (기본값: 공란)",
                        "INQR_DVSN": "조회구분 (기본값: 00 - 전체)",
                        "UNPR_DVSN": "단가구분 (기본값: 01)",
                        "FUND_STTL_ICLD_YN": "펀드결제포함여부 (기본값: N)",
                        "FNCG_AMT_AUTO_RDPT_YN": "융자금액자동상환여부 (기본값: N)",
                        "PRCS_DVSN": "PRCS_DVSN (기본값: 00 - 전일매매포함)",
                        "COST_ICLD_YN": "비용포함여부 (기본값: 공란)",
                        "CTX_AREA_FK100": "연속조회검색조건100 (기본값: 공란)",
                        "CTX_AREA_NK100": "연속조회키100 (기본값: 공란)"
                    }
                },
                {
                    "name": "overseas_present_balance",
                    "display_name": "해외주식 체결기준현재잔고",
                    "description": "해외주식 체결기준현재잔고 API (v1_해외주식-008)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "WCRC_FRCR_DVSN_CD": "원화외화구분코드 (기본값: 01 - 원화)",
                        "NATN_CD": "국가코드 (기본값: 000 - 전체)",
                        "TR_MKET_CD": "거래시장코드 (기본값: 00 - 전체)",
                        "INQR_DVSN_CD": "조회구분코드 (기본값: 00 - 전체)"
                    }
                },
                {
                    "name": "overseas_order_history",
                    "display_name": "해외주식 주문체결내역",
                    "description": "해외주식 주문체결내역 API (v1_해외주식-007)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "PDNO": "상품번호 (기본값: % - 전종목)",
                        "ORD_STRT_DT": "주문시작일자 (기본값: 하루전 YYYYMMDD)",
                        "ORD_END_DT": "주문종료일자 (기본값: 오늘 YYYYMMDD)",
                        "SLL_BUY_DVSN": "매도매수구분 (기본값: 00 - 전체)",
                        "CCLD_NCCS_DVSN": "체결미체결구분 (기본값: 00 - 전체)",
                        "OVRS_EXCG_CD": "해외거래소코드 (기본값: % - 전종목)",
                        "SORT_SQN": "정렬순서 (기본값: DS - 정순)",
                        "ORD_DT": "주문일자 (기본값: 공란)",
                        "ORD_GNO_BRNO": "주문채번지점번호 (기본값: 공란)",
                        "ODNO": "주문번호 (기본값: 공란)",
                        "CTX_AREA_NK200": "연속조회키200 (기본값: 공란)",
                        "CTX_AREA_FK200": "연속조회검색조건200 (기본값: 공란)"
                    }
                },
                {
                    "name": "overseas_profit",
                    "display_name": "해외주식 기간손익",
                    "description": "해외주식 기간손익 API (v1_해외주식-032)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "OVRS_EXCG_CD": "해외거래소코드 (기본값: 공란 - 전체)",
                        "NATN_CD": "국가코드 (기본값: 공란 - Default)",
                        "CRCY_CD": "통화코드 (기본값: 공란 - 전체)",
                        "PDNO": "상품번호 (기본값: 공란 - 전체)",
                        "INQR_STRT_DT": "조회시작일자 (기본값: 하루전 YYYYMMDD)",
                        "INQR_END_DT": "조회종료일자 (기본값: 오늘 YYYYMMDD)",
                        "WCRC_FRCR_DVSN_CD": "원화외화구분코드 (기본값: 01 - 외화)",
                        "CTX_AREA_FK200": "연속조회검색조건200 (기본값: 공란)",
                        "CTX_AREA_NK200": "연속조회키200 (기본값: 공란)"
                    }
                },
                {
                    "name": "domestic_profit",
                    "display_name": "기간별매매손익현황조회",
                    "description": "기간별매매손익현황조회 API (v1_국내주식-060)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "SORT_DVSN": "정렬구분 (기본값: 00 - 최근 순)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "PDNO": "상품번호 (기본값: 공란 - 전체)",
                        "INQR_STRT_DT": "조회시작일자 (기본값: 하루전 YYYYMMDD)",
                        "INQR_END_DT": "조회종료일자 (기본값: 오늘 YYYYMMDD)",
                        "CTX_AREA_NK100": "연속조회키100 (기본값: 공란)",
                        "CBLC_DVSN": "잔고구분 (기본값: 00 - 전체)",
                        "CTX_AREA_FK100": "연속조회검색조건100 (기본값: 공란)"
                    }
                },
                {
                    "name": "buy_possible",
                    "display_name": "매수가능조회",
                    "description": "매수가능조회 API (v1_국내주식-007)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "PDNO": "상품번호 (기본값: 공란 - 종목번호 6자리)",
                        "ORD_UNPR": "주문단가 (기본값: 공란 - 1주당 가격)",
                        "ORD_DVSN": "주문구분 (기본값: 01 - 시장가)",
                        "CMA_EVLU_AMT_ICLD_YN": "CMA평가금액포함여부 (기본값: N)",
                        "OVRS_ICLD_YN": "해외포함여부 (기본값: N)"
                    }
                },
                {
                    "name": "daily_price",
                    "display_name": "주식현재가 일자별",
                    "description": "주식현재가 일자별 API (v1_국내주식-010)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "FID_COND_MRKT_DIV_CODE": "조건 시장 분류 코드 (기본값: J - KRX)",
                        "FID_INPUT_ISCD": "입력 종목코드 (기본값: 005930)",
                        "FID_PERIOD_DIV_CODE": "기간 분류 코드 (기본값: D - 일)",
                        "FID_ORG_ADJ_PRC": "수정주가 원주가 가격 (기본값: 1 - 수정주가반영)"
                    }
                },
                {
                    "name": "period_price",
                    "display_name": "국내주식기간별시세",
                    "description": "국내주식기간별시세 API (v1_국내주식-016)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "FID_COND_MRKT_DIV_CODE": "조건 시장 분류 코드 (기본값: J - KRX)",
                        "FID_INPUT_ISCD": "입력 종목코드 (기본값: 005930)",
                        "FID_INPUT_DATE_1": "입력 날짜 1 (기본값: 30일 전 YYYYMMDD)",
                        "FID_INPUT_DATE_2": "입력 날짜 2 (기본값: 오늘 YYYYMMDD, 최대 100개)",
                        "FID_PERIOD_DIV_CODE": "기간분류코드 (기본값: D - 일봉)",
                        "FID_ORG_ADJ_PRC": "수정주가 원주가 가격 여부 (기본값: 0 - 수정주가)"
                    }
                },
                {
                    "name": "current_price",
                    "display_name": "주식현재가 시세",
                    "description": "주식현재가 시세 API (v1_국내주식-008)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "FID_COND_MRKT_DIV_CODE": "조건 시장 분류 코드 (기본값: J - KRX)",
                        "FID_INPUT_ISCD": "입력 종목코드 (기본값: 005930)"
                    }
                },
                {
                    "name": "domestic_order",
                    "display_name": "주식일별주문체결조회",
                    "description": "주식일별주문체결조회 API (v1_국내주식-005)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "INQR_STRT_DT": "조회시작일자 (기본값: 하루전 YYYYMMDD)",
                        "INQR_END_DT": "조회종료일자 (기본값: 오늘 YYYYMMDD)",
                        "SLL_BUY_DVSN_CD": "매도매수구분코드 (기본값: 00 - 전체)",
                        "PDNO": "상품번호 (기본값: 공란 - 전체)",
                        "ORD_GNO_BRNO": "주문채번지점번호 (기본값: 공란)",
                        "ODNO": "주문번호 (기본값: 공란)",
                        "CCLD_DVSN": "체결구분 (기본값: 00 - 전체)",
                        "INQR_DVSN": "조회구분 (기본값: 00 - 역순)",
                        "INQR_DVSN_1": "조회구분1 (기본값: 공란 - 전체)",
                        "INQR_DVSN_3": "조회구분3 (기본값: 00 - 전체)",
                        "EXCG_ID_DVSN_CD": "거래소ID구분코드 (기본값: KRX)",
                        "CTX_AREA_FK100": "연속조회검색조건100 (기본값: 공란)",
                        "CTX_AREA_NK100": "연속조회키100 (기본값: 공란)"
                    }
                },
                {
                    "name": "domestic_minute_price",
                    "display_name": "주식일별분봉조회",
                    "description": "주식일별분봉조회 API (국내주식-213)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "FID_COND_MRKT_DIV_CODE": "조건 시장 분류 코드 (기본값: J - KRX)",
                        "FID_INPUT_ISCD": "입력 종목코드 (기본값: 005930)",
                        "FID_INPUT_HOUR_1": "입력 시간1 (기본값: 공백 - 현재시간)",
                        "FID_INPUT_DATE_1": "입력 날짜1 (기본값: 오늘 YYYYMMDD)",
                        "FID_PW_DATA_INCU_YN": "과거 데이터 포함 여부 (기본값: Y)",
                        "FID_FAKE_TICK_INCU_YN": "허봉 포함 여부 (기본값: 공백)"
                    }
                },
                {
                    "name": "domestic_order_revise",
                    "display_name": "주식주문(정정취소)",
                    "description": "주식주문(정정취소) API (v1_국내주식-003)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "KRX_FWDG_ORD_ORGNO": "한국거래소전송주문조직번호 (기본값: 공란)",
                        "ORGN_ODNO": "원주문번호 (기본값: 공란)",
                        "ORD_DVSN": "주문구분 (기본값: 00 - 지정가)",
                        "RVSE_CNCL_DVSN_CD": "정정취소구분코드 (기본값: 02 - 취소)",
                        "ORD_QTY": "주문수량 (기본값: 0)",
                        "ORD_UNPR": "주문단가 (기본값: 0)",
                        "QTY_ALL_ORD_YN": "잔량전부주문여부 (기본값: Y - 전량)",
                        "CNDT_PRIC": "조건가격 (기본값: 공란)",
                        "EXCG_ID_DVSN_CD": "거래소ID구분코드 (기본값: KRX)"
                    }
                },
                {
                    "name": "overseas_minute_price",
                    "display_name": "해외주식분봉조회",
                    "description": "해외주식분봉조회 API (v1_해외주식-030)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "AUTH": "사용자권한정보 (기본값: 공백)",
                        "EXCD": "거래소코드 (기본값: NAS - 나스닥)",
                        "SYMB": "종목코드 (기본값: AAPL)",
                        "NMIN": "분갭 (기본값: 1 - 1분봉)",
                        "PINC": "전일포함여부 (기본값: 0 - 당일)",
                        "NEXT": "다음여부 (기본값: 공백)",
                        "NREC": "요청갯수 (기본값: 120, 최대 120)",
                        "FILL": "미체결채움구분 (기본값: 공백)",
                        "KEYB": "NEXT KEY BUFF (기본값: 공백)"
                    }
                },
                {
                    "name": "overseas_order_revise",
                    "display_name": "해외주식 정정취소주문",
                    "description": "해외주식 정정취소주문 API (v1_해외주식-003)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "CANO": "종합계좌번호 (기본값: 00000000)",
                        "ACNT_PRDT_CD": "계좌상품코드 (기본값: 01)",
                        "OVRS_EXCG_CD": "해외거래소코드 (기본값: NASD - 나스닥)",
                        "PDNO": "상품번호 (기본값: 공란)",
                        "ORGN_ODNO": "원주문번호 (기본값: 공란)",
                        "RVSE_CNCL_DVSN_CD": "정정취소구분코드 (기본값: 02 - 취소)",
                        "ORD_QTY": "주문수량 (기본값: 0)",
                        "OVRS_ORD_UNPR": "해외주문단가 (기본값: 0)",
                        "MGCO_APTM_ODNO": "운용사지정주문번호 (기본값: 공란)",
                        "ORD_SVR_DVSN_CD": "주문서버구분코드 (기본값: 0)"
                    }
                },
                {
                    "name": "overseas_period_price",
                    "display_name": "해외주식 기간별시세",
                    "description": "해외주식 기간별시세 API (v1_해외주식-010)",
                    "supported_environments": ["real", "virtual"],
                    "parameters": {
                        "AUTH": "사용자권한정보 (기본값: 공백)",
                        "EXCD": "거래소코드 (기본값: NAS - 나스닥)",
                        "SYMB": "종목코드 (기본값: AAPL)",
                        "GUBN": "일/주/월구분 (기본값: 0 - 일)",
                        "BYMD": "조회기준일자 (기본값: 공백 - 오늘)",
                        "MODP": "수정주가반영여부 (기본값: 0 - 미반영)",
                        "KEYB": "NEXT KEY BUFF (기본값: 공백)"
                    }
                },
                {
                    "name": "overseas_current_price",
                    "display_name": "해외주식 현재가상세",
                    "description": "해외주식 현재가상세 API (v1_해외주식-029)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "AUTH": "사용자권한정보 (기본값: 공백)",
                        "EXCD": "거래소명 (기본값: NAS - 나스닥)",
                        "SYMB": "종목코드 (기본값: AAPL)"
                    }
                },
                {
                    "name": "overseas_news",
                    "display_name": "해외뉴스종합(제목)",
                    "description": "해외뉴스종합(제목) API (해외주식-053)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "INFO_GB": "뉴스구분 (기본값: 공백 - 전체)",
                        "CLASS_CD": "중분류 (기본값: 공백 - 전체)",
                        "NATION_CD": "국가코드 (기본값: 공백 - 전체, CN: 중국, HK: 홍콩, US: 미국)",
                        "EXCHANGE_CD": "거래소코드 (기본값: 공백 - 전체)",
                        "SYMB": "종목코드 (기본값: 공백 - 전체)",
                        "DATA_DT": "조회일자 (기본값: 공백 - 전체, YYYYMMDD)",
                        "DATA_TM": "조회시간 (기본값: 공백 - 전체, HHMMSS)",
                        "CTS": "다음키 (기본값: 공백)"
                    }
                },
                {
                    "name": "domestic_news",
                    "display_name": "종합 시황/공시(제목)",
                    "description": "종합 시황/공시(제목) API (국내주식-141)",
                    "supported_environments": ["real"],
                    "parameters": {
                        "FID_NEWS_OFER_ENTP_CODE": "뉴스 제공 업체 코드 (기본값: 공백)",
                        "FID_COND_MRKT_CLS_CODE": "조건 시장 구분 코드 (기본값: 공백)",
                        "FID_INPUT_ISCD": "입력 종목코드 (기본값: 공백 - 전체)",
                        "FID_TITL_CNTT": "제목 내용 (기본값: 공백)",
                        "FID_INPUT_DATE_1": "입력 날짜 (기본값: 공백 - 현재기준, 00YYYYMMDD)",
                        "FID_INPUT_HOUR_1": "입력 시간 (기본값: 공백 - 현재기준, 0000HHMMSS)",
                        "FID_RANK_SORT_CLS_CODE": "순위 정렬 구분 코드 (기본값: 공백)",
                        "FID_INPUT_SRNO": "입력 일련번호 (기본값: 공백)"
                    }
                }
            ]
        }
    
    def get_service_info(self) -> Dict[str, Any]:
        """서비스 정보 반환
        
        KIS 테스트 서비스의 기본 정보를 반환합니다.
        서비스 이름, 설명, 지원 환경, 지원 API 목록, 버전 정보를 포함합니다.
        
        Returns:
            Dict[str, Any]: 서비스 정보
        """
        return {
            "service_name": "KIS Test Service",
            "description": "한국투자증권 KIS API 테스트 서비스",
            "supported_environments": ["real", "virtual"],
            "supported_apis": ["balance_inquiry_rlz_pl", "buy_possible", "current_price", "daily_price", "domestic_minute_price", "domestic_news", "domestic_order", "domestic_order_revise", "domestic_profit", "overseas_current_price", "overseas_minute_price", "overseas_news", "overseas_order_history", "overseas_order_revise", "overseas_period_price", "overseas_present_balance", "overseas_profit", "period_price"],
            "version": "1.0.0"
        }

