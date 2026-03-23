# app/features/kis_test/models/kis_test_models.py

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime, timedelta


class KISBalanceOutput1(BaseModel):
    """주식잔고조회_실현손익 output1 - 개별 종목 잔고 정보"""
    
    pdno: str = Field(description="상품번호 (종목번호 뒷 6자리)")
    prdt_name: str = Field(description="상품명 (종목명)")
    trad_dvsn_name: str = Field(description="매매구분명 (매수매도구분)")
    bfdy_buy_qty: str = Field(description="전일매수수량")
    bfdy_sll_qty: str = Field(description="전일매도수량")
    thdt_buyqty: str = Field(description="금일매수수량")
    thdt_sll_qty: str = Field(description="금일매도수량")
    hldg_qty: str = Field(description="보유수량")
    ord_psbl_qty: str = Field(description="주문가능수량")
    pchs_avg_pric: str = Field(description="매입평균가격 (매입금액 / 보유수량)")
    pchs_amt: str = Field(description="매입금액")
    prpr: str = Field(description="현재가")
    evlu_amt: str = Field(description="평가금액")
    evlu_pfls_amt: str = Field(description="평가손익금액 (평가금액 - 매입금액)")
    evlu_pfls_rt: str = Field(description="평가손익율")
    evlu_erng_rt: str = Field(description="평가수익율")
    loan_dt: str = Field(description="대출일자")
    loan_amt: str = Field(description="대출금액")
    stln_slng_chgs: str = Field(description="대주매각대금 (신용거래에서 고객이 증권회사로부터 대부받은 주식의 매각대금)")
    expd_dt: str = Field(description="만기일자")
    stck_loan_unpr: str = Field(description="주식대출단가")
    bfdy_cprs_icdc: str = Field(description="전일대비증감")
    fltt_rt: str = Field(description="등락율")


class KISBalanceOutput2(BaseModel):
    """주식잔고조회_실현손익 output2 - 잔고 요약 정보"""
    
    dnca_tot_amt: str = Field(description="예수금총금액")
    nxdy_excc_amt: str = Field(description="익일정산금액")
    prvs_rcdl_excc_amt: str = Field(description="가수도정산금액")
    cma_evlu_amt: str = Field(description="CMA평가금액")
    bfdy_buy_amt: str = Field(description="전일매수금액")
    thdt_buy_amt: str = Field(description="금일매수금액")
    nxdy_auto_rdpt_amt: str = Field(description="익일자동상환금액")
    bfdy_sll_amt: str = Field(description="전일매도금액")
    thdt_sll_amt: str = Field(description="금일매도금액")
    d2_auto_rdpt_amt: str = Field(description="D+2자동상환금액")
    bfdy_tlex_amt: str = Field(description="전일제비용금액")
    thdt_tlex_amt: str = Field(description="금일제비용금액")
    tot_loan_amt: str = Field(description="총대출금액")
    scts_evlu_amt: str = Field(description="유가평가금액")
    tot_evlu_amt: str = Field(description="총평가금액")
    nass_amt: str = Field(description="순자산금액")
    fncg_gld_auto_rdpt_yn: str = Field(description="융자금자동상환여부")
    pchs_amt_smtl_amt: str = Field(description="매입금액합계금액")
    evlu_amt_smtl_amt: str = Field(description="평가금액합계금액")
    evlu_pfls_smtl_amt: str = Field(description="평가손익합계금액")
    tot_stln_slng_chgs: str = Field(description="총대주매각대금")
    bfdy_tot_asst_evlu_amt: str = Field(description="전일총자산평가금액")
    asst_icdc_amt: str = Field(description="자산증감액")
    asst_icdc_erng_rt: str = Field(description="자산증감수익율")
    rlzt_pfls: str = Field(description="실현손익")
    rlzt_erng_rt: str = Field(description="실현수익율")
    real_evlu_pfls: str = Field(description="실평가손익")
    real_evlu_pfls_erng_rt: str = Field(description="실평가손익수익율")


class KISOverseasBalanceRequest(BaseModel):
    """KIS 해외주식 체결기준현재잔고 요청 모델
    
    한국투자증권의 해외주식 체결기준현재잔고 API (v1_해외주식-008)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    WCRC_FRCR_DVSN_CD: str = Field(
        default="01", 
        description="원화외화구분코드 (01: 원화, 02: 외화, 기본값: 01)"
    )
    NATN_CD: str = Field(
        default="000", 
        description="국가코드 (000: 전체, 840: 미국, 344: 홍콩, 156: 중국, 392: 일본, 704: 베트남, 기본값: 000)"
    )
    TR_MKET_CD: str = Field(
        default="00", 
        description="거래시장코드 (00: 전체, 국가별 세부 코드 참조, 기본값: 00)"
    )
    INQR_DVSN_CD: str = Field(
        default="00", 
        description="조회구분코드 (00: 전체, 01: 일반해외주식, 02: 미니스탁, 기본값: 00)"
    )


class KISOverseasBalanceOutput1(BaseModel):
    """해외주식 체결기준현재잔고 output1 - 개별 종목 잔고 정보"""
    
    prdt_name: str = Field(description="상품명 (종목명)")
    cblc_qty13: str = Field(description="잔고수량13 (결제보유수량)")
    thdt_buy_ccld_qty1: str = Field(description="당일매수체결수량1 (당일 매수 체결 완료 수량)")
    thdt_sll_ccld_qty1: str = Field(description="당일매도체결수량1 (당일 매도 체결 완료 수량)")
    ccld_qty_smtl1: str = Field(description="체결수량합계1 (체결기준 현재 보유수량)")
    ord_psbl_qty1: str = Field(description="주문가능수량1 (주문 가능한 주문 수량)")
    frcr_pchs_amt: str = Field(description="외화매입금액 (해당 종목의 외화 기준 매입금액)")
    frcr_evlu_amt2: str = Field(description="외화평가금액2 (해당 종목의 외화 기준 평가금액)")
    evlu_pfls_amt2: str = Field(description="평가손익금액2 (해당 종목의 매입금액과 평가금액의 외화기준 비교 손익)")
    evlu_pfls_rt1: str = Field(description="평가손익율1 (해당 종목의 평가손익을 기준으로 한 수익률)")
    pdno: str = Field(description="상품번호 (종목코드)")
    bass_exrt: str = Field(description="기준환율 (원화 평가 시 적용 환율)")
    buy_crcy_cd: str = Field(description="매수통화코드 (USD: 미국달러, HKD: 홍콩달러, CNY: 중국위안화, JPY: 일본엔화, VND: 베트남동)")
    ovrs_now_pric1: str = Field(description="해외현재가격1 (해당 종목의 현재가)")
    avg_unpr3: str = Field(description="평균단가3 (해당 종목의 매수 평균 단가)")
    tr_mket_name: str = Field(description="거래시장명 (해당 종목의 거래시장명)")
    natn_kor_name: str = Field(description="국가한글명 (거래 국가명)")
    pchs_rmnd_wcrc_amt: str = Field(description="매입잔액원화금액")
    thdt_buy_ccld_frcr_amt: str = Field(description="당일매수체결외화금액 (당일 매수 외화금액)")
    thdt_sll_ccld_frcr_amt: str = Field(description="당일매도체결외화금액 (당일 매도 외화금액)")
    unit_amt: str = Field(description="단위금액")
    std_pdno: str = Field(description="표준상품번호")
    prdt_type_cd: str = Field(description="상품유형코드")
    scts_dvsn_name: str = Field(description="유가증권구분명")
    loan_rmnd: str = Field(description="대출잔액 (대출 미상환 금액)")
    loan_dt: str = Field(description="대출일자 (대출 실행일자)")
    loan_expd_dt: str = Field(description="대출만기일자 (대출 만기일자)")
    ovrs_excg_cd: str = Field(description="해외거래소코드 (NASD: 나스닥, NYSE: 뉴욕, AMEX: 아멕스, SEHK: 홍콩, SHAA: 중국상해, SZAA: 중국심천, TKSE: 일본, HASE: 하노이거래소, VNSE: 호치민거래소)")
    item_lnkg_excg_cd: str = Field(description="종목연동거래소코드")


class KISOverseasBalanceOutput2(BaseModel):
    """해외주식 체결기준현재잔고 output2 - 통화별 잔고 정보"""
    
    crcy_cd: str = Field(description="통화코드")
    crcy_cd_name: str = Field(description="통화코드명")
    frcr_buy_amt_smtl: str = Field(description="외화매수금액합계 (해당 통화로 매수한 종목 전체의 매수금액)")
    frcr_sll_amt_smtl: str = Field(description="외화매도금액합계 (해당 통화로 매도한 종목 전체의 매도금액)")
    frcr_dncl_amt_2: str = Field(description="외화예수금액2 (외화로 표시된 외화사용가능금액)")
    frst_bltn_exrt: str = Field(description="최초고시환율")
    frcr_buy_mgn_amt: str = Field(description="외화매수증거금액 (매수증거금으로 사용된 외화금액)")
    frcr_etc_mgna: str = Field(description="외화기타증거금")
    frcr_drwg_psbl_amt_1: str = Field(description="외화출금가능금액1 (출금가능한 외화금액)")
    frcr_evlu_amt2: str = Field(description="출금가능원화금액 (출금가능한 원화금액)")
    acpl_cstd_crcy_yn: str = Field(description="현지보관통화여부")
    nxdy_frcr_drwg_psbl_amt: str = Field(description="익일외화출금가능금액")


class KISOverseasBalanceOutput3(BaseModel):
    """해외주식 체결기준현재잔고 output3 - 계좌 전체 요약 정보"""
    
    pchs_amt_smtl: str = Field(description="매입금액합계 (해외유가증권 매수금액의 원화 환산 금액)")
    evlu_amt_smtl: str = Field(description="평가금액합계 (해외유가증권 평가금액의 원화 환산 금액)")
    evlu_pfls_amt_smtl: str = Field(description="평가손익금액합계 (해외유가증권 평가손익의 원화 환산 금액)")
    dncl_amt: str = Field(description="예수금액")
    cma_evlu_amt: str = Field(description="CMA평가금액")
    tot_dncl_amt: str = Field(description="총예수금액")
    etc_mgna: str = Field(description="기타증거금")
    wdrw_psbl_tot_amt: str = Field(description="인출가능총금액")
    frcr_evlu_tota: str = Field(description="외화평가총액")
    evlu_erng_rt1: str = Field(description="평가수익율1")
    pchs_amt_smtl_amt: str = Field(description="매입금액합계금액")
    evlu_amt_smtl_amt: str = Field(description="평가금액합계금액")
    tot_evlu_pfls_amt: str = Field(description="총평가손익금액")
    tot_asst_amt: str = Field(description="총자산금액")
    buy_mgn_amt: str = Field(description="매수증거금액")
    mgna_tota: str = Field(description="증거금총액")
    frcr_use_psbl_amt: str = Field(description="외화사용가능금액")
    ustl_sll_amt_smtl: str = Field(description="미결제매도금액합계")
    ustl_buy_amt_smtl: str = Field(description="미결제매수금액합계")
    tot_frcr_cblc_smtl: str = Field(description="총외화잔고합계")
    tot_loan_amt: str = Field(description="총대출금액")


class KISOverseasBalanceResponse(BaseModel):
    """KIS 해외주식 체결기준현재잔고 응답 모델
    
    한국투자증권 해외주식 체결기준현재잔고 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[List[KISOverseasBalanceOutput1], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="체결기준현재잔고 상세 정보 배열 (개별 종목별 잔고 정보)"
    )
    output2: Optional[Union[List[KISOverseasBalanceOutput2], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="통화별 잔고 정보 배열"
    )
    output3: Optional[Union[KISOverseasBalanceOutput3, Dict[str, Any]]] = Field(
        default=None, 
        description="계좌 전체 요약 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDomesticProfitRequest(BaseModel):
    """KIS 기간별매매손익현황조회 요청 모델
    
    한국투자증권의 기간별매매손익현황조회 API (v1_국내주식-060)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    조회시작일자는 하루전, 조회종료일자는 오늘로 기본 설정됩니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    SORT_DVSN: str = Field(
        default="00", 
        description="정렬구분 (00: 최근 순, 01: 과거 순, 02: 최근 순)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    PDNO: str = Field(
        default="", 
        description="상품번호 (공란 입력 시 전체)"
    )
    INQR_STRT_DT: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        description="조회시작일자 (YYYYMMDD 형식, 기본값: 하루전)"
    )
    INQR_END_DT: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="조회종료일자 (YYYYMMDD 형식, 기본값: 오늘)"
    )
    CTX_AREA_NK100: str = Field(
        default="", 
        description="연속조회키100"
    )
    CBLC_DVSN: str = Field(
        default="00", 
        description="잔고구분 (00: 전체)"
    )
    CTX_AREA_FK100: str = Field(
        default="", 
        description="연속조회검색조건100"
    )


class KISDomesticProfitOutput1(BaseModel):
    """기간별매매손익현황조회 output1 - 개별 종목별 매매손익 정보"""
    
    trad_dt: str = Field(description="매매일자")
    pdno: str = Field(description="상품번호 (종목번호 뒤 6자리만 해당)")
    prdt_name: str = Field(description="상품명")
    trad_dvsn_name: str = Field(description="매매구분명")
    loan_dt: str = Field(description="대출일자")
    hldg_qty: str = Field(description="보유수량")
    pchs_unpr: str = Field(description="매입단가")
    buy_qty: str = Field(description="매수수량")
    buy_amt: str = Field(description="매수금액")
    sll_pric: str = Field(description="매도가격")
    sll_qty: str = Field(description="매도수량")
    sll_amt: str = Field(description="매도금액")
    rlzt_pfls: str = Field(description="실현손익")
    pfls_rt: str = Field(description="손익률")
    fee: str = Field(description="수수료")
    tl_tax: str = Field(description="제세금")
    loan_int: str = Field(description="대출이자")


class KISDomesticProfitOutput2(BaseModel):
    """기간별매매손익현황조회 output2 - 전체 요약 정보"""
    
    sll_qty_smtl: str = Field(description="매도수량합계")
    sll_tr_amt_smtl: str = Field(description="매도거래금액합계")
    sll_fee_smtl: str = Field(description="매도수수료합계")
    sll_tltx_smtl: str = Field(description="매도제세금합계")
    sll_excc_amt_smtl: str = Field(description="매도정산금액합계")
    buyqty_smtl: str = Field(description="매수수량합계")
    buy_tr_amt_smtl: str = Field(description="매수거래금액합계")
    buy_fee_smtl: str = Field(description="매수수수료합계")
    buy_tax_smtl: str = Field(description="매수제세금합계")
    buy_excc_amt_smtl: str = Field(description="매수정산금액합계")
    tot_qty: str = Field(description="총수량")
    tot_tr_amt: str = Field(description="총거래금액")
    tot_fee: str = Field(description="총수수료")
    tot_tltx: str = Field(description="총제세금")
    tot_excc_amt: str = Field(description="총정산금액")
    tot_rlzt_pfls: str = Field(description="총실현손익")
    loan_int: str = Field(description="대출이자")
    tot_pftrt: str = Field(description="총수익률")


class KISDomesticProfitResponse(BaseModel):
    """KIS 기간별매매손익현황조회 응답 모델
    
    한국투자증권 기간별매매손익현황조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    ctx_area_nk100: str = Field(description="연속조회키100")
    ctx_area_fk100: str = Field(description="연속조회검색조건100")
    output1: Optional[Union[List[KISDomesticProfitOutput1], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="개별 종목별 매매손익 정보 배열"
    )
    output2: Optional[Union[KISDomesticProfitOutput2, Dict[str, Any]]] = Field(
        default=None, 
        description="전체 요약 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISBuyPossibleRequest(BaseModel):
    """KIS 매수가능조회 요청 모델
    
    한국투자증권의 매수가능조회 API (v1_국내주식-007)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    PDNO: str = Field(
        default="", 
        description="상품번호 (종목번호 6자리, 공란 입력 시 매수수량 없이 매수금액만 조회)"
    )
    ORD_UNPR: str = Field(
        default="", 
        description="주문단가 (1주당 가격, 시장가 조회 시 공란, 공란 입력 시 매수수량 없이 매수금액만 조회)"
    )
    ORD_DVSN: str = Field(
        default="01", 
        description="주문구분 (00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 04: 최우선지정가 등)"
    )
    CMA_EVLU_AMT_ICLD_YN: str = Field(
        default="N", 
        description="CMA평가금액포함여부 (Y: 포함, N: 포함하지 않음)"
    )
    OVRS_ICLD_YN: str = Field(
        default="N", 
        description="해외포함여부 (Y: 포함, N: 포함하지 않음)"
    )


class KISBuyPossibleOutput(BaseModel):
    """매수가능조회 output - 매수가능 정보"""
    
    ord_psbl_cash: str = Field(description="주문가능현금 (예수금으로 계산된 주문가능금액)")
    ord_psbl_sbst: str = Field(description="주문가능대용")
    ruse_psbl_amt: str = Field(description="재사용가능금액 (전일/금일 매도대금으로 계산된 주문가능금액)")
    fund_rpch_chgs: str = Field(description="펀드환매대금")
    psbl_qty_calc_unpr: str = Field(description="가능수량계산단가")
    nrcvb_buy_amt: str = Field(description="미수없는매수금액 (미수를 사용하지 않을 경우 확인)")
    nrcvb_buy_qty: str = Field(description="미수없는매수수량 (미수를 사용하지 않을 경우 확인)")
    max_buy_amt: str = Field(description="최대매수금액 (미수를 사용하는 경우 확인)")
    max_buy_qty: str = Field(description="최대매수수량 (미수를 사용하는 경우 확인)")
    cma_evlu_amt: str = Field(description="CMA평가금액")
    ovrs_re_use_amt_wcrc: str = Field(description="해외재사용금액원화")
    ord_psbl_frcr_amt_wcrc: str = Field(description="주문가능외화금액원화")


class KISBuyPossibleResponse(BaseModel):
    """KIS 매수가능조회 응답 모델
    
    한국투자증권 매수가능조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISBuyPossibleOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="매수가능 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDailyPriceRequest(BaseModel):
    """KIS 주식현재가 일자별 요청 모델
    
    한국투자증권의 주식현재가 일자별 API (v1_국내주식-010)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    FID_COND_MRKT_DIV_CODE: str = Field(
        default="J", 
        description="조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)"
    )
    FID_INPUT_ISCD: str = Field(
        default="005930", 
        description="입력 종목코드 (예: 005930 삼성전자)"
    )
    FID_PERIOD_DIV_CODE: str = Field(
        default="D", 
        description="기간 분류 코드 (D: 일/최근 30거래일, W: 주/최근 30주, M: 월/최근 30개월)"
    )
    FID_ORG_ADJ_PRC: str = Field(
        default="1", 
        description="수정주가 원주가 가격 (0: 수정주가미반영, 1: 수정주가반영)"
    )


class KISDailyPriceOutput(BaseModel):
    """주식현재가 일자별 output - 일별 시세 정보"""
    
    stck_bsop_date: str = Field(description="주식 영업 일자")
    stck_oprc: str = Field(description="주식 시가")
    stck_hgpr: str = Field(description="주식 최고가")
    stck_lwpr: str = Field(description="주식 최저가")
    stck_clpr: str = Field(description="주식 종가")
    acml_vol: str = Field(description="누적 거래량")
    prdy_vrss_vol_rate: str = Field(description="전일 대비 거래량 비율")
    prdy_vrss: str = Field(description="전일 대비")
    prdy_vrss_sign: str = Field(description="전일 대비 부호")
    prdy_ctrt: str = Field(description="전일 대비율")
    hts_frgn_ehrt: str = Field(description="HTS 외국인 소진율")
    frgn_ntby_qty: str = Field(description="외국인 순매수 수량")
    flng_cls_code: str = Field(description="락 구분 코드 (01: 권리락, 02: 배당락, 03: 분배락, 04: 권배락, 05: 중간배당락, 06: 권리중간배당락, 07: 권리분기배당락)")
    acml_prtt_rate: str = Field(description="누적 분할 비율")


class KISDailyPriceResponse(BaseModel):
    """KIS 주식현재가 일자별 응답 모델
    
    한국투자증권 주식현재가 일자별 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[List[KISDailyPriceOutput], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="일별 시세 정보 배열"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISPeriodPriceRequest(BaseModel):
    """KIS 국내주식기간별시세 요청 모델
    
    한국투자증권의 국내주식기간별시세 API (v1_국내주식-016)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    FID_COND_MRKT_DIV_CODE: str = Field(
        default="J", 
        description="조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)"
    )
    FID_INPUT_ISCD: str = Field(
        default="005930", 
        description="입력 종목코드 (예: 005930 삼성전자)"
    )
    FID_INPUT_DATE_1: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
        description="입력 날짜 1 (조회 시작일자, YYYYMMDD 형식, 기본값: 30일 전)"
    )
    FID_INPUT_DATE_2: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="입력 날짜 2 (조회 종료일자, YYYYMMDD 형식, 기본값: 오늘, 최대 100개)"
    )
    FID_PERIOD_DIV_CODE: str = Field(
        default="D", 
        description="기간분류코드 (D: 일봉, W: 주봉, M: 월봉, Y: 년봉)"
    )
    FID_ORG_ADJ_PRC: str = Field(
        default="0", 
        description="수정주가 원주가 가격 여부 (0: 수정주가, 1: 원주가)"
    )


class KISPeriodPriceOutput1(BaseModel):
    """국내주식기간별시세 output1 - 현재 시세 요약 정보"""
    
    prdy_vrss: str = Field(description="전일 대비")
    prdy_vrss_sign: str = Field(description="전일 대비 부호")
    prdy_ctrt: str = Field(description="전일 대비율")
    stck_prdy_clpr: str = Field(description="주식 전일 종가")
    acml_vol: str = Field(description="누적 거래량")
    acml_tr_pbmn: str = Field(description="누적 거래 대금")
    hts_kor_isnm: str = Field(description="HTS 한글 종목명")
    stck_prpr: str = Field(description="주식 현재가")
    stck_shrn_iscd: str = Field(description="주식 단축 종목코드")
    prdy_vol: str = Field(description="전일 거래량")
    stck_mxpr: str = Field(description="주식 상한가")
    stck_llam: str = Field(description="주식 하한가")
    stck_oprc: str = Field(description="주식 시가")
    stck_hgpr: str = Field(description="주식 최고가")
    stck_lwpr: str = Field(description="주식 최저가")
    stck_prdy_oprc: str = Field(description="주식 전일 시가")
    stck_prdy_hgpr: str = Field(description="주식 전일 최고가")
    stck_prdy_lwpr: str = Field(description="주식 전일 최저가")
    askp: str = Field(description="매도호가")
    bidp: str = Field(description="매수호가")
    prdy_vrss_vol: str = Field(description="전일 대비 거래량")
    vol_tnrt: str = Field(description="거래량 회전율")
    stck_fcam: str = Field(description="주식 액면가")
    lstn_stcn: str = Field(description="상장 주수")
    cpfn: str = Field(description="자본금")
    hts_avls: str = Field(description="HTS 시가총액")
    per: str = Field(description="PER")
    eps: str = Field(description="EPS")
    pbr: str = Field(description="PBR")
    itewhol_loan_rmnd_ratem: str = Field(description="전체 융자 잔고 비율")


class KISPeriodPriceOutput2(BaseModel):
    """국내주식기간별시세 output2 - 기간별 시세 정보"""
    
    stck_bsop_date: str = Field(description="주식 영업 일자")
    stck_clpr: str = Field(description="주식 종가")
    stck_oprc: str = Field(description="주식 시가")
    stck_hgpr: str = Field(description="주식 최고가")
    stck_lwpr: str = Field(description="주식 최저가")
    acml_vol: str = Field(description="누적 거래량")
    acml_tr_pbmn: str = Field(description="누적 거래 대금")
    flng_cls_code: str = Field(description="락 구분 코드 (01: 권리락, 02: 배당락, 03: 분배락, 04: 권배락, 05: 중간배당락, 06: 권리중간배당락, 07: 권리분기배당락)")
    prtt_rate: str = Field(description="분할 비율 (기준가/전일 종가)")
    mod_yn: str = Field(description="변경 여부 (현재 영업일에 체결이 발생하지 않아 시가가 없을경우 Y로 표시)")
    prdy_vrss_sign: str = Field(description="전일 대비 부호")
    prdy_vrss: str = Field(description="전일 대비")
    revl_issu_reas: str = Field(description="재평가사유코드 (00: 해당없음, 01: 회사분할, 02: 자본감소, 03: 장기간정지, 04: 초과분배, 05: 대규모배당, 06: 회사분할합병, 07: ETN증권병합/분할, 08: 신종증권기세조정, 99: 기타)")


class KISPeriodPriceResponse(BaseModel):
    """KIS 국내주식기간별시세 응답 모델
    
    한국투자증권 국내주식기간별시세 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[KISPeriodPriceOutput1, Dict[str, Any]]] = Field(
        default=None, 
        description="현재 시세 요약 정보"
    )
    output2: Optional[Union[List[KISPeriodPriceOutput2], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="기간별 시세 정보 배열"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasMinuteRequest(BaseModel):
    """KIS 해외주식분봉조회 요청 모델
    
    한국투자증권의 해외주식분봉조회 API (v1_해외주식-030)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    AUTH: str = Field(
        default="", 
        description="사용자권한정보 (공백으로 입력)"
    )
    EXCD: str = Field(
        default="NAS", 
        description="거래소코드 (NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, HKS: 홍콩, SHS: 상해, SZS: 심천, HSX: 호치민, HNX: 하노이, TSE: 도쿄, BAY: 뉴욕(주간), BAQ: 나스닥(주간), BAA: 아멕스(주간))"
    )
    SYMB: str = Field(
        default="AAPL", 
        description="종목코드 (예: AAPL, TSLA)"
    )
    NMIN: str = Field(
        default="1", 
        description="분갭 (분단위: 1: 1분봉, 2: 2분봉 등)"
    )
    PINC: str = Field(
        default="0", 
        description="전일포함여부 (0: 당일, 1: 전일포함, 다음조회 시 반드시 1로 입력)"
    )
    NEXT: str = Field(
        default="", 
        description="다음여부 (처음조회 시 공백, 다음조회 시 1 입력)"
    )
    NREC: str = Field(
        default="120", 
        description="요청갯수 (레코드요청갯수, 최대 120)"
    )
    FILL: str = Field(
        default="", 
        description="미체결채움구분 (공백으로 입력)"
    )
    KEYB: str = Field(
        default="", 
        description="NEXT KEY BUFF (처음조회 시 공백, 다음조회 시 이전 조회 결과의 마지막 분봉 데이터를 이용하여 1분 전 혹은 n분 전의 시간 입력, 형식: YYYYMMDDHHMMSS)"
    )


class KISOverseasMinuteOutput1(BaseModel):
    """해외주식분봉조회 output1 - 메타 정보"""
    
    rsym: str = Field(description="실시간종목코드")
    zdiv: str = Field(description="소수점자리수")
    stim: str = Field(description="장시작현지시간")
    etim: str = Field(description="장종료현지시간")
    sktm: str = Field(description="장시작한국시간")
    ektm: str = Field(description="장종료한국시간")
    next: str = Field(description="다음가능여부")
    more: str = Field(description="추가데이타여부")
    nrec: str = Field(description="레코드갯수")


class KISOverseasMinuteOutput2(BaseModel):
    """해외주식분봉조회 output2 - 분봉 시세 정보"""
    
    tymd: str = Field(description="현지영업일자")
    xymd: str = Field(description="현지기준일자")
    xhms: str = Field(description="현지기준시간")
    kymd: str = Field(description="한국기준일자")
    khms: str = Field(description="한국기준시간")
    open: str = Field(description="시가")
    high: str = Field(description="고가")
    low: str = Field(description="저가")
    last: str = Field(description="종가")
    evol: str = Field(description="체결량")
    eamt: str = Field(description="체결대금")


class KISOverseasMinuteResponse(BaseModel):
    """KIS 해외주식분봉조회 응답 모델
    
    한국투자증권 해외주식분봉조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[KISOverseasMinuteOutput1, Dict[str, Any]]] = Field(
        default=None, 
        description="메타 정보 (장시간, 다음조회 가능여부 등)"
    )
    output2: Optional[Union[List[KISOverseasMinuteOutput2], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="분봉 시세 정보 배열 (최대 120개)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDomesticMinuteRequest(BaseModel):
    """KIS 주식일별분봉조회 요청 모델
    
    한국투자증권의 주식일별분봉조회 API (국내주식-213)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    FID_COND_MRKT_DIV_CODE: str = Field(
        default="J", 
        description="조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)"
    )
    FID_INPUT_ISCD: str = Field(
        default="005930", 
        description="입력 종목코드 (예: 005930 삼성전자)"
    )
    FID_INPUT_HOUR_1: str = Field(
        default="", 
        description="입력 시간1 (예: 13시 → 130000, 공백: 현재시간)"
    )
    FID_INPUT_DATE_1: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="입력 날짜1 (YYYYMMDD 형식, 기본값: 오늘)"
    )
    FID_PW_DATA_INCU_YN: str = Field(
        default="Y", 
        description="과거 데이터 포함 여부"
    )
    FID_FAKE_TICK_INCU_YN: str = Field(
        default="", 
        description="허봉 포함 여부 (공백 필수 입력)"
    )


class KISDomesticMinuteOutput1(BaseModel):
    """주식일별분봉조회 output1 - 현재 시세 요약 정보"""
    
    prdy_vrss: str = Field(description="전일 대비")
    prdy_vrss_sign: str = Field(description="전일 대비 부호")
    prdy_ctrt: str = Field(description="전일 대비율")
    stck_prdy_clpr: str = Field(description="주식 전일 종가")
    acml_vol: str = Field(description="누적 거래량")
    acml_tr_pbmn: str = Field(description="누적 거래 대금")
    hts_kor_isnm: str = Field(description="HTS 한글 종목명")
    stck_prpr: str = Field(description="주식 현재가")


class KISDomesticMinuteOutput2(BaseModel):
    """주식일별분봉조회 output2 - 분봉 시세 정보"""
    
    stck_bsop_date: str = Field(description="주식 영업 일자")
    stck_cntg_hour: str = Field(description="주식 체결 시간")
    stck_prpr: str = Field(description="주식 현재가")
    stck_oprc: str = Field(description="주식 시가")
    stck_hgpr: str = Field(description="주식 최고가")
    stck_lwpr: str = Field(description="주식 최저가")
    cntg_vol: str = Field(description="체결 거래량")
    acml_tr_pbmn: str = Field(description="누적 거래 대금")


class KISDomesticMinuteResponse(BaseModel):
    """KIS 주식일별분봉조회 응답 모델
    
    한국투자증권 주식일별분봉조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[KISDomesticMinuteOutput1, Dict[str, Any]]] = Field(
        default=None, 
        description="현재 시세 요약 정보"
    )
    output2: Optional[Union[List[KISDomesticMinuteOutput2], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="분봉 시세 정보 배열"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISCurrentPriceRequest(BaseModel):
    """KIS 주식현재가 시세 요청 모델
    
    한국투자증권의 주식현재가 시세 API (v1_국내주식-008)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    FID_COND_MRKT_DIV_CODE: str = Field(
        default="J", 
        description="조건 시장 분류 코드 (J: KRX, NX: NXT, UN: 통합)"
    )
    FID_INPUT_ISCD: str = Field(
        default="005930", 
        description="입력 종목코드 (예: 005930 삼성전자, ETN은 종목코드 6자리 앞에 Q 입력 필수)"
    )


class KISCurrentPriceOutput(BaseModel):
    """주식현재가 시세 output - 현재가 상세 정보"""
    
    iscd_stat_cls_code: str = Field(description="종목 상태 구분 코드 (51: 관리종목, 52: 투자위험, 53: 투자경고, 54: 투자주의, 55: 신용가능, 57: 증거금100%, 58: 거래정지, 59: 단기과열종목)")
    marg_rate: str = Field(description="증거금 비율")
    rprs_mrkt_kor_name: str = Field(description="대표 시장 한글 명")
    new_hgpr_lwpr_cls_code: str = Field(description="신 고가 저가 구분 코드")
    bstp_kor_isnm: str = Field(description="업종 한글 종목명")
    temp_stop_yn: str = Field(description="임시 정지 여부")
    oprc_rang_cont_yn: str = Field(description="시가 범위 연장 여부")
    clpr_rang_cont_yn: str = Field(description="종가 범위 연장 여부")
    crdt_able_yn: str = Field(description="신용 가능 여부")
    grmn_rate_cls_code: str = Field(description="보증금 비율 구분 코드")
    elw_pblc_yn: str = Field(description="ELW 발행 여부")
    stck_prpr: str = Field(description="주식 현재가")
    prdy_vrss: str = Field(description="전일 대비")
    prdy_vrss_sign: str = Field(description="전일 대비 부호")
    prdy_ctrt: str = Field(description="전일 대비율")
    acml_tr_pbmn: str = Field(description="누적 거래 대금")
    acml_vol: str = Field(description="누적 거래량")
    prdy_vrss_vol_rate: str = Field(description="전일 대비 거래량 비율")
    stck_oprc: str = Field(description="주식 시가")
    stck_hgpr: str = Field(description="주식 최고가")
    stck_lwpr: str = Field(description="주식 최저가")
    stck_mxpr: str = Field(description="주식 상한가")
    stck_llam: str = Field(description="주식 하한가")
    stck_sdpr: str = Field(description="주식 기준가")
    wghn_avrg_stck_prc: str = Field(description="가중 평균 주식 가격")
    hts_frgn_ehrt: str = Field(description="HTS 외국인 소진율")
    frgn_ntby_qty: str = Field(description="외국인 순매수 수량")
    pgtr_ntby_qty: str = Field(description="프로그램매매 순매수 수량")
    pvt_scnd_dmrs_prc: str = Field(description="피벗 2차 디저항 가격")
    pvt_frst_dmrs_prc: str = Field(description="피벗 1차 디저항 가격")
    pvt_pont_val: str = Field(description="피벗 포인트 값")
    pvt_frst_dmsp_prc: str = Field(description="피벗 1차 디지지 가격")
    pvt_scnd_dmsp_prc: str = Field(description="피벗 2차 디지지 가격")
    dmrs_val: str = Field(description="디저항 값")
    dmsp_val: str = Field(description="디지지 값")
    cpfn: str = Field(description="자본금")
    rstc_wdth_prc: str = Field(description="제한 폭 가격")
    stck_fcam: str = Field(description="주식 액면가")
    stck_sspr: str = Field(description="주식 대용가")
    aspr_unit: str = Field(description="호가단위")
    hts_deal_qty_unit_val: str = Field(description="HTS 매매 수량 단위 값")
    lstn_stcn: str = Field(description="상장 주수")
    hts_avls: str = Field(description="HTS 시가총액")
    per: str = Field(description="PER")
    pbr: str = Field(description="PBR")
    stac_month: str = Field(description="결산 월")
    vol_tnrt: str = Field(description="거래량 회전율")
    eps: str = Field(description="EPS")
    bps: str = Field(description="BPS")
    d250_hgpr: str = Field(description="250일 최고가")
    d250_hgpr_date: str = Field(description="250일 최고가 일자")
    d250_hgpr_vrss_prpr_rate: str = Field(description="250일 최고가 대비 현재가 비율")
    d250_lwpr: str = Field(description="250일 최저가")
    d250_lwpr_date: str = Field(description="250일 최저가 일자")
    d250_lwpr_vrss_prpr_rate: str = Field(description="250일 최저가 대비 현재가 비율")
    stck_dryy_hgpr: str = Field(description="주식 연중 최고가")
    dryy_hgpr_vrss_prpr_rate: str = Field(description="연중 최고가 대비 현재가 비율")
    dryy_hgpr_date: str = Field(description="연중 최고가 일자")
    stck_dryy_lwpr: str = Field(description="주식 연중 최저가")
    dryy_lwpr_vrss_prpr_rate: str = Field(description="연중 최저가 대비 현재가 비율")
    dryy_lwpr_date: str = Field(description="연중 최저가 일자")
    w52_hgpr: str = Field(description="52주일 최고가")
    w52_hgpr_vrss_prpr_ctrt: str = Field(description="52주일 최고가 대비 현재가 대비")
    w52_hgpr_date: str = Field(description="52주일 최고가 일자")
    w52_lwpr: str = Field(description="52주일 최저가")
    w52_lwpr_vrss_prpr_ctrt: str = Field(description="52주일 최저가 대비 현재가 대비")
    w52_lwpr_date: str = Field(description="52주일 최저가 일자")
    whol_loan_rmnd_rate: str = Field(description="전체 융자 잔고 비율")
    ssts_yn: str = Field(description="공매도가능여부")
    stck_shrn_iscd: str = Field(description="주식 단축 종목코드")
    fcam_cnnm: str = Field(description="액면가 통화명")
    cpfn_cnnm: str = Field(description="자본금 통화명")
    apprch_rate: str = Field(description="접근도")
    frgn_hldn_qty: str = Field(description="외국인 보유 수량")
    vi_cls_code: str = Field(description="VI적용구분코드")
    ovtm_vi_cls_code: str = Field(description="시간외단일가VI적용구분코드")
    last_ssts_cntg_qty: str = Field(description="최종 공매도 체결 수량")
    invt_caful_yn: str = Field(description="투자유의여부")
    mrkt_warn_cls_code: str = Field(description="시장경고코드")
    short_over_yn: str = Field(description="단기과열여부")
    sltr_yn: str = Field(description="정리매매여부")
    mang_issu_cls_code: str = Field(description="관리종목여부")


class KISCurrentPriceResponse(BaseModel):
    """KIS 주식현재가 시세 응답 모델
    
    한국투자증권 주식현재가 시세 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISCurrentPriceOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="현재가 상세 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDomesticOrderRequest(BaseModel):
    """KIS 주식일별주문체결조회 요청 모델
    
    한국투자증권의 주식일별주문체결조회 API (v1_국내주식-005)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    조회시작일자는 하루전, 조회종료일자는 오늘로 기본 설정됩니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    INQR_STRT_DT: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        description="조회시작일자 (YYYYMMDD 형식, 기본값: 하루전)"
    )
    INQR_END_DT: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="조회종료일자 (YYYYMMDD 형식, 기본값: 오늘)"
    )
    SLL_BUY_DVSN_CD: str = Field(
        default="00", 
        description="매도매수구분코드 (00: 전체, 01: 매도, 02: 매수)"
    )
    PDNO: str = Field(
        default="", 
        description="상품번호 (종목번호 6자리, 공란: 전체)"
    )
    ORD_GNO_BRNO: str = Field(
        default="", 
        description="주문채번지점번호 (주문시 한국투자증권 시스템에서 지정된 영업점코드)"
    )
    ODNO: str = Field(
        default="", 
        description="주문번호 (주문시 한국투자증권 시스템에서 채번된 주문번호)"
    )
    CCLD_DVSN: str = Field(
        default="00", 
        description="체결구분 (00: 전체, 01: 체결, 02: 미체결)"
    )
    INQR_DVSN: str = Field(
        default="00", 
        description="조회구분 (00: 역순, 01: 정순)"
    )
    INQR_DVSN_1: str = Field(
        default="", 
        description="조회구분1 (공란: 전체, 1: ELW, 2: 프리보드)"
    )
    INQR_DVSN_3: str = Field(
        default="00", 
        description="조회구분3 (00: 전체, 01: 현금, 02: 신용, 03: 담보, 04: 대주, 05: 대여, 06: 자기융자신규/상환, 07: 유통융자신규/상환)"
    )
    EXCG_ID_DVSN_CD: str = Field(
        default="KRX", 
        description="거래소ID구분코드 (KRX: 한국거래소, NXT: 대체거래소, SOR: Smart Order Routing, ALL: 전체, 모의투자는 KRX만 제공)"
    )
    CTX_AREA_FK100: str = Field(
        default="", 
        description="연속조회검색조건100 (공란: 최초 조회시, 이전 조회 Output 값: 다음페이지 조회시)"
    )
    CTX_AREA_NK100: str = Field(
        default="", 
        description="연속조회키100 (공란: 최초 조회시, 이전 조회 Output 값: 다음페이지 조회시)"
    )


class KISDomesticOrderOutput1(BaseModel):
    """주식일별주문체결조회 output1 - 주문체결 상세 정보"""
    
    ord_dt: str = Field(description="주문일자")
    ord_gno_brno: str = Field(description="주문채번지점번호")
    odno: str = Field(description="주문번호")
    orgn_odno: str = Field(description="원주문번호")
    ord_dvsn_name: str = Field(description="주문구분명")
    sll_buy_dvsn_cd: str = Field(description="매도매수구분코드")
    sll_buy_dvsn_cd_name: str = Field(description="매도매수구분코드명")
    pdno: str = Field(description="상품번호")
    prdt_name: str = Field(description="상품명")
    ord_qty: str = Field(description="주문수량")
    ord_unpr: str = Field(description="주문단가")
    ord_tmd: str = Field(description="주문시각")
    tot_ccld_qty: str = Field(description="총체결수량")
    avg_prvs: str = Field(description="평균가")
    cncl_yn: str = Field(description="취소여부")
    tot_ccld_amt: str = Field(description="총체결금액")
    loan_dt: str = Field(description="대출일자")
    ordr_empno: str = Field(description="주문자사번")
    ord_dvsn_cd: str = Field(description="주문구분코드")
    cnc_cfrm_qty: str = Field(description="취소확인수량")
    rmn_qty: str = Field(description="잔여수량")
    rjct_qty: str = Field(description="거부수량")
    ccld_cndt_name: str = Field(description="체결조건명")
    inqr_ip_addr: str = Field(description="조회IP주소")
    cpbc_ordp_ord_rcit_dvsn_cd: str = Field(description="전산주문표주문접수구분코드")
    cpbc_ordp_infm_mthd_dvsn_cd: str = Field(description="전산주문표통보방법구분코드")
    infm_tmd: str = Field(description="통보시각")
    ctac_tlno: str = Field(description="연락전화번호")
    prdt_type_cd: str = Field(description="상품유형코드")
    excg_dvsn_cd: str = Field(description="거래소구분코드")
    cpbc_ordp_mtrl_dvsn_cd: str = Field(description="전산주문표자료구분코드")
    ord_orgno: str = Field(description="주문조직번호")
    rsvn_ord_end_dt: str = Field(description="예약주문종료일자")
    excg_id_dvsn_Cd: str = Field(description="거래소ID구분코드")
    stpm_cndt_pric: str = Field(description="스톱지정가조건가격")
    stpm_efct_occr_dtmd: str = Field(description="스톱지정가효력발생상세시각")


class KISDomesticOrderOutput2(BaseModel):
    """주식일별주문체결조회 output2 - 전체 요약 정보"""
    
    tot_ord_qty: str = Field(description="총주문수량")
    tot_ccld_qty: str = Field(description="총체결수량")
    tot_ccld_amt: str = Field(description="총체결금액")
    prsm_tlex_smtl: str = Field(description="추정제비용합계")
    pchs_avg_pric: str = Field(description="매입평균가격")


class KISDomesticOrderResponse(BaseModel):
    """KIS 주식일별주문체결조회 응답 모델
    
    한국투자증권 주식일별주문체결조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    ctx_area_fk100: str = Field(description="연속조회검색조건100")
    ctx_area_nk100: str = Field(description="연속조회키100")
    output1: Optional[Union[List[KISDomesticOrderOutput1], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="주문체결 상세 정보 배열"
    )
    output2: Optional[Union[KISDomesticOrderOutput2, Dict[str, Any]]] = Field(
        default=None, 
        description="전체 요약 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDomesticOrderReviseRequest(BaseModel):
    """KIS 주식주문(정정취소) 요청 모델
    
    한국투자증권의 주식주문(정정취소) API (v1_국내주식-003)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    KRX_FWDG_ORD_ORGNO: str = Field(
        default="", 
        description="한국거래소전송주문조직번호"
    )
    ORGN_ODNO: str = Field(
        default="", 
        description="원주문번호"
    )
    ORD_DVSN: str = Field(
        default="00", 
        description="주문구분 (00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 04: 최우선지정가 등)"
    )
    RVSE_CNCL_DVSN_CD: str = Field(
        default="02", 
        description="정정취소구분코드 (01: 정정, 02: 취소)"
    )
    ORD_QTY: str = Field(
        default="0", 
        description="주문수량"
    )
    ORD_UNPR: str = Field(
        default="0", 
        description="주문단가"
    )
    QTY_ALL_ORD_YN: str = Field(
        default="Y", 
        description="잔량전부주문여부 (Y: 전량, N: 일부)"
    )
    CNDT_PRIC: str = Field(
        default="", 
        description="조건가격 (스탑지정가호가에서 사용)"
    )
    EXCG_ID_DVSN_CD: str = Field(
        default="KRX", 
        description="거래소ID구분코드 (KRX: 한국거래소, NXT: 대체거래소, SOR: Smart Order Routing, 미입력시 KRX, 모의투자는 KRX만 가능)"
    )


class KISDomesticOrderReviseOutput(BaseModel):
    """주식주문(정정취소) output - 주문 결과 정보"""
    
    krx_fwdg_ord_orgno: str = Field(description="한국거래소전송주문조직번호")
    odno: str = Field(description="주문번호")
    ord_tmd: str = Field(description="주문시각")


class KISDomesticOrderReviseResponse(BaseModel):
    """KIS 주식주문(정정취소) 응답 모델
    
    한국투자증권 주식주문(정정취소) API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISDomesticOrderReviseOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="주문 결과 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasOrderReviseRequest(BaseModel):
    """KIS 해외주식 정정취소주문 요청 모델
    
    한국투자증권의 해외주식 정정취소주문 API (v1_해외주식-003)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    OVRS_EXCG_CD: str = Field(
        default="NASD", 
        description="해외거래소코드 (NASD: 나스닥, NYSE: 뉴욕, AMEX: 아멕스, SEHK: 홍콩, SHAA: 중국상해, SZAA: 중국심천, TKSE: 일본, HASE: 베트남하노이, VNSE: 베트남호치민)"
    )
    PDNO: str = Field(
        default="", 
        description="상품번호 (종목코드)"
    )
    ORGN_ODNO: str = Field(
        default="", 
        description="원주문번호 (정정 또는 취소할 원주문번호)"
    )
    RVSE_CNCL_DVSN_CD: str = Field(
        default="02", 
        description="정정취소구분코드 (01: 정정, 02: 취소)"
    )
    ORD_QTY: str = Field(
        default="0", 
        description="주문수량"
    )
    OVRS_ORD_UNPR: str = Field(
        default="0", 
        description="해외주문단가 (취소주문 시 0 입력)"
    )
    MGCO_APTM_ODNO: str = Field(
        default="", 
        description="운용사지정주문번호"
    )
    ORD_SVR_DVSN_CD: str = Field(
        default="0", 
        description="주문서버구분코드 (기본값: 0)"
    )


class KISOverseasOrderReviseOutput(BaseModel):
    """해외주식 정정취소주문 output - 주문 결과 정보"""
    
    KRX_FWDG_ORD_ORGNO: str = Field(description="한국거래소전송주문조직번호 (주문시 한국투자증권 시스템에서 지정된 영업점코드)")
    ODNO: str = Field(description="주문번호 (주문시 한국투자증권 시스템에서 채번된 주문번호)")
    ORD_TMD: str = Field(description="주문시각 (시분초 HHMMSS)")


class KISOverseasOrderReviseResponse(BaseModel):
    """KIS 해외주식 정정취소주문 응답 모델
    
    한국투자증권 해외주식 정정취소주문 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISOverseasOrderReviseOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="주문 결과 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasPeriodPriceRequest(BaseModel):
    """KIS 해외주식 기간별시세 요청 모델
    
    한국투자증권의 해외주식 기간별시세 API (v1_해외주식-010)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    AUTH: str = Field(
        default="", 
        description="사용자권한정보 (기본값: 공백)"
    )
    EXCD: str = Field(
        default="NAS", 
        description="거래소코드 (HKS: 홍콩, NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, TSE: 도쿄, SHS: 상해, SZS: 심천, SHI: 상해지수, SZI: 심천지수, HSX: 호치민, HNX: 하노이)"
    )
    SYMB: str = Field(
        default="AAPL", 
        description="종목코드 (예: AAPL)"
    )
    GUBN: str = Field(
        default="0", 
        description="일/주/월구분 (0: 일, 1: 주, 2: 월)"
    )
    BYMD: str = Field(
        default="", 
        description="조회기준일자 (YYYYMMDD, 공란 시 오늘 날짜)"
    )
    MODP: str = Field(
        default="0", 
        description="수정주가반영여부 (0: 미반영, 1: 반영)"
    )
    KEYB: str = Field(
        default="", 
        description="NEXT KEY BUFF (다음 조회시 응답값 그대로 설정)"
    )


class KISOverseasPeriodPriceOutput1(BaseModel):
    """해외주식 기간별시세 output1 - 기본 정보"""
    
    rsym: str = Field(description="실시간조회종목코드 (D+시장구분(3자리)+종목코드, 예: DNASAAPL)")
    zdiv: str = Field(description="소수점자리수")
    nrec: str = Field(description="전일종가")


class KISOverseasPeriodPriceOutput2(BaseModel):
    """해외주식 기간별시세 output2 - 일자별 시세 정보"""
    
    xymd: str = Field(description="일자 (YYYYMMDD)")
    clos: str = Field(description="종가 (해당 일자의 종가)")
    sign: str = Field(description="대비기호 (1: 상한, 2: 상승, 3: 보합, 4: 하한, 5: 하락)")
    diff: str = Field(description="대비 (해당일 종가 - 해당 전일 종가)")
    rate: str = Field(description="등락율 (해당 전일 대비 / 해당일 종가 * 100)")
    open: str = Field(description="시가 (해당일 최초 거래가격)")
    high: str = Field(description="고가 (해당일 가장 높은 거래가격)")
    low: str = Field(description="저가 (해당일 가장 낮은 거래가격)")
    tvol: str = Field(description="거래량 (해당일 거래량)")
    tamt: str = Field(description="거래대금 (해당일 거래대금)")
    pbid: str = Field(description="매수호가 (마지막 체결 시점의 매수호가, 거래량 0인 경우 미수신)")
    vbid: str = Field(description="매수호가잔량 (거래량 0인 경우 미수신)")
    pask: str = Field(description="매도호가 (마지막 체결 시점의 매도호가, 거래량 0인 경우 미수신)")
    vask: str = Field(description="매도호가잔량 (거래량 0인 경우 미수신)")


class KISOverseasPeriodPriceResponse(BaseModel):
    """KIS 해외주식 기간별시세 응답 모델
    
    한국투자증권 해외주식 기간별시세 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[KISOverseasPeriodPriceOutput1, Dict[str, Any]]] = Field(
        default=None, 
        description="기본 정보 (실시간조회종목코드, 소수점자리수, 전일종가)"
    )
    output2: Optional[Union[List[KISOverseasPeriodPriceOutput2], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="일자별 시세 정보 리스트 (일자, 종가, 시가, 고가, 저가, 거래량 등)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasCurrentPriceRequest(BaseModel):
    """KIS 해외주식 현재가상세 요청 모델
    
    한국투자증권의 해외주식 현재가상세 API (v1_해외주식-029)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    AUTH: str = Field(
        default="", 
        description="사용자권한정보"
    )
    EXCD: str = Field(
        default="NAS", 
        description="거래소명 (HKS: 홍콩, NYS: 뉴욕, NAS: 나스닥, AMS: 아멕스, TSE: 도쿄, SHS: 상해, SZS: 심천, SHI: 상해지수, SZI: 심천지수, HSX: 호치민, HNX: 하노이, BAY: 뉴욕주간, BAQ: 나스닥주간, BAA: 아멕스주간)"
    )
    SYMB: str = Field(
        default="AAPL", 
        description="종목코드 (예: AAPL)"
    )


class KISOverseasCurrentPriceOutput(BaseModel):
    """해외주식 현재가상세 output - 상세 정보"""
    
    rsym: str = Field(description="실시간조회종목코드")
    pvol: str = Field(description="전일거래량")
    open: str = Field(description="시가")
    high: str = Field(description="고가")
    low: str = Field(description="저가")
    last: str = Field(description="현재가")
    base: str = Field(description="전일종가")
    tomv: str = Field(description="시가총액")
    pamt: str = Field(description="전일거래대금")
    uplp: str = Field(description="상한가")
    dnlp: str = Field(description="하한가")
    h52p: str = Field(description="52주최고가")
    h52d: str = Field(description="52주최고일자")
    l52p: str = Field(description="52주최저가")
    l52d: str = Field(description="52주최저일자")
    perx: str = Field(description="PER")
    pbrx: str = Field(description="PBR")
    epsx: str = Field(description="EPS")
    bpsx: str = Field(description="BPS")
    shar: str = Field(description="상장주수")
    mcap: str = Field(description="자본금")
    curr: str = Field(description="통화")
    zdiv: str = Field(description="소수점자리수")
    vnit: str = Field(description="매매단위")
    t_xprc: str = Field(description="원환산당일가격")
    t_xdif: str = Field(description="원환산당일대비")
    t_xrat: str = Field(description="원환산당일등락")
    p_xprc: str = Field(description="원환산전일가격")
    p_xdif: str = Field(description="원환산전일대비")
    p_xrat: str = Field(description="원환산전일등락")
    t_rate: str = Field(description="당일환율")
    p_rate: str = Field(description="전일환율")
    t_xsgn: str = Field(description="원환산당일기호 (HTS 색상표시용)")
    p_xsng: str = Field(description="원환산전일기호 (HTS 색상표시용)")
    e_ordyn: str = Field(description="거래가능여부")
    e_hogau: str = Field(description="호가단위")
    e_icod: str = Field(description="업종(섹터)")
    e_parp: str = Field(description="액면가")
    tvol: str = Field(description="거래량")
    tamt: str = Field(description="거래대금")
    etyp_nm: str = Field(description="ETP 분류명")


class KISOverseasCurrentPriceResponse(BaseModel):
    """KIS 해외주식 현재가상세 응답 모델
    
    한국투자증권 해외주식 현재가상세 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISOverseasCurrentPriceOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="현재가 상세 정보 (40개 이상의 풍부한 필드)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasNewsRequest(BaseModel):
    """KIS 해외뉴스종합(제목) 요청 모델
    
    한국투자증권의 해외뉴스종합(제목) API (해외주식-053)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    INFO_GB: str = Field(
        default="", 
        description="뉴스구분 (전체: 공백)"
    )
    CLASS_CD: str = Field(
        default="", 
        description="중분류 (전체: 공백)"
    )
    NATION_CD: str = Field(
        default="", 
        description="국가코드 (전체: 공백, CN: 중국, HK: 홍콩, US: 미국)"
    )
    EXCHANGE_CD: str = Field(
        default="", 
        description="거래소코드 (전체: 공백)"
    )
    SYMB: str = Field(
        default="", 
        description="종목코드 (전체: 공백)"
    )
    DATA_DT: str = Field(
        default="", 
        description="조회일자 (전체: 공백, 특정일자: YYYYMMDD 예: 20240502)"
    )
    DATA_TM: str = Field(
        default="", 
        description="조회시간 (전체: 공백, 특정시간: HHMMSS 예: 093500)"
    )
    CTS: str = Field(
        default="", 
        description="다음키 (공백 입력)"
    )


class KISOverseasNewsOutput(BaseModel):
    """해외뉴스종합 outblock1 - 뉴스 정보"""
    
    info_gb: str = Field(description="뉴스구분")
    news_key: str = Field(description="뉴스키")
    data_dt: str = Field(description="조회일자")
    data_tm: str = Field(description="조회시간")
    class_cd: str = Field(description="중분류")
    class_name: str = Field(description="중분류명")
    source: str = Field(description="자료원")
    nation_cd: str = Field(description="국가코드")
    exchange_cd: str = Field(description="거래소코드")
    symb: str = Field(description="종목코드")
    symb_name: str = Field(description="종목명")
    title: str = Field(description="제목")


class KISOverseasNewsResponse(BaseModel):
    """KIS 해외뉴스종합(제목) 응답 모델
    
    한국투자증권 해외뉴스종합(제목) API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    outblock1: Optional[Union[List[KISOverseasNewsOutput], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="뉴스 정보 리스트 (뉴스키, 일자, 시간, 중분류, 자료원, 국가, 거래소, 종목, 제목)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISDomesticNewsRequest(BaseModel):
    """KIS 종합 시황/공시(제목) 요청 모델
    
    한국투자증권의 종합 시황/공시(제목) API (국내주식-141)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    FID_NEWS_OFER_ENTP_CODE: str = Field(
        default="", 
        description="뉴스 제공 업체 코드 (공백 필수 입력)"
    )
    FID_COND_MRKT_CLS_CODE: str = Field(
        default="", 
        description="조건 시장 구분 코드 (공백 필수 입력)"
    )
    FID_INPUT_ISCD: str = Field(
        default="", 
        description="입력 종목코드 (공백: 전체, 종목코드: 해당코드가 등록된 뉴스)"
    )
    FID_TITL_CNTT: str = Field(
        default="", 
        description="제목 내용 (공백 필수 입력)"
    )
    FID_INPUT_DATE_1: str = Field(
        default="", 
        description="입력 날짜 (공백: 현재기준, 조회일자: 00YYYYMMDD)"
    )
    FID_INPUT_HOUR_1: str = Field(
        default="", 
        description="입력 시간 (공백: 현재기준, 조회시간: 0000HHMMSS)"
    )
    FID_RANK_SORT_CLS_CODE: str = Field(
        default="", 
        description="순위 정렬 구분 코드 (공백 필수 입력)"
    )
    FID_INPUT_SRNO: str = Field(
        default="", 
        description="입력 일련번호 (공백 필수 입력)"
    )


class KISDomesticNewsOutput(BaseModel):
    """종합 시황/공시 output - 뉴스 정보"""
    
    cntt_usiq_srno: str = Field(description="내용 조회용 일련번호")
    news_ofer_entp_code: str = Field(description="뉴스 제공 업체 코드 (2: 한경, 4: 이데일리, 5: 머니투데이, 6: 연합뉴스, F: 장내공시 등)")
    data_dt: str = Field(description="작성일자")
    data_tm: str = Field(description="작성시간")
    hts_pbnt_titl_cntt: str = Field(description="HTS 공시 제목 내용")
    news_lrdv_code: str = Field(description="뉴스 대구분 (1:0:종합, 1:FGHIN:공시, 2:F:거래소 등)")
    dorg: str = Field(description="자료원")
    iscd1: str = Field(description="종목 코드1")
    iscd2: str = Field(description="종목 코드2")
    iscd3: str = Field(description="종목 코드3")
    iscd4: str = Field(description="종목 코드4")
    iscd5: str = Field(description="종목 코드5")


class KISDomesticNewsResponse(BaseModel):
    """KIS 종합 시황/공시(제목) 응답 모델
    
    한국투자증권 종합 시황/공시(제목) API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[List[KISDomesticNewsOutput], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="뉴스 정보 리스트 (일련번호, 업체코드, 일자, 시간, 제목, 대구분, 자료원, 종목코드1-5)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasProfitRequest(BaseModel):
    """KIS 해외주식 기간손익 요청 모델
    
    한국투자증권의 해외주식 기간손익 API (v1_해외주식-032)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    조회시작일자는 하루전, 조회종료일자는 오늘로 기본 설정됩니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    OVRS_EXCG_CD: str = Field(
        default="", 
        description="해외거래소코드 (공란: 전체, NASD: 미국, SEHK: 홍콩, SHAA: 중국, TKSE: 일본, HASE: 베트남)"
    )
    NATN_CD: str = Field(
        default="", 
        description="국가코드 (공란: Default)"
    )
    CRCY_CD: str = Field(
        default="", 
        description="통화코드 (공란: 전체, USD: 미국달러, HKD: 홍콩달러, CNY: 중국위안화, JPY: 일본엔화, VND: 베트남동)"
    )
    PDNO: str = Field(
        default="", 
        description="상품번호 (공란: 전체)"
    )
    INQR_STRT_DT: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        description="조회시작일자 (YYYYMMDD 형식, 기본값: 하루전)"
    )
    INQR_END_DT: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="조회종료일자 (YYYYMMDD 형식, 기본값: 오늘)"
    )
    WCRC_FRCR_DVSN_CD: str = Field(
        default="01", 
        description="원화외화구분코드 (01: 외화, 02: 원화)"
    )
    CTX_AREA_FK200: str = Field(
        default="", 
        description="연속조회검색조건200"
    )
    CTX_AREA_NK200: str = Field(
        default="", 
        description="연속조회키200"
    )


class KISOverseasProfitOutput1(BaseModel):
    """해외주식 기간손익 output1 - 개별 종목별 기간손익 정보"""
    
    trad_day: str = Field(description="매매일")
    ovrs_pdno: str = Field(description="해외상품번호")
    ovrs_item_name: str = Field(description="해외종목명")
    slcl_qty: str = Field(description="매도청산수량")
    pchs_avg_pric: str = Field(description="매입평균가격")
    frcr_pchs_amt1: str = Field(description="외화매입금액1")
    avg_sll_unpr: str = Field(description="평균매도단가")
    frcr_sll_amt_smtl1: str = Field(description="외화매도금액합계1")
    stck_sll_tlex: str = Field(description="주식매도제비용")
    ovrs_rlzt_pfls_amt: str = Field(description="해외실현손익금액")
    pftrt: str = Field(description="수익률")
    exrt: str = Field(description="환율")
    ovrs_excg_cd: str = Field(description="해외거래소코드")
    frst_bltn_exrt: str = Field(description="최초고시환율")


class KISOverseasProfitOutput2(BaseModel):
    """해외주식 기간손익 output2 - 전체 요약 정보"""
    
    stck_sll_amt_smtl: str = Field(description="주식매도금액합계 (WCRC_FRCR_DVSN_CD가 01이고 OVRS_EXCG_CD가 공란인 경우 출력값 무시)")
    stck_buy_amt_smtl: str = Field(description="주식매수금액합계 (WCRC_FRCR_DVSN_CD가 01이고 OVRS_EXCG_CD가 공란인 경우 출력값 무시)")
    smtl_fee1: str = Field(description="합계수수료1 (WCRC_FRCR_DVSN_CD가 01이고 OVRS_EXCG_CD가 공란인 경우 출력값 무시)")
    excc_dfrm_amt: str = Field(description="정산지급금액 (WCRC_FRCR_DVSN_CD가 01이고 OVRS_EXCG_CD가 공란인 경우 출력값 무시)")
    ovrs_rlzt_pfls_tot_amt: str = Field(description="해외실현손익총금액 (WCRC_FRCR_DVSN_CD가 01이고 OVRS_EXCG_CD가 공란인 경우 출력값 무시)")
    tot_pftrt: str = Field(description="총수익률")
    bass_dt: str = Field(description="기준일자")
    exrt: str = Field(description="환율")


class KISOverseasProfitResponse(BaseModel):
    """KIS 해외주식 기간손익 응답 모델
    
    한국투자증권 해외주식 기간손익 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[List[KISOverseasProfitOutput1], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="개별 종목별 기간손익 정보 배열"
    )
    output2: Optional[Union[KISOverseasProfitOutput2, Dict[str, Any]]] = Field(
        default=None, 
        description="전체 요약 정보"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISOverseasOrderRequest(BaseModel):
    """KIS 해외주식 주문체결내역 요청 모델
    
    한국투자증권의 해외주식 주문체결내역 API (v1_해외주식-007)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    주문시작일자는 하루전, 주문종료일자는 오늘로 기본 설정됩니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    PDNO: str = Field(
        default="%", 
        description="상품번호 (전종목일 경우 '%' 입력, 모의투자계좌는 ''(전체 조회)만 가능)"
    )
    ORD_STRT_DT: str = Field(
        default_factory=lambda: (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        description="주문시작일자 (YYYYMMDD 형식, 현지시각 기준, 기본값: 하루전)"
    )
    ORD_END_DT: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="주문종료일자 (YYYYMMDD 형식, 현지시각 기준, 기본값: 오늘)"
    )
    SLL_BUY_DVSN: str = Field(
        default="00", 
        description="매도매수구분 (00: 전체, 01: 매도, 02: 매수, 모의투자계좌는 00만 가능)"
    )
    CCLD_NCCS_DVSN: str = Field(
        default="00", 
        description="체결미체결구분 (00: 전체, 01: 체결, 02: 미체결, 모의투자계좌는 00만 가능)"
    )
    OVRS_EXCG_CD: str = Field(
        default="%", 
        description="해외거래소코드 (전종목일 경우 '%' 입력, 모의투자계좌는 ''(전체 조회)만 가능)"
    )
    SORT_SQN: str = Field(
        default="DS", 
        description="정렬순서 (DS: 정순, AS: 역순, 모의투자계좌는 정렬순서 사용불가)"
    )
    ORD_DT: str = Field(
        default="", 
        description="주문일자 (반드시 ''(Null 값 설정))"
    )
    ORD_GNO_BRNO: str = Field(
        default="", 
        description="주문채번지점번호 (반드시 ''(Null 값 설정))"
    )
    ODNO: str = Field(
        default="", 
        description="주문번호 (주문번호로 검색 불가능, 반드시 ''(Null 값 설정))"
    )
    CTX_AREA_NK200: str = Field(
        default="", 
        description="연속조회키200 (공란: 최초 조회시, 이전 조회 Output CTX_AREA_NK200 값: 다음페이지 조회시)"
    )
    CTX_AREA_FK200: str = Field(
        default="", 
        description="연속조회검색조건200 (공란: 최초 조회시, 이전 조회 Output CTX_AREA_FK200 값: 다음페이지 조회시)"
    )


class KISOverseasOrderOutput(BaseModel):
    """해외주식 주문체결내역 output - 주문체결내역 상세 정보"""
    
    ord_dt: str = Field(description="주문일자 (주문접수 일자, 현지시각 기준)")
    ord_gno_brno: str = Field(description="주문채번지점번호 (계좌 개설 시 관리점으로 선택한 영업점의 고유번호)")
    odno: str = Field(description="주문번호 (접수한 주문의 일련번호, 정정취소주문 시 해당 값 사용)")
    orgn_odno: str = Field(description="원주문번호 (정정 또는 취소 대상 주문의 일련번호)")
    sll_buy_dvsn_cd: str = Field(description="매도매수구분코드 (01: 매도, 02: 매수)")
    sll_buy_dvsn_cd_name: str = Field(description="매도매수구분코드명")
    rvse_cncl_dvsn: str = Field(description="정정취소구분 (01: 정정, 02: 취소)")
    rvse_cncl_dvsn_name: str = Field(description="정정취소구분명")
    pdno: str = Field(description="상품번호")
    prdt_name: str = Field(description="상품명")
    ft_ord_qty: str = Field(description="FT주문수량 (주문수량)")
    ft_ord_unpr3: str = Field(description="FT주문단가3 (주문가격)")
    ft_ccld_qty: str = Field(description="FT체결수량 (체결된 수량)")
    ft_ccld_unpr3: str = Field(description="FT체결단가3 (체결된 가격)")
    ft_ccld_amt3: str = Field(description="FT체결금액3 (체결된 금액)")
    nccs_qty: str = Field(description="미체결수량")
    prcs_stat_name: str = Field(description="처리상태명 (완료, 거부, 전송)")
    rjct_rson: str = Field(description="거부사유 (정상 처리되지 못하고 거부된 주문의 사유)")
    rjct_rson_name: str = Field(description="거부사유명")
    ord_tmd: str = Field(description="주문시각 (주문 접수 시간)")
    tr_mket_name: str = Field(description="거래시장명")
    tr_natn: str = Field(description="거래국가")
    tr_natn_name: str = Field(description="거래국가명")
    ovrs_excg_cd: str = Field(description="해외거래소코드 (NASD: 나스닥, NYSE: 뉴욕, AMEX: 아멕스, SEHK: 홍콩, SHAA: 중국상해, SZAA: 중국심천, TKSE: 일본, HASE: 베트남 하노이, VNSE: 베트남 호치민)")
    tr_crcy_cd: str = Field(description="거래통화코드")
    dmst_ord_dt: str = Field(description="국내주문일자")
    thco_ord_tmd: str = Field(description="당사주문시각")
    loan_type_cd: str = Field(description="대출유형코드 (00: 해당사항없음, 01: 자기융자일반형, 03: 자기융자투자형, 05: 유통융자일반형, 06: 유통융자투자형, 07: 자기대주, 09: 유통대주, 10: 현금, 11: 주식담보대출, 12: 수익증권담보대출, 13: ELS담보대출, 14: 채권담보대출, 15: 해외주식담보대출, 16: 기업신용공여, 31: 소액자동담보대출, 41: 매도담보대출, 42: 환매자금대출, 43: 매입환매자금대출, 44: 대여매도담보대출, 81: 대차거래, 82: 법인CMA론, 91: 공모주청약자금대출, 92: 매입자금, 93: 미수론서비스, 94: 대여)")
    loan_dt: str = Field(description="대출일자")
    mdia_dvsn_name: str = Field(description="매체구분명 (ex) OpenAPI, 모바일)")
    usa_amk_exts_rqst_yn: str = Field(description="미국애프터마켓연장신청여부 (Y/N)")
    splt_buy_attr_name: str = Field(description="분할매수/매도속성명 (정규장 종료 주문 시에는 '정규장 종료', 시간 입력 시에는 from ~ to 시간 표시)")


class KISOverseasOrderResponse(BaseModel):
    """KIS 해외주식 주문체결내역 응답 모델
    
    한국투자증권 해외주식 주문체결내역 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    ctx_area_fk200: str = Field(description="연속조회검색조건200")
    ctx_area_nk200: str = Field(description="연속조회키200")
    output: Optional[Union[List[KISOverseasOrderOutput], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="주문체결내역 상세 정보 배열"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISBalanceInquiryRequest(BaseModel):
    """KIS 주식잔고조회_실현손익 요청 모델
    
    한국투자증권의 주식잔고조회_실현손익 API (v1_국내주식-041)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    """
    
    CANO: str = Field(
        default="00000000", 
        description="종합계좌번호 (계좌번호 체계 8-2의 앞 8자리, 예: 00000000)"
    )
    ACNT_PRDT_CD: str = Field(
        default="01", 
        description="계좌상품코드 (계좌번호 체계 8-2의 뒤 2자리, 예: 01)"
    )
    AFHR_FLPR_YN: str = Field(
        default="N", 
        description="시간외단일가여부 (N: 기본값, Y: 시간외단일가)"
    )
    OFL_YN: str = Field(
        default="", 
        description="오프라인여부 (공란: 기본값)"
    )
    INQR_DVSN: str = Field(
        default="00", 
        description="조회구분 (00: 전체, 기본값)"
    )
    UNPR_DVSN: str = Field(
        default="01", 
        description="단가구분 (01: 기본값)"
    )
    FUND_STTL_ICLD_YN: str = Field(
        default="N", 
        description="펀드결제포함여부 (N: 포함하지 않음, Y: 포함, 기본값: N)"
    )
    FNCG_AMT_AUTO_RDPT_YN: str = Field(
        default="N", 
        description="융자금액자동상환여부 (N: 기본값)"
    )
    PRCS_DVSN: str = Field(
        default="00", 
        description="PRCS_DVSN (00: 전일매매포함, 01: 전일매매미포함, 기본값: 00)"
    )
    COST_ICLD_YN: str = Field(
        default="", 
        description="비용포함여부 (공란: 기본값)"
    )
    CTX_AREA_FK100: str = Field(
        default="", 
        description="연속조회검색조건100 (공란: 최초 조회시, 이전 조회 Output CTX_AREA_FK100 값: 다음페이지 조회시)"
    )
    CTX_AREA_NK100: str = Field(
        default="", 
        description="연속조회키100 (공란: 최초 조회시, 이전 조회 Output CTX_AREA_NK100 값: 다음페이지 조회시)"
    )


class KISBalanceInquiryResponse(BaseModel):
    """KIS 주식잔고조회_실현손익 응답 모델
    
    한국투자증권 주식잔고조회_실현손익 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 1: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output1: Optional[Union[List[KISBalanceOutput1], List[Dict[str, Any]]]] = Field(
        default=None, 
        description="주식잔고 상세 정보 배열 (개별 종목별 잔고 및 손익 정보)"
    )
    output2: Optional[Union[List[KISBalanceOutput2], List[Dict[str, Any]], Dict[str, Any]]] = Field(
        default=None, 
        description="잔고 요약 정보 (배열 또는 딕셔너리 형태로 반환)"
    )
    ctx_area_fk100: Optional[str] = Field(
        default=None, 
        description="연속조회검색조건100 (다음 페이지 조회시 사용)"
    )
    ctx_area_nk100: Optional[str] = Field(
        default=None, 
        description="연속조회키100 (다음 페이지 조회시 사용)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISTestResponse(BaseModel):
    """KIS 테스트 응답 모델
    
    KIS API 테스트의 일반적인 응답을 담는 모델입니다.
    """
    
    success: bool = Field(description="테스트 성공 여부")
    data: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="응답 데이터"
    )
    error: Optional[str] = Field(
        default=None, 
        description="에러 메시지 (실패시)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터"
    )


class KISEnvironment(str, Enum):
    """KIS 환경 타입"""
    REAL = "real"  # 실전
    VIRTUAL = "virtual"  # 모의투자


class KISAPITestRequest(BaseModel):
    """KIS API 테스트 요청 모델
    
    범용적인 KIS API 테스트를 위한 요청 모델입니다.
    """
    
    api_name: str = Field(description="테스트할 API 이름 (예: balance_inquiry_rlz_pl)")
    environment: KISEnvironment = Field(
        default=KISEnvironment.REAL, 
        description="테스트 환경 (real: 실전, virtual: 모의투자)"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="API별 파라미터 (선택사항)"
    )


class KISAPITestResponse(BaseModel):
    """KIS API 테스트 응답 모델
    
    범용적인 KIS API 테스트의 응답을 담는 모델입니다.
    """
    
    api_name: str = Field(description="테스트한 API 이름")
    environment: str = Field(description="테스트 환경")
    success: bool = Field(description="API 호출 성공 여부")
    response_data: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="API 응답 데이터"
    )
    error: Optional[str] = Field(
        default=None, 
        description="에러 메시지 (실패시)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )


class KISTokenRefreshRequest(BaseModel):
    """KIS 토큰 일괄 갱신 요청 모델
    
    provider가 KIS인 토큰들 중 만료가 임박한 토큰들을 일괄로 갱신합니다.
    """
    
    threshold_hours: int = Field(
        default=24,
        ge=1,
        le=168,
        description="갱신 임계 시간 (시간 단위, 기본값: 24시간, 최대: 168시간=7일)"
    )
    provider: str = Field(
        default="KIS",
        description="토큰 제공업체 (기본값: KIS, KIS_VIRTUAL 지원)"
    )


class KISTokenRefreshResult(BaseModel):
    """개별 토큰 갱신 결과"""
    
    tr_id: str = Field(description="거래 ID")
    base_url: str = Field(description="기본 URL")
    success: bool = Field(description="갱신 성공 여부")
    old_expires_at: Optional[str] = Field(default=None, description="이전 만료일시 (UTC)")
    new_expires_at: Optional[str] = Field(default=None, description="새로운 만료일시 (UTC)")
    error_message: Optional[str] = Field(default=None, description="오류 메시지 (실패 시)")


class KISTokenRefreshResponse(BaseModel):
    """KIS 토큰 일괄 갱신 응답 모델"""
    
    total_tokens: int = Field(description="갱신 대상 토큰 총 개수")
    success_count: int = Field(description="갱신 성공 개수")
    failure_count: int = Field(description="갱신 실패 개수")
    threshold_hours: int = Field(description="사용된 임계 시간 (시간 단위)")
    threshold_datetime: str = Field(description="임계 기준 일시 (UTC)")
    results: List[KISTokenRefreshResult] = Field(description="개별 토큰 갱신 결과 목록")


class BootstrapStepResult(BaseModel):
    """Bootstrap 단계별 실행 결과"""
    
    step_name: str = Field(description="단계 이름")
    step_description: str = Field(description="단계 설명")
    success: bool = Field(description="성공 여부")
    duration_seconds: Optional[float] = Field(default=None, description="실행 시간 (초)")
    result_summary: Optional[Dict[str, Any]] = Field(default=None, description="결과 요약")
    error_message: Optional[str] = Field(default=None, description="오류 메시지 (실패 시)")


class BootstrapRequest(BaseModel):
    """장전 기초데이터 일괄 갱신 요청 모델"""
    
    skip_token_refresh: bool = Field(
        default=False,
        description="토큰 갱신 단계 스킵 여부 (기본값: False)"
    )
    skip_fred_ingest: bool = Field(
        default=False,
        description="FRED 데이터 수집 단계 스킵 여부 (기본값: False)"
    )
    skip_yahoo_ingest: bool = Field(
        default=False,
        description="Yahoo Finance 데이터 수집 단계 스킵 여부 (기본값: False)"
    )
    skip_risk_refresh: bool = Field(
        default=False,
        description="프리마켓 리스크 스냅샷 갱신 단계 스킵 여부 (기본값: False)"
    )
    skip_signal_update: bool = Field(
        default=False,
        description="시그널 갱신 단계 스킵 여부 (기본값: False)"
    )
    token_threshold_hours: int = Field(
        default=24,
        ge=1,
        le=168,
        description="토큰 갱신 임계 시간 (시간 단위, 기본값: 24시간)"
    )
    fred_lookback_days: int = Field(
        default=30,
        ge=1,
        le=365,
        description="FRED 데이터 수집 기간 (일 단위, 기본값: 30일)"
    )
    yahoo_period: str = Field(
        default="1mo",
        description="Yahoo Finance 데이터 수집 기간 (기본값: 1mo)"
    )


class BootstrapResponse(BaseModel):
    """장전 기초데이터 일괄 갱신 응답 모델"""
    
    overall_success: bool = Field(description="전체 작업 성공 여부")
    total_steps: int = Field(description="전체 단계 수")
    successful_steps: int = Field(description="성공한 단계 수")
    failed_steps: int = Field(description="실패한 단계 수")
    skipped_steps: int = Field(description="스킵한 단계 수")
    total_duration_seconds: float = Field(description="전체 실행 시간 (초)")
    started_at: str = Field(description="시작 시간 (UTC)")
    completed_at: str = Field(description="완료 시간 (UTC)")
    steps: List[BootstrapStepResult] = Field(description="각 단계별 실행 결과")


class KISDomesticHolidayRequest(BaseModel):
    """KIS 국내휴장일조회 요청 모델
    
    한국투자증권의 국내휴장일조회 API (국내주식-040)를 테스트하기 위한 요청 모델입니다.
    모든 파라미터는 KIS API 문서 스펙에 맞는 기본값이 설정되어 있습니다.
    기준일자는 오늘로 기본 설정됩니다.
    """
    
    BASS_DT: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d"),
        description="기준일자 (YYYYMMDD 형식, 기본값: 오늘)"
    )
    CTX_AREA_NK: str = Field(
        default="", 
        description="연속조회키 (공백으로 입력, tr_cont를 이용한 다음조회 불가 API)"
    )
    CTX_AREA_FK: str = Field(
        default="", 
        description="연속조회검색조건 (공백으로 입력, tr_cont를 이용한 다음조회 불가 API)"
    )


class KISDomesticHolidayOutput(BaseModel):
    """국내휴장일조회 output - 휴장일 정보"""
    
    bass_dt: str = Field(description="기준일자 (YYYYMMDD)")
    wday_dvsn_cd: str = Field(description="요일구분코드 (01:일요일, 02:월요일, 03:화요일, 04:수요일, 05:목요일, 06:금요일, 07:토요일)")
    bzdy_yn: str = Field(description="영업일여부 (Y/N) - 금융기관이 업무를 하는 날")
    tr_day_yn: str = Field(description="거래일여부 (Y/N) - 증권 업무가 가능한 날(입출금, 이체 등의 업무 포함)")
    opnd_yn: str = Field(description="개장일여부 (Y/N) - 주식시장이 개장되는 날, 주문을 넣고자 할 경우 개장일여부(opnd_yn)를 사용")
    sttl_day_yn: str = Field(description="결제일여부 (Y/N) - 주식 거래에서 실제로 주식을 인수하고 돈을 지불하는 날")


class KISDomesticHolidayResponse(BaseModel):
    """KIS 국내휴장일조회 응답 모델
    
    한국투자증권 국내휴장일조회 API의 응답을 담는 모델입니다.
    KIS API의 원본 응답을 그대로 보존하여 전달합니다.
    """
    
    rt_cd: str = Field(description="성공 실패 여부 (0: 성공, 0 이외의 값: 실패)")
    msg_cd: str = Field(description="응답코드 (8자리)")
    msg1: str = Field(description="응답메시지 (최대 80자)")
    output: Optional[Union[KISDomesticHolidayOutput, Dict[str, Any]]] = Field(
        default=None, 
        description="휴장일 정보 (기준일자, 요일구분코드, 영업일여부, 거래일여부, 개장일여부, 결제일여부)"
    )
    raw_response: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="KIS API 원본 응답 데이터 (완전한 원본 보존)"
    )

