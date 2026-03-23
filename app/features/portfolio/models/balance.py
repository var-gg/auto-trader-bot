from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class RequestHeader:
    content_type: Optional[str] = None    #컨텐츠타입
    authorization: str    #접근토큰
    appkey: str    #앱키 
    appsecret: str    #앱시크릿키
    personalseckey: Optional[str] = None    #고객식별키
    tr_id: str    #거래ID
    tr_cont: Optional[str] = None    #연속 거래 여부
    custtype: Optional[str] = None    #고객타입
    seq_no: Optional[str] = None    #일련번호
    mac_address: Optional[str] = None    #맥주소
    phone_number: Optional[str] = None    #핸드폰번호
    ip_addr: Optional[str] = None    #접속 단말 공인 IP
    gt_uid: Optional[str] = None    #Global UID

@dataclass
class RequestQueryParam:
    CANO: str    #종합계좌번호
    ACNT_PRDT_CD: str    #계좌상품코드
    WCRC_FRCR_DVSN_CD: str    #원화외화구분코드
    NATN_CD: str    #국가코드
    TR_MKET_CD: str    #거래시장코드
    INQR_DVSN_CD: str    #조회구분코드

@dataclass
class ResponseHeader:
    content_type: str    #컨텐츠타입
    tr_id: str    #거래ID
    tr_cont: str    #연속 거래 여부
    gt_uid: str    #Global UID

@dataclass
class ResponseBodythdt_buy_ccld_frcr_amt:
    pass  # 빈 클래스 (필드가 없음)

@dataclass
class ResponseBodyoutput1:
    prdt_name: str    #상품명
    cblc_qty13: str    #잔고수량13
    thdt_buy_ccld_qty1: str    #당일매수체결수량1
    thdt_sll_ccld_qty1: str    #당일매도체결수량1
    ccld_qty_smtl1: str    #체결수량합계1
    ord_psbl_qty1: str    #주문가능수량1
    frcr_pchs_amt: str    #외화매입금액
    frcr_evlu_amt2: str    #외화평가금액2
    evlu_pfls_amt2: str    #평가손익금액2
    evlu_pfls_rt1: str    #평가손익율1
    pdno: str    #상품번호
    bass_exrt: str    #기준환율
    buy_crcy_cd: str    #매수통화코드
    ovrs_now_pric1: str    #해외현재가격1
    avg_unpr3: str    #평균단가3
    tr_mket_name: str    #거래시장명
    natn_kor_name: str    #국가한글명
    pchs_rmnd_wcrc_amt: str    #매입잔액원화금액
    thdt_buy_ccld_frcr_amt: ResponseBodythdt_buy_ccld_frcr_amt    #당일매수체결외화금액
    thdt_sll_ccld_frcr_amt: str    #당일매도체결외화금액
    unit_amt: str    #단위금액
    std_pdno: str    #표준상품번호
    prdt_type_cd: str    #상품유형코드
    scts_dvsn_name: str    #유가증권구분명
    loan_rmnd: str    #대출잔액
    loan_dt: str    #대출일자
    loan_expd_dt: str    #대출만기일자
    ovrs_excg_cd: str    #해외거래소코드
    item_lnkg_excg_cd: str    #종목연동거래소코드

@dataclass
class ResponseBodyoutput2:
    crcy_cd: str    #통화코드
    crcy_cd_name: str    #통화코드명
    frcr_buy_amt_smtl: str    #외화매수금액합계
    frcr_sll_amt_smtl: str    #외화매도금액합계
    frcr_dncl_amt_2: str    #외화예수금액2
    frst_bltn_exrt: str    #최초고시환율
    frcr_buy_mgn_amt: str    #외화매수증거금액
    frcr_etc_mgna: str    #외화기타증거금
    frcr_drwg_psbl_amt_1: str    #외화출금가능금액1
    frcr_evlu_amt2: str    #출금가능원화금액
    acpl_cstd_crcy_yn: str    #현지보관통화여부
    nxdy_frcr_drwg_psbl_amt: str    #익일외화출금가능금액

@dataclass
class ResponseBodyoutput3:
    pchs_amt_smtl: str    #매입금액합계
    evlu_amt_smtl: str    #평가금액합계
    evlu_pfls_amt_smtl: str    #평가손익금액합계
    dncl_amt: str    #예수금액
    cma_evlu_amt: str    #CMA평가금액
    tot_dncl_amt: str    #총예수금액
    etc_mgna: str    #기타증거금
    wdrw_psbl_tot_amt: str    #인출가능총금액
    frcr_evlu_tota: str    #외화평가총액
    evlu_erng_rt1: str    #평가수익율1
    pchs_amt_smtl_amt: str    #매입금액합계금액
    evlu_amt_smtl_amt: str    #평가금액합계금액
    tot_evlu_pfls_amt: str    #총평가손익금액
    tot_asst_amt: str    #총자산금액
    buy_mgn_amt: str    #매수증거금액
    mgna_tota: str    #증거금총액
    frcr_use_psbl_amt: str    #외화사용가능금액
    ustl_sll_amt_smtl: str    #미결제매도금액합계
    ustl_buy_amt_smtl: str    #미결제매수금액합계
    tot_frcr_cblc_smtl: str    #총외화잔고합계
    tot_loan_amt: str    #총대출금액

@dataclass
class ResponseBody:
    rt_cd: str    #성공 실패 여부
    msg_cd: str    #응답코드
    msg1: str    #응답메세지
    output1: List[ResponseBodyoutput1] = field(default_factory=list)    #응답상세1 (체결기준 잔고)
    output2: List[ResponseBodyoutput2] = field(default_factory=list)    #응답상세2
    output3: ResponseBodyoutput3    #응답상세3
