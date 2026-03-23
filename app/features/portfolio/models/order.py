from dataclasses import dataclass
from typing import Optional

@dataclass
class OrderRequestHeader:
    content_type: Optional[str] = "application/json; charset=utf-8"
    authorization: str = ""  # 접근토큰
    appkey: str = ""  # 앱키
    appsecret: str = ""  # 앱시크릿키
    personalseckey: Optional[str] = None  # 고객식별키
    tr_id: str = ""  # 거래ID
    tr_cont: Optional[str] = None  # 연속 거래 여부
    custtype: Optional[str] = None  # 고객타입
    seq_no: Optional[str] = None  # 일련번호
    mac_address: Optional[str] = None  # 맥주소
    phone_number: Optional[str] = None  # 핸드폰번호
    ip_addr: Optional[str] = None  # 접속 단말 공인 IP
    gt_uid: Optional[str] = None  # Global UID

@dataclass
class OrderRequestBody:
    CANO: str = ""  # 종합계좌번호
    ACNT_PRDT_CD: str = ""  # 계좌상품코드
    OVRS_EXCG_CD: str = ""  # 해외거래소코드
    PDNO: str = ""  # 상품번호
    ORD_QTY: str = ""  # 주문수량
    OVRS_ORD_UNPR: str = ""  # 해외주문단가
    CTAC_TLNO: Optional[str] = None  # 연락전화번호
    MGCO_APTM_ODNO: Optional[str] = None  # 운용사지정주문번호
    SLL_TYPE: Optional[str] = None  # 판매유형
    ORD_SVR_DVSN_CD: str = "0"  # 주문서버구분코드
    ORD_DVSN: str = "00"  # 주문구분 (00: 지정가)
    START_TIME: Optional[str] = None  # 시작시간
    END_TIME: Optional[str] = None  # 종료시간
    ALGO_ORD_TMD_DVSN_CD: Optional[str] = None  # 알고리즘주문시간구분코드

@dataclass
class OrderResponseHeader:
    content_type: str = ""  # 컨텐츠타입
    tr_id: str = ""  # 거래ID
    tr_cont: str = ""  # 연속 거래 여부
    gt_uid: str = ""  # Global UID

@dataclass
class OrderResponseBody:
    rt_cd: str = ""  # 성공 실패 여부
    msg_cd: str = ""  # 응답코드
    msg1: str = ""  # 응답메세지
    KRX_FWDG_ORD_ORGNO: str = ""  # 한국거래소전송주문조직번호
    ODNO: str = ""  # 주문번호
    ORD_TMD: str = ""  # 주문시각
