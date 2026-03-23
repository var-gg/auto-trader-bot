# app/features/earnings/models/flutter_financial_model.py
from typing import Optional
from pydantic import BaseModel


class FlutterFinancialItem(BaseModel):
    """DART 정기보고서 재무정보 개별 항목"""
    rcept_no: Optional[str] = None  # 접수번호
    bsns_year: Optional[str] = None  # 사업연도
    stock_code: Optional[str] = None  # 종목코드
    reprt_code: Optional[str] = None  # 보고서코드
    account_nm: Optional[str] = None  # 계정명
    fs_div: Optional[str] = None  # 개별/연결구분
    fs_nm: Optional[str] = None  # 개별/연결명
    sj_div: Optional[str] = None  # 재무제표구분
    sj_nm: Optional[str] = None  # 재무제표명
    thstrm_nm: Optional[str] = None  # 당기명
    thstrm_dt: Optional[str] = None  # 당기일자
    thstrm_amount: Optional[str] = None  # 당기금액
    thstrm_add_amount: Optional[str] = None  # 당기누적금액
    frmtrm_nm: Optional[str] = None  # 전기명
    frmtrm_dt: Optional[str] = None  # 전기일자
    frmtrm_amount: Optional[str] = None  # 전기금액
    frmtrm_add_amount: Optional[str] = None  # 전기누적금액
    bfefrmtrm_nm: Optional[str] = None  # 전전기명
    bfefrmtrm_dt: Optional[str] = None  # 전전기일자
    bfefrmtrm_amount: Optional[str] = None  # 전전기금액
    ord: Optional[str] = None  # 계정과목 정렬순서
    currency: Optional[str] = None  # 통화단위


class FlutterFinancialRequest(BaseModel):
    """DART 정기보고서 재무정보 API 요청 파라미터"""
    corp_code: str  # 고유번호 (8자리)
    bsns_year: str  # 사업연도 (4자리)
    reprt_code: str  # 보고서코드 (11011:사업보고서, 11012:반기보고서, 11013:1분기보고서, 11014:3분기보고서)


class FlutterFinancialResponse(BaseModel):
    """DART 정기보고서 재무정보 API 응답"""
    status: Optional[str] = None  # 에러 및 정보 코드
    msg: Optional[str] = None  # 에러 및 정보 메시지
    list: Optional[list] = None  # 재무정보 항목 리스트


class FlutterFinancialTestResponse(BaseModel):
    """테스트용 응답 - 원본 데이터 그대로 반환"""
    success: bool
    message: str
    raw_data: dict  # 원본 API 응답 데이터


class FlutterFinancialAnalytics:
    """정기보고서 재무정보 데이터 분석 유틸리티"""
    
    @staticmethod
    def validate_request_params(request: FlutterFinancialRequest) -> dict:
        """요청 파라미터 검증"""
        validation_result = {
            "is_valid": True,
            "errors": []
        }
        
        # 고유번호 검증 (8자리 숫자)
        if not request.corp_code or len(request.corp_code) != 8:
            validation_result["is_valid"] = False
            validation_result["errors"].append("corp_code must be 8 digits")
        
        # 사업연도 검증 (4자리 숫자)
        if not request.bsns_year or len(request.bsns_year) != 4:
            validation_result["is_valid"] = False
            validation_result["errors"].append("bsns_year must be 4 digits")
        
        # 보고서코드 검증
        valid_reprt_codes = ["11011", "11012", "11013", "11014"]
        if request.reprt_code not in valid_reprt_codes:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"reprt_code must be one of {valid_reprt_codes}")
        
        return validation_result
    
    @staticmethod
    def extract_key_financial_data(response_data: dict) -> dict:
        """주요 재무데이터 추출"""
        if not response_data.get("list"):
            return {"status": "NO_DATA", "data": None}
        
        financial_items = response_data["list"]
        
        # 재무상태표(BS)와 손익계산서(IS) 분리
        balance_sheet_items = [item for item in financial_items if item.get("sj_div") == "BS"]
        income_statement_items = [item for item in financial_items if item.get("sj_div") == "IS"]
        
        # 개별(OFS)과 연결(CFS) 분리
        ofs_items = [item for item in financial_items if item.get("fs_div") == "OFS"]
        cfs_items = [item for item in financial_items if item.get("fs_div") == "CFS"]
        
        summary = {
            "status": "SUCCESS",
            "data": {
                "total_items": len(financial_items),
                "balance_sheet_count": len(balance_sheet_items),
                "income_statement_count": len(income_statement_items),
                "ofs_count": len(ofs_items),
                "cfs_count": len(cfs_items),
                "sample_balance_sheet": balance_sheet_items[:3] if balance_sheet_items else [],
                "sample_income_statement": income_statement_items[:3] if income_statement_items else [],
                "report_info": {
                    "bsns_year": financial_items[0].get("bsns_year") if financial_items else None,
                    "reprt_code": financial_items[0].get("reprt_code") if financial_items else None,
                    "stock_code": financial_items[0].get("stock_code") if financial_items else None,
                }
            }
        }
        
        return summary
    
    @staticmethod
    def format_for_testing(raw_response: dict) -> dict:
        """테스트용 데이터 포맷팅"""
        return {
            "success": True,
            "message": "Raw DART API response for financial statement testing",
            "raw_data": raw_response,
            "structure_analysis": {
                "has_status": "status" in raw_response,
                "has_msg": "msg" in raw_response,
                "has_list": "list" in raw_response and raw_response["list"] is not None,
                "list_length": len(raw_response.get("list", [])),
                "status_code": raw_response.get("status"),
                "message": raw_response.get("msg"),
            },
            "sample_data": raw_response.get("list", [])[:2] if raw_response.get("list") else []
        }
