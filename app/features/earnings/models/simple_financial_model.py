# app/features/earnings/models/simple_financial_model.py
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, List, Any
from datetime import datetime


class SimpleFinancialRequest(BaseModel):
    """간단한 재무 데이터 요청 모델"""
    corp_code: str = Field(..., description="기업고유번호 (8자리)")
    bsns_year: str = Field(..., description="사업연도 (4자리)")
    fs_div: str = Field(default="CFS", description="개별/연결구분 (OFS:재무제표, CFS:연결재무제표)")
    
    @validator('corp_code')
    def corp_code_must_be_8_digits(cls, v):
        if not v.isdigit() or len(v) != 8:
            raise ValueError('기업고유번호는 8자리 숫자여야 합니다')
        return v
    
    @validator('bsns_year')
    def bsns_year_must_be_4_digits(cls, v):
        if not v.isdigit() or len(v) != 4:
            raise ValueError('사업연도는 4자리 숫자여야 합니다')
        return v
    
    @validator('fs_div')
    def fs_div_must_be_valid(cls, v):
        if v not in ['OFS', 'CFS']:
            raise ValueError('fs_div는 OFS 또는 CFS여야 합니다')
        return v


class AccountData(BaseModel):
    """개별 계정 데이터"""
    account_nm: str = Field(..., description="계정명")
    account_id: str = Field(..., description="계정ID")
    sj_div: str = Field(..., description="재무제표 구분")
    sj_nm: str = Field(..., description="재무제표명")
    
    # 금액 데이터 (DART 원본)
    thstrm_nm: Optional[str] = Field(None, description="당기명")
    thstrm_amount: Optional[str] = Field(None, description="당기금액")
    thstrm_add_amount: Optional[str] = Field(None, description="당기누적금액")
    
    frmtrm_nm: Optional[str] = Field(None, description="전기명")
    frmtrm_amount: Optional[str] = Field(None, description="전기금액")
    frmtrm_add_amount: Optional[str] = Field(None, description="전기누적금액")
    
    # 사업보고서의 경우 전전기 데이터
    bfefrmtrm_nm: Optional[str] = Field(None, description="전전기명")
    bfefrmtrm_amount: Optional[str] = Field(None, description="전전기금액")
    
    ord: Optional[str] = Field(None, description="계정과목 정렬순서")
    currency: Optional[str] = Field(None, description="통화 단위")


class QuarterlyFinancialData(BaseModel):
    """분기별 재무 데이터 - DART 원본 데이터 그대로"""
    reprt_code: str = Field(..., description="보고서 코드")
    quarter_name: str = Field(..., description="분기명")
    
    # 요청한 3개 계정 데이터만 반환
    revenue: Optional[AccountData] = Field(None, description="매출액 계정 데이터")
    net_income: Optional[AccountData] = Field(None, description="당기순이익(손실) 계정 데이터")
    basic_eps: Optional[AccountData] = Field(None, description="기본주당이익(손실) 계정 데이터")
    diluted_eps: Optional[AccountData] = Field(None, description="희석주당이익(손실) 계정 데이터")


class SimpleFinancialResponse(BaseModel):
    """간단한 재무 데이터 응답"""
    success: bool = Field(..., description="성공 여부")
    message: str = Field(..., description="응답 메시지")
    
    corp_info: Optional[Dict[str, str]] = Field(None, description="기업 정보")
    quarterly_data: List[QuarterlyFinancialData] = Field(default_factory=list, description="분기별 재무 데이터")
    
    # 데이터 설명
    data_explanation: Optional[Dict[str, Any]] = Field(None, description="데이터 설명")
    error_details: Optional[Dict[str, Any]] = Field(None, description="에러 상세정보")


class DARTAllResponse(BaseModel):
    """DART fnlttSinglAcntAll 응답 모델"""
    status: str = Field(..., description="응답 상태")
    message: str = Field(..., description="응답 메시지")
    list: List[Dict[str, Any]] = Field(default_factory=list, description="계정 목록")
