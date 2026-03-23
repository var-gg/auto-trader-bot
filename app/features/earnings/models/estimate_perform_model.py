# app/features/earnings/models/estimate_perform_model.py
from typing import List, Optional
from pydantic import BaseModel


class EstimatePerformOutput1(BaseModel):
    """output1: 기본 정보"""
    sht_cd: str  # W단축종목코드
    item_kor_nm: str  # HTS한글종목명
    name1: str  # ELW현재가
    name2: str  # 전일대비
    estdate: str  # 전일대비부호
    rcmd_name: str  # 전일대비율
    capital: str  # 누적거래량
    forn_item_lmtrt: str  # 행사가 (소문자 l 수정)


class EstimatePerformOutputItem(BaseModel):
    """output2~3의 개별 데이터 항목 (추정손익계산서, 투자지표)"""
    data1: str  # DATA1
    data2: str  # DATA2
    data3: str  # DATA3
    data4: str  # DATA4
    data5: str  # DATA5


class EstimatePerformOutput4(BaseModel):
    """output4: 결산년월 정보"""
    dt: str  # 결산년월 (DATA1~5 결산월 정보)


class EstimatePerformRequest(BaseModel):
    """실적추정 API 요청 파라미터"""
    sht_cd: str  # 종목코드


class EstimatePerformResponse(BaseModel):
    """실적추정 API 응답"""
    rt_cd: str  # 성공 실패 여부
    msg_cd: str  # 응답코드
    msg1: str  # 응답메시지
    output1: Optional[EstimatePerformOutput1] = None  # 기본 정보
    output2: Optional[List[EstimatePerformOutputItem]] = None  # 추정손익계산서 (6개 array)
    output3: Optional[List[EstimatePerformOutputItem]] = None  # 투자지표 (8개 array)
    output4: Optional[List[EstimatePerformOutput4]] = None  # 결산년월 정보


class EstimatePerformAnalytics:
    """실적추정 데이터 분석 유틸리티"""
    
    @staticmethod
    def analyze_financial_data(response: EstimatePerformResponse) -> dict:
        """실적추정 데이터 분석"""
        if not response.output1:
            return {"status": "NO_BASIC_DATA"}
        
        basic_info = {
            "symbol": response.output1.sht_cd,
            "name": response.output1.item_kor_nm,
            "current_price": response.output1.name1,
            "price_change": response.output1.name2,
            "price_change_sign": response.output1.estdate,
            "price_change_rate": response.output1.rcmd_name,
            "trading_volume": response.output1.capital,
        }
        
        analysis = {
            "status": "SUCCESS",
            "basic_info": basic_info,
            "financial_strength": response.output2 is not None,
            "investment_indicators": response.output3 is not None,
            "settlement_periods": response.output4 is not None,
        }
        
        return analysis
    
    @staticmethod
    def extract_key_metrics(response: EstimatePerformResponse) -> dict:
        """주요 지표 추출"""
        if not response.output1:
            return {}
        
        metrics = {
            "stock_code": response.output1.sht_cd,
            "stock_name": response.output1.item_kor_nm,
            "current_price": response.output1.name1,
            "price_change": response.output1.name2,
            "price_change_sign": response.output1.estdate,
            "price_change_rate": response.output1.rcmd_name,
        }
        
        # 추정손익계산서 데이터 (output2)
        if response.output2 and len(response.output2) > 0:
            metrics["income_statement"] = [
                item.__dict__ for item in response.output2
            ]
        
        # 투자지표 데이터 (output3)
        if response.output3 and len(response.output3) > 0:
            metrics["investment_metrics"] = [
                item.__dict__ for item in response.output3
            ]
        
        # 결산년월 (output4)
        if response.output4 and len(response.output4) > 0:
            metrics["settlement_months"] = [
                item.dt for item in response.output4
            ]
        
        return metrics
    
    @staticmethod
    def summarize_for_testing(response: EstimatePerformResponse) -> dict:
        """테스트용 요약 정보"""
        if response.rt_cd != '0':
            return {
                "status": "API_ERROR",
                "error_code": response.msg_cd,
                "error_message": response.msg1,
            }
        
        return {
            "status": "SUCCESS",
            "response_structure": {
                "has_output1": response.output1 is not None,
                "has_output2": response.output2 is not None,
                "has_output3": response.output3 is not None, 
                "has_output4": response.output4 is not None,
            },
            "data_summary": EstimatePerformAnalytics.extract_key_metrics(response),
        }