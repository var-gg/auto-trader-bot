# app/features/earnings/services/flutter_financial_service.py
import logging
import requests
from typing import Dict, Optional, Any
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.features.earnings.models.flutter_financial_model import (
    FlutterFinancialRequest,
    FlutterFinancialResponse,
    FlutterFinancialTestResponse,
    FlutterFinancialAnalytics
)

logger = logging.getLogger(__name__)
settings = get_settings()


class FlutterFinancialService:
    """DART 정기보고서 재무정보 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.base_url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt"
    
    def get_financial_statement_raw(self, request: FlutterFinancialRequest) -> Optional[dict]:
        """
        DART 정기보고서 재무정보 API 호출 - 원본 데이터 반환
        """
        try:
            # 파라미터 검증
            validation = FlutterFinancialAnalytics.validate_request_params(request)
            if not validation["is_valid"]:
                logger.error(f"Invalid request parameters: {validation['errors']}")
                return {
                    "status": "400",
                    "msg": f"Invalid parameters: {', '.join(validation['errors'])}",
                    "list": None
                }
            
            # API 요청 파라미터 구성
            params = {
                "crtfc_key": settings.DART_API_KEY,
                "corp_code": request.corp_code,
                "bsns_year": request.bsns_year,
                "reprt_code": request.reprt_code
            }
            
            logger.info(f"Requesting DART financial statement API with params: {params}")
            
            # DART API 호출
            response = requests.get(
                f"{self.base_url}.json",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"DART API HTTP error: {response.status_code}")
                return {
                    "status": str(response.status_code),
                    "msg": f"HTTP error: {response.status_code}",
                    "list": None
                }
            
            # JSON 응답 파싱
            data = response.json()
            logger.info(f"DART API response status: {data.get('status')}")
            
            # API 응답 검증
            if data.get("status") != "000":
                logger.warning(f"DART API returned error: {data.get('status')} - {data.get('message')}")
                return data
            
            logger.info(f"Successfully retrieved financial statement data, got {len(data.get('list', []))} items")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("DART API request timeout")
            return {
                "status": "408",
                "msg": "Request timeout",
                "list": None
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"DART API request error: {str(e)}")
            return {
                "status": "500",
                "msg": f"Request error: {str(e)}",
                "list": None
            }
        except Exception as e:
            logger.error(f"Unexpected error in get_financial_statement_raw: {str(e)}")
            return {
                "status": "999",
                "msg": f"Unexpected error: {str(e)}",
                "list": None
            }
    
    def get_financial_statement_test(self, corp_code: str, bsns_year: str, reprt_code: str) -> Optional[FlutterFinancialTestResponse]:
        """
        테스트용 정기보고서 재무정보 조회 - 원본 데이터 그대로 반환
        """
        try:
            request = FlutterFinancialRequest(
                corp_code=corp_code,
                bsns_year=bsns_year,
                reprt_code=reprt_code
            )
            
            raw_data = self.get_financial_statement_raw(request)
            
            if raw_data is None:
                return FlutterFinancialTestResponse(
                    success=False,
                    message="No data received from DART API",
                    raw_data={}
                )
            
            # 테스트용 포맷팅 적용
            test_data = FlutterFinancialAnalytics.format_for_testing(raw_data)
            
            logger.info(f"Returning test data for corp_code: {corp_code}, bsns_year: {bsns_year}, reprt_code: {reprt_code}")
            
            return FlutterFinancialTestResponse(
                success=True,
                message="Financial statement test data retrieved successfully",
                raw_data=test_data
            )
            
        except Exception as e:
            logger.error(f"Error in get_financial_statement_test: {str(e)}")
            return FlutterFinancialTestResponse(
                success=False,
                message=f"Error retrieving test data: {str(e)}",
                raw_data={}
            )
    
    def get_sample_corporations_test(self) -> Dict[str, Any]:
        """
        샘플 기업들의 재무정보 테스트
        - 삼성전자, SK하이닉스, LG전자 등의 최신 사업보고서 데이터 테스트
        """
        try:
            sample_corporations = [
                {"corp_code": "00126380", "name": "삼성전자", "stock_code": "005930"},
                {"corp_code": "00164728", "name": "SK하이닉스", "stock_code": "000660"},
                {"corp_code": "00163879", "name": "LG전자", "stock_code": "066570"},
                {"corp_code": "00356361", "name": "NAVER", "stock_code": "035420"},
            ]
            
            # 최근 3년간 사업보고서 데이터 테스트
            test_years = ["2023", "2022", "2021"]
            test_reprt_code = "11011"  # 사업보고서
            
            results = {}
            
            for corp in sample_corporations:
                corp_results = {}
                
                for year in test_years:
                    try:
                        test_data = self.get_financial_statement_test(
                            corp_code=corp["corp_code"],
                            bsns_year=year,
                            reprt_code=test_reprt_code
                        )
                        
                        corp_results[year] = {
                            "success": test_data.success if test_data else False,
                            "message": test_data.message if test_data else "No data",
                            "has_data": len(test_data.raw_data.get("raw_data", {}).get("list", [])) > 0 if test_data else False,
                            "data_count": len(test_data.raw_data.get("raw_data", {}).get("list", [])) if test_data else 0
                        }
                        
                    except Exception as e:
                        logger.warning(f"Failed to get test data for {corp['name']} ({year}): {str(e)}")
                        corp_results[year] = {
                            "success": False,
                            "message": str(e),
                            "has_data": False,
                            "data_count": 0
                        }
                
                results[f"{corp['name']}({corp['stock_code']})"] = {
                    "corp_code": corp["corp_code"],
                    "stock_code": corp["stock_code"],
                    "years": corp_results
                }
            
            return {
                "status": "COMPLETED",
                "description": "Sample corporations financial statement test completed",
                "results": results,
                "summary": {
                    "total_corporations": len(sample_corporations),
                    "test_years": test_years,
                    "report_type": "사업보고서",
                    "test_period": f"{test_years[-1]} ~ {test_years[0]}"  # 최신순으로 표시
                }
            }
            
        except Exception as e:
            logger.error(f"Error in get_sample_corporations_test: {str(e)}")
            return {
                "status": "ERROR",
                "message": str(e),
                "results": {}
            }
    
    def analyze_financial_data(self, raw_data: dict) -> Dict[str, Any]:
        """
        재무정보 원본 데이터 분석
        """
        try:
            analysis = FlutterFinancialAnalytics.extract_key_financial_data(raw_data)
            return analysis
            
        except Exception as e:
            logger.error(f"Error in analyze_financial_data: {str(e)}")
            return {
                "status": "ERROR",
                "message": str(e),
                "data": None
            }
    
    def get_raw_api_response(self, corp_code: str, bsns_year: str, reprt_code: str) -> Dict[str, Any]:
        """
        DART API 원본 응답 반환 (디버깅용)
        """
        try:
            request = FlutterFinancialRequest(
                corp_code=corp_code,
                bsns_year=bsns_year,
                reprt_code=reprt_code
            )
            
            raw_data = self.get_financial_statement_raw(request)
            
            return {
                "success": raw_data is not None,
                "raw_response": raw_data if raw_data else {},
                "request_params": request.dict(),
                "analysis": FlutterFinancialAnalytics.extract_key_financial_data(raw_data) if raw_data else None
            }
            
        except Exception as e:
            logger.error(f"Error in get_raw_api_response: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "raw_response": {},
                "request_params": {
                    "corp_code": corp_code,
                    "bsns_year": bsns_year,
                    "reprt_code": reprt_code
                }
            }
