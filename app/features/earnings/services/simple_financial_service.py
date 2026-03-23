# app/features/earnings/services/simple_financial_service.py
import logging
import requests
from typing import Optional, Dict, List, Any
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.features.earnings.models.simple_financial_model import (
    SimpleFinancialRequest,
    SimpleFinancialResponse,
    QuarterlyFinancialData,
    AccountData
)
from app.features.earnings.services.dart_corp_code_service import DartCorpCodeService

logger = logging.getLogger(__name__)
settings = get_settings()


class SimpleFinancialService:
    """간단한 재무 데이터 서비스 - DART 원본 데이터 그대로 반환"""
    
    def __init__(self, db: Session):
        self.db = db
        self.dart_api_key = settings.DART_API_KEY
        self.dart_service = DartCorpCodeService(db)
        self.base_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll"
        
        # 보고서 코드 매핑
        self.report_codes = {
            "11013": {"name": "1분기보고서", "quarter": 1},
            "11012": {"name": "반기보고서", "quarter": 2},
            "11014": {"name": "3분기보고서", "quarter": 3},
            "11011": {"name": "사업보고서", "quarter": 4}
        }
        
        # 추출할 account_id 기반 타겟 (정확한 매칭)
        self.target_accounts = {
            "revenue": {
                "account_id": "ifrs-full_Revenue",
                "account_nm": "매출액"
            },
            "net_income": {
                "account_id": "ifrs-full_ProfitLoss",
                "account_detail": "별도재무제표 [member]",
                "account_nm": "당기순이익"
            },
            "basic_eps": {
                "account_id": "ifrs-full_BasicEarningsLossPerShare",
                "account_nm": "기본주당이익"
            }
        }
    
    def get_financial_data_for_corporation(self, request: SimpleFinancialRequest) -> SimpleFinancialResponse:
        """
        기업의 분기별 재무 데이터 조회 (DART 원본 데이터 그대로)
        
        Args:
            request: 재무 데이터 요청
            
        Returns:
            SimpleFinancialResponse: 분기별 재무 데이터
        """
        try:
            logger.info(f"Starting financial data collection for corp_code: {request.corp_code}, year: {request.bsns_year}")
            
            # 1. 기업 정보 조회
            corp_info = self._get_corporation_info(request.corp_code)
            if not corp_info:
                return SimpleFinancialResponse(
                    success=False,
                    message=f"기업 정보를 찾을 수 없습니다: {request.corp_code}"
                )
            
            # 2. 각 분기별 데이터 수집
            quarterly_data = []
            
            for reprt_code, reprt_info in self.report_codes.items():
                try:
                    quarter_data = self._get_quarterly_accounts(
                        request.corp_code,
                        request.bsns_year,
                        reprt_code,
                        request.fs_div,
                        reprt_info
                    )
                    
                    if quarter_data:
                        quarterly_data.append(quarter_data)
                        
                except Exception as e:
                    logger.warning(f"Failed to get data for quarter {reprt_info['name']}: {str(e)}")
                    continue
            
            # 3. 데이터 설명 생성
            data_explanation = self._create_data_explanation()
            
            if quarterly_data:
                return SimpleFinancialResponse(
                    success=True,
                    message="재무 데이터 수집이 성공적으로 완료되었습니다",
                    corp_info=corp_info,
                    quarterly_data=quarterly_data,
                    data_explanation=data_explanation
                )
            else:
                return SimpleFinancialResponse(
                    success=False,
                    message="분기별 재무 데이터를 찾을 수 없었습니다",
                    corp_info=corp_info,
                    data_explanation=data_explanation
                )
        
        except Exception as e:
            logger.error(f"Unexpected error in financial data collection: {str(e)}")
            return SimpleFinancialResponse(
                success=False,
                message=f"재무 데이터 수집 중 오류 발생: {str(e)}",
                data_explanation=self._create_data_explanation()
            )
    
    def _get_corporation_info(self, corp_code: str) -> Optional[Dict[str, str]]:
        """기업 정보 조회"""
        try:
            corp = self.dart_service.get_corp_by_code(corp_code)
            return {
                "corp_code": corp.corp_code,
                "corp_name": corp.corp_name,
                "stock_code": corp.stock_code
            } if corp else None
        except Exception as e:
            logger.error(f"Error getting corp info for {corp_code}: {str(e)}")
            return None
    
    def _get_quarterly_accounts(self, corp_code: str, bsns_year: str, reprt_code: str, 
                               fs_div: str, reprt_info: Dict[str, str]) -> Optional[QuarterlyFinancialData]:
        """특정 분기의 계정 데이터 조회"""
        try:
            # DART API 호출
            params = {
                "crtfc_key": self.dart_api_key,
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div
            }
            
            logger.info(f"Requesting DART data - reprt_code: {reprt_code}")
            
            response = requests.get(
                f"{self.base_url}.json",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"DART API HTTP error: {response.status_code}")
                return None
            
            data = response.json()
            
            # DART 응답 전체를 디버그 로그로 출력
            logger.debug(f"Raw DART API Response for {reprt_info['name']} (reprt_code: {reprt_code})")
            
            import json
            formatted_json = json.dumps(data, ensure_ascii=False, indent=2)
            logger.debug(f"Response JSON:\n{formatted_json}")
            
            logger.debug(f"Response Info: Status={data.get('status')}, Message={data.get('message')}, Total Items={len(data.get('list', []))}")
            
            if data.get("status") != "000":
                logger.warning(f"DART API returned error: {data.get('status')} - {data.get('message')}")
                return None
            
            # 응답 데이터 파싱
            account_list = data.get("list", [])
            if not account_list:
                logger.warning(f"No account data found for {reprt_code}")
                return None
            
            logger.info(f"Received {len(account_list)} accounts for {reprt_code}")
            
            # 디버깅: 사용 가능한 계정명들 확인
            available_accounts = [acc.get("account_nm", "") for acc in account_list[:20]]  # 처음 20개
            logger.info(f"Available account names (first 20): {available_accounts}")
            
            # 정확한 account_id 기반으로 계정 데이터 추출
            extracted_accounts = {}
            
            for account_type, target_config in self.target_accounts.items():
                account_data = self._extract_account_by_id(account_list, target_config)
                extracted_accounts[account_type] = account_data
                
                if account_data:
                    logger.info(f"✅ Found {account_type}: {account_data.account_nm} (ID: {account_data.account_id})")
                else:
                    logger.warning(f"❌ No data for {account_type} with config: {target_config}")
                    # 디버깅: 비슷한 account_id 검색
                    similar_ids = [acc.get("account_id") for acc in account_list if 
                                 target_config["account_id"].split("_")[1] in acc.get("account_id", "")]
                    if similar_ids:
                        logger.info(f"🔍 Similar account_ids found: {similar_ids[:3]}")
            
            # 분기 데이터 생성
            quarterly_data = QuarterlyFinancialData(
                reprt_code=reprt_code,
                quarter_name=reprt_info["name"],
                revenue=extracted_accounts.get("revenue"),
                net_income=extracted_accounts.get("net_income"),
                basic_eps=extracted_accounts.get("basic_eps"),
                diluted_eps=None  # 희석주당이익은 별도 처리 필요시 추가
            )
            
            return quarterly_data
            
        except Exception as e:
            logger.error(f"Error getting quarterly accounts for {reprt_code}: {str(e)}")
            return None
    
    def _extract_account_by_id(self, account_list: List[Dict[str, Any]], 
                               target_config: Dict[str, str]) -> Optional[AccountData]:
        """account_id 기반으로 정확한 계정 데이터 추출"""
        try:
            target_account_id = target_config["account_id"]
            target_account_detail = target_config.get("account_detail")
            
            for account in account_list:
                account_id = account.get("account_id", "")
                
                # 정확한 account_id 매칭
                if account_id == target_account_id:
                    # 당기순이익의 경우 account_detail도 체크
                    if target_account_detail:
                        account_detail = account.get("account_detail", "")
                        if account_detail != target_account_detail:
                            continue
                    
                    logger.info(f"Found exact match: account_id='{account_id}', account_detail='{account.get('account_detail', 'N/A')}'")
                    
                    # AccountData 객체 생성
                    account_data = AccountData(
                        account_nm=account.get("account_nm", ""),
                        account_id=account_id,
                        sj_div=account.get("sj_div", ""),
                        sj_nm=account.get("sj_nm", ""),
                        thstrm_nm=account.get("thstrm_nm"),
                        thstrm_amount=account.get("thstrm_amount"),
                        thstrm_add_amount=account.get("thstrm_add_amount"),
                        frmtrm_nm=account.get("frmtrm_nm"),
                        frmtrm_amount=account.get("frmtrm_amount"),
                        frmtrm_add_amount=account.get("frmtrm_add_amount"),
                        bfefrmtrm_nm=account.get("bfefrmtrm_nm"),
                        bfefrmtrm_amount=account.get("bfefrmtrm_amount"),
                        ord=account.get("ord"),
                        currency=account.get("currency")
                    )
                    
                    return account_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting account by ID: {str(e)}")
            return None
    
    def _create_data_explanation(self) -> Dict[str, Any]:
        """데이터 설명 생성"""
        return {
            "field_descriptions": {
                "매출액": {
                    "meaning": "회사의 총 매출액",
                    "field_name": "revenue",
                    "units": ["원", "금액"]
                },
                "당기순이익(손실)": {
                    "meaning": "모든 수익과 비용을 차감한 최종 순익",
                    "field_name": "net_income", 
                    "units": ["원", "금액"]
                },
                "기본주당이익(손실)": {
                    "meaning": "보통주 기준 주당 순이익",
                    "field_name": "basic_eps",
                    "units": ["원", "주당"]
                },
                "희석주당이익(손실)": {
                    "meaning": "가능한 주식까지 고려한 주당 순이익",
                    "field_name": "diluted_eps",
                    "units": ["원", "주당"]
                }
            },
            "amount_fields": {
                "thstrm_amount": "당기금액 (해당 분기 금액)",
                "thstrm_add_amount": "당기누적금액 (연초부터 누적)",
                "frmtrm_amount": "전기금액 (전년 동분기 금액)",
                "frmtrm_add_amount": "전기누적금액 (전년 누적)",
                "bfefrmtrm_amount": "전전기금액 (사업보고서만 제공)"
            },
            "report_periods": {
                "11013": "1분기보고서",
                "11012": "반기보고서 (상반기)",
                "11014": "3분기보고서 (1-3분기)",
                "11011": "사업보고서 (연간)"
            }
        }
    
    def _get_data_explanation(self) -> Dict[str, Any]:
        """데이터 필드 설명 반환"""
        return self._create_data_explanation()
