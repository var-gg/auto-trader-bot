# app/features/earnings/services/estimate_perform_service.py
import logging
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session

from app.core.kis_client import KISClient
from app.features.earnings.models.estimate_perform_model import (
    EstimatePerformRequest,
    EstimatePerformResponse,
    EstimatePerformAnalytics
)

logger = logging.getLogger(__name__)


class EstimatePerformService:
    """국내주식 실적추정 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
    
    def get_estimate_perform(self, symbol: str) -> Optional[EstimatePerformResponse]:
        """
        실적추정 데이터 조회
        - symbol: 종목코드 (예: 005930)
        """
        try:
            logger.info(f"Fetching estimate perform data for symbol: {symbol}")
            
            # KIS API 호출
            raw_response = self.kis_client.estimate_perform(symbol)
            
            # 응답 검증
            if raw_response.get('rt_cd') != '0':
                logger.error(f"KIS API error: {raw_response.get('msg_cd')} - {raw_response.get('msg1', 'Unknown error')}")
                return EstimatePerformResponse(
                    rt_cd=raw_response.get('rt_cd', '9999'),
                    msg_cd=raw_response.get('msg_cd', 'UNKNOWN_ERROR'),
                    msg1=raw_response.get('msg1', 'Unknown error'),
                    output=None
                )
            
            # 데이터 변환
            logger.info(f"Successfully fetched estimate perform data for {symbol}")
            logger.debug(f"Raw API response: {raw_response}")
            
            # 실제 응답 구조 확인을 위해 로그 출력
            logger.info(f"Response structure - output1: {raw_response.get('output1')}")
            logger.info(f"Response structure - output2: {raw_response.get('output2')}")
            logger.info(f"Response structure - output3: {raw_response.get('output3')}")
            logger.info(f"Response structure - output4: {raw_response.get('output4')}")
            
            return EstimatePerformResponse(
                rt_cd=raw_response.get('rt_cd', '0'),
                msg_cd=raw_response.get('msg_cd', '0000'),
                msg1=raw_response.get('msg1', '정상처리'),
                output1=raw_response.get('output1'),
                output2=raw_response.get('output2'),
                output3=raw_response.get('output3'),
                output4=raw_response.get('output4')
            )
            
        except Exception as e:
            logger.error(f"Error fetching estimate perform data for {symbol}: {str(e)}")
            return None
    
    def analyze_estimate_perform(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        실적추정 데이터 분석
        - symbol: 종목코드 (예: 005930)
        """
        try:
            response = self.get_estimate_perform(symbol)
            if not response:
                return {
                    "status": "ERROR",
                    "message": "No response from API",
                    "analysis": None
                }
            
            # 데이터 분석
            summary = EstimatePerformAnalytics.summarize_for_testing(response)
            
            logger.info(f"Successfully analyzed estimate perform data for {symbol}")
            return summary
            
        except Exception as e:
            logger.error(f"Error analyzing estimate perform data for {symbol}: {str(e)}")
            return {
                "status": "ERROR",
                "message": str(e),
                "analysis": None
            }
    
    def get_multiple_symbols_analysis(self, symbols: List[str]) -> Dict[str, Any]:
        """
        여러 종목의 실적추정 데이터 비교 분석
        - symbols: 종목코드 리스트 (예: ["005930", "000660", "035420"])
        """
        try:
            logger.info(f"Analyzing estimate perform data for {len(symbols)} symbols")
            
            results = {}
            summary_data = []
            
            for symbol in symbols:
                analysis = self.analyze_estimate_perform(symbol)
                results[symbol] = analysis
                
                if analysis and analysis.get("status") == "SUCCESS" and analysis.get("analysis"):
                    summary_data.append({
                        "symbol": symbol,
                        "name": analysis["analysis"].get("name"),
                        "current_price": analysis["analysis"].get("current_price"),
                        "price_change": analysis["analysis"].get("price_change"),
                        "foreign_ownership": analysis["analysis"].get("foreign_ownership"),
                        "price_trend": analysis["analysis"].get("price_trend"),
                        "foreign_status": analysis["analysis"].get("foreign_status")
                    })
            
            # 비교 분석 결과
            comparison_result = {
                "status": "SUCCESS",
                "total_symbols": len(symbols),
                "successful_symbols": len([r for r in results.values() if r.get("status") == "SUCCESS"]),
                "results": results,
                "comparison_summary": summary_data,
                "top_performers": {
                    "by_price_change": sorted(
                        [item for item in summary_data if item.get("price_change")], 
                        key=lambda x: float(x["price_change"]) if x["price_change"] else 0, 
                        reverse=True
                    )[:3],
                    "by_foreign_ownership": sorted(
                        [item for item in summary_data if item.get("foreign_ownership")], 
                        key=lambda x: float(x["foreign_ownership"]) if x["foreign_ownership"] else 0, 
                        reverse=True
                    )[:3]
                }
            }
            
            logger.info(f"Successfully completed comparison analysis for {len(symbols)} symbols")
            return comparison_result
            
        except Exception as e:
            logger.error(f"Error in multiple symbols analysis: {str(e)}")
            return {
                "status": "ERROR",
                "message": str(e),
                "results": {}
            }
