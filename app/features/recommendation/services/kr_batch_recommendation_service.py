# app/features/recommendation/services/kr_batch_recommendation_service.py

import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from app.features.marketdata.services.kr_market_holiday_service import KRMarketHolidayService
from app.features.recommendation.services.kr_recommendation_service import KrRecommendationService
from app.features.recommendation.services.kr_analyst_ai_service import KrAnalystAIService
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation

logger = logging.getLogger("kr_batch_recommendation_service")


class KrBatchRecommendationService:
    """배치 추천 생성 서비스 (국내주식 전용)"""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def generate_batch_recommendations_async(self) -> Dict[str, Any]:
        """
        배치 추천 생성을 실행합니다.
        
        Returns:
            Dict[str, Any]: 배치 처리 결과와 메타데이터
        """
        logger.info("배치 추천 생성 시작 (국내주식)")
        
        try:
            # 1. 휴장 여부 확인
            if self._is_market_closed():
                logger.info("현재 휴장 중이므로 배치 추천 생성을 중단합니다.")
                return {
                    "status": "skipped",
                    "reason": "market_closed",
                    "message": "현재 휴장 중이므로 추천 생성을 중단합니다.",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "total_candidates": 0,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "recommendations": []
                }
            
            # 2. 후보 티커 조회 (days_back=1 고정)
            candidate_tickers = self._get_candidate_tickers()
            
            if not candidate_tickers:
                logger.info("추천 후보 티커가 없습니다.")
                return {
                    "status": "completed",
                    "reason": "no_candidates",
                    "message": "추천 후보 티커가 없습니다.",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "total_candidates": 0,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "recommendations": []
                }
            
            # 2.5. 마켓데이터 동기화 (후보 티커들의 최신 데이터 수집)
            sync_result = self._sync_market_data(candidate_tickers)
            if not sync_result["success"]:
                logger.warning(f"마켓데이터 동기화 실패: {sync_result['message']}")
                # 동기화 실패해도 추천 생성은 계속 진행
            
            # 3. 병렬 처리 실행 (비동기)
            batch_results = await self._process_tickers_in_batches_async(candidate_tickers)
            
            # 4. 결과 집계
            total_processed = len(candidate_tickers)
            successful = sum(1 for result in batch_results if result["success"])
            failed = total_processed - successful
            
            logger.info(f"배치 추천 생성 완료 (국내주식): 총 {total_processed}개 중 성공 {successful}개, 실패 {failed}개")
            
            return {
                "status": "completed",
                "reason": "batch_processing_done",
                "message": f"배치 처리 완료: {successful}/{total_processed} 성공",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_candidates": len(candidate_tickers),
                "processed": total_processed,
                "successful": successful,
                "failed": failed,
                "market_data_sync": sync_result,  # 마켓데이터 동기화 결과 추가
                "recommendations": [result for result in batch_results if result["success"]],
                "errors": [result for result in batch_results if not result["success"]]
            }
            
        except Exception as e:
            logger.error(f"배치 추천 생성 중 오류 발생 (국내주식): {str(e)}")
            return {
                "status": "error",
                "reason": "processing_error",
                "message": f"배치 처리 중 오류 발생: {str(e)}",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_candidates": 0,
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "recommendations": [],
                "error": str(e)
            }
    
    def _is_market_closed(self) -> bool:
        """휴장 여부를 확인합니다."""
        try:
            service = KRMarketHolidayService(self.db)
            return service.is_market_closed_now()
        except Exception as e:
            logger.warning(f"휴장 여부 확인 실패: {str(e)}")
            # 오류 시 안전하게 진행 (휴장이 아닌 것으로 간주)
            return False
    
    def _get_candidate_tickers(self) -> List[Dict[str, Any]]:
        """후보 티커 목록을 조회합니다 (days_back=1 고정, country="KR" 고정)."""
        try:
            from app.features.recommendation.repositories.recommendation_repository import RecommendationRepository
            repository = RecommendationRepository(self.db)
            service = KrRecommendationService(repository)
            result = service.get_candidate_tickers(days_back=1, country="KR")
            return result.get("candidate_tickers", [])
        except Exception as e:
            logger.error(f"후보 티커 조회 실패: {str(e)}")
            return []
    
    def _sync_market_data(self, candidate_tickers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """후보 티커들의 마켓데이터를 동기화합니다."""
        try:
            if not candidate_tickers:
                return {"success": True, "message": "동기화할 티커가 없습니다", "counts": {}}
            
            # 티커 ID 목록 추출
            ticker_ids = [ticker["id"] for ticker in candidate_tickers]
            
            logger.info(f"마켓데이터 동기화 시작 (국내주식): {len(ticker_ids)}개 티커 (days=1)")
            
            # KRDailyIngestor를 사용하여 국내주식 마켓데이터 동기화
            from app.features.marketdata.services.kr_daily_ingestor import KRDailyIngestor
            ingestor = KRDailyIngestor(self.db)
            counts = ingestor.sync_for_ticker_ids(ticker_ids, days=1)
            
            total_upserted = sum(counts.values()) if counts else 0
            logger.info(f"마켓데이터 동기화 완료 (국내주식): {len(ticker_ids)}개 티커, {total_upserted}개 데이터 업서트")
            
            return {
                "success": True,
                "message": f"{len(ticker_ids)}개 티커의 마켓데이터 동기화 완료",
                "counts": counts,
                "total_upserted": total_upserted
            }
            
        except Exception as e:
            logger.error(f"마켓데이터 동기화 실패 (국내주식): {str(e)}")
            return {
                "success": False,
                "message": f"마켓데이터 동기화 실패: {str(e)}",
                "counts": {},
                "total_upserted": 0
            }
    
    async def _process_tickers_in_batches_async(self, tickers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """티커들을 10개씩 나누어 병렬 처리합니다."""
        batch_size = 10
        total_tickers = len(tickers)
        results = []
        
        logger.info(f"총 {total_tickers}개 티커를 {batch_size}개씩 {((total_tickers - 1) // batch_size) + 1}개 배치로 처리 (국내주식)")
        
        # 배치 단위로 처리
        for batch_idx in range(0, total_tickers, batch_size):
            batch_tickers = tickers[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            logger.info(f"배치 {batch_num} 처리 시작 (국내주식): {len(batch_tickers)}개 티커")
            
            # 현재 배치 병렬 처리 (비동기)
            batch_results = await self._process_batch_async(batch_tickers, batch_num)
            results.extend(batch_results)
            
            logger.info(f"배치 {batch_num} 처리 완료 (국내주식): {len(batch_results)}개 결과")
        
        return results
    
    async def _process_batch_async(self, batch_tickers: List[Dict[str, Any]], batch_num: int) -> List[Dict[str, Any]]:
        """단일 배치의 티커들을 병렬 처리합니다 (asyncio 기반)."""
        # asyncio.gather를 사용한 진짜 비동기 병렬 처리
        tasks = [
            self._process_single_ticker_async(ticker, batch_num) 
            for ticker in batch_tickers
        ]
        
        try:
            # 모든 작업을 동시에 실행하고 결과 대기
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 예외 처리
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    ticker = batch_tickers[i]
                    logger.error(f"배치 {batch_num} - 티커 {ticker['symbol']} 처리 중 예외 (국내주식): {str(result)}")
                    processed_results.append({
                        "success": False,
                        "ticker_id": ticker["id"],
                        "ticker_symbol": ticker["symbol"],
                        "ticker_exchange": ticker.get("exchange", ""),
                        "error": str(result),
                        "recommendation_id": None
                    })
                else:
                    processed_results.append(result)
                    logger.info(f"배치 {batch_num} - 티커 {result['ticker_symbol']} 처리 완료 (국내주식): {'성공' if result['success'] else '실패'}")
            
            return processed_results
            
        except Exception as e:
            logger.error(f"배치 {batch_num} 병렬 처리 중 전체 예외 (국내주식): {str(e)}")
            # 개별 티커별로 오류 처리
            return [{
                "success": False,
                "ticker_id": ticker["id"],
                "ticker_symbol": ticker["symbol"],
                "ticker_exchange": ticker.get("exchange", ""),
                "error": f"배치 처리 오류: {str(e)}",
                "recommendation_id": None
            } for ticker in batch_tickers]
    
    async def _process_single_ticker_async(self, ticker: Dict[str, Any], batch_num: int) -> Dict[str, Any]:
        """단일 티커에 대해 AI 분석 및 추천을 생성합니다 (비동기)."""
        ticker_id = ticker["id"]
        ticker_symbol = ticker["symbol"]
        
        try:
            # 새로운 DB 세션 생성 (비동기 병렬 처리 시 안전성)
            from app.core.db import SessionLocal
            db = SessionLocal()
            
            try:
                # AI 분석 서비스 호출 (비동기)
                ai_service = KrAnalystAIService(db)
                recommendation_result = await ai_service.generate_analyst_recommendation_async(ticker_id)
                
                return {
                    "success": True,
                    "ticker_id": ticker_id,
                    "ticker_symbol": ticker_symbol,
                    "ticker_exchange": ticker.get("exchange", ""),
                    "recommendation_id": recommendation_result["recommendation_id"],
                    "position_type": recommendation_result["position_type"],
                    "entry_price": recommendation_result["entry_price"],
                    "target_price": recommendation_result["target_price"],
                    "stop_price": recommendation_result.get("stop_price"),
                    "confidence_score": recommendation_result["confidence_score"],
                    "is_latest": recommendation_result.get("is_latest", True),
                    "generated_at": recommendation_result["generated_at"]
                }
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"티커 {ticker_symbol} (ID: {ticker_id}) 처리 실패 (국내주식): {str(e)}")
            return {
                "success": False,
                "ticker_id": ticker_id,
                "ticker_symbol": ticker_symbol,
                "ticker_exchange": ticker.get("exchange", ""),
                "error": str(e),
                "recommendation_id": None
            }
