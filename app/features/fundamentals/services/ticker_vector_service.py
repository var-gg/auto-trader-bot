# app/features/fundamentals/services/ticker_vector_service.py
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.vertex_ai_client import VertexAIClient
from app.features.fundamentals.models.ticker_vector import TickerVector
from app.features.fundamentals.services.ticker_source_text_service import TickerSourceTextService
from app.shared.models.ticker import Ticker

logger = logging.getLogger(__name__)

class TickerVectorService:
    def __init__(self, db: Session):
        self.db = db
        self.vertex_ai_client = VertexAIClient()
        self.source_text_service = TickerSourceTextService(db)

    def create_ticker_vector(self, ticker_id: int, force_update: bool = False) -> Optional[TickerVector]:
        """
        티커 벡터 생성 (소스텍스트 생성 → 임베딩 생성 → DB 저장)
        
        Args:
            ticker_id: 티커 ID
            force_update: 기존 벡터가 있어도 강제 업데이트 여부
            
        Returns:
            생성된 TickerVector 객체 또는 None
        """
        try:
            # 티커 정보 확인
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                logger.error(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
                return None
            
            # 기존 벡터 확인
            existing_vector = self.db.query(TickerVector).filter(
                and_(
                    TickerVector.ticker_id == ticker_id,
                    TickerVector.model_name == self.vertex_ai_client.model_name
                )
            ).first()
            
            if existing_vector and not force_update:
                logger.info(f"티커 ID {ticker_id}의 벡터가 이미 존재합니다.")
                return existing_vector
            
            # 소스텍스트 생성
            source_result = self.source_text_service.generate_source_text_for_ticker(ticker_id)
            if not source_result:
                logger.error(f"티커 ID {ticker_id}의 소스텍스트 생성에 실패했습니다.")
                return None
            
            source_text = source_result["source_text"]
            
            # 임베딩 생성
            embedding_result = self.vertex_ai_client.get_embedding(source_text)
            if not embedding_result:
                logger.error(f"티커 ID {ticker_id}의 임베딩 생성에 실패했습니다.")
                return None
            
            embedding = embedding_result["embedding"]
            text_length = embedding_result["text_length"]
            token_length = embedding_result["token_length"]
            processing_time_ms = embedding_result["processing_time_ms"]
            
            # TickerVector 객체 생성 또는 업데이트
            if existing_vector:
                existing_vector.embedding_vector = embedding
                existing_vector.vector_dimension = len(embedding)
                existing_vector.text_length = text_length
                existing_vector.token_length = token_length
                existing_vector.processing_time_ms = processing_time_ms
                existing_vector.source_text = source_text
                self.db.commit()
                logger.info(f"티커 ID {ticker_id}의 벡터를 업데이트했습니다.")
                return existing_vector
            else:
                ticker_vector = TickerVector(
                    ticker_id=ticker_id,
                    model_name=self.vertex_ai_client.model_name,
                    vector_dimension=len(embedding),
                    embedding_vector=embedding,
                    source_text=source_text,
                    text_length=text_length,
                    token_length=token_length,
                    processing_time_ms=processing_time_ms
                )
                self.db.add(ticker_vector)
                self.db.commit()
                logger.info(f"티커 ID {ticker_id}의 벡터를 새로 생성했습니다.")
                return ticker_vector
                
        except Exception as e:
            logger.error(f"티커 ID {ticker_id} 벡터 생성 중 오류: {str(e)}")
            self.db.rollback()
            return None

    def update_ticker_vector(self, ticker_id: int, source_text: str) -> Optional[TickerVector]:
        """
        기존 티커 벡터 업데이트 (외부에서 받은 소스텍스트로 임베딩 재생성)
        
        Args:
            ticker_id: 티커 ID
            source_text: 새로운 소스텍스트
            
        Returns:
            업데이트된 TickerVector 객체 또는 None
        """
        try:
            # 기존 벡터 조회
            existing_vector = self.db.query(TickerVector).filter(
                and_(
                    TickerVector.ticker_id == ticker_id,
                    TickerVector.model_name == self.vertex_ai_client.model_name
                )
            ).first()
            
            if not existing_vector:
                logger.error(f"티커 ID {ticker_id}의 벡터를 찾을 수 없습니다.")
                return None
            
            # 새로운 임베딩 생성
            embedding_result = self.vertex_ai_client.get_embedding(source_text)
            if not embedding_result:
                logger.error(f"티커 ID {ticker_id}의 임베딩 생성에 실패했습니다.")
                return None
            
            embedding = embedding_result["embedding"]
            text_length = embedding_result["text_length"]
            token_length = embedding_result["token_length"]
            processing_time_ms = embedding_result["processing_time_ms"]
            
            # 벡터 업데이트
            existing_vector.embedding_vector = embedding
            existing_vector.vector_dimension = len(embedding)
            existing_vector.source_text = source_text
            existing_vector.text_length = text_length
            existing_vector.token_length = token_length
            existing_vector.processing_time_ms = processing_time_ms
            
            self.db.commit()
            logger.info(f"티커 ID {ticker_id}의 벡터를 업데이트했습니다.")
            return existing_vector
            
        except Exception as e:
            logger.error(f"티커 ID {ticker_id} 벡터 업데이트 중 오류: {str(e)}")
            self.db.rollback()
            return None

    def get_ticker_vector(self, ticker_id: int) -> Optional[TickerVector]:
        """티커 벡터 조회"""
        return self.db.query(TickerVector).filter(
            and_(
                TickerVector.ticker_id == ticker_id,
                TickerVector.model_name == self.vertex_ai_client.model_name
            )
        ).first()

    def get_tickers_without_vector(self, limit: int = 100) -> List[Ticker]:
        """벡터가 없는 티커 목록 조회"""
        # 서브쿼리로 벡터가 있는 티커 ID 조회
        vectorized_ticker_ids = self.db.query(TickerVector.ticker_id).filter(
            TickerVector.model_name == self.vertex_ai_client.model_name
        ).subquery()
        
        # 벡터가 없는 티커 조회
        tickers_without_vector = self.db.query(Ticker).filter(
            Ticker.id.notin_(vectorized_ticker_ids)
        ).limit(limit).all()
        
        return tickers_without_vector

    def delete_ticker_vector(self, ticker_id: int) -> bool:
        """티커 벡터 삭제"""
        try:
            vector = self.db.query(TickerVector).filter(
                and_(
                    TickerVector.ticker_id == ticker_id,
                    TickerVector.model_name == self.vertex_ai_client.model_name
                )
            ).first()
            
            if not vector:
                logger.error(f"티커 ID {ticker_id}의 벡터를 찾을 수 없습니다.")
                return False
            
            self.db.delete(vector)
            self.db.commit()
            logger.info(f"티커 ID {ticker_id}의 벡터를 삭제했습니다.")
            return True
            
        except Exception as e:
            logger.error(f"티커 ID {ticker_id} 벡터 삭제 중 오류: {str(e)}")
            self.db.rollback()
            return False

    def get_model_info(self) -> Dict[str, Any]:
        """현재 사용 중인 모델 정보 반환"""
        return self.vertex_ai_client.get_model_info()

    def get_vector_stats(self) -> Dict[str, Any]:
        """벡터 통계 조회"""
        total_count = self.db.query(TickerVector).filter(
            TickerVector.model_name == self.vertex_ai_client.model_name
        ).count()
        
        total_tickers = self.db.query(Ticker).count()
        
        return {
            "total_tickers": total_tickers,
            "vectorized_tickers": total_count,
            "remaining_tickers": total_tickers - total_count,
            "coverage_rate": round(total_count / total_tickers * 100, 2) if total_tickers > 0 else 0,
            "model_info": self.get_model_info()
        }


    async def batch_create_vectors_async(self, batch_size: int = 10, force_update: bool = False) -> Dict[str, Any]:
        """
        벡터가 없는 티커들에 대해 비동기 배치로 벡터 생성
        
        Args:
            batch_size: 배치 크기
            force_update: 기존 벡터 강제 업데이트 여부
            
        Returns:
            배치 처리 결과
        """
        logger.info("비동기 배치 벡터 생성 시작")
        
        try:
            # 벡터가 없는 티커 목록 조회
            vectorized_ticker_ids = self.db.query(TickerVector.ticker_id).filter(
                TickerVector.model_name == self.vertex_ai_client.model_name
            ).subquery()
            
            if force_update:
                # 강제 업데이트인 경우 모든 티커 대상
                tickers_to_process = self.db.query(Ticker).limit(batch_size).all()
            else:
                # 벡터가 없는 티커만 대상
                tickers_to_process = self.db.query(Ticker).filter(
                    ~Ticker.id.in_(vectorized_ticker_ids)
                ).limit(batch_size).all()
            
            if not tickers_to_process:
                return {
                    "status": "completed",
                    "message": "처리할 티커가 없습니다.",
                    "processed_count": 0,
                    "success_count": 0,
                    "failed_count": 0,
                    "failed_tickers": [],
                    "generated_at": datetime.now(timezone.utc).isoformat()
                }
            
            logger.info(f"총 {len(tickers_to_process)}개 티커를 비동기 배치 처리 시작")
            
            # 비동기 배치 처리 실행
            batch_results = await self._process_tickers_in_batches_async(tickers_to_process)
            
            # 결과 집계
            success_count = sum(1 for result in batch_results if result["success"])
            failed_count = len(batch_results) - success_count
            
            logger.info(f"비동기 배치 벡터 생성 완료: 성공 {success_count}개, 실패 {failed_count}개")
            
            return {
                "status": "completed",
                "message": f"비동기 배치 처리 완료: {len(tickers_to_process)}개 중 성공 {success_count}개, 실패 {failed_count}개",
                "processed_count": len(tickers_to_process),
                "success_count": success_count,
                "failed_count": failed_count,
                "failed_tickers": [result for result in batch_results if not result["success"]],
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"비동기 배치 벡터 생성 중 오류: {str(e)}")
            return {
                "status": "error",
                "message": f"비동기 배치 처리 중 오류 발생: {str(e)}",
                "processed_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "failed_tickers": [],
                "generated_at": datetime.now(timezone.utc).isoformat()
            }

    async def _process_tickers_in_batches_async(self, tickers: List[Ticker]) -> List[Dict[str, Any]]:
        """티커들을 10개씩 나누어 비동기 병렬 처리합니다."""
        batch_size = 10
        total_tickers = len(tickers)
        results = []
        
        logger.info(f"총 {total_tickers}개 티커를 {batch_size}개씩 {((total_tickers - 1) // batch_size) + 1}개 배치로 처리")
        
        # 배치 단위로 처리
        for batch_idx in range(0, total_tickers, batch_size):
            batch_tickers = tickers[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            logger.info(f"배치 {batch_num} 처리 시작: {len(batch_tickers)}개 티커")
            
            # 현재 배치 병렬 처리 (비동기)
            batch_results = await self._process_batch_async(batch_tickers, batch_num)
            results.extend(batch_results)
            
            logger.info(f"배치 {batch_num} 처리 완료: {len(batch_results)}개 결과")
        
        return results

    async def _process_batch_async(self, batch_tickers: List[Ticker], batch_num: int) -> List[Dict[str, Any]]:
        """단일 배치의 티커들을 비동기 병렬 처리합니다."""
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
                    logger.error(f"배치 {batch_num} - 티커 {ticker.symbol} 처리 중 예외: {str(result)}")
                    processed_results.append({
                        "success": False,
                        "ticker_id": ticker.id,
                        "ticker_symbol": ticker.symbol,
                        "error": str(result)
                    })
                else:
                    processed_results.append(result)
                    logger.info(f"배치 {batch_num} - 티커 {result['ticker_symbol']} 처리 완료: {'성공' if result['success'] else '실패'}")
            
            return processed_results
            
        except Exception as e:
            logger.error(f"배치 {batch_num} 병렬 처리 중 전체 예외: {str(e)}")
            # 개별 티커별로 오류 처리
            return [{
                "success": False,
                "ticker_id": ticker.id,
                "ticker_symbol": ticker.symbol,
                "error": f"배치 처리 오류: {str(e)}"
            } for ticker in batch_tickers]

    async def _process_single_ticker_async(self, ticker: Ticker, batch_num: int) -> Dict[str, Any]:
        """단일 티커에 대해 벡터를 생성합니다 (비동기)."""
        ticker_id = ticker.id
        ticker_symbol = ticker.symbol
        
        try:
            # 새로운 DB 세션 생성 (비동기 병렬 처리 시 안전성)
            from app.core.db import SessionLocal
            db = SessionLocal()
            
            try:
                # 벡터 생성 서비스 호출
                service = TickerVectorService(db)
                result = service.create_ticker_vector(ticker_id)
                
                if result:
                    return {
                        "success": True,
                        "ticker_id": ticker_id,
                        "ticker_symbol": ticker_symbol,
                        "vector_id": result.id,
                        "model_name": result.model_name,
                        "vector_dimension": result.vector_dimension
                    }
                else:
                    return {
                        "success": False,
                        "ticker_id": ticker_id,
                        "ticker_symbol": ticker_symbol,
                        "error": "벡터 생성 실패"
                    }
                    
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"티커 {ticker_symbol} (ID: {ticker_id}) 벡터 생성 중 오류: {str(e)}")
            return {
                "success": False,
                "ticker_id": ticker_id,
                "ticker_symbol": ticker_symbol,
                "error": str(e)
            }
