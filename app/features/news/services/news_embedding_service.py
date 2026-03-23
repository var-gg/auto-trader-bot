# app/features/news/services/news_embedding_service.py
import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.vertex_ai_client import VertexAIClient
from app.features.news.models.news import News
from app.features.news.models.news_vector import NewsVector
from app.features.news.repositories.news_repository import NewsRepository

logger = logging.getLogger(__name__)

class NewsEmbeddingService:
    def __init__(self, db: Session):
        self.db = db
        self.vertex_ai_client = VertexAIClient()
        self.news_repository = NewsRepository(db)

    def create_embedding_for_news(self, news_id: int, force_update: bool = False) -> Optional[NewsVector]:
        """
        특정 뉴스에 대한 임베딩 생성
        
        Args:
            news_id: 뉴스 ID
            force_update: 기존 임베딩이 있어도 강제 업데이트 여부
            
        Returns:
            생성된 NewsVector 객체 또는 None
        """
        try:
            # 뉴스 정보 조회
            news = self.news_repository.get_by_id(news_id)
            if not news:
                logger.error(f"뉴스 ID {news_id}를 찾을 수 없습니다.")
                return None
            
            if not news.content:
                logger.warning(f"뉴스 ID {news_id}의 본문이 비어있습니다.")
                return None
            
            # 기존 임베딩 확인
            existing_vector = self.db.query(NewsVector).filter(
                and_(
                    NewsVector.news_id == news_id,
                    NewsVector.model_name == self.vertex_ai_client.model_name
                )
            ).first()
            
            if existing_vector and not force_update:
                logger.info(f"뉴스 ID {news_id}의 임베딩이 이미 존재합니다.")
                return existing_vector
            
            # 임베딩 생성
            embedding_result = self.vertex_ai_client.get_embedding(news.content)
            if not embedding_result:
                logger.error(f"뉴스 ID {news_id}의 임베딩 생성에 실패했습니다.")
                return None
            
            embedding = embedding_result["embedding"]
            text_length = embedding_result["text_length"]
            token_length = embedding_result["token_length"]
            processing_time_ms = embedding_result["processing_time_ms"]
            
            # NewsVector 객체 생성 또는 업데이트
            if existing_vector:
                existing_vector.embedding_vector = embedding
                existing_vector.vector_dimension = len(embedding)
                existing_vector.text_length = text_length
                existing_vector.token_length = token_length
                existing_vector.processing_time_ms = processing_time_ms
                existing_vector.status = "SUCCESS"
                existing_vector.error_message = None
                self.db.commit()
                logger.info(f"뉴스 ID {news_id}의 임베딩을 업데이트했습니다.")
                return existing_vector
            else:
                news_vector = NewsVector(
                    news_id=news_id,
                    model_name=self.vertex_ai_client.model_name,
                    vector_dimension=len(embedding),
                    embedding_vector=embedding,
                    text_length=text_length,
                    token_length=token_length,
                    processing_time_ms=processing_time_ms,
                    status="SUCCESS"
                )
                self.db.add(news_vector)
                self.db.commit()
                logger.info(f"뉴스 ID {news_id}의 임베딩을 새로 생성했습니다.")
                return news_vector
                
        except Exception as e:
            logger.error(f"뉴스 ID {news_id} 임베딩 생성 중 오류: {str(e)}")
            # 오류 상태로 NewsVector 저장
            try:
                error_vector = NewsVector(
                    news_id=news_id,
                    model_name=self.vertex_ai_client.model_name,
                    vector_dimension=0,
                    embedding_vector=[],
                    text_length=len(news.content) if news.content else 0,
                    token_length=0,
                    status="FAILED",
                    error_message=str(e)
                )
                self.db.add(error_vector)
                self.db.commit()
            except Exception as commit_error:
                logger.error(f"오류 상태 저장 실패: {str(commit_error)}")
                self.db.rollback()
            return None

    def create_embeddings_batch(self, news_ids: List[int], force_update: bool = False) -> Dict[str, Any]:
        """
        여러 뉴스에 대한 임베딩을 배치로 생성
        
        Args:
            news_ids: 뉴스 ID 리스트
            force_update: 기존 임베딩이 있어도 강제 업데이트 여부
            
        Returns:
            처리 결과 통계
        """
        results = {
            "total": len(news_ids),
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }
        
        for news_id in news_ids:
            try:
                result = self.create_embedding_for_news(news_id, force_update)
                if result:
                    if result.status == "SUCCESS":
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                        results["errors"].append(f"뉴스 ID {news_id}: {result.error_message}")
                else:
                    results["failed"] += 1
                    results["errors"].append(f"뉴스 ID {news_id}: 임베딩 생성 실패")
                    
            except Exception as e:
                results["failed"] += 1
                results["errors"].append(f"뉴스 ID {news_id}: {str(e)}")
                logger.error(f"배치 처리 중 뉴스 ID {news_id} 오류: {str(e)}")
        
        logger.info(f"배치 임베딩 처리 완료 - 성공: {results['success']}, 실패: {results['failed']}")
        return results

    def get_news_without_embedding(self, limit: int = 100) -> List[News]:
        """
        임베딩이 없는 뉴스 목록 조회
        
        Args:
            limit: 조회할 최대 개수
            
        Returns:
            임베딩이 없는 뉴스 리스트
        """
        # 서브쿼리로 임베딩이 있는 뉴스 ID 조회
        embedded_news_ids = self.db.query(NewsVector.news_id).filter(
            NewsVector.model_name == self.vertex_ai_client.model_name
        ).subquery()
        
        # 임베딩이 없고 본문이 있는 뉴스 조회
        news_without_embedding = self.db.query(News).filter(
            and_(
                News.id.notin_(embedded_news_ids),
                News.content.isnot(None),
                News.content != ""
            )
        ).limit(limit).all()
        
        return news_without_embedding

    def get_embedding_by_news_id(self, news_id: int) -> Optional[NewsVector]:
        """
        특정 뉴스의 임베딩 조회
        
        Args:
            news_id: 뉴스 ID
            
        Returns:
            NewsVector 객체 또는 None
        """
        return self.db.query(NewsVector).filter(
            and_(
                NewsVector.news_id == news_id,
                NewsVector.model_name == self.vertex_ai_client.model_name
            )
        ).first()

    def get_model_info(self) -> Dict[str, Any]:
        """
        현재 사용 중인 모델 정보 반환
        
        Returns:
            모델 정보 딕셔너리
        """
        return self.vertex_ai_client.get_model_info()
