# app/features/news/services/news_v2_vector_service.py
import logging
from typing import List, Dict, Any, Optional

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.features.news.models.news_vector import NewsVector
from app.core.vertex_ai_client import VertexAIClient

logger = logging.getLogger(__name__)

class NewsV2VectorService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo
        self.vertex_ai_client = VertexAIClient()

    def run(self, limit: int = 100) -> Dict[str, Any]:
        """
        본문이 있지만 벡터가 없는 뉴스들의 벡터 생성
        """
        logger.info(f"🚀 NewsV2 벡터 생성 시작 - 최대 {limit}개 처리")
        
        items = self.repo.list_for_vector_generation(limit=limit)
        
        if not items:
            logger.info("📭 처리할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0}
        
        logger.info(f"📊 총 {len(items)}개 뉴스 벡터 생성 시작")
        
        success_count = 0
        failed_count = 0
        
        for i, news in enumerate(items, 1):
            try:
                logger.debug(f"[{i}/{len(items)}] 🔍 뉴스 {news.id} 처리 시작")
                
                # 이미 벡터가 있는지 확인
                existing_vector = self.repo.get_news_vector(news.id, self.vertex_ai_client.model_name)
                if existing_vector:
                    logger.debug(f"[{i}/{len(items)}] ✅ 벡터 이미 존재: {news.id}")
                    success_count += 1
                    continue
                
                # 본문이 없으면 스킵
                if not news.content or len(news.content.strip()) < 100:
                    logger.warning(f"[{i}/{len(items)}] ⚠️ 본문이 너무 짧음: {news.id} ({len(news.content or '')}자)")
                    failed_count += 1
                    continue
                
                logger.debug(f"[{i}/{len(items)}] 📝 본문 확인 완료: {news.id} ({len(news.content)}자)")
                
                # 벡터 생성
                vector_result = self._generate_vector(news)
                if vector_result:
                    success_count += 1
                    logger.debug(f"[{i}/{len(items)}] ✅ 벡터 생성 성공: {news.id} - {news.title[:50]}...")
                else:
                    failed_count += 1
                    logger.debug(f"[{i}/{len(items)}] ❌ 벡터 생성 실패: {news.id} - {news.title[:50]}...")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"[{i}/{len(items)}] 💥 벡터 생성 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items)
        }
        
        logger.info(f"🎉 NewsV2 벡터 생성 완료: 성공 {success_count}개, 실패 {failed_count}개")
        return result

    def _generate_vector(self, news: News) -> Optional[NewsVector]:
        """단일 뉴스의 벡터 생성"""
        try:
            logger.debug(f"🔮 [{news.id}] VertexAI 임베딩 생성 시작 - 모델: {self.vertex_ai_client.model_name}")
            
            # 임베딩 생성
            embedding_result = self.vertex_ai_client.get_embedding(news.content)
            if not embedding_result:
                logger.error(f"❌ [{news.id}] 임베딩 생성 실패")
                return None
            
            embedding = embedding_result["embedding"]
            text_length = embedding_result["text_length"]
            token_length = embedding_result["token_length"]
            processing_time_ms = embedding_result["processing_time_ms"]
            
            logger.debug(f"✨ [{news.id}] 임베딩 생성 완료 - {text_length}자, {token_length}토큰, {processing_time_ms}ms")
            
            # 벡터 저장
            logger.debug(f"💾 [{news.id}] 벡터 DB 저장 시작")
            news_vector = self.repo.create_news_vector(
                news_id=news.id,
                model_name=self.vertex_ai_client.model_name,
                embedding_vector=embedding,
                text_length=text_length,
                token_length=token_length,
                processing_time_ms=processing_time_ms
            )
            
            logger.debug(f"✅ [{news.id}] 벡터 DB 저장 완료 - ID: {news_vector.id}")
            return news_vector
            
        except Exception as e:
            logger.error(f"💥 [{news.id}] 벡터 생성 중 오류: {str(e)}", exc_info=True)
            return None
