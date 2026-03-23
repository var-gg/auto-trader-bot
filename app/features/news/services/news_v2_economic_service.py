# app/features/news/services/news_v2_economic_service.py
import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text, func, select, desc, literal_column, bindparam, literal
from sqlalchemy.orm import Session

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.core.vertex_ai_client import VertexAIClient
from app.features.news.models.news_vector import NewsVector
from app.features.news.models.news_anchor_vector import NewsAnchorVector

logger = logging.getLogger(__name__)

class NewsV2EconomicService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo
        self.vertex_ai_client = VertexAIClient()

    def run(self, limit: int = 100) -> Dict[str, Any]:
        """
        벡터는 있지만 경제관련 분류가 안된 뉴스들의 경제관련 여부 판단
        """
        logger.info(f"🧠 NewsV2 경제관련 분류 시작 - 최대 {limit}개 처리")
        
        items = self.repo.list_for_economic_classification(limit=limit)
        
        if not items:
            logger.info("📭 처리할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0}
        
        logger.info(f"📊 총 {len(items)}개 뉴스 경제관련 분류 시작")
        
        success_count = 0
        failed_count = 0
        
        for i, news in enumerate(items, 1):
            try:
                logger.debug(f"[{i}/{len(items)}] 🔍 뉴스 {news.id} 경제관련 분류 시작")
                
                # 경제관련 여부 판단
                result = self._classify_economic_impact(news)
                if result:
                    is_finance_related, confidence, top_anchor, top_sim, second_sim, gap = result
                    
                    # 결과 저장
                    self.repo.mark_economic_classification(
                        news=news,
                        is_finance_related=is_finance_related,
                        confidence=confidence,
                        top_anchor=top_anchor,
                        top_similarity=top_sim,
                        second_similarity=second_sim,
                        similarity_gap=gap
                    )
                    
                    success_count += 1
                    status_emoji = "💰" if is_finance_related else "📰"
                    logger.debug(f"[{i}/{len(items)}] {status_emoji} 경제관련 분류 완료: {news.id} - {is_finance_related} ({confidence:.4f}) - {top_anchor}")
                else:
                    failed_count += 1
                    logger.debug(f"[{i}/{len(items)}] ❌ 경제관련 분류 실패: {news.id}")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"[{i}/{len(items)}] 💥 경제관련 분류 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items)
        }
        
        logger.info(f"🎉 NewsV2 경제관련 분류 완료: 성공 {success_count}개, 실패 {failed_count}개")
        return result

    def _classify_economic_impact(self, news: News) -> Optional[Tuple[bool, float, str, float, float, float]]:
        """
        단일 뉴스의 경제관련 여부 판단
        제공해주신 SQL을 ORM으로 변환
        """
        try:
            logger.debug(f"🔍 [{news.id}] 경제관련 분류 시작 - 모델명: {self.vertex_ai_client.model_name}")
            
            # 뉴스 벡터 조회
            logger.debug(f"📊 [{news.id}] 뉴스 벡터 조회 시작")
            news_vector = self.repo.get_news_vector(news.id, self.vertex_ai_client.model_name)
            if not news_vector:
                logger.warning(f"⚠️ [{news.id}] 뉴스 벡터를 찾을 수 없음 - 모델명: {self.vertex_ai_client.model_name}")
                return None
            logger.debug(f"✅ [{news.id}] 뉴스 벡터 조회 완료")
            
            # 앵커 벡터들 조회
            logger.debug(f"⚓ [{news.id}] 앵커 벡터 조회 시작")
            anchor_vectors = self.repo.get_anchor_vectors(self.vertex_ai_client.model_name)
            if not anchor_vectors:
                logger.warning(f"⚠️ [{news.id}] 앵커 벡터를 찾을 수 없음 - 모델명: {self.vertex_ai_client.model_name}")
                return None
            logger.debug(f"✅ [{news.id}] 앵커 벡터 조회 완료 - {len(anchor_vectors)}개 발견")
            
            # 테이블 별칭으로 가독성 향상
            logger.debug(f"🧮 [{news.id}] 유사도 계산 SQL 실행 시작")
            
            # 테이블 별칭(가독성)
            n = NewsVector.__table__.alias("n")
            a = NewsAnchorVector.__table__.alias("a")
            
            # ✅ 함수 방식 - 스키마까지 박아서 절대 실패 안 함
            cos_dist = func.cosine_distance(
                n.c.embedding_vector,
                a.c.embedding_vector
            )
            
            # similarity = 1.0 - cosine_distance
            similarity = (literal(1.0) - cos_dist).label("similarity")
            
            stmt = (
                select(
                    a.c.code.label("anchor_code"),
                    similarity,
                )
                .select_from(n.join(a, n.c.model_name == a.c.model_name))
                .where(n.c.news_id == bindparam("news_id"))
                .order_by(desc(literal_column("similarity")))
            )
            
            similarities_result = self.repo.db.execute(stmt, {"news_id": news.id}).fetchall()
            logger.debug(f"✅ [{news.id}] SQL 실행 완료 - {len(similarities_result)}개 결과")
            
            similarities = [
                {
                    'anchor_code': row.anchor_code,
                    'similarity': float(row.similarity)
                }
                for row in similarities_result
            ]
            logger.debug(f"📝 [{news.id}] 유사도 결과 파싱 완료")
            
            if len(similarities) < 2:
                logger.warning(f"⚠️ [{news.id}] 앵커 벡터가 부족함 - {len(similarities)}개만 발견")
                return None
            
            top_anchor = similarities[0]['anchor_code']
            top_sim = similarities[0]['similarity']
            second_sim = similarities[1]['similarity']
            gap = top_sim - second_sim
            
            logger.debug(f"📈 [{news.id}] 유사도 분석 - 1위: {top_anchor}({top_sim:.4f}), 2위: {second_sim:.4f}, gap: {gap:.4f}")
            
            # 경제관련 여부 판단
            is_finance_related = False
            impact_flag = "NOISE"
            
            if top_sim < 0.52:
                impact_flag = "NOISE"
            elif gap < 0.005:
                impact_flag = "AMBIGUOUS"
            elif top_anchor in ['EARNINGS', 'MACRO', 'POLICY', 'COMPANY', 'SECTOR']:
                impact_flag = "PRICE_IMPACT_HIGH"
                is_finance_related = True
            elif top_anchor == 'MISC':
                impact_flag = "NOISE"
            else:
                impact_flag = "PRICE_IMPACT_LOW"
                is_finance_related = True
            
            flag_emoji = {"NOISE": "📰", "AMBIGUOUS": "❓", "PRICE_IMPACT_HIGH": "🚀", "PRICE_IMPACT_LOW": "📈"}
            logger.debug(f"🏷️ [{news.id}] 분류 결과 - {flag_emoji.get(impact_flag, '❓')} {impact_flag}, 경제관련: {is_finance_related}")
            
            # PRICE_IMPACT_LOW, PRICE_IMPACT_HIGH면 경제관련으로 판단
            return (is_finance_related, top_sim, top_anchor, top_sim, second_sim, gap)
            
        except Exception as e:
            logger.error(f"💥 [{news.id}] 경제관련 분류 중 오류: {str(e)}", exc_info=True)
            return None
