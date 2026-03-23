# app/features/news/services/news_v2_ticker_service.py
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text, select, desc, literal_column, bindparam, literal, func

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.core.vertex_ai_client import VertexAIClient
from app.features.news.models.news_vector import NewsVector
from app.features.fundamentals.models.ticker_vector import TickerVector
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n

logger = logging.getLogger(__name__)

class NewsV2TickerService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo
        self.vertex_ai_client = VertexAIClient()

    def run(self, limit: int = 100) -> Dict[str, Any]:
        """
        경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들의 티커 매핑
        """
        logger.info(f"NewsV2 티커 매핑 시작 - 최대 {limit}개 처리")
        
        items = self.repo.list_for_ticker_mapping(limit=limit)
        
        if not items:
            logger.info("처리할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0, "ticker_mappings": 0}
        
        logger.info(f"총 {len(items)}개 뉴스 티커 매핑 시작")
        
        success_count = 0
        failed_count = 0
        total_ticker_mappings = 0
        
        for news in items:
            try:
                # 티커 매핑 수행
                mappings = self._map_tickers_to_news(news)
                if mappings is not None:
                    success_count += 1
                    total_ticker_mappings += len(mappings)
                    logger.debug(f"티커 매핑 완료: {news.id} - {len(mappings)}개 티커")
                else:
                    failed_count += 1
                    logger.debug(f"티커 매핑 실패: {news.id}")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"티커 매핑 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items),
            "ticker_mappings": total_ticker_mappings
        }
        
        logger.info(f"NewsV2 티커 매핑 완료: 성공 {success_count}개, 실패 {failed_count}개, 총 매핑 {total_ticker_mappings}개")
        return result

    def _map_tickers_to_news(self, news: News) -> Optional[List[Dict]]:
        """
        단일 뉴스에 대한 티커 매핑
        제공해주신 SQL을 ORM으로 변환
        """
        try:
            # 뉴스 벡터 조회
            news_vector = self.repo.get_news_vector(news.id, self.vertex_ai_client.model_name)
            if not news_vector:
                logger.warning(f"뉴스 벡터를 찾을 수 없음: {news.id}")
                return None
            
            # 테이블 별칭으로 가독성 향상
            # 테이블 별칭(가독성)
            n = NewsVector.__table__.alias("n")
            n2 = News.__table__.alias("n2")
            t = TickerVector.__table__.alias("t")
            tt = Ticker.__table__.alias("tt")
            tin = TickerI18n.__table__.alias("tin")
            
            # ✅ 함수 방식 - 스키마까지 박아서 절대 실패 안 함
            cos_dist = func.cosine_distance(
                n.c.embedding_vector,
                t.c.embedding_vector
            )
            
            # similarity = 1.0 - cosine_distance
            similarity = (literal(1.0) - cos_dist).label("similarity")
            
            stmt = (
                select(
                    n2.c.title,
                    n.c.news_id,
                    t.c.ticker_id,
                    tt.c.symbol,
                    tin.c.name,
                    similarity,
                )
                .select_from(
                    n.join(n2, n.c.news_id == n2.c.id)
                    .join(t, n.c.model_name == t.c.model_name)
                    .join(tt, tt.c.id == t.c.ticker_id)
                    .join(tin, tin.c.ticker_id == tt.c.id)
                )
                .where(n2.c.id == bindparam("news_id"))
                .where(tin.c.lang_code == 'ko')
                .order_by(desc(literal_column("similarity")))
                .limit(20)
            )
            
            similarities = self.repo.db.execute(stmt, {"news_id": news.id}).fetchall()
            
            if not similarities:
                logger.warning(f"유사한 티커를 찾을 수 없음: {news.id} - ticker_vector 테이블에 데이터가 없거나 SQL 오류")
                return []
            
            # 매핑 로직 적용
            mappings = []
            
            # 0.59 이상인 것들 전부 매핑
            high_confidence_tickers = [s for s in similarities if s.similarity >= 0.59]
            
            if high_confidence_tickers:
                for ticker_info in high_confidence_tickers:
                    mapping = self.repo.create_news_ticker(
                        news_id=news.id,
                        ticker_id=ticker_info.ticker_id,
                        confidence=ticker_info.similarity,
                        method="vector_similarity_high"
                    )
                    mappings.append({
                        "ticker_id": ticker_info.ticker_id,
                        "symbol": ticker_info.symbol,
                        "name": ticker_info.name,
                        "similarity": ticker_info.similarity,
                        "method": "high_confidence"
                    })
                    logger.debug(f"고신뢰도 매핑: {ticker_info.symbol} ({ticker_info.similarity:.4f})")
            
            else:
                # 0.56 이상인 것들 중 3개만 매핑
                medium_confidence_tickers = [s for s in similarities if s.similarity >= 0.56][:3]
                
                if medium_confidence_tickers:
                    for ticker_info in medium_confidence_tickers:
                        mapping = self.repo.create_news_ticker(
                            news_id=news.id,
                            ticker_id=ticker_info.ticker_id,
                            confidence=ticker_info.similarity,
                            method="vector_similarity_medium"
                        )
                        mappings.append({
                            "ticker_id": ticker_info.ticker_id,
                            "symbol": ticker_info.symbol,
                            "name": ticker_info.name,
                            "similarity": ticker_info.similarity,
                            "method": "medium_confidence"
                        })
                        logger.debug(f"중신뢰도 매핑: {ticker_info.symbol} ({ticker_info.similarity:.4f})")
                else:
                    logger.debug(f"매핑 조건 미충족: {news.id} - 최고 유사도 {similarities[0].similarity:.4f} (0.56 미만)")
            
            return mappings
            
        except Exception as e:
            logger.error(f"티커 매핑 중 오류: {news.id} - {str(e)}")
            return None
