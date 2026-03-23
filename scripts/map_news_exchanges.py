#!/usr/bin/env python3
"""
뉴스 요약 데이터가 있지만 거래소 매핑이 없는 뉴스에 대해 GPT를 통해 거래소 매핑 정보를 생성하는 스크립트
"""

import sys
import os
import logging
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.news.models.news import News
from app.features.news.models.news_summary import NewsSummary
from app.features.news.models.news_exchange import NewsExchange
from app.features.news.services.news_ai_service import NewsAIService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("map_news_exchanges") 

def get_news_without_exchange_mapping(db: Session, limit: int = 100) -> List[Dict[str, Any]]:
    """
    뉴스 요약은 있지만 거래소 매핑이 없는 뉴스 목록을 조회
    """
    query = db.query(
        News.id,
        News.title,
        NewsSummary.title_localized,
        NewsSummary.summary_text
    ).join(
        NewsSummary, News.id == NewsSummary.news_id
    ).outerjoin(
        NewsExchange, News.id == NewsExchange.news_id
    ).filter(
        NewsSummary.lang == 'ko',
        NewsSummary.summary_text.isnot(None),
        NewsExchange.id.is_(None)  # 거래소 매핑이 없는 경우
    ).limit(limit)
    
    return [
        {
            'news_id': row.id,
            'title': row.title,
            'title_ko': row.title_localized,
            'summary_ko': row.summary_text
        }
        for row in query.all()
    ]

def upsert_news_exchange(db: Session, news_id: int, exchange_code: str, confidence: float) -> NewsExchange:
    """
    뉴스 거래소 매핑 정보를 저장 (중복 방지)
    """
    existing = db.query(NewsExchange).filter(
        NewsExchange.news_id == news_id,
        NewsExchange.exchange_code == exchange_code
    ).first()
    
    if existing:
        # 기존 데이터가 있으면 confidence가 더 높을 때만 업데이트
        if confidence > (existing.confidence or 0):
            existing.confidence = confidence
            db.commit()
            logger.info(f"Updated exchange mapping: news_id={news_id}, exchange={exchange_code}, confidence={confidence}")
        return existing
    
    # 새로 생성
    new_mapping = NewsExchange(
        news_id=news_id,
        exchange_code=exchange_code,
        confidence=confidence,
        method="ai_retroactive"
    )
    db.add(new_mapping)
    db.commit()
    logger.info(f"Created exchange mapping: news_id={news_id}, exchange={exchange_code}, confidence={confidence}")
    return new_mapping

def process_news_exchange_mapping(db: Session, news_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    개별 뉴스에 대해 거래소 매핑을 수행
    """
    news_id = news_data['news_id']
    title_ko = news_data['title_ko']
    summary_ko = news_data['summary_ko']
    
    try:
        # GPT를 통해 거래소 매핑 정보 추출
        exchanges = NewsAIService.map_exchanges_from_summary(
            title_ko=title_ko,
            summary_ko=summary_ko,
            news_id=news_id,
            max_exchanges=3
        )
        
        # 매핑 정보 저장
        saved_count = 0
        for exchange in exchanges:
            upsert_news_exchange(
                db=db,
                news_id=news_id,
                exchange_code=exchange['exchange_code'],
                confidence=exchange['confidence']
            )
            saved_count += 1
        
        return {
            'news_id': news_id,
            'status': 'success',
            'mapped_exchanges': len(exchanges),
            'saved_count': saved_count,
            'exchanges': exchanges
        }
        
    except Exception as e:
        logger.error(f"Error processing news_id={news_id}: {str(e)}")
        return {
            'news_id': news_id,
            'status': 'error',
            'error': str(e),
            'mapped_exchanges': 0,
            'saved_count': 0
        }

def main():
    """
    메인 실행 함수
    """
    logger.info("Starting news exchange mapping process...")
    
    # 데이터베이스 연결
    db = next(get_db())
    
    try:
        # 처리할 뉴스 목록 조회
        logger.info("Fetching news without exchange mapping...")
        news_list = get_news_without_exchange_mapping(db, limit=100)
        
        if not news_list:
            logger.info("No news found without exchange mapping.")
            return
        
        logger.info(f"Found {len(news_list)} news items to process")
        
        # 통계 변수
        total_processed = 0
        total_success = 0
        total_error = 0
        total_exchanges_mapped = 0
        
        # 각 뉴스에 대해 거래소 매핑 수행
        for i, news_data in enumerate(news_list, 1):
            logger.info(f"Processing {i}/{len(news_list)}: news_id={news_data['news_id']}")
            
            result = process_news_exchange_mapping(db, news_data)
            total_processed += 1
            
            if result['status'] == 'success':
                total_success += 1
                total_exchanges_mapped += result['mapped_exchanges']
                logger.info(f"✓ Success: mapped {result['mapped_exchanges']} exchanges")
            else:
                total_error += 1
                logger.error(f"✗ Error: {result.get('error', 'Unknown error')}")
        
        # 최종 통계 출력
        logger.info("=" * 50)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Total processed: {total_processed}")
        logger.info(f"Successful: {total_success}")
        logger.info(f"Errors: {total_error}")
        logger.info(f"Total exchanges mapped: {total_exchanges_mapped}")
        logger.info(f"Success rate: {(total_success/total_processed)*100:.1f}%")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
