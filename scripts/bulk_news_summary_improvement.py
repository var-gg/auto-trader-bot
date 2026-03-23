#!/usr/bin/env python3
"""
한국경제 뉴스 요약 벌크 개선 스크립트

한국경제 뉴스들의 기존 summary_text를 벌크로 GPT에 전달하여:
1. 핵심 정보만 담은 새로운 요약문 생성
2. 1줄짜리 핵심 제목 생성
3. news_summary 테이블 업데이트

모델: gpt-5-mini 사용
"""

import sys
import os
import logging
import json
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from sqlalchemy import and_, text
from app.core.db import get_db
from app.core.gpt_client import responses_json
from app.features.news.models.news import News
from app.features.news.models.news_summary import NewsSummary

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("bulk_news_summary_improvement")

# 벌크 요약 개선을 위한 GPT 스키마
BULK_SUMMARY_IMPROVEMENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "improved_summaries": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "news_id": {"type": "integer"},
                    "improved_summary": {"type": "string"},
                    "improved_title": {"type": "string"}
                },
                "required": ["news_id", "improved_summary", "improved_title"]
            }
        }
    },
    "required": ["improved_summaries"]
}


def get_hankyung_news_summaries(db: Session, limit: int = 100) -> List[Dict[str, Any]]:
    """
    한국경제 뉴스의 요약 데이터를 조회
    
    Args:
        db: 데이터베이스 세션
        limit: 조회할 최대 개수
        
    Returns:
        List[Dict]: news_id, summary_text가 포함된 딕셔너리 리스트
    """
    logger.info(f"🔍 한국경제 뉴스 요약 데이터 조회 중... (최대 {limit}개)")
    
    query = (
        db.query(News.id, NewsSummary.summary_text)
        .join(NewsSummary, News.id == NewsSummary.news_id)
        .filter(
            and_(
                News.source.like("한국경제%"),
                NewsSummary.lang == "ko",
                NewsSummary.summary_text.isnot(None),
                NewsSummary.summary_text != ""
            )
        )
        .limit(limit)
    )
    
    results = []
    for news_id, summary_text in query.all():
        results.append({
            "news_id": news_id,
            "summary_text": summary_text
        })
    
    logger.info(f"📋 총 {len(results)}개의 한국경제 뉴스 요약을 조회했습니다.")
    return results


def create_bulk_prompt(news_data: List[Dict[str, Any]]) -> str:
    """
    벌크 처리용 프롬프트 생성
    
    Args:
        news_data: 뉴스 데이터 리스트
        
    Returns:
        str: GPT에 전달할 프롬프트
    """
    # 뉴스 데이터를 텍스트로 변환
    news_items = []
    for item in news_data:
        news_items.append(f"뉴스ID: {item['news_id']}\n요약: {item['summary_text']}\n")
    
    news_text = "\n".join(news_items)
    
    prompt = f"""
다음 뉴스들의 기존 요약을 개선해주세요.

[개선 요구사항]
1. 기존 요약의 인용과 표현을 바꿔서 새로운 문장으로 재구성
2. 불필요한 서술과 수식어 제거
3. 핵심 정보만 담은 간결한 요약문 생성 (2-3문장)
4. 각 뉴스에 대해 1줄짜리 핵심이 담긴 제목 생성

[처리할 뉴스들]
{news_text}

각 뉴스에 대해 다음 JSON 형식으로 응답해주세요:
{{
  "improved_summaries": [
    {{
      "news_id": 뉴스ID,
      "improved_summary": "개선된 요약문",
      "improved_title": "핵심 제목"
    }}
  ]
}}
"""
    return prompt


def call_gpt_for_bulk_improvement(news_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    GPT를 호출하여 벌크로 요약 개선
    
    Args:
        news_data: 뉴스 데이터 리스트
        
    Returns:
        List[Dict]: 개선된 요약과 제목이 포함된 딕셔너리 리스트
    """
    logger.info(f"🤖 GPT-5-mini로 {len(news_data)}개 뉴스 요약 개선 요청 중...")
    
    prompt = create_bulk_prompt(news_data)
    
    try:
        response = responses_json(
            model="gpt-5-mini",
            schema_name="bulk_summary_improvement",
            schema=BULK_SUMMARY_IMPROVEMENT_SCHEMA,
            user_text=prompt,
            temperature=0.1,
            task="bulk_news_summary_improvement"
        )
        
        improved_summaries = response.get("improved_summaries", [])
        logger.info(f"✅ GPT 응답으로 {len(improved_summaries)}개의 개선된 요약을 받았습니다.")
        
        return improved_summaries
        
    except Exception as e:
        logger.error(f"❌ GPT 호출 실패: {str(e)}")
        raise


def update_news_summaries(db: Session, improved_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    개선된 요약과 제목으로 news_summary 테이블 업데이트
    
    Args:
        db: 데이터베이스 세션
        improved_data: 개선된 데이터 리스트
        
    Returns:
        Dict[str, int]: 업데이트 결과 통계
    """
    logger.info(f"💾 {len(improved_data)}개의 뉴스 요약 업데이트 중...")
    
    stats = {
        "total": len(improved_data),
        "updated": 0,
        "failed": 0,
        "not_found": 0
    }
    
    for item in improved_data:
        try:
            news_id = item["news_id"]
            improved_summary = item["improved_summary"]
            improved_title = item["improved_title"]
            
            # news_summary 레코드 조회 및 업데이트
            news_summary = db.query(NewsSummary).filter(
                and_(
                    NewsSummary.news_id == news_id,
                    NewsSummary.lang == "ko"
                )
            ).first()
            
            if not news_summary:
                logger.warning(f"⚠️ NewsSummary not found for news_id: {news_id}")
                stats["not_found"] += 1
                continue
            
            # 업데이트
            news_summary.summary_text = improved_summary
            news_summary.title_localized = improved_title
            news_summary.model = "gpt-5-mini"
            
            stats["updated"] += 1
            logger.info(f"✅ News ID {news_id} 업데이트 완료")
            
        except Exception as e:
            logger.error(f"❌ News ID {item.get('news_id', 'unknown')} 업데이트 실패: {str(e)}")
            stats["failed"] += 1
    
    # 변경사항 커밋
    try:
        db.commit()
        logger.info("💾 모든 변경사항이 데이터베이스에 저장되었습니다.")
    except Exception as e:
        logger.error(f"❌ 데이터베이스 커밋 실패: {str(e)}")
        db.rollback()
        raise
    
    return stats


def main():
    """메인 실행 함수"""
    logger.info("🚀 한국경제 뉴스 요약 벌크 개선 스크립트 시작")
    
    # 하드코딩된 설정값
    MAX_NEWS_COUNT = 100
    MODEL_NAME = "gpt-5-mini"
    
    try:
        # 데이터베이스 세션 생성
        db = next(get_db())
        
        # 1. 한국경제 뉴스 요약 데이터 조회
        news_data = get_hankyung_news_summaries(db, MAX_NEWS_COUNT)
        
        if not news_data:
            logger.info("✅ 처리할 한국경제 뉴스가 없습니다.")
            return
        
        # 2. GPT로 벌크 요약 개선
        improved_data = call_gpt_for_bulk_improvement(news_data)
        
        if not improved_data:
            logger.warning("⚠️ GPT로부터 개선된 데이터를 받지 못했습니다.")
            return
        
        # 3. 데이터베이스 업데이트
        update_stats = update_news_summaries(db, improved_data)
        
        # 결과 출력
        logger.info("📊 벌크 요약 개선 작업 완료!")
        logger.info(f"  총 처리: {update_stats['total']}개")
        logger.info(f"  업데이트 성공: {update_stats['updated']}개")
        logger.info(f"  업데이트 실패: {update_stats['failed']}개")
        logger.info(f"  레코드 없음: {update_stats['not_found']}개")
        
        if update_stats["updated"] > 0:
            logger.info(f"🎉 {update_stats['updated']}개의 뉴스 요약이 성공적으로 개선되었습니다!")
        else:
            logger.info("ℹ️ 업데이트된 뉴스가 없습니다.")
            
    except Exception as e:
        logger.error(f"❌ 스크립트 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            db.close()
            logger.info("🔒 데이터베이스 연결이 종료되었습니다.")


if __name__ == "__main__":
    main()
