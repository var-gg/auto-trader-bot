# app/features/news/services/news_v2_reevaluation_service.py
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.features.news.models.news_ticker import NewsTicker
from app.features.news.schemas.news_ai_schemas import NEWS_V2_REEVALUATION_SCHEMA
from app.core.gpt_client import responses_json
from app.shared.models.ticker import Ticker
from sqlalchemy import text

logger = logging.getLogger(__name__)

# GPT-5 모델명 (환경변수에서 가져오거나 기본값 사용)
MODEL_GPT5_NANO = "gpt-5-nano"

class NewsV2ReevaluationService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo

    def run(self, limit: int = 100) -> Dict[str, Any]:
        """
        GPT-5 기반 티커 재평가: 티커 매핑 완료된 뉴스의 본문으로 신뢰도 재평가
        """
        logger.info(f"🤖 NewsV2 GPT-5 재평가 시작 - 최대 {limit}개 처리")
        
        items = self.repo.list_for_reevaluation(limit=limit)
        
        if not items:
            logger.info("📭 재평가할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0}
        
        logger.info(f"📊 총 {len(items)}개 뉴스 재평가 시작")
        
        success_count = 0
        failed_count = 0
        total_updated = 0
        
        for i, news in enumerate(items, 1):
            try:
                logger.debug(f"[{i}/{len(items)}] 🔍 뉴스 {news.id} 재평가 시작")
                
                # 재평가 실행
                updated_count = self._reevaluate_news_tickers(news)
                if updated_count >= 0:  # ✅ 0도 성공 (관련 티커 없음도 정상 결과)
                    success_count += 1
                    total_updated += updated_count
                    if updated_count > 0:
                        logger.debug(f"[{i}/{len(items)}] ✅ 재평가 완료: {news.id} - {updated_count}개 티커 업데이트")
                    else:
                        logger.debug(f"[{i}/{len(items)}] ✅ 재평가 완료: {news.id} - 관련 티커 없음")
                else:  # None이나 음수 (에러)
                    failed_count += 1
                    logger.debug(f"[{i}/{len(items)}] ❌ 재평가 실패: {news.id}")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"[{i}/{len(items)}] 💥 재평가 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items),
            "total_updated_tickers": total_updated
        }
        
        logger.info(f"🎉 NewsV2 재평가 완료: 성공 {success_count}개, 실패 {failed_count}개, 총 {total_updated}개 티커 업데이트")
        return result

    def _reevaluate_news_tickers(self, news: News) -> int:
        """단일 뉴스의 티커들에 대한 재평가 (본문 기반)"""
        try:
            logger.debug(f"🔍 [{news.id}] 티커 재평가 시작")
            
            # 1. 뉴스 본문 확인
            if not news.content:
                logger.warning(f"⚠️ [{news.id}] 뉴스 본문이 없음")
                return 0
            
            logger.debug(f"📄 [{news.id}] 뉴스 본문 확인 완료: {len(news.content)}자")
            
            # 2. 보유 티커 목록 조회
            held_tickers = self.repo.get_held_tickers()
            if not held_tickers:
                logger.warning(f"⚠️ [{news.id}] 보유 티커 목록이 비어있음")
                return 0
            
            logger.debug(f"📊 [{news.id}] 보유 티커 {len(held_tickers)}개 조회 완료")
            
            # 3. 전체 보유 티커를 GPT-5에 전달하여 관련 티커 선택 및 재평가
            updated_count = self._reevaluate_all_tickers(
                news, held_tickers
            )
            
            logger.debug(f"✅ [{news.id}] 재평가 완료: {updated_count}개 티커 업데이트")
            return updated_count
            
        except Exception as e:
            logger.error(f"💥 [{news.id}] 티커 재평가 중 오류: {str(e)}", exc_info=True)
            return 0


    def _reevaluate_all_tickers(self, news: News, held_tickers: List) -> int:
        """전체 보유 티커를 GPT-5에 전달하여 관련 티커 선택 및 재평가 (본문 기반, upsert 방식)"""
        try:
            logger.debug(f"🤖 [{news.id}] {len(held_tickers)}개 보유 티커 대상 재평가 시작")
            
            # 프롬프트 구성
            prompt = self._build_reevaluation_prompt(news, held_tickers)
            logger.debug(f"📝 [{news.id}] 프롬프트 구성 완료: {len(prompt)}자")
            
            # GPT-5 호출
            logger.debug(f"🚀 [{news.id}] GPT-5 API 호출 시작 - 모델: {MODEL_GPT5_NANO}")
            result = responses_json(
                model=MODEL_GPT5_NANO,
                schema_name="NewsV2Reevaluation",
                schema=NEWS_V2_REEVALUATION_SCHEMA,
                user_text=prompt,
                temperature=0.3,
                task="reevaluate_ticker_confidence",
                news_id=news.id
            )
            logger.debug(f"✅ [{news.id}] GPT-5 API 호출 완료")
            
            # 결과 파싱
            results = result.get("results", [])
            if not results:
                logger.debug(f"📭 [{news.id}] 재평가 결과 없음 (관련 티커 없음) - 기존 티커들 confidence=0으로 업데이트")
                # 기존 news_ticker들의 confidence를 0으로 업데이트 (다음번 조회에서 제외)
                self._mark_tickers_as_irrelevant(news.id)
                return 0
            
            # NewsTicker upsert (있으면 업데이트, 없으면 새로 생성)
            upserted_count = 0
            for item in results:
                try:
                    ticker_id = item.get("id")
                    new_confidence = item.get("confi")
                    
                    # 결과 검증
                    if not ticker_id or not new_confidence or not (0.8 <= new_confidence <= 1.0):
                        logger.warning(f"⚠️ [{news.id}] 무효한 재평가 결과: {item}")
                        continue
                    
                    # NewsTicker upsert
                    upserted_ticker = self._upsert_news_ticker(
                        news.id, ticker_id, new_confidence
                    )
                    
                    if upserted_ticker:
                        upserted_count += 1
                        logger.debug(f"📈 [{news.id}] 티커 {ticker_id} upsert 완료: 신뢰도 {new_confidence:.3f}")
                    else:
                        logger.warning(f"⚠️ [{news.id}] 티커 {ticker_id} upsert 실패")
                        
                except Exception as e:
                    logger.error(f"💥 [{news.id}] 티커 {item.get('id', 'unknown')} upsert 중 오류: {str(e)}")
            
            logger.debug(f"✅ [{news.id}] 재평가 완료: {upserted_count}개 티커 upsert")
            return upserted_count
                
        except Exception as e:
            logger.error(f"💥 [{news.id}] 재평가 중 오류: {str(e)}", exc_info=True)
            return 0

    def _mark_tickers_as_irrelevant(self, news_id: int):
        """해당 뉴스의 모든 티커를 관련 없음(confidence=0)으로 표시하고 뉴스를 FILTERED_NEG로 변경"""
        try:
            # 1. news_ticker들의 confidence를 0으로 업데이트
            self.repo.db.execute(
                text("""
                    UPDATE trading.news_ticker 
                    SET confidence = 0, 
                        method = 'gpt5_irrelevant'
                    WHERE news_id = :news_id
                """),
                {"news_id": news_id}
            )
            
            # 2. 뉴스 status를 FILTERED_NEG로 업데이트 (관련 없음 확정)
            from app.features.news.models.news import NewsStatus
            self.repo.db.execute(
                text("""
                    UPDATE trading.news 
                    SET status = :status
                    WHERE id = :news_id
                """),
                {"news_id": news_id, "status": NewsStatus.FILTERED_NEGATIVE.value}
            )
            
            self.repo.db.commit()
            logger.debug(f"📝 [{news_id}] 티커 confidence=0, 뉴스 status=FILTERED_NEG로 업데이트 완료")
        except Exception as e:
            logger.error(f"💥 [{news_id}] 업데이트 중 오류: {str(e)}")
            self.repo.db.rollback()

    def _upsert_news_ticker(self, news_id: int, ticker_id: int, confidence: float):
        """NewsTicker upsert (있으면 업데이트, 없으면 새로 생성)"""
        try:
            # 1. ticker_id가 실제로 존재하는지 확인
            ticker_exists = self.repo.db.execute(
                select(Ticker).where(Ticker.id == ticker_id)
            ).scalar_one_or_none()
            
            if not ticker_exists:
                logger.warning(f"⚠️ [{news_id}] ticker_id={ticker_id}가 존재하지 않음 - 스킵")
                return None
            
            # 2. 기존 NewsTicker 조회
            existing = self.repo.db.execute(
                select(NewsTicker).where(
                    and_(
                        NewsTicker.news_id == news_id,
                        NewsTicker.ticker_id == ticker_id
                    )
                )
            ).scalar_one_or_none()
            
            if existing:
                # 기존 것 업데이트
                existing.confidence = confidence
                existing.method = "gpt5_reevaluation"
                self.repo.db.commit()
                return existing
            else:
                # 새로 생성
                new_ticker = self.repo.create_news_ticker(
                    news_id=news_id,
                    ticker_id=ticker_id,
                    confidence=confidence,
                    method="gpt5_reevaluation"
                )
                return new_ticker
                
        except Exception as e:
            logger.error(f"💥 NewsTicker upsert 중 오류: {str(e)}")
            return None

    def _build_reevaluation_prompt(self, news: News, held_tickers: List) -> str:
        """재평가용 프롬프트 구성 (본문 기반, 전체 보유 티커에서 관련 티커 선택)"""
        
        # 보유 티커 목록을 문자열로 변환 (심볼 포함)
        held_tickers_str = "\n".join([
            f"ID: {ticker.id}, 기업명: {ticker.name}({ticker.symbol})"
            for ticker in held_tickers
        ])
        
        prompt = f"""당신은 주식 시장 전문가입니다. 주어진 뉴스 본문을 바탕으로, 이 뉴스로 인해 주가에 영향을 받을 기업들을 분석해주세요.

## 뉴스 본문
{news.content}

## 보유 티커 목록
{held_tickers_str}

## 요청사항
위 뉴스 본문을 분석하여, 해당 뉴스가 보유 티커 목록 중 어떤 기업들에 주가 영향을 미치는지 판단해주세요.

뉴스가 특정 기업에 영향을 미친다고 판단되면, 해당 기업의 티커 ID와 0.8~1.0 사이의 신뢰도를 제공해주세요. 최대 20개.

뉴스가 보유 티커들과 관련이 없다고 판단되면, 빈 배열을 반환해주세요.

응답 형식:
{{"results": [{{"id": 티커ID, "confi": 신뢰도}}, ...]}}

예시:
{{"results": [{{"id": 123, "confi": 0.85}}, {{"id": 456, "confi": 0.92}}]}}
또는 관련 없으면: {{"results": []}}"""

        return prompt
