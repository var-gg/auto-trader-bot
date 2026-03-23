# app/features/news/services/news_v2_summary_service.py
import logging
from typing import List, Dict, Any, Optional

from app.features.news.repositories.news_v2_repository import NewsV2Repository
from app.features.news.models.news import News
from app.features.news.models.news_ticker import NewsTicker
from app.features.news.schemas.news_ai_schemas import NEWS_V2_SUMMARY_SCHEMA
from app.core.gpt_client import responses_json
from app.core.config import MODEL_SUMMARIZE

logger = logging.getLogger(__name__)

class NewsV2SummaryService:
    def __init__(self, repo: NewsV2Repository):
        self.repo = repo

    def run(self, limit: int = 100) -> Dict[str, Any]:
        """
        티커 매핑은 완료되었지만 요약이 안된 뉴스들의 요약 생성
        """
        logger.info(f"📝 NewsV2 요약 생성 시작 - 최대 {limit}개 처리")
        
        items = self.repo.list_for_summary(limit=limit)
        
        if not items:
            logger.info("📭 처리할 뉴스가 없습니다.")
            return {"success": 0, "failed": 0, "total": 0}
        
        logger.info(f"📊 총 {len(items)}개 뉴스 요약 생성 시작")
        
        success_count = 0
        failed_count = 0
        
        for i, news in enumerate(items, 1):
            try:
                logger.debug(f"[{i}/{len(items)}] 🔍 뉴스 {news.id} 요약 생성 시작")
                
                # 요약 생성
                summary_result = self._generate_summary(news)
                if summary_result:
                    summary_text, title_localized = summary_result
                    
                    logger.debug(f"[{i}/{len(items)}] 💾 [{news.id}] 요약 DB 저장 시작")
                    self.repo.create_news_summary(
                        news_id=news.id,
                        summary_text=summary_text,
                        title_localized=title_localized,
                        model=MODEL_SUMMARIZE
                    )
                    
                    # V2 프로세스 완료 표시
                    logger.debug(f"[{i}/{len(items)}] 🏁 [{news.id}] V2 프로세스 완료 표시")
                    self.repo.mark_v2_completed(news)
                    
                    success_count += 1
                    logger.debug(f"[{i}/{len(items)}] ✅ 요약 생성 완료: {news.id} - {news.title[:50]}...")
                else:
                    failed_count += 1
                    logger.debug(f"[{i}/{len(items)}] ❌ 요약 생성 실패: {news.id} - {news.title[:50]}...")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"[{i}/{len(items)}] 💥 요약 생성 중 오류: {news.id} - {str(e)}")
        
        result = {
            "success": success_count,
            "failed": failed_count,
            "total": len(items)
        }
        
        logger.info(f"🎉 NewsV2 요약 생성 완료: 성공 {success_count}개, 실패 {failed_count}개")
        return result

    def _generate_summary(self, news: News) -> Optional[tuple[str, str]]:
        """단일 뉴스의 요약 생성 (요약 텍스트, 현지화된 제목)"""
        try:
            logger.debug(f"🔍 [{news.id}] 요약 생성 시작")
            
            # 관련 티커 정보 조회 (참고용)
            ticker_info = self._get_related_tickers(news.id)
            if ticker_info:
                logger.debug(f"📊 [{news.id}] 관련 티커 {len(ticker_info)}개 확인됨")
            
            # 프롬프트 구성
            logger.debug(f"📝 [{news.id}] 프롬프트 구성 시작")
            user_text = self._build_summary_prompt(news, ticker_info)
            logger.debug(f"✅ [{news.id}] 프롬프트 구성 완료 - {len(user_text)}자")
            
            # GPT 호출 - structured output 사용
            logger.debug(f"🤖 [{news.id}] GPT API 호출 시작 - 모델: {MODEL_SUMMARIZE}")
            obj = responses_json(
                model=MODEL_SUMMARIZE,
                schema_name="NewsV2Summary",
                schema=NEWS_V2_SUMMARY_SCHEMA,
                user_text=user_text,
                temperature=0.3,
                task="summarize_v2",
                news_id=news.id
            )
            logger.debug(f"✅ [{news.id}] GPT API 호출 완료")
            
            summary_text = obj.get("summary", "").strip()
            title_localized = obj.get("title", "").strip()
            
            logger.debug(f"📄 [{news.id}] 응답 파싱 완료 - 요약: {len(summary_text)}자, 제목: {len(title_localized)}자")
            
            # 요약 길이 검증
            if len(summary_text) < 50 or len(summary_text) > 1000:
                logger.warning(f"⚠️ [{news.id}] 요약 길이 부적절: {len(summary_text)}자")
                return None
            
            # 제목 길이 검증
            if len(title_localized) < 10 or len(title_localized) > 200:
                logger.warning(f"⚠️ [{news.id}] 제목 길이 부적절: {len(title_localized)}자")
                return None
            
            logger.debug(f"✅ [{news.id}] 요약 검증 통과")
            return (summary_text, title_localized)
            
        except Exception as e:
            logger.error(f"💥 [{news.id}] 요약 생성 중 오류: {str(e)}", exc_info=True)
            return None

    def _get_related_tickers(self, news_id: int) -> List[Dict]:
        """뉴스와 관련된 티커 정보 조회"""
        try:
            # 뉴스-티커 매핑 정보 조회
            from sqlalchemy import text
            mappings = self.repo.db.execute(
                text("""
                    SELECT 
                        nt.ticker_id,
                        nt.confidence,
                        t.symbol,
                        ti.name as ticker_name
                    FROM trading.news_ticker nt
                    JOIN trading.ticker t ON t.id = nt.ticker_id
                    LEFT JOIN trading.ticker_i18n ti ON ti.ticker_id = t.id AND ti.lang_code = 'ko'
                    WHERE nt.news_id = :news_id
                    ORDER BY nt.confidence DESC
                """),
                {"news_id": news_id}
            ).fetchall()
            
            ticker_info = []
            for mapping in mappings:
                ticker_info.append({
                    "symbol": mapping.symbol,
                    "name": mapping.ticker_name or mapping.symbol,
                    "confidence": mapping.confidence
                })
            
            return ticker_info
            
        except Exception as e:
            logger.error(f"관련 티커 조회 중 오류: {news_id} - {str(e)}")
            return []

    def _build_summary_prompt(self, news: News, ticker_info: List[Dict]) -> str:
        """요약 프롬프트 구성"""
        
        user_text = f"""
당신은 "투자 분석 전문 뉴스 에디터"입니다.  
입력된 뉴스 원문은 언론 스타일, 감정적 어조, 반복된 인용, 광고성 문구 등 불필요한 요소가 섞여 있습니다.  
이를 완전히 정제하여, **투자 판단에 의미 있는 핵심 정보만 남긴 한글 요약문**을 150토큰 이내로 작성하십시오.

[핵심 지침]
1. **출력은 반드시 한글**로 작성합니다.  
2. **원문의 직접 인용문, 감탄사, 비유적 표현, 기자 코멘트, 인용부호(" ")** 등은 **절대 사용하지 않습니다.**  
   - 대신, 해당 인용의 의미를 **간결하고 객관적인 서술로 재구성**하십시오.  
   - 예: `"대한민국 대박!"` → `한국 시장에 긍정적 반응이 나타났다.`  
   - 예: `"We're doomed," said CEO` → `CEO는 사업 전망이 어둡다고 밝혔다.`  
3. 문체는 **냉정하고 분석적인 보고서형 문체**로, 문장 수는 **최대 3~4문장**을 넘지 않습니다.  
4. **투자에 직접 관련된 정보(기업, 산업, 정책, 금리, 실적, 규제, 인플레, 환율 등)**만 남기고  
   **사건과 수치(날짜, 수익률, 발표 수치 등)**는 반드시 보존하십시오.  
5. 배경 설명·이력·불필요한 수식어는 모두 제거합니다.  
6. **요약문을 기반으로, 해당 뉴스의 핵심 의미를 25자 이내로 압축한 한 줄 제목**을 작성하십시오.  
   - 제목은 **사실 중심 + 경제신문식 간결 어투**로 작성하십시오.  
   - 예: `테슬라, 저가형 모델 Y 출시로 시장 방어` / `파월, 금리 인하 지연 시사`

본문:
{news.content}
"""
        
        return user_text
