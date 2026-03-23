# app/features/recommendation/services/us_analyst_ai_service.py

import logging
import json
import asyncio
import numpy as np
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

from app.core.gpt_client import responses_json
from app.core.async_gpt_client import responses_json_async
from app.core.config import MODEL_ANALYST_AI  # 애널리스트 분석용 GPT5 모델
from app.features.earnings.services.earnings_service import EarningsService
from app.features.news.services.news_summary_service import NewsSummaryService
from app.features.news.repositories.kis_news_repository import KisNewsRepository
from app.features.fred.services.macro_snapshot_service import MacroSnapshotService
from app.features.marketdata.services.marketdata_prompt_common_service import MarketdataPromptCommonService
from app.features.fundamentals.services.us_fundamental_service import FundamentalService
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation, PositionType
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.features.signals.services.signal_detection_service import SignalDetectionService
from app.features.signals.models.similarity_models import SimilaritySearchRequest
from app.features.signals.models.signal_models import AlgorithmVersion

logger = logging.getLogger("us_analyst_ai_service")


def summarize_pattern_analysis(similar_signals: List[dict]) -> str:
    """
    유사 패턴 기반 빅데이터 분석 요약 (프롬프트용 1줄)
    - 시그널 방향(direction)에 따라 실제 수익률 부호를 정규화
    - 방향별 기대값/확률 분리 계산
    """
    if not similar_signals:
        return "빅데이터패턴분석결과: 유사 패턴 없음."

    sims = np.array([s["similarity"] for s in similar_signals])
    chgs = np.array([
        s["change_7_24d"] if s["direction"] == "UP" else -s["change_7_24d"]
        for s in similar_signals
    ])
    weights = sims / np.sum(sims)

    up_mask = chgs > 0
    down_mask = chgs < 0

    p_up = np.sum(weights[up_mask])
    p_down = np.sum(weights[down_mask])

    exp_up = np.sum(weights[up_mask] * chgs[up_mask]) / max(p_up, 1e-9)
    exp_down = np.sum(weights[down_mask] * chgs[down_mask]) / max(p_down, 1e-9)

    exp_ret = np.sum(weights * chgs)
    std_ret = np.sqrt(np.sum(weights * (chgs - exp_ret) ** 2))

    if p_up >= p_down:
        direction = "상승"
        prob = p_up
        exp_val = exp_up
    else:
        direction = "하락"
        prob = p_down
        exp_val = abs(exp_down)

    return (
        f"빅데이터패턴분석결과: {direction}확률 {prob*100:.1f}%, "
        f"예상{direction}률 {exp_val*100:.1f}%, "
        f"리스크(표준편차) {std_ret*100:.1f}%"
    )

# GPT 응답 스키마 정의 (전역변수)
ANALYST_RECOMMENDATION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "position_type": {
            "type": "string",
            "enum": ["LONG", "SHORT"]
        },
        "entry_price": {
            "type": "number",
            "minimum": 0
        },
        "target_price": {
            "type": "number",
            "minimum": 0
        },
        "stop_price": {
            "type": ["number", "null"],
            "minimum": 0
        },
        "valid_until": {
            "type": "string",
            "format": "date-time"
        },
        "reason": {
            "type": "string"
        },
        "confidence_score": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        }
    },
    "required": ["position_type", "entry_price", "target_price", "stop_price", "valid_until", "reason", "confidence_score"]
}


class UsAnalystAIService:
    """애널리스트 AI 분석 및 추천 생성 서비스 (미국주식 전용)"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def generate_analyst_recommendation(self, ticker_id: int) -> Dict[str, Any]:
        """
        애널리스트 AI를 통해 추천을 생성하고 데이터베이스에 저장합니다.
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            Dict[str, Any]: 생성된 추천 정보와 메타데이터
        """
        logger.info(f"애널리스트 AI 추천 생성 시작: 티커 ID {ticker_id}")
        
        try:
            # 티커 존재 여부 확인
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
            
            # 각 데이터 소스에서 정보 수집
            earnings_data = self._get_earnings_data(ticker_id)
            news_data = self._get_news_data(ticker_id)
            kis_news_titles = self._get_kis_news_titles(ticker_id)
            macro_data = self._get_macro_data()
            marketdata_data = self._get_marketdata_data(ticker_id)
            fundamentals_data = self._get_fundamentals_data(ticker_id)
            pattern_analysis = self._get_pattern_analysis(ticker_id)
            current_time_kst = self._get_current_time_kst()
            
            # GPT 분석 호출
            gpt_response = self._call_analyst_ai(
                ticker_symbol=ticker.symbol,
                ticker_exchange=ticker.exchange,
                earnings_data=earnings_data,
                news_data=news_data,
                kis_news_titles=kis_news_titles,
                macro_data=macro_data,
                marketdata_data=marketdata_data,
                fundamentals_data=fundamentals_data,
                pattern_analysis=pattern_analysis,
                current_time_kst=current_time_kst
            )
            
            # 마켓데이터에서 분석 당시 최근가격 추출
            analysis_price = None
            if marketdata_data.get("marketdata") and "current_price" in marketdata_data["marketdata"]:
                analysis_price = marketdata_data["marketdata"]["current_price"]
            
            # 데이터베이스에 추천 저장 (분석 당시 최근가격 포함)
            recommendation = self._save_recommendation(ticker_id, gpt_response, analysis_price)
            
            result = {
                "recommendation_id": recommendation.id,
                "ticker_id": ticker_id,
                "ticker_symbol": ticker.symbol,
                "ticker_exchange": ticker.exchange,
                "position_type": gpt_response["position_type"],
                "entry_price": gpt_response["entry_price"],
                "target_price": gpt_response["target_price"],
                "stop_price": gpt_response.get("stop_price"),
                "analysis_price": analysis_price,  # 분석 당시 최근가격 추가
                "valid_until": gpt_response["valid_until"],
                "reason": gpt_response["reason"],
                "confidence_score": gpt_response["confidence_score"],
                "is_latest": recommendation.is_latest,  # 최신 추천 여부 추가
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"애널리스트 AI 추천 생성 완료: 추천 ID {recommendation.id}")
            return result
            
        except Exception as e:
            logger.error(f"애널리스트 AI 추천 생성 중 오류 발생: {str(e)}")
            raise
    
    def _get_earnings_data(self, ticker_id: int) -> Dict[str, Any]:
        """어닝 데이터를 조회합니다."""
        try:
            service = EarningsService(self.db)
            earnings_data = service.get_earnings_for_analyst(ticker_id)
            return {"earnings": earnings_data} if earnings_data else {"earnings": {}}
        except Exception as e:
            logger.warning(f"어닝 데이터 조회 실패: {str(e)}")
            return {"earnings": {}}
    
    def _get_news_data(self, ticker_id: int) -> Dict[str, Any]:
        """뉴스 데이터를 조회합니다 (limit=10 고정)."""
        try:
            service = NewsSummaryService(self.db)
            news_result = service.get_news_summary_for_ticker(ticker_id, limit=10)
            
            if "error" in news_result:
                logger.warning(f"뉴스 데이터 조회 실패: {news_result['error']}")
                return {"news_summaries": []}
            
            # 필요한 필드만 추출
            news_summaries = []
            for item in news_result.get("news_summaries", []):
                news_summaries.append({
                    "id": item["id"],
                    "summary_text": item["summary_text"],
                    "published_date_kst": item.get("published_date_kst")
                })
            
            return {"news_summaries": news_summaries}
        except Exception as e:
            logger.warning(f"뉴스 데이터 조회 실패: {str(e)}")
            return {"news_summaries": []}
    
    def _get_kis_news_titles(self, ticker_id: int) -> List[str]:
        """KIS 뉴스 제목을 조회합니다 (limit=20 고정)."""
        try:
            repo = KisNewsRepository(self.db)
            kis_news_list = repo.list_by_ticker(ticker_id, limit=20)
            
            # 제목만 추출
            titles = [news.title for news in kis_news_list if news.title]
            return titles
        except Exception as e:
            logger.warning(f"KIS 뉴스 제목 조회 실패: {str(e)}")
            return []
    
    def _get_macro_data(self) -> Dict[str, Any]:
        """매크로 데이터를 조회합니다."""
        try:
            service = MacroSnapshotService(self.db)
            macro_result = service.build_compact_snapshot()
            return {"macro": macro_result}
        except Exception as e:
            logger.warning(f"매크로 데이터 조회 실패: {str(e)}")
            return {"macro": {}}
    
    def _get_marketdata_data(self, ticker_id: int) -> Dict[str, Any]:
        """마켓 데이터를 조회합니다 (days=50 고정)."""
        try:
            marketdata = MarketdataPromptCommonService.build_ticker_prompt_static(self.db, ticker_id, days=50)
            return {"marketdata": marketdata}
        except Exception as e:
            logger.warning(f"마켓 데이터 조회 실패: {str(e)}")
            return {"marketdata": {}}
    
    def _get_fundamentals_data(self, ticker_id: int) -> Dict[str, Any]:
        """펀더멘털 데이터를 조회합니다."""
        try:
            service = FundamentalService(self.db)
            fundamentals_result = service.get_fundamental_prompt_data(ticker_id)
            
            if "error" in fundamentals_result:
                logger.warning(f"펀더멘털 데이터 조회 실패: {fundamentals_result['error']}")
                return {"fundamentals": {}, "dividend_history": []}
            
            return {
                "fundamentals": fundamentals_result.get("fundamentals", {}),
                "dividend_history": fundamentals_result.get("dividend_history", [])
            }
        except Exception as e:
            logger.warning(f"펀더멘털 데이터 조회 실패: {str(e)}")
            return {"fundamentals": {}, "dividend_history": []}
    
    def _get_pattern_analysis(self, ticker_id: int) -> str:
        """패턴 분석 데이터를 조회하고 요약합니다."""
        try:
            signal_service = SignalDetectionService(self.db)
            
            # 유사도 검색 요청 (lookback=10, top_k=10, version=v3)
            request = SimilaritySearchRequest(
                ticker_id=ticker_id,
                reference_date=None,  # 오늘 기준
                lookback=10,
                top_k=10,
                direction_filter=None,  # 전체 방향
                version=AlgorithmVersion.V3
            )
            
            response = signal_service.search_similar_signals(request)
            
            # 응답을 dict 리스트로 변환
            similar_signals = []
            for signal in response.similar_signals:
                similar_signals.append({
                    "similarity": signal.similarity,
                    "change_7_24d": signal.change_7_24d,
                    "direction": signal.direction  # ✅ direction 필드 추가
                })
            
            # 요약 생성
            summary = summarize_pattern_analysis(similar_signals)
            return summary
            
        except Exception as e:
            logger.warning(f"패턴 분석 조회 실패: {str(e)}")
            return "빅데이터패턴분석결과: 데이터 부족으로 분석 불가."
    
    def _get_current_time_kst(self) -> str:
        """현재 시간을 KST 형식으로 반환합니다."""
        kst = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst)
        return now_kst.isoformat()
    
    def _get_korean_company_name(self, ticker_id: int) -> str:
        """티커의 한글 기업명을 조회합니다."""
        try:
            ticker_i18n = (
                self.db.query(TickerI18n.name)
                .filter(
                    TickerI18n.ticker_id == ticker_id,
                    TickerI18n.lang_code == 'ko'
                )
                .first()
            )
            
            if ticker_i18n:
                return ticker_i18n.name
            else:
                # 한글명이 없으면 기본 티커 심볼 반환
                ticker = self.db.query(Ticker.symbol).filter(Ticker.id == ticker_id).first()
                return ticker.symbol if ticker else f"Ticker_{ticker_id}"
                
        except Exception as e:
            logger.warning(f"한글 기업명 조회 실패 (티커 ID: {ticker_id}): {str(e)}")
            # 오류 시 기본값 반환
            ticker = self.db.query(Ticker.symbol).filter(Ticker.id == ticker_id).first()
            return ticker.symbol if ticker else f"Ticker_{ticker_id}"
    
    def _call_analyst_ai(
        self,
        ticker_symbol: str,
        ticker_exchange: str,
        earnings_data: Dict[str, Any],
        news_data: Dict[str, Any],
        kis_news_titles: List[str],
        macro_data: Dict[str, Any],
        marketdata_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        pattern_analysis: str,
        current_time_kst: str
    ) -> Dict[str, Any]:
        """애널리스트 AI를 호출하여 추천을 생성합니다."""
        
        # 전역 스키마 사용
        schema = ANALYST_RECOMMENDATION_SCHEMA
        
        # 한글 기업명 조회 (티커 ID는 ticker_symbol로부터 추론해야 함)
        # 이 경우 ticker_symbol과 ticker_exchange로 티커 ID를 찾아야 함
        ticker = self.db.query(Ticker.id).filter(
            Ticker.symbol == ticker_symbol,
            Ticker.exchange == ticker_exchange
        ).first()
        
        korean_company_name = self._get_korean_company_name(ticker.id) if ticker else ticker_symbol
        
        # 프롬프트 템플릿 구성
        user_prompt = self._build_analyst_prompt(
            ticker_symbol=ticker_symbol,
            ticker_exchange=ticker_exchange,
            korean_company_name=korean_company_name,
            earnings_data=earnings_data,
            news_data=news_data,
            kis_news_titles=kis_news_titles,
            macro_data=macro_data,
            marketdata_data=marketdata_data,
            fundamentals_data=fundamentals_data,
            pattern_analysis=pattern_analysis,
            current_time_kst=current_time_kst
        )
        
        # GPT 호출
        gpt_response = responses_json(
            model=MODEL_ANALYST_AI,
            schema_name="AnalystRecommendation",
            schema=schema,
            user_text=user_prompt,
            temperature=0.3,  # 창의성과 일관성의 균형
            task="analyst_recommendation",
            extra={
                "ticker_symbol": ticker_symbol,
                "ticker_exchange": ticker_exchange
            }
        )
        
        return gpt_response
    
    def _build_analyst_prompt(
        self,
        ticker_symbol: str,
        ticker_exchange: str,
        korean_company_name: str,
        earnings_data: Dict[str, Any],
        news_data: Dict[str, Any],
        kis_news_titles: List[str],
        macro_data: Dict[str, Any],
        marketdata_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        pattern_analysis: str,
        current_time_kst: str
    ) -> str:
        """애널리스트 AI용 프롬프트를 구성합니다."""
        
        import json
        
        # JSON 데이터를 문자열로 변환
        def json_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif hasattr(obj, 'isoformat'):  # datetime 객체
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        earnings_json = json.dumps(earnings_data, ensure_ascii=False, indent=2, default=json_serializer)
        news_json = json.dumps(news_data, ensure_ascii=False, indent=2, default=json_serializer)
        
        # KIS 뉴스 제목 리스트 포맷팅
        kis_news_text = "\n".join([f"  - {title}" for title in kis_news_titles]) if kis_news_titles else "  (없음)"
        
        macro_json = json.dumps(macro_data, ensure_ascii=False, indent=2, default=json_serializer)
        marketdata_json = json.dumps(marketdata_data, ensure_ascii=False, indent=2, default=json_serializer)
        fundamentals_json = json.dumps(fundamentals_data, ensure_ascii=False, indent=2, default=json_serializer)
        
        prompt = f"""
[SYSTEM]
당신은 월스트리트 최상위 단기 스윙 트레이딩 애널리스트입니다.
뉴스·어닝·기술·펀더멘털·매크로를 종합해 **하나의 방향(LONG 또는 SHORT)**만 제시하십시오.
분석 범위는 **2~20거래일**이며, 먼저 **향후 0~3일 단기 바이어스**를 판정한 뒤 수치를 산정합니다.

[관망(Observe) — 핵심]
- "관망"은 포지션 미개시를 뜻하지 않습니다.
- 신호가 약하거나 상충될 때는 **관망이 합리적임을 reason에 명시**하고,
  **체결확률이 매우 낮은(≈1~5%) 유리한 제한가('베이트')**를 entry_price로 제시하십시오.
- 베이트는 "현 수준에서의 성급한 진입을 지양한다"는 메시지로, **최근 저점/재테스트/0.3~0.5×ATR14** 등 합리적 근거를 붙입니다.
- 수량/주문유형은 다루지 않습니다(별도 모듈의 영역).

[STYLE]
- reason은 **3~4문장**, 각 문장 끝 줄바꿈. 오직 분석적 근거·논리만 자연어로 기술.
  입력항목, 출력항목 등 이미 아는 내용은 반복거론하지 않는다.
- 날짜=YYYY-MM-DD, 분기="YYYY년 N분기".
- 수치 소수점 2자리, 단위 명확($, %, bp). 모호어 금지.

[CONTEXT — INPUT JSONS]
(제공된 JSON들을 서술형으로 자연스럽게 인용하십시오: "발표 자료에 따르면/보도에 의하면/시장 데이터에 따르면")
- Company: {ticker_symbol} ({ticker_exchange}) - 한글명: {korean_company_name}
- now_kst: "{current_time_kst}"
- {pattern_analysis}
- Earnings JSON : {earnings_json}
- News Summaries JSON: {news_json}
- KIS News Titles (증권사 뉴스 제목):
{kis_news_text}
- Macro Snapshot JSON: {macro_json}
- MarketData JSON: {marketdata_json}
- Fundamentals JSON (단위: 백만 USD): {fundamentals_json}

[POLICY]
1) 방향 결정(0~3일 게이트):
   - 다음을 종합해 단기 바이어스를 판정: 뉴스 톤/모멘텀, 어닝 근접도(T±3영업일),
     MA20·MA50 기울기와 현재가의 상하관계, ATR14 대비 일중 범위, 매크로 스탠스, 단기 price_change.
   - **단기 하락 바이어스**이며 강한 상방 요인(≥2개)이 없으면 **SHORT**, 그 외는 **LONG**.

2) 가격 일관성:
   - LONG: target > entry > stop / SHORT: stop > entry > target.

3) 가격 설계(핵심 지표만 사용):
   - MA20/50(방향·크로스·위치), RSI14(30/50/70 레짐), MACD(12,26,9; 값/시그널/히스토그램),
     ATR14(절대·%화), 단기 price_change(예: 5D, 10D)만 활용.
   - entry_price는 현재가 대비 **유리한 구간**으로 산정(최근 스윙 고/저·피벗·0.3~0.5×ATR14 근방).
   - target은 **선택한 단기 범위의 기대 이동**을 반영(최근 스윙 폭/뉴스·어닝 촉발/MA20 기울기).
   - stop은 **기대수익 대비 1/2~1/3 리스크** 수준 또는 ‘무효화 레벨 + 0.2~0.3×ATR14’로 둡니다.

4) valid_until:
   - **2~15거래일** 내 합리적으로 결정(ISO는 JSON에만). 특별 사유 있을 때만 reason에 근거 명시.

5) reason(3~4문장, 200토큰 이내, 각 줄 끝 "\\n", 마지막 줄은 개행 금지):
   - **방향과 진입 조건(관망/베이트 여부 포함)**을 자연스럽게 드러낼 것.
   - 이어서 [어닝, 뉴스, 기술, 펀더멘털, 매크로] 중 **뉴스 포함 최소 1개 이상**을 **해석형**으로 연결(단순 나열 금지).
   - 필요 시 유효기간 사유를 명시.

[CONFIDENCE]
- 0.40±: 신호 상충/촉발 부재 → **관망 합리 + 베이트 제한가**(체결확률 매우 낮음) 제시
- 0.55±: 신호 2개 정렬, 근접 이벤트
- 0.70±: 3개 이상 정렬·유동성·매크로 우호
- 0.95±: 변수 거의 없음(드문 케이스)

[SELF-CHECK]
- 단기(0~3일) 바이어스를 먼저 결정했는가?
- 관망이 합리한 경우, **베이트 제한가**가 근거와 함께 제시되었는가?
- target/entry/stop 순서가 일관적인가?
- reason이 3~4문장으로 구성되어 있고, 200토큰 이내이며, 날짜·분기·단위 표기 규칙을 지키며, 
  실제 경제지 기고문에 그대로 실려도 어색하지 않을 만큼 자연스럽게 작성되었는가?
        """
        return prompt
    
    def _save_recommendation(self, ticker_id: int, gpt_response: Dict[str, Any], analysis_price: Optional[float] = None) -> AnalystRecommendation:
        """GPT 응답을 데이터베이스에 저장합니다."""
        
        # position_type을 Enum으로 변환
        position_type = PositionType(gpt_response["position_type"])
        
        # 추천 데이터 구성
        recommendation_data = {
            "ticker_id": ticker_id,
            "position_type": position_type,
            "entry_price": gpt_response["entry_price"],
            "target_price": gpt_response["target_price"],
            "stop_price": gpt_response.get("stop_price"),
            "analysis_price": analysis_price,  # 분석 당시 최근가격 추가
            "valid_until": datetime.fromisoformat(gpt_response["valid_until"].replace('Z', '+00:00')),
            "reason": gpt_response["reason"],
            "confidence_score": gpt_response["confidence_score"],
            "recommended_at": datetime.now(timezone.utc),
            "is_latest": True  # 새 추천은 항상 최신으로 설정
        }
        
        # 트랜잭션 시작: 기존 추천들을 is_latest=false로 업데이트 후 새 추천 저장
        try:
            # 1. 해당 티커의 기존 추천들을 모두 is_latest=false로 업데이트
            self.db.query(AnalystRecommendation)\
                .filter(AnalystRecommendation.ticker_id == ticker_id)\
                .filter(AnalystRecommendation.is_latest == True)\
                .update({"is_latest": False}, synchronize_session=False)
            
            # 2. 새 추천을 is_latest=true로 저장
            recommendation = AnalystRecommendation(**recommendation_data)
            self.db.add(recommendation)
            self.db.commit()
            self.db.refresh(recommendation)
            
            logger.info(f"티커 ID {ticker_id}의 새 추천 저장 완료 (ID: {recommendation.id})")
            return recommendation
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"추천 저장 중 오류 발생: {str(e)}")
            raise
    
    async def generate_analyst_recommendation_async(self, ticker_id: int) -> Dict[str, Any]:
        """
        애널리스트 AI를 통해 추천을 생성하고 데이터베이스에 저장합니다 (비동기 버전).
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            Dict[str, Any]: 생성된 추천 정보와 메타데이터
        """
        logger.info(f"애널리스트 AI 추천 생성 시작 (비동기): 티커 ID {ticker_id}")
        
        try:
            # 티커 존재 여부 확인
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
            
            # 각 데이터 소스에서 정보 수집
            earnings_data = self._get_earnings_data(ticker_id)
            news_data = self._get_news_data(ticker_id)
            kis_news_titles = self._get_kis_news_titles(ticker_id)
            macro_data = self._get_macro_data()
            marketdata_data = self._get_marketdata_data(ticker_id)
            fundamentals_data = self._get_fundamentals_data(ticker_id)
            pattern_analysis = self._get_pattern_analysis(ticker_id)
            current_time_kst = self._get_current_time_kst()
            
            # GPT 분석 호출 (비동기)
            gpt_response = await self._call_analyst_ai_async(
                ticker_symbol=ticker.symbol,
                ticker_exchange=ticker.exchange,
                earnings_data=earnings_data,
                news_data=news_data,
                kis_news_titles=kis_news_titles,
                macro_data=macro_data,
                marketdata_data=marketdata_data,
                fundamentals_data=fundamentals_data,
                pattern_analysis=pattern_analysis,
                current_time_kst=current_time_kst
            )
            
            # 마켓데이터에서 분석 당시 최근가격 추출
            analysis_price = None
            if marketdata_data.get("marketdata") and "current_price" in marketdata_data["marketdata"]:
                analysis_price = marketdata_data["marketdata"]["current_price"]
            
            # 데이터베이스에 추천 저장 (분석 당시 최근가격 포함)
            recommendation = self._save_recommendation(ticker_id, gpt_response, analysis_price)
            
            result = {
                "recommendation_id": recommendation.id,
                "ticker_id": ticker_id,
                "ticker_symbol": ticker.symbol,
                "ticker_exchange": ticker.exchange,
                "position_type": gpt_response["position_type"],
                "entry_price": gpt_response["entry_price"],
                "target_price": gpt_response["target_price"],
                "stop_price": gpt_response.get("stop_price"),
                "analysis_price": analysis_price,  # 분석 당시 최근가격 추가
                "valid_until": gpt_response["valid_until"],
                "reason": gpt_response["reason"],
                "confidence_score": gpt_response["confidence_score"],
                "is_latest": recommendation.is_latest,  # 최신 추천 여부 추가
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"애널리스트 AI 추천 생성 완료 (비동기): 추천 ID {recommendation.id}")
            return result
            
        except Exception as e:
            logger.error(f"애널리스트 AI 추천 생성 중 오류 발생 (비동기): {str(e)}")
            raise
    
    async def _call_analyst_ai_async(
        self,
        ticker_symbol: str,
        ticker_exchange: str,
        earnings_data: Dict[str, Any],
        news_data: Dict[str, Any],
        kis_news_titles: List[str],
        macro_data: Dict[str, Any],
        marketdata_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        pattern_analysis: str,
        current_time_kst: str
    ) -> Dict[str, Any]:
        """애널리스트 AI를 호출하여 추천을 생성합니다 (비동기 버전)."""
        
        # 전역 스키마 사용
        schema = ANALYST_RECOMMENDATION_SCHEMA
        
        # 한글 기업명 조회 (티커 ID는 ticker_symbol로부터 추론해야 함)
        # 이 경우 ticker_symbol과 ticker_exchange로 티커 ID를 찾아야 함
        ticker = self.db.query(Ticker.id).filter(
            Ticker.symbol == ticker_symbol,
            Ticker.exchange == ticker_exchange
        ).first()
        
        korean_company_name = self._get_korean_company_name(ticker.id) if ticker else ticker_symbol
        
        # 프롬프트 템플릿 구성
        user_prompt = self._build_analyst_prompt(
            ticker_symbol=ticker_symbol,
            ticker_exchange=ticker_exchange,
            korean_company_name=korean_company_name,
            earnings_data=earnings_data,
            news_data=news_data,
            kis_news_titles=kis_news_titles,
            macro_data=macro_data,
            marketdata_data=marketdata_data,
            fundamentals_data=fundamentals_data,
            pattern_analysis=pattern_analysis,
            current_time_kst=current_time_kst
        )
        
        # GPT 호출 (비동기)
        gpt_response = await responses_json_async(
            model=MODEL_ANALYST_AI,
            schema_name="AnalystRecommendation",
            schema=schema,
            user_text=user_prompt,
            temperature=0.3,  # 창의성과 일관성의 균형
            task="analyst_recommendation",
            extra={
                "ticker_symbol": ticker_symbol,
                "ticker_exchange": ticker_exchange
            }
        )
        
        return gpt_response
    
    def generate_analyst_prompt_only(self, ticker_id: int) -> str:
        """
        애널리스트 AI용 프롬프트만 생성합니다 (GPT 호출 없이).
        /recommendations/analyst-prompt/{ticker_id} API용
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            str: 완성된 프롬프트 텍스트
        """
        logger.info(f"애널리스트 AI 프롬프트 생성 시작: 티커 ID {ticker_id}")
        
        try:
            # 티커 존재 여부 확인
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                raise ValueError(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
            
            # 각 데이터 소스에서 정보 수집
            earnings_data = self._get_earnings_data(ticker_id)
            news_data = self._get_news_data(ticker_id)
            kis_news_titles = self._get_kis_news_titles(ticker_id)
            macro_data = self._get_macro_data()
            marketdata_data = self._get_marketdata_data(ticker_id)
            fundamentals_data = self._get_fundamentals_data(ticker_id)
            pattern_analysis = self._get_pattern_analysis(ticker_id)
            current_time_kst = self._get_current_time_kst()
            
            # 한글 기업명 조회
            korean_company_name = self._get_korean_company_name(ticker_id)
            
            # 프롬프트 템플릿 구성
            user_prompt = self._build_analyst_prompt(
                ticker_symbol=ticker.symbol,
                ticker_exchange=ticker.exchange,
                korean_company_name=korean_company_name,
                earnings_data=earnings_data,
                news_data=news_data,
                kis_news_titles=kis_news_titles,
                macro_data=macro_data,
                marketdata_data=marketdata_data,
                fundamentals_data=fundamentals_data,
                pattern_analysis=pattern_analysis,
                current_time_kst=current_time_kst
            )
            
            logger.info(f"애널리스트 AI 프롬프트 생성 완료: 티커 ID {ticker_id}")
            return user_prompt
            
        except Exception as e:
            logger.error(f"애널리스트 AI 프롬프트 생성 중 오류 발생: {str(e)}")
            raise
