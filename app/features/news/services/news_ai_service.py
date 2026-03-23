from __future__ import annotations
import json
from typing import Tuple, List, Dict, Any, Optional
from app.core.gpt_client import responses_json
from app.core.config import MODEL_CLASSIFY, MODEL_SUMMARIZE, MODEL_TICKER_PICK
from .news_prompt_service import NewsPromptService

# 고정 테마 (id:1..18)
FIXED_THEMES = [
    {"id":1, "code":"Artificial Intelligence"},
    {"id":2, "code":"Semiconductors"},
    {"id":3, "code":"Cloud Computing"},
    {"id":4, "code":"Electric Vehicles"},
    {"id":5, "code":"Batteries & Storage"},
    {"id":6, "code":"Renewable Energy"},
    {"id":7, "code":"Healthcare & Biotech"},
    {"id":8, "code":"Pharmaceuticals"},
    {"id":9, "code":"Cybersecurity"},
    {"id":10, "code":"Fintech"},
    {"id":11, "code":"ESG & Sustainability"},
    {"id":12, "code":"Defense & Aerospace"},
    {"id":13, "code":"5G & Next-Gen Connectivity"},
    {"id":14, "code":"REITs"},
    {"id":15, "code":"E-commerce"},
    {"id":16, "code":"Banking & Financials"},
    {"id":17, "code":"Consumer & Retail"},
    {"id":18, "code":"Energy & Materials"},
]

class NewsAIService:
    """뉴스 전용: 분류/요약+테마/티커·스코프 제안"""

    @staticmethod
    def classify_finance_relevance(
        title: str, summary: str | None = None, *, news_id: int | None = None
    ) -> Tuple[bool, float, str]:
        # 분리된 프롬프트 생성 함수 사용
        schema, user = NewsPromptService.generate_classify_finance_relevance_prompt(title, summary)
        
        obj = responses_json(
            model=MODEL_CLASSIFY,
            schema_name="FinanceRelevance",
            schema=schema,
            user_text=user,
            temperature=None,           # gpt-5-mini 호환(생략)
            task="classify",
            news_id=news_id,
        )
        score = float(obj["score"])
        is_related = score > 0.0
        return is_related, score, MODEL_CLASSIFY

    @staticmethod
    def summarize_and_tag_themes(
        title: str, content: str, *, max_themes: int = 5, news_id: int | None = None
    ):
        """
        - 한국어 제목/요약
        - 테마 선택 (0..1)
        - 거래소 매핑 (0..1)
        - market_wide: 거시/지수/다수 섹터 동시 영향 힌트
        - market_wide=True면 1..18 전부 confidence=1.0로 정규화
        """
        # 분리된 프롬프트 생성 함수 사용
        schema, user = NewsPromptService.generate_summarize_and_tag_themes_prompt(title, content, max_themes=max_themes)
        cat = [{"id":t["id"], "code":t["code"]} for t in FIXED_THEMES]
        
        obj = responses_json(
            model=MODEL_SUMMARIZE,
            schema_name="NewsSummaryThemesV2",
            schema=schema,
            user_text=user,
            temperature=None,           # gpt-5-mini 호환
            task="summarize",
            news_id=news_id,
            extra={"fixed_themes": cat}
        )

        picked = [
            {"theme_id": int(x["theme_id"]), "confidence": float(x["confidence"])}
            for x in (obj.get("themes") or [])
        ]
        if obj.get("market_wide"):
            picked = [{"theme_id": t["id"], "confidence": 1.0} for t in FIXED_THEMES]

        exchanges = [
            {"exchange_code": str(x["exchange_code"]).upper(), "confidence": float(x["confidence"])}
            for x in (obj.get("exchanges") or [])
        ]

        return obj["title_ko"], obj["summary_ko"], "ko", MODEL_SUMMARIZE, picked, exchanges, bool(obj.get("market_wide"))

    @staticmethod
    def suggest_tickers_or_scope(
        *, title_ko: str, summary_ko: str, themes: List[Dict], news_id: int | None = None, max_tickers: int = 10, db: Session = None
    ) -> Dict[str, Any]:
        """
        후보 티커 없이 스코프/티커 추출.
        - market_scope: "ALL" | "SECTOR" | "TICKERS"
        - SECTOR → sector_theme_ids (필수) : 고정 테마 id 배열
        - TICKERS → 야후파이낸스 거래소코드(NMS/NYQ/AMEX/ARCA/… 등) 우선, country는 US/KR/JP…
        """
        # 분리된 프롬프트 생성 함수 사용
        schema, user = NewsPromptService.generate_suggest_tickers_or_scope_prompt(
            title_ko=title_ko, summary_ko=summary_ko, themes=themes, max_tickers=max_tickers, db=db
        )
        theme_ids = [t["theme_id"] for t in themes]

        obj = responses_json(
            model=MODEL_TICKER_PICK,
            schema_name="TickerOrScopeV2",
            schema=schema,
            user_text=user,
            temperature=None,           # gpt-5-mini 호환
            task="ticker_pick",
            news_id=news_id,
            extra={"themes": theme_ids}
        )

        scope = obj.get("market_scope")
        # 보정: SECTOR인데 sector_theme_ids 없으면 빈배열 세팅
        if scope == "SECTOR" and "sector_theme_ids" not in obj:
            obj["sector_theme_ids"] = []
        # TICKERS 정규화
        tickers = []
        if scope == "TICKERS":
            raw = obj.get("tickers") or []
            for t in raw[:max_tickers]:
                sym = (t.get("symbol") or "").strip().upper()
                exch = (t.get("exchange") or "").strip().upper() or None  # 야후코드 그대로
                ctry = (t.get("country") or "").strip().upper() or None
                conf = float(t.get("confidence") or 0)
                if sym:
                    tickers.append({"symbol": sym, "exchange": exch, "country": ctry, "confidence": conf})
        return {"market_scope": scope, "sector_theme_ids": obj.get("sector_theme_ids") or [], "tickers": tickers}

    @staticmethod
    def map_exchanges_from_summary(
        title_ko: str, summary_ko: str, *, news_id: int | None = None, max_exchanges: int = 3
    ) -> List[Dict[str, Any]]:
        """
        뉴스 요약을 기반으로 관련 거래소를 매핑
        - title_ko: 한국어 제목
        - summary_ko: 한국어 요약
        - max_exchanges: 최대 거래소 개수 (기본 3개)
        """
        # 분리된 프롬프트 생성 함수 사용
        schema, user = NewsPromptService.generate_exchange_mapping_prompt(
            title_ko=title_ko, summary_ko=summary_ko, max_exchanges=max_exchanges
        )

        obj = responses_json(
            model=MODEL_SUMMARIZE,
            schema_name="ExchangeMapping",
            schema=schema,
            user_text=user,
            temperature=None,
            task="exchange_mapping",
            news_id=news_id,
        )

        exchanges = []
        for ex in (obj.get("exchanges") or [])[:max_exchanges]:
            exchange_code = str(ex["exchange_code"]).upper().strip()
            confidence = float(ex["confidence"])
            
            # 유효한 거래소 코드인지 확인
            valid_codes = ["NMS", "NYQ", "TSE", "KOE", "LSE", "FRA", "HKG", "ASX"]
            if exchange_code in valid_codes:
                exchanges.append({
                    "exchange_code": exchange_code,
                    "confidence": confidence
                })

        return exchanges
