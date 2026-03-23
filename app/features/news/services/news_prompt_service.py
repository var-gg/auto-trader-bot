# app/features/news/services/news_prompt_service.py

from __future__ import annotations
import json
from typing import Tuple, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from app.features.news.schemas.news_ai_schemas import (
    FINANCE_RELEVANCE_SCHEMA,
    NEWS_SUMMARY_THEMES_SCHEMA, 
    TICKER_OR_SCOPE_SCHEMA,
    EXCHANGE_MAPPING_SCHEMA
)
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n

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

class NewsPromptService:
    """뉴스 AI 프롬프트 생성 전용 서비스"""

    @staticmethod
    def _get_ticker_list_compressed(db: Session) -> str:
        """
        전체 티커 목록을 토큰 최소화 형식으로 반환
        형식: "기업명=종목코드; 기업명=종목코드; ..."
        """
        # Ticker + TickerI18n 조인해서 한국어 기업명과 종목코드 조회
        query = (
            db.execute(
                select(Ticker.symbol, TickerI18n.name)
                .join(TickerI18n, Ticker.id == TickerI18n.ticker_id)
                .where(TickerI18n.lang_code == 'ko')
                .order_by(TickerI18n.name)
            )
            .all()
        )
        
        # 토큰 최소화 형식으로 변환
        ticker_pairs = []
        for symbol, name in query:
            if name and symbol:  # None 체크
                ticker_pairs.append(f"{name}={symbol}")
        
        return "; ".join(ticker_pairs) + ";"

    @staticmethod
    def generate_classify_finance_relevance_prompt(
        title: str, summary: str | None = None
    ) -> Tuple[Dict, str]:
        """
        뉴스 관련성 분류 프롬프트를 생성합니다.
        
        Returns:
            Tuple[Dict, str]: (schema, user_prompt)
        """
        user = (
            "뉴스가 자본시장(금융시장)에 얼마나 관련 있는지 0.0~1.0 점수를 주어라.\n"
            "- 관련: 거시/정책(금리, 인플레, 고용, GDP, 무역, 환율 등), "
            "자산시장(주식·채권·원자재·크립토), "
            "기업 이벤트(실적, M&A, 투자, 감산, 구조조정, 신제품, 경영진, 계약, 규제·관세·조사, "
            "소송·벌금·합의 등), 지정학/재해로 인한 시장 영향.\n"
            "- 비관련(0.0~0.1): 정치, 외교, 범죄, 연예 등.\n"
            "- 점수: 0.0=무관, 0.01–0.29=약함, 0.30–0.59=중간, 0.60–1.0=강함.\n"
            "- 기업 사건(소송, 규제, 벌금 등)은 반드시 0.3 이상.\n\n"
            "JSON {\"score\": number} 형식만 반환.\n\n"
            f"title: {title}\nsummary: {summary or 'N/A'}"
        )
        return FINANCE_RELEVANCE_SCHEMA, user

    @staticmethod
    def generate_summarize_and_tag_themes_prompt(
        title: str, content: str, *, max_themes: int = 5
    ) -> Tuple[Dict, str]:
        """
        뉴스 요약 및 테마 태깅 프롬프트를 생성합니다.
        
        Returns:
            Tuple[Dict, str]: (schema, user_prompt)
        """
        cat = [{"id":t["id"], "code":t["code"]} for t in FIXED_THEMES]

        user = (
            f"아래 기사를 **직접 인용 없이 요약·재구성**하십시오. "
            "원문 문장을 그대로 쓰지 말고, 동일한 의미를 새로운 문장 구조로 표현하십시오. "
            "요약은 기사의 핵심 사실·데이터·시장영향 요인(실적, 가이던스, 규제, 거시 등)에 집중하고, "
            "불필요한 수식어나 기자 코멘트는 제거하십시오.\n\n"

            "[출력 형식]\n"
            "- title_ko: 기사 요약 내용을 근거로 **완전히 새로 작성한 1문장 제목** (원제목과 달라야 함)\n"
            "- summary_ko: 3~5문장, 객관적 사실 중심의 요약문 (직접 인용·표현 재사용 금지)\n\n"

            "[테마 선택 규칙]\n"
            "- theme_id는 반드시 아래 고정 목록에서만 선택\n"
            "- 시장 전반(거시, 지수/대다수 섹터 파급)이면 market_wide=true, 모든 테마(1..18) confidence=1.0로 반환\n"
            "- 특정 섹터 전반 영향이면 해당 섹터 테마만 confidence=1.0 (다른 섹터 만점 금지)\n"
            "- 특정 기업 중심 이슈(실적/제품/소송/경영 등)면 어떤 테마도 만점(1.0) 금지. 관련 테마는 0.35~0.7 범위\n"
            "- 그 외 일반적 관련성: 0.15~0.95 (직접=0.75~0.95, 간접=0.35~0.6)\n"
            f"- 최대 {max_themes}개. 단 market_wide=true면 18개 전부 만점으로 반환\n\n"

            "[거래소 선택 규칙]\n"
            "- exchange_code는 주요 거래소 코드 사용: NMS(NASDAQ), NYQ(NYSE), TSE(도쿄), KOE(한국), LSE(런던) 등\n"
            "- 직접 언급된 거래소나 해당 지역 기업이 상장된 거래소만 선택\n"
            "- confidence: 직접 언급=0.8~1.0, 지역 관련=0.5~0.7, 간접 관련=0.3~0.5\n"
            "- 최대 3개 거래소까지 선택 가능\n\n"

            f"[원문 제목]\n{title}\n\n"
            f"[기사 본문]\n{content[:12000]}\n\n"
            f"[고정 테마 목록]\n{json.dumps(cat, ensure_ascii=False)}"
        )

        return NEWS_SUMMARY_THEMES_SCHEMA, user

    @staticmethod
    def generate_suggest_tickers_or_scope_prompt(
        *, title_ko: str, summary_ko: str, themes: List[Dict], max_tickers: int = 10, db: Session = None
    ) -> Tuple[Dict, str]:
        """
        티커/스코프 제안 프롬프트를 생성합니다.
        
        Returns:
            Tuple[Dict, str]: (schema, user_prompt)
        """
        theme_ids = [t["theme_id"] for t in themes]
        cat = [{"id":t["id"], "code":t["code"]} for t in FIXED_THEMES]

        # 야후 거래소코드 안내(모델 힌트)
        yf_hint = (
            "야후파이낸스 거래소코드 예시: "
            "NASDAQ=NMS, NYSE=NYQ, NYSEARCA=ARCA, AMEX=ASE, CBOE=BATS, "
            "도쿄=TSE/JPX/TYO, 한국=KOE(KRX) 등. 가능한 경우 이 표기를 사용."
        )

        # 전체 티커 목록 가져오기 (토큰 최소화 형식)
        ticker_list = ""
        if db:
            try:
                ticker_list = NewsPromptService._get_ticker_list_compressed(db)
            except Exception as e:
                # DB 조회 실패시 빈 문자열로 처리
                ticker_list = ""

        user = (
            "아래 한국어 제목/요약과 선택된 테마를 바탕으로 이번 뉴스의 영향 범위를 판정하고, "
            f"개별 기업에 영향일 경우 상장 심볼을 제시하세요(최대 {max_tickers}개).\n"
            "[판정 기준]\n"
            "- 시장 전반(ALL): 금리/매크로/지수/대다수 섹터에 동시 영향이면 ALL\n"
            "- 섹터 전반(SECTOR): 특정 섹터 전반에 광범위 영향이면 SECTOR → 반드시 sector_theme_ids에 고정 테마 id 반환\n"
            "- 개별 기업(TICKERS): 특정 회사/경쟁사/공급망 중심이면 TICKERS\n"
            "[티커 출력]\n"
            "- 가능하면 exchange는 '야후파이낸스 거래소코드'로, country는 US/KR/JP 등으로\n"
            "- 한국 기업 symbol은 종목코드 6자리만 반환\n"
            "- 직접 언급/핵심이면 confidence 0.7~0.95, 간접/동종업계는 0.3~0.6\n"
            "- 확실하지 않으면 보수적으로 제외\n"
            f"- 참고: {yf_hint}\n\n"
            f"[제목(ko)]\n{title_ko}\n\n[요약(ko)]\n{summary_ko}\n\n"
            f"[선택 테마 id]\n{json.dumps(theme_ids, ensure_ascii=False)}\n"
            f"[테마 카탈로그]\n{json.dumps(cat, ensure_ascii=False)}\n"
            f"[전체 티커 목록]\n{ticker_list}"
        )
        return TICKER_OR_SCOPE_SCHEMA, user

    @staticmethod
    def generate_exchange_mapping_prompt(
        title_ko: str, summary_ko: str, *, max_exchanges: int = 3
    ) -> Tuple[Dict, str]:
        """
        거래소 매핑 프롬프트를 생성합니다.
        
        Returns:
            Tuple[Dict, str]: (schema, user_prompt)
        """
        user = (
            "아래 한국어 뉴스 제목과 요약을 분석하여 관련된 거래소를 매핑하세요.\n"
            "[거래소 매핑 규칙]\n"
            "- exchange_code는 표준 거래소 코드 사용:\n"
            "  * NMS (NASDAQ)\n"
            "  * NYQ (NYSE)\n"
            "  * TSE (도쿄증권거래소)\n"
            "  * KOE (한국거래소/KRX)\n"
            "  * LSE (런던증권거래소)\n"
            "  * FRA (프랑크푸르트증권거래소)\n"
            "  * HKG (홍콩증권거래소)\n"
            "  * ASX (호주증권거래소)\n"
            "- 선택 기준:\n"
            "  * 직접 언급된 거래소 (예: '나스닥에서 상장', '뉴욕증권거래소')\n"
            "  * 해당 지역 기업이 상장된 거래소 (예: 미국 기업 → NMS/NYQ)\n"
            "  * 뉴스 내용이 해당 지역 시장에 영향을 미치는 경우\n"
            "- confidence 점수:\n"
            "  * 직접 언급: 0.8~1.0\n"
            "  * 지역 관련 (해당 지역 기업): 0.5~0.7\n"
            "  * 간접 관련 (시장 영향): 0.3~0.5\n"
            "- 최대 3개 거래소까지 선택\n"
            "- 확실하지 않으면 제외\n\n"
            f"[제목]\n{title_ko}\n\n[요약]\n{summary_ko}\n\n"
            "JSON {\"exchanges\": [{\"exchange_code\": \"NMS\", \"confidence\": 0.8}]} 형식으로 반환하세요."
        )
        return EXCHANGE_MAPPING_SCHEMA, user
