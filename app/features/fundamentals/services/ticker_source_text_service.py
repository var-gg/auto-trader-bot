# app/features/fundamentals/services/ticker_source_text_service.py
import logging
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.core.gpt_client import responses_json

logger = logging.getLogger(__name__)

class TickerSourceTextService:
    def __init__(self, db: Session):
        self.db = db
        
        # 시스템 프롬프트
        self.system_prompt = """
You are an expert market analyst who writes concise company summaries for AI embedding.
Your goal is to produce one dense, factual paragraph (80–150 tokens) that captures a company's
business essence, industry context, major products, and market positioning — not financial data.
Do not include numbers or valuations. Output only the paragraph.
"""

    def get_company_name_by_country(self, ticker_id: int, country: str) -> Optional[str]:
        """
        국가에 따라 적절한 기업명 조회
        
        Args:
            ticker_id: 티커 ID
            country: 국가 코드 (US, KR 등)
            
        Returns:
            기업명 또는 None
        """
        try:
            # US는 영어명, KR은 한국어명 사용
            language = "en" if country.upper() == "US" else "ko"
            
            ticker_i18n = self.db.query(TickerI18n).filter(
                TickerI18n.ticker_id == ticker_id,
                TickerI18n.lang_code == language
            ).first()
            
            if ticker_i18n and ticker_i18n.name:
                return ticker_i18n.name
            
            # i18n 데이터가 없으면 기본 티커 정보에서 이름 조회
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if ticker:
                return ticker.name
                
            return None
            
        except Exception as e:
            logger.error(f"티커 ID {ticker_id}의 기업명 조회 중 오류: {str(e)}")
            return None

    def generate_company_summary(self, ticker_id: int, ticker_symbol: str, country: str = "United States") -> Optional[str]:
        """
        GPT-5-mini를 사용하여 회사 설명 생성
        
        Args:
            ticker_id: 티커 ID
            ticker_symbol: 티커 심볼
            country: 국가명
            
        Returns:
            생성된 회사 설명 또는 None
        """
        try:
            # 국가에 따라 적절한 기업명 조회
            company_name = self.get_company_name_by_country(ticker_id, country)
            if not company_name:
                logger.error(f"티커 ID {ticker_id}의 기업명을 찾을 수 없습니다.")
                return None
            
            # 사용자 프롬프트 생성
            user_prompt = f"""
Generate a concise English company description for the following public company:

Company name: {company_name}
Ticker: {ticker_symbol}
Country: {country}

Focus on the company's main business lines, products, and market domain.
"""
            
            # GPT-5-mini 호출 (JSON 스키마 사용)
            schema = {
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "Company description paragraph"
                    }
                },
                "required": ["description"]
            }
            
            response = responses_json(
                model="gpt-5-mini",
                schema_name="company_description",
                schema=schema,
                user_text=f"{self.system_prompt}\n\n{user_prompt}",
                task="ticker_source_text_generation",
                extra={"ticker_id": ticker_id, "ticker_symbol": ticker_symbol, "country": country}
            )
            
            if response and "description" in response:
                generated_text = response["description"].strip()
                logger.info(f"티커 {ticker_symbol}의 소스텍스트 생성 완료 (길이: {len(generated_text)})")
                return generated_text
            else:
                logger.error(f"티커 {ticker_symbol}의 소스텍스트 생성 실패")
                return None
                
        except Exception as e:
            logger.error(f"티커 {ticker_symbol} 소스텍스트 생성 중 오류: {str(e)}")
            return None

    def generate_source_text_for_ticker(self, ticker_id: int) -> Optional[Dict[str, Any]]:
        """
        티커 ID로 소스텍스트 생성 (국가 정보 자동 조회)
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            소스텍스트와 메타데이터 또는 None
        """
        try:
            # 티커 정보 조회
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                logger.error(f"티커 ID {ticker_id}를 찾을 수 없습니다.")
                return None
            
            # 국가 정보 추출 (exchange에서 추출하거나 기본값 사용)
            country = "United States"  # 기본값
            if hasattr(ticker, 'exchange') and ticker.exchange:
                if 'KR' in ticker.exchange.upper():
                    country = "South Korea"
                elif 'US' in ticker.exchange.upper():
                    country = "United States"
            
            # 소스텍스트 생성
            source_text = self.generate_company_summary(
                ticker_id=ticker_id,
                ticker_symbol=ticker.symbol,
                country=country
            )
            
            if source_text:
                return {
                    "ticker_id": ticker_id,
                    "ticker_symbol": ticker.symbol,
                    "company_name": self.get_company_name_by_country(ticker_id, country),
                    "country": country,
                    "source_text": source_text
                }
            
            return None
            
        except Exception as e:
            logger.error(f"티커 ID {ticker_id} 소스텍스트 생성 중 오류: {str(e)}")
            return None
