from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime, timezone
import json
import logging

from app.features.portfolio.repositories.asset_snapshot_repository import AssetSnapshotRepository
from app.features.portfolio.models.asset_snapshot import MarketType
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation
from app.features.marketdata.services.kr_price_detail_ingestor import KRPriceDetailIngestor
from app.features.portfolio.models.trading_models import OrderPlan
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.core import config as settings
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA

class KrBuyOrderPromptService:
    """국내주식 매수주문생성용 프롬프트 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.snapshot_repo = AssetSnapshotRepository(db)
    
    def generate_buy_order_prompt(self) -> str:
        """
        국내주식 매수주문생성용 프롬프트를 생성합니다.
        
        Returns:
            통합된 프롬프트 텍스트 (### [SYSTEM PROMPT]로 시작)
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("=== Generating KR Buy Order Prompt ===")
            
            # 1. 최신 자산 스냅샷 조회 (KR 시장)
            portfolio_data = self._get_latest_portfolio_data()
            logger.info(f"Portfolio data: {len(portfolio_data.get('positions', []))} positions")
            
            # 2. 유효기간 내 LONG 추천서 조회 (country='KR', confidence_score 내림차순)
            recommendations = self._get_valid_long_recommendations()
            logger.info(f"Valid LONG recommendations: {len(recommendations)} items")
            
            # 3. 프롬프트 템플릿 생성
            system_prompt = self._generate_system_prompt()
            user_input_template = self._generate_user_input_template(portfolio_data, recommendations)
            
            # 4. 통합된 텍스트 생성
            full_prompt = f"{system_prompt}\n\n{user_input_template}"
            
            logger.info("✅ KR Buy Order Prompt generated successfully")
            return full_prompt
            
        except Exception as e:
            logger.error(f"❌ Failed to generate KR buy order prompt: {str(e)}")
            raise
    
    def _get_latest_portfolio_data(self) -> Dict[str, Any]:
        """최신 자산 스냅샷 데이터를 조회합니다."""
        logger = logging.getLogger(__name__)
        
        # 계좌 ID 결정 (가상환경에 따라)
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
        
        # KR 시장의 최신 스냅샷 조회
        latest_snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, MarketType.KR)
        
        if not latest_snapshot:
            logger.warning(f"No KR asset snapshot found for account: {account_uid}")
            return {
                "positions": [],
                "account_info": None,
                "snapshot_id": None,
                "asof_kst": None
            }
        
        logger.info(f"Latest KR snapshot ID: {latest_snapshot.snapshot_id}")
        
        # 포지션 데이터 구성 (신규 엔티티 기준 직관적 필드명)
        positions = []
        for position in latest_snapshot.positions:
            positions.append({
                "ticker_id": position.ticker_id,
                "quantity": float(position.qty) if position.qty else 0.0,
                "market_value": float(position.market_value_ccy) if position.market_value_ccy else 0.0
            })
        
        # 계좌 데이터 구성 (신규 엔티티 기준 직관적 필드명)
        account_info = {
            "currency": "KRW",
            "buying_power": float(latest_snapshot.buying_power_ccy) if latest_snapshot.buying_power_ccy else 0.0,
            "total_equity": float(latest_snapshot.total_equity_ccy) if latest_snapshot.total_equity_ccy else 0.0
        }
        
        return {
            "positions": positions,
            "account_info": account_info,
            "snapshot_id": latest_snapshot.snapshot_id,
            "asof_kst": latest_snapshot.asof_kst.isoformat()
        }
    
    def _get_valid_long_recommendations(self) -> List[Dict[str, Any]]:
        """유효기간 내 LONG 추천서를 조회합니다 (country='KR')."""
        logger = logging.getLogger(__name__)
        
        now = datetime.now(timezone.utc)
        
        recommendations = (
            self.db.query(AnalystRecommendation, Ticker, TickerI18n)
            .join(Ticker, AnalystRecommendation.ticker_id == Ticker.id)
            .outerjoin(TickerI18n, 
                      (TickerI18n.ticker_id == Ticker.id) & 
                      (TickerI18n.lang_code == 'ko'))
            .filter(
                AnalystRecommendation.valid_until > now,  # 유효기간 내 (활성 상태)
                AnalystRecommendation.position_type == "LONG",  # LONG만 필터링
                AnalystRecommendation.analysis_price > 0,  # 분석당시 가격이 0보다 큰 것만
                AnalystRecommendation.confidence_score > 0.57,  # confidence_score가 0.57 이상인 것만
                AnalystRecommendation.is_latest == True,  # 최신 추천서만
                Ticker.country == 'KR',  # 국내 주식만 대상
                # NOT EXISTS: 이미 사용된 추천서 제외 (대용량 테이블에서 효율적)
                ~self.db.query(OrderPlan.id)
                .filter(OrderPlan.recommendation_id == AnalystRecommendation.id)
                .exists()
            )
            .order_by(desc(AnalystRecommendation.created_at), desc(AnalystRecommendation.confidence_score))  # 생성일자 내림차순, confidence_score 내림차순
            .limit(20)  # 최대 20개 제한
            .all()
        )
        
        logger.info(f"Found {len(recommendations)} valid KR LONG recommendations")
        
        # 현재가 조회를 위한 KRPriceDetailIngestor 초기화
        price_ingestor = KRPriceDetailIngestor(self.db)
        
        # 추천서 데이터 구성
        recommendation_data = []
        for rec, ticker, ticker_i18n in recommendations:
            # 현재가 조회 (해외주식과 동일한 방식)
            current_price = None
            try:
                price_result = price_ingestor.sync_price_detail_for_ticker_id(rec.ticker_id)
                if price_result.get("status") == "success" and price_result.get("data"):
                    current_price = price_result["data"].get("close")
                    logger.info(f"Current price for {ticker.symbol}: {current_price}")
                else:
                    logger.warning(f"Failed to get current price for {ticker.symbol}: {price_result.get('message')}")
            except Exception as e:
                logger.error(f"Error getting current price for {ticker.symbol}: {str(e)}")
            
            # 종목명 추출 (한국어 우선, 없으면 심볼 사용)
            company_name = ticker_i18n.name if ticker_i18n else ticker.symbol
            
            recommendation_data.append({
                "recommendation_id": rec.id,
                "ticker_id": rec.ticker_id,
                "position_type": rec.position_type.value if rec.position_type else "LONG",  # enum을 문자열로 변환
                "entry_price": float(rec.entry_price) if rec.entry_price else 0.0,
                "target_price": float(rec.target_price) if rec.target_price else 0.0,
                "stop_price": float(rec.stop_price) if rec.stop_price else 0.0,
                "analysis_price": float(rec.analysis_price) if rec.analysis_price else 0.0,  # 분석당시 최근가
                "current_price": float(current_price) if current_price else None,  # 현재가
                "valid_until": rec.valid_until.isoformat()
            })
        
        return recommendation_data
    
    def _get_current_price(self, ticker_id: int) -> Optional[float]:
        """국내주식 현재가를 조회합니다 (API 갱신 후 DB에서 조회)."""
        logger = logging.getLogger(__name__)
        
        try:
            # 현재가 조회를 위한 KRPriceDetailIngestor 초기화
            price_ingestor = KRPriceDetailIngestor(self.db)
            
            # API로 현재가 갱신 후 조회 (미국주식과 동일한 방식)
            price_result = price_ingestor.sync_price_detail_for_ticker_id(ticker_id)
            
            if price_result.get("status") == "success" and price_result.get("data"):
                current_price = price_result["data"].get("close")
                if current_price is not None:
                    logger.debug(f"Current price for ticker_id {ticker_id}: {current_price}")
                    return float(current_price)
            
            logger.warning(f"Failed to get current price for ticker_id {ticker_id}: {price_result.get('message', 'Unknown error')}")
            return None
                
        except Exception as e:
            logger.error(f"Error getting current price for ticker_id {ticker_id}: {str(e)}")
            return None
    
    def _generate_system_prompt(self) -> str:
        """SYSTEM PROMPT를 생성합니다."""
        return """
[SYSTEM]
당신은 “단기 스윙 매수 주문 설계자”입니다. 입력은 (1) LONG 추천서 리스트, (2) 계좌/포지션 스냅샷입니다.
허용 주문유형: **LIMIT만**.
목표: **탐욕적 저가 진입** + **확률가중 예산 관리**.
모든 결정 근거는 plan.note / batch.notes에 **한국어**로 **짧게** 기록.

[CAP 모드]
- cap_mode = **HARD** (기본) | EXPECTED_ONLY *(브로커가 예약한도 초과 허용 시에만)*
- **HARD**: ΣNominal ≤ BP **및** ΣExpected ≤ BP.
- **EXPECTED_ONLY**: ΣExpected ≤ BP, 종목 가드레일 준수(ΣNominal 제한 해제).
- batch.notes에 `cap_mode=…` 명시.

[핵심 컨셉 — 낮은 체결확률로 넓게 깔기]
- **체결률 목표 ≤ 10%**. **현재가 근접 금지**(즉시체결 위험 회피).

[가격 앵커 + 호가·상하한]
- 참고(있으면): entry_price, 전일저가/고가, 일중저가/고가, ATR%(14), 갭%, 거래대금/체결강도.
- **base_anchor = min(entry_price, 전일저가, current_price × 0.985)**  // 미제공 항목 제외
- 각 레그 **LIMIT_PRICE = 호가단위 반영 ‘내림’( base_anchor × (1 − δ) )**, **δ ∈ [0.003, 0.010]**
- **일일 상하한(±30%)** 이내에서만 주문.
- **유효성**: discount = (current_price − LIMIT_PRICE)/current_price **≥ max(1.2%, 0.4×ATR%)**.

[레그 체결확률(티어)]
- **Tier A**: discount ≥ max(2.5%, 0.8×ATR%) → **p=5%**
- **Tier B**: discount ≥ max(2.0%, 0.6×ATR%) → **p=8%**
- **Tier C**: discount ≥ max(1.5%, 0.5×ATR%) → **p=10%**
- 미달 레그 **생성 금지**.

[확률가중 예산(P-Weighted Exposure)]
- 레그 i: **Nominal = LIMIT_PRICE×shares**, **Expected = Nominal×p**.
- **HARD(기본)**: ΣNominal **≤ BP** **및** ΣExpected **≤ BP**.
- **EXPECTED_ONLY(옵션)**: ΣExpected **≤ BP**, 종목별 가드레일 **≤ min(BP×0.20, Eq×0.04)**.
- 요구 시 **shares 자동 축소**로 캡 충족.

[채택/스킵]
- 방향과 무관하게 **base_anchor 기준 유효 할인 레그**부터 시도.
- **스킵 사유 한정**:
  - **DISCOUNT**(유효성 미달), **EXPIRED**, **CAP**(예산/가드레일 불가), **RISK**, **OTHER**
- *“SPREAD/역진입” 코드·문구 금지.*
- **보유 종목**은 저가 **LIMIT 보강** 우선, **신규**는 유효성 필수.

[수량/분할]
- 종목당 **2~4 leg 권장(최대 5)**, 레그 간 **최소 0.3% 간격**, **shares 정수**.
- 호가단위 준수(1/5/10/50/100/500/1000원 체계, 내림).

[메모 규칙]
- **plan.note**: discount/티어/p%/base_anchor/limit범위 등 **수치 1~2줄**.
- **batch.notes**: `cap_mode`, **ΣNominal/ΣExpected**, 종목 가드레일·호가·상하한 준수 요약(숫자).

[입력 데이터]
"""
    
    def _generate_user_input_template(self, portfolio_data: Dict[str, Any], recommendations: List[Dict[str, Any]]) -> str:
        """USER INPUT TEMPLATE을 생성합니다."""
        
        # 현재 시각 (KST)
        from datetime import timedelta
        kst_timezone = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst_timezone).isoformat()
        
        # 계좌 정보 (신규 엔티티 기준)
        account_info = portfolio_data.get("account_info", {})
        
        # 포트폴리오 포지션
        positions = portfolio_data.get("positions", [])
        
        # 포트폴리오 스냅샷 (신규 엔티티 기준)
        portfolio_snapshots = positions
        
        # 추천서 데이터 (수량제한 없음)
        recommendations_data = recommendations
        
        return f"""
[CONTEXT — INPUT JSON]
(제공된 JSON 데이터를 기반으로 국내주식 매수주문을 설계하십시오)
- now_kst: "{now_kst}"
- Account JSON: {json.dumps(account_info, ensure_ascii=False, indent=2)}
- Portfolio Snapshots JSON: {json.dumps(portfolio_snapshots, ensure_ascii=False, indent=2)}
- Recommendations JSON: {json.dumps(recommendations_data, ensure_ascii=False, indent=2)}

[SELF-CHECK]
- 통화 **KRW/주**, **호가단위 내림**, **일일 상하한 준수**.
- LIMIT가 **current 아래**인지, **유효성·티어·간격·캡** 충족.
- 스킵 시 **산술 로그** 첨부: `current, base, limit@δ=1.0%, required, verdict`.
"""
