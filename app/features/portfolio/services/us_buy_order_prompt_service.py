from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime, timezone
import json
import logging

from app.features.portfolio.repositories.asset_snapshot_repository import AssetSnapshotRepository
from app.features.portfolio.models.asset_snapshot import MarketType
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation
from app.features.marketdata.services.us_price_detail_ingestor import USPriceDetailIngestor
from app.features.portfolio.models.trading_models import OrderPlan
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.core import config as settings
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA

class BuyOrderPromptService:
    """매수주문생성용 프롬프트 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.snapshot_repo = AssetSnapshotRepository(db)
    
    def generate_buy_order_prompt(self) -> str:
        """
        매수주문생성용 프롬프트를 생성합니다.
        
        Returns:
            통합된 프롬프트 텍스트 (### [SYSTEM PROMPT]로 시작)
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("=== Generating Buy Order Prompt ===")
            
            # 1. 최신 포트폴리오 스냅샷 조회
            portfolio_data = self._get_latest_portfolio_data()
            logger.info(f"Portfolio data: {len(portfolio_data.get('positions', []))} positions")
            
            # 2. 유효기간 내 LONG 추천서 조회 (confidence_score 내림차순 5개)
            recommendations = self._get_valid_long_recommendations()
            logger.info(f"Valid LONG recommendations: {len(recommendations)} items")
            
            # 3. 프롬프트 템플릿 생성
            system_prompt = self._generate_system_prompt()
            user_input_template = self._generate_user_input_template(portfolio_data, recommendations)
            
            # 4. 통합된 텍스트 생성
            full_prompt = f"{system_prompt}\n\n{user_input_template}"
            
            logger.info("✅ Buy order prompt generated successfully")
            
            return full_prompt
            
        except Exception as e:
            logger.error(f"❌ Failed to generate buy order prompt: {str(e)}")
            raise
    
    def _get_latest_portfolio_data(self) -> Dict[str, Any]:
        """최신 자산 스냅샷 데이터를 조회합니다."""
        logger = logging.getLogger(__name__)
        
        # 계좌 ID 결정 (가상환경에 따라)
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
        
        # OVRS 시장의 최신 스냅샷 조회
        latest_snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, MarketType.OVRS)
        
        if not latest_snapshot:
            logger.warning(f"No OVRS asset snapshot found for account: {account_uid}")
            return {
                "positions": [],
                "currencies": [],
                "account_totals": None
            }
        
        logger.info(f"Latest OVRS snapshot ID: {latest_snapshot.snapshot_id}")
        
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
            "currency": "USD",
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
        """
        유효기간 내 LONG 추천서를 조회합니다. (confidence_score 내림차순 10개)
        
        ⭐ 매수주문용 추천서 추출 라인 - 추후 다채로운 수정을 위해 눈에 띄게 표시
        """
        logger = logging.getLogger(__name__)
        
        now = datetime.now(timezone.utc)
        
        # ⭐ 매수주문용 추천서 추출 로직 - 유효기간 내, LONG만, analysis_price > 0, is_latest=True, confidence_score 내림차순
        # 추가: NOT EXISTS를 사용하여 이미 order_plan에 사용된 추천서는 제외 (성능 최적화)
        # 추가: country = 'US'인 티커만 대상
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
                Ticker.country == 'US',  # 미국 주식만 대상
                # NOT EXISTS: 이미 사용된 추천서 제외 (대용량 테이블에서 효율적)
                ~self.db.query(OrderPlan.id)
                .filter(OrderPlan.recommendation_id == AnalystRecommendation.id)
                .exists()
            )
            .order_by(desc(AnalystRecommendation.created_at), desc(AnalystRecommendation.confidence_score))  # 생성일자 내림차순, confidence_score 내림차순
            .limit(20)  # 최대 20개 제한
            .all()
        )
        
        # 사용된 추천서 개수 확인
        used_count = self.db.query(OrderPlan.recommendation_id).filter(OrderPlan.recommendation_id.isnot(None)).distinct().count()
        
        logger.info(f"⭐ Found {len(recommendations)} valid LONG recommendations (excluded {used_count} already used)")
        
        # 현재가 조회를 위한 USPriceDetailIngestor 초기화
        price_ingestor = USPriceDetailIngestor(self.db)
        
        result = []
        for rec, ticker, ticker_i18n in recommendations:
            # 현재가 조회
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
            
            # 종목명 추출 (영어 우선, 없으면 심볼 사용)
            company_name = ticker_i18n.name if ticker_i18n else ticker.symbol
            
            result.append({
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
        
        return result
    
    def _generate_system_prompt(self) -> str:
        """SYSTEM PROMPT를 생성합니다."""
        return """
[SYSTEM]
당신은 “단기 스윙 매수 주문 설계자”입니다. 입력은 (1) LONG 추천서 리스트, (2) 계좌/포지션 스냅샷입니다.
주문유형: **LIMIT**, **LOC(보유 보조 1 leg 이하)**.
목표: **탐욕적 저가 진입(깊은 할인)** + **확률가중 예산 관리**.
모든 결정 근거는 plan.note / batch.notes에 **한국어**로 **짧게** 기록.

[CAP 모드]
- cap_mode = **EXPECTED_ONLY** (기본) | HARD
- EXPECTED_ONLY: ΣExpected ≤ BP만 강제(ΣNominal 제한 해제).
- HARD: ΣNominal ≤ BP **동시에** ΣExpected ≤ BP.
- batch.notes에 `cap_mode=…` 명시.

[핵심 컨셉 — 낮은 체결확률로 넓게 깔기]
- **체결률 목표 ≤ 10%**. 지금 사지 말고 **깊은 할인 구간**에만 깔기.
- **현재가 근접(즉시 체결 우려) 금지**.

[가격 앵커]
- 참고(있으면): entry, prev_day_low/high, intraday_low/high, ATR%(14), gap%, rvol.
- **base_anchor = min(entry, prev_day_low, current × 0.985)**  // 미제공 항목은 제외
- *(중요)* entry ≶ current와 **무관**하게 base_anchor로 계산, 모든 LIMIT는 **current 아래**에서 산출.
- 각 레그 **limit = round_to_cent(base_anchor × (1 − δ))**, **δ ∈ [0.003, 0.010]**.
- **유효성**: discount = (current − limit)/current **≥ max(1.2%, 0.4×ATR%)**  *(ATR 없으면 1.2%만 적용)*.

[레그 체결확률(티어)]
- **Tier A**: discount ≥ max(2.5%, 0.8×ATR%) → **p=5%**
- **Tier B**: discount ≥ max(2.0%, 0.6×ATR%) → **p=8%**
- **Tier C**: discount ≥ max(1.5%, 0.5×ATR%) → **p=10%**
- 미달 레그 **생성 금지**(체결률 ≤10% 유지).

[확률가중 예산(P-Weighted)]
- 레그 i: **Nominal = limit×shares**, **Expected = Nominal×p**.
- **EXPECTED_ONLY 모드:** ΣExpected **≤ BP × 1.00**, 종목별 ΣExpected **≤ min(BP×0.20, Eq×0.04)**.
- **HARD 모드:** 위 조건 **+** ΣNominal **≤ BP**.
- 필요 시 **수량 자동 축소**로 캡 충족.

[채택/스킵 규칙]
- ✅ LONG에서 **current ≥ entry든, entry > current든 채택 가능**(방향 스킵 금지).
- ❌ 스킵 **사유만** 허용:
  - **DISCOUNT**: 유효성 미달(최대 δ=1.0% 적용해도 discount<임계)
  - **EXPIRED**: valid_until 만료
  - **CAP**: 하드/소프트캡·가드레일 충족 불가(수량 축소해도)
  - **RISK**: 내부 리스크 사유(포지션/섹터 과밀 등)
  - **OTHER**: 상기 외 불가피 사유
- *“SPREAD/역진입 불가/상승 초과” 단어/코드 사용 금지.*

[수량/분할]
- 종목당 **2~4 leg 권장(최대 5)**, 아래로 갈수록 **더 낮은 가격**. 레그 간 **최소 0.3% 간격**.
- **shares 정수**.
- **LOC**: 보유 종목 리밸런스용 **1 leg 이내**, 해당 종목 총계획 수량의 **≤10%**. 신규 종목 LOC 금지.

[메모 규칙]
- **plan.note**: discount/티어/p%/base_anchor/limit범위 등 **수치 1~2줄**.
- **batch.notes**: `cap_mode`, **ΣNominal/ΣExpected**, 종목별 가드레일 준수 여부를 **숫자**로 요약.
- 방향성 문구 금지(예: “역진입 불가/상승 초과”).

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
(제공된 JSON 데이터를 기반으로 매수주문을 설계하십시오)
- now_kst: "{now_kst}"
- Account JSON: {json.dumps(account_info, ensure_ascii=False, indent=2)}
- Portfolio Snapshots JSON: {json.dumps(portfolio_snapshots, ensure_ascii=False, indent=2)}
- Recommendations JSON: {json.dumps(recommendations_data, ensure_ascii=False, indent=2)}

[SELF-CHECK]
- 모든 note **한국어**, 가격 **$0.01 틱**, 수량 **정수**.
- LIMIT가 **current 아래**인지, **유효성·티어·간격·캡** 준수 확인.
- 스킵이면 **산술 로그** 첨부: `current, base, limit@δ=1.0%, required, verdict`.
"""
