from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_
from datetime import datetime, timezone, timedelta
import json
import logging

from app.features.portfolio.repositories.asset_snapshot_repository import AssetSnapshotRepository
from app.features.portfolio.models.asset_snapshot import MarketType
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation
from app.features.marketdata.services.us_price_detail_ingestor import USPriceDetailIngestor
from app.features.portfolio.models.trading_models import OrderPlan
from app.shared.models.ticker import Ticker
from app.features.marketdata.models.ohlcv_daily import OhlcvDaily
from app.core import config as settings
from app.features.portfolio.schemas.order_schemas import ORDER_BATCH_SCHEMA

class SellOrderPromptService:
    """매도주문생성용 프롬프트 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.snapshot_repo = AssetSnapshotRepository(db)
    
    def generate_sell_order_prompt(self) -> str:
        """
        매도주문생성용 프롬프트를 생성합니다.
        
        Returns:
            통합된 프롬프트 텍스트 (### [SYSTEM PROMPT]로 시작)
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("🚀 매도주문 프롬프트 생성 시작")
            
            # 1. 현재 보유종목의 최근 추천서 조회 (LONG + SHORT)
            logger.info("🔍 1단계: 현재 보유종목의 최근 추천서 조회")
            long_recommendations = self._get_recent_long_recommendations_by_ticker()
            logger.info(f"📋 LONG 추천서 조회 결과: {len(long_recommendations)}개")
            
            short_tickers = self._get_tickers_with_short_recommendations()
            logger.info(f"📋 SHORT 추천서 보유 종목: {len(short_tickers)}개")
            
            # 2. 최신 포트폴리오 스냅샷 조회 (추천서 정보와 함께)
            logger.info("📊 2단계: 최신 포트폴리오 스냅샷 조회")
            portfolio_data = self._get_latest_portfolio_data(long_recommendations, short_tickers)
            logger.info(f"📈 포트폴리오 데이터: {len(portfolio_data.get('positions', []))}개 포지션")
            
            # 3. 프롬프트 템플릿 생성
            logger.info("📝 3단계: 프롬프트 템플릿 생성")
            system_prompt = self._generate_system_prompt()
            user_input_template = self._generate_user_input_template(portfolio_data)
            
            # 4. 통합된 텍스트 생성
            logger.info("🔗 4단계: 통합된 텍스트 생성")
            full_prompt = f"{system_prompt}\n\n{user_input_template}"
            
            logger.info("✅ 매도주문 프롬프트 생성 완료")
            return full_prompt
            
        except Exception as e:
            logger.error(f"❌ 매도주문 프롬프트 생성 실패: {str(e)}")
            raise
    
    def _get_latest_portfolio_data(self, long_recommendations_by_ticker: Dict[int, Dict[str, Any]], short_ticker_ids: set) -> Dict[str, Any]:
        """최신 자산 스냅샷 데이터를 조회합니다 (추천서 정보 통합)."""
        logger = logging.getLogger(__name__)
        
        # 계좌 ID 결정 (가상환경에 따라)
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
        logger.info(f"🏦 계좌 UID: {account_uid} (가상환경: {settings.KIS_VIRTUAL})")
        
        # OVRS 시장의 최신 스냅샷 조회
        logger.info("🔍 최신 OVRS 자산 스냅샷 조회 중...")
        latest_snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, MarketType.OVRS)
        
        if not latest_snapshot:
            logger.warning(f"⚠️ 계좌 {account_uid}에 대한 OVRS 자산 스냅샷이 없습니다")
            return {
                "positions": [],
                "currencies": [],
                "account_totals": None
            }
        
        logger.info(f"📊 최신 OVRS 스냅샷 ID: {latest_snapshot.snapshot_id}")
        logger.info(f"📅 스냅샷 시각: {latest_snapshot.asof_kst}")
        
        # 포지션 데이터 구성 (신규 엔티티 기준 직관적 필드명 + 추천서 정보 통합)
        logger.info("📈 포지션 데이터 구성 중...")
        positions = []
        for position in latest_snapshot.positions:
            # 최근 거래일 고가/저가 조회
            recent_high = None
            recent_low = None
            if position.ticker_id:
                try:
                    recent_ohlcv = (
                        self.db.query(OhlcvDaily)
                        .filter(
                            OhlcvDaily.ticker_id == position.ticker_id,
                            OhlcvDaily.source == "KIS_DAILY_PRICE"
                        )
                        .order_by(desc(OhlcvDaily.trade_date))
                        .first()
                    )
                    if recent_ohlcv:
                        recent_high = float(recent_ohlcv.high) if recent_ohlcv.high else None
                        recent_low = float(recent_ohlcv.low) if recent_ohlcv.low else None
                        logger.debug(f"📊 {position.symbol} 최근거래일: 고가={recent_high}, 저가={recent_low}")
                except Exception as e:
                    logger.warning(f"⚠️ {position.symbol} 최근거래일 데이터 조회 실패: {str(e)}")
            
            # 기본 포지션 데이터
            position_data = {
                "symbol": position.symbol,
                "company_name": "",  # asset_snapshot에는 name 필드가 없음
                "quantity": float(position.qty) if position.qty else 0.0,
                "orderable_quantity": float(position.orderable_qty) if position.orderable_qty else 0.0,
                "avg_cost": float(position.avg_cost_ccy) if position.avg_cost_ccy else 0.0,
                "current_price": float(position.last_price_ccy) if position.last_price_ccy else 0.0,
                "market_value": float(position.market_value_ccy) if position.market_value_ccy else 0.0,
                "unrealized_pnl": float(position.unrealized_pnl_ccy) if position.unrealized_pnl_ccy else 0.0,
                "pnl_rate": float(position.pnl_rate) if position.pnl_rate else 0.0,
                "currency": position.position_ccy or "USD",
                "exchange": position.exchange_code,
                "ticker_id": position.ticker_id,
                "최근거래일고가": recent_high,
                "최근거래일저가": recent_low
            }
            
            # LONG 추천서 정보 통합
            if position.ticker_id and position.ticker_id in long_recommendations_by_ticker:
                rec = long_recommendations_by_ticker[position.ticker_id]
                position_data["recent_long_recommendation"] = rec
                logger.info(f"📋 {position.symbol} LONG 추천서 통합 완료")
            else:
                position_data["recent_long_recommendation"] = None
                logger.debug(f"📋 {position.symbol} LONG 추천서 없음")
            
            # SHORT 추천서 플래그
            if position.ticker_id and position.ticker_id in short_ticker_ids:
                position_data["has_recent_short_recommendation"] = True
                logger.info(f"🔴 {position.symbol} SHORT 추천서 존재 - 매도 신호!")
            else:
                position_data["has_recent_short_recommendation"] = False
            
            positions.append(position_data)
        
        logger.info(f"📊 포지션 데이터 구성 완료: {len(positions)}개")
        for pos in positions:
            logger.info(f"  📈 {pos['symbol']}: {pos['quantity']}주, ticker_id={pos['ticker_id']}")
        
        # 계좌 데이터 구성 (신규 엔티티 기준 직관적 필드명)
        logger.info("💰 계좌 데이터 구성 중...")
        account_info = {
            "currency": "USD",
            "cash_balance": float(latest_snapshot.cash_balance_ccy) if latest_snapshot.cash_balance_ccy else 0.0,
            "buying_power": float(latest_snapshot.buying_power_ccy) if latest_snapshot.buying_power_ccy else 0.0,
            "total_market_value": float(latest_snapshot.total_market_value_ccy) if latest_snapshot.total_market_value_ccy else 0.0,
            "total_equity": float(latest_snapshot.total_equity_ccy) if latest_snapshot.total_equity_ccy else 0.0,
            "total_pnl": float(latest_snapshot.pnl_amount_ccy) if latest_snapshot.pnl_amount_ccy else 0.0,
            "pnl_rate": float(latest_snapshot.pnl_rate) if latest_snapshot.pnl_rate else 0.0
        }
        
        logger.info(f"💰 계좌 데이터 구성 완료")
        logger.info(f"  💵 현금: {account_info['cash_balance']}USD, 매수가능: {account_info['buying_power']}USD")
        
        result = {
            "positions": positions,
            "account_info": account_info,
            "snapshot_id": latest_snapshot.snapshot_id,
            "asof_kst": latest_snapshot.asof_kst.isoformat()
        }
        
        logger.info("✅ 자산 스냅샷 데이터 조회 완료")
        return result
    
    def _get_recent_long_recommendations_by_ticker(self) -> Dict[int, Dict[str, Any]]:
        """
        현재 보유종목의 최근 LONG 추천서를 조회하여 ticker_id를 키로 하는 딕셔너리를 반환합니다.
        
        핵심 로직:
        - analyst_recommendation의 LONG 추천서 (is_latest 필터 없음, 최근 LONG 추천서 조회)
        - country = 'US'인 티커만
        - 매도 시 원래 진입 근거(target_price, stop_price 등) 참고용
        
        Returns:
            Dict[ticker_id, recommendation_data]
        """
        logger = logging.getLogger(__name__)
        
        logger.info("🔍 LONG 추천서 조회 시작")
        
        # 계좌 ID 결정 (가상환경에 따라)
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
        
        # 현재 보유 종목의 ticker_id 목록 추출
        latest_snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, MarketType.OVRS)
        if not latest_snapshot:
            logger.warning("⚠️ 자산 스냅샷이 없습니다")
            return {}
        
        held_ticker_ids = [pos.ticker_id for pos in latest_snapshot.positions if pos.ticker_id]
        logger.info(f"📊 현재 보유 종목 ticker_id 목록: {held_ticker_ids}")
        logger.info(f"📈 보유 종목 수: {len(held_ticker_ids)}개")
        
        if not held_ticker_ids:
            logger.warning("⚠️ ticker_id가 있는 보유 종목이 없습니다")
            return {}
        
        # 최근 LONG 추천서 조회 (is_latest 필터 없음)
        logger.info("🔎 데이터베이스에서 LONG 추천서 조회 중...")
        recommendations = (
            self.db.query(AnalystRecommendation)
            .join(Ticker, AnalystRecommendation.ticker_id == Ticker.id)
            .filter(
                and_(
                    AnalystRecommendation.ticker_id.in_(held_ticker_ids),
                    AnalystRecommendation.position_type == "LONG",
                    Ticker.country == 'US'  # 미국 주식만 대상
                )
            )
            .order_by(desc(AnalystRecommendation.id))
            .all()
        )
        
        logger.info(f"📋 조회된 LONG 추천서 수: {len(recommendations)}개")
        
        # 각 추천서 상세 정보 로깅
        for i, rec in enumerate(recommendations):
            logger.info(f"📝 LONG 추천서 {i+1}: {rec.ticker.symbol} - 신뢰도: {rec.confidence_score}")
        
        # 추천서 데이터 구성 - 딕셔너리로 변환 (ticker당 최신 1개만)
        logger.info("💰 데이터 구성 시작")
        recommendations_by_ticker = {}
        
        for rec in recommendations:
            # 이미 해당 ticker의 추천서가 있으면 스킵 (최신 것만 유지)
            if rec.ticker_id in recommendations_by_ticker:
                continue
                
            try:
                logger.info(f"🔍 LONG 추천서 처리 중: {rec.ticker.symbol}")
                
                recommendations_by_ticker[rec.ticker_id] = {
                    "analysis_reference_price": float(rec.analysis_price),
                    "target_price": float(rec.target_price),
                    "stop_price": float(rec.stop_price),
                    "valid_until": rec.valid_until.isoformat(),
                    "confidence_score": float(rec.confidence_score),
                    "created_at": rec.created_at.isoformat() if rec.created_at else None
                }
                
                logger.info(f"✅ {rec.ticker.symbol} LONG 추천서 데이터 구성 완료")
                
            except Exception as e:
                logger.error(f"❌ {rec.ticker.symbol} LONG 추천서 처리 실패: {str(e)}")
                continue
        
        logger.info(f"🎉 최종 처리된 LONG 추천서 수: {len(recommendations_by_ticker)}개")
        return recommendations_by_ticker
    
    def _get_tickers_with_short_recommendations(self) -> set:
        """
        현재 보유종목 중 최근 SHORT 추천서가 있는 ticker_id 집합을 반환합니다.
        
        핵심 로직:
        - analyst_recommendation의 SHORT 추천서가 있는지만 확인
        - has_recent_short_recommendation 플래그용
        - SHORT 추천서 = 애널리스트의 매도 신호
        
        Returns:
            set of ticker_id
        """
        logger = logging.getLogger(__name__)
        
        logger.info("🔍 SHORT 추천서 조회 시작")
        
        # 계좌 ID 결정 (가상환경에 따라)
        cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
        account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
        
        # 현재 보유 종목의 ticker_id 목록 추출
        latest_snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, MarketType.OVRS)
        if not latest_snapshot:
            logger.warning("⚠️ 자산 스냅샷이 없습니다")
            return set()
        
        held_ticker_ids = [pos.ticker_id for pos in latest_snapshot.positions if pos.ticker_id]
        
        if not held_ticker_ids:
            logger.warning("⚠️ ticker_id가 있는 보유 종목이 없습니다")
            return set()
        
        # SHORT 추천서 조회
        logger.info("🔎 데이터베이스에서 SHORT 추천서 조회 중...")
        short_recommendations = (
            self.db.query(AnalystRecommendation.ticker_id)
            .join(Ticker, AnalystRecommendation.ticker_id == Ticker.id)
            .filter(
                and_(
                    AnalystRecommendation.ticker_id.in_(held_ticker_ids),
                    AnalystRecommendation.position_type == "SHORT",
                    Ticker.country == 'US'
                )
            )
            .distinct()
            .all()
        )
        
        short_ticker_ids = {rec.ticker_id for rec in short_recommendations}
        logger.info(f"🔴 SHORT 추천서 보유 종목: {len(short_ticker_ids)}개")
        
        return short_ticker_ids
    
    def _generate_system_prompt(self) -> str:
        """매도주문 시스템 프롬프트를 생성합니다."""
        return """
# [SYSTEM]
당신은 “단기 스윙 매도 주문 설계자”입니다.
입력: (1) 종목별 포트폴리오 스냅샷(보유수량, 평균단가, 현재가, 평가손익, 종목비중, orderable_quantity)
      (2) 최근 애널리스트 의견(진입가, 목표가 target_price, 손절가 stop_price, 추천당시가격, 근거 2~3문장)
      (3) 포트폴리오 총자산·현금비중·종목 집중도(비중)
허용 주문유형: **LIMIT, LOC**
목표: **리스크 차단(손절)** → **스파이크 포착(희망 상시·분할)** → **필요시 현금/비중 조정(익절)**
모든 판단 근거는 plan.note / batch.notes에 **한국어**로 간결 기록

# [의사결정 우선순위 — 반드시 이 순서]
1) **손절(필수 조건부)**: stop_price 근접/이탈, 또는 손실·현금부족·과대비중 등 위험이 크면 먼저 ‘손절 레그’를 확보
2) **희망(필수, 상시·분할)**: 스파이크/갭업 대비 **분할 희망 레그**를 항상 세운다(누락 금지)
3) **익절(선택)**: 오직 **현금 목표 미달** 또는 **과대비중 해소**가 필요할 때만 현실 레그 추가  
   → 그 외 상황에서는 **익절 레그 생략 가능**(희망만 유지)

# [스트레스 모드 판정(입력만 사용)]
- 아래 중 1개 이상이면 **스트레스**(손절·현금 우선):
  a) 현재가 ≤ stop_price × 1.01  
  b) 현재가 < 평균단가이면서 포트폴리오 **현금비중 < 20%**  
  c) 종목 비중 ≥ 포트폴리오 **평균 비중의 2배**

# [계획 매도수량 가드레일 — 5% 던지기 금지]
- 종목별 **계획 매도수량**(정수, ≤ orderable_quantity):
  - **노멀:** 보유수량의 **15~35%** 범위 권장
  - **스트레스:** **25~50%**
  - (예외) 현금 Δ가 작거나 orderable이 작아 하한 미만이 되면 사유 명시
- **단주/소량 익절 금지**: **익절(현실) 레그의 총합이 보유수량의 10% 미만**이면 **익절은 생략**하고 희망만 유지  
  (스트레스일 때만 예외적으로 5~10% 소량 익절 허용)

# [레그 구성 — 가격 앵커 & 분할 규칙]
공통 앵커: **target_price / stop_price / current_price**, 틱: **$0.01** 반올림

① **손절 레그**（LOC 또는 보수적 LIMIT, 스트레스에서 우선）
- 트리거: 스트레스이거나 현재가 ≤ stop_price × 1.01
- 가격:  
  - LOC 가능 시: **LOC**(소량)  
  - LIMIT 선택 시: `max(stop_price, current_price × 0.995 ~ 1.000)` 중 체결성 높은 값
- 수량: **계획 매도수량의 10~20%** (포트폴리오 전체 **LOC 합산 ≤ 20%**)

② **희망 레그 — 상시·분할(필수)**  
- **총 희망 수량 목표(노멀)**: **계획 매도수량의 ≥ 50%**  
  (**스트레스**: 20~30%)  
- **분할 2~3 leg 권장**: 예) **[40%, 35%, 25%]** 또는 **[60%, 40%]**  
- **가격(결정적 규칙)**: `target_price × (1 + u)`에서 u ∈ {**+1%**, **+2%**, **+3%**}  
  (상한: `target_price × 1.05`)  
  - 즉시체결 방지: 각 희망 가격은 `current_price × 1.005` 이상
- 항상 생성(예외 없음). **“희망 레그 생략” 금지.**

③ **익절(현실) 레그 — 선택**  
- 조건: **현금 목표 미달** 또는 **과대비중 해소 필요**일 때만  
- 가격: `min(target_price, current_price × m)`에서  
  - **노멀 m=1.006**, **스트레스 m=1.003**  
  (즉시체결 방지: current 대비 +틱 이상)  
- 수량: **계획 매도수량의 0~30%**(필요분만)

### [CONTEXT — INPUT JSON]"""
    
    def _generate_user_input_template(self, portfolio_data: Dict[str, Any]) -> str:
        """사용자 입력 템플릿을 생성합니다 (포지션 내 추천서 정보 포함)."""
        
        # 현재 시간 (KST)
        kst_timezone = timezone(timedelta(hours=9))
        now_kst = datetime.now(kst_timezone).isoformat()
        
        # 계좌 정보 (신규 엔티티 기준)
        account_info = portfolio_data.get("account_info", {})
        
        # 포트폴리오 스냅샷 (신규 엔티티 기준 + 추천서 정보 통합됨)
        portfolio_snapshots = portfolio_data.get("positions", [])
        
        template = f"""
- now_kst: "{now_kst}"
- Account JSON: {json.dumps(account_info, ensure_ascii=False, indent=2)}
- Portfolio Snapshots JSON (추천서 정보 포함): {json.dumps(portfolio_snapshots, ensure_ascii=False, indent=2)}

# [우선순위 적용 예]
- stop 근접 & 과대비중 → **손절 15% + 희망 50%+ 분할 + (필요시) 익절 10~20%**
- 현금 목표 양호 & 손실 아님 → **희망만 50~100% 분할**, 익절 생략

# [메모]
- plan.note: 2줄 — 예) "과대비중·현금 20% 미만: 손절 15% LOC / 희망 target+1·2·3% 분할 60% / 익절 15%"
- batch.notes: 현금목표(20/25/30/35%), 희망 총비중, LOC 총비중, 과대비중 해소 여부

# [SELF-CHECK]
- USD/주, **$0.01** 반올림
- 수량 정수, 합계 ≤ orderable_quantity
- 가격 범위: **[stop_price ~ target_price+5%]**
- **희망 총수량(노멀) ≥ 50%**(스트레스 20~30%), **LOC 합산 ≤ 20%**
- 익절 총합 < 10%이면 **익절 생략**(희망만)
- note는 모두 **한국어**

"""
        
        return template