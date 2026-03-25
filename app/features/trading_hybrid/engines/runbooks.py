# app/features/trading_hybrid/engines/runbooks.py
from sqlalchemy.orm import Session
from app.features.trading_hybrid.engines.hybrid_trader_engine import HybridTraderEngine, EngineConfig
from app.features.portfolio.services.trade_realized_pnl_service import TradeRealizedPnlService
from app.features.portfolio.services.asset_snapshot_service import AssetSnapshotService
from app.features.portfolio.services.overseas_fill_collection_service import OverseasFillCollectionService
from app.features.portfolio.services.domestic_fill_collection_service import DomesticFillCollectionService
from app.features.marketdata.services.kr_market_holiday_service import KRMarketHolidayService
from app.features.marketdata.services.us_market_holiday_service import USMarketHolidayService
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def _check_market_open(db: Session, market: str) -> bool:
    """
    시장 개장 여부 확인 (KIS API 기반)
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
    
    Returns:
        True: 시장 개장 (거래 가능)
        False: 시장 휴장 (거래 불가)
    
    Note:
        - KR: KIS API의 domestic_holiday_check를 사용하여 opnd_yn (개장일여부) 체크
        - US: 기존 방식 (시간대 + 휴장일 체크)
    """
    try:
        if market == "KR":
            # 국내주식: KIS API로 실제 개장 여부 확인
            service = KRMarketHolidayService(db)
            
            # KIS API를 사용하여 실제 개장 여부 확인
            is_open = service.is_market_open_now_kis()
            
            if not is_open:
                logger.warning(f"🚫 {market} 시장 휴장 (KIS API 기준): 트레이딩 스킵")
                return False
            else:
                logger.info(f"✅ {market} 시장 개장 확인 (KIS API 기준)")
                return True
        else:
            # 미국주식: 기존 방식 (시간대 + 휴장일 체크)
            service = USMarketHolidayService(db)
            
            is_closed = service.is_market_closed_now()
            
            if is_closed:
                logger.warning(f"🚫 {market} 시장 휴장: 트레이딩 스킵")
                return False
            else:
                logger.info(f"✅ {market} 시장 개장 확인")
                return True
            
    except Exception as e:
        logger.error(f"❌ 휴장 체크 실패 ({market}): {str(e)}, 안전을 위해 스킵", exc_info=True)
        return False

async def _sync_profit_and_account(db: Session, market: str):
    """
    손익&계좌 동기화 (시장별 최적화)
    
    Args:
        db: DB 세션
        market: "KR" (한국) 또는 "US" (미국)
    
    실행 내용:
    - 손익 데이터 수집 (3일전~오늘, 양쪽 시장 통합)
    - 해당 시장 계좌 스냅샷
    - 해당 시장 체결정보 수집 (7일간)
    """
    sync_success = False
    try:
        market_name = "한국" if market == "KR" else "미국"
        logger.info(f"🔄 {market_name} 시장 손익&계좌 동기화 시작")
        
        # 1. 손익 데이터 수집 (async) - 양쪽 시장 통합
        today = datetime.now().date()
        three_days_ago = today - timedelta(days=3)
        start_date_str = three_days_ago.strftime("%Y%m%d")
        end_date_str = today.strftime("%Y%m%d")
        
        pnl_service = TradeRealizedPnlService(db)
        pnl_result = await pnl_service.collect_and_save_realized_pnl(start_date_str, end_date_str)
        logger.info(f"✅ 손익 데이터 수집 완료: {pnl_result.get('total_saved', 0)}건")
        
        # 2. 해당 시장 계좌 스냅샷
        asset_service = AssetSnapshotService(db)
        if market == "KR":
            kr_result = asset_service.collect_kr_account_snapshot(account_uid=None)
            if not kr_result.get("success"):
                raise RuntimeError(f"KR account snapshot failed: {kr_result.get('error') or kr_result.get('message')}")
            logger.info(f"✅ 국내 계좌 스냅샷 완료: snapshot_id={kr_result.get('snapshot_id')}")
        else:
            ovrs_result = asset_service.collect_ovrs_account_snapshot(account_uid=None)
            if not ovrs_result.get("success"):
                raise RuntimeError(f"OVRS account snapshot failed: {ovrs_result.get('error') or ovrs_result.get('message')}")
            logger.info(f"✅ 해외 계좌 스냅샷 완료: snapshot_id={ovrs_result.get('snapshot_id')}")
        
        # 3. 해당 시장 체결정보 수집 (async)
        if market == "KR":
            domestic_fill_service = DomesticFillCollectionService(db)
            fill_result = await domestic_fill_service.collect_domestic_fills(days_back=7)
            logger.info(f"✅ 국내주식 체결정보 수집 완료: 처리 {fill_result.get('processed_count', 0)}건, 업서트 {fill_result.get('upserted_count', 0)}건")
        else:
            overseas_fill_service = OverseasFillCollectionService(db)
            fill_result = await overseas_fill_service.collect_overseas_fills(days_back=7)
            logger.info(f"✅ 해외주식 체결정보 수집 완료: 처리 {fill_result.get('processed_count', 0)}건, 업서트 {fill_result.get('upserted_count', 0)}건")
        
        sync_success = True
        logger.info(f"✅ {market_name} 시장 손익&계좌 동기화 완료")
        
    except Exception as e:
        logger.error(f"❌ {market_name} 시장 손익&계좌 동기화 실패: {str(e)}", exc_info=True)
        # ⚠️ 동기화 실패는 치명적 - 정확한 계좌 상태 없이 거래하면 위험
        raise RuntimeError(f"Profit and account sync failed for {market}: {str(e)}")
    
    # ✅ 동기화 검증
    if not sync_success:
        raise RuntimeError(f"Profit and account sync incomplete for {market}")

async def run_kr_open(db: Session, test_mode: bool = False):
    """한국 시장 장초 탐욕 레그"""
    # 휴장 체크
    if not _check_market_open(db, "KR"):
        return {"buy_plans": [], "sell_plans": [], "skipped": [], "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0}, "message": "시장 휴장"}
    
    await _sync_profit_and_account(db, market="KR")
    eng = HybridTraderEngine(db, EngineConfig(market="KR", currency="KRW", test_mode=test_mode))
    return eng.run_open_greedy()

async def run_us_open(db: Session, test_mode: bool = False):
    """미국 시장 장초 탐욕 레그"""
    # 휴장 체크
    if not _check_market_open(db, "US"):
        return {"buy_plans": [], "sell_plans": [], "skipped": [], "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0}, "message": "시장 휴장"}
    
    await _sync_profit_and_account(db, market="US")
    eng = HybridTraderEngine(db, EngineConfig(market="US", currency="USD", test_mode=test_mode))
    return eng.run_open_greedy()

async def run_kr_intraday(db: Session, test_mode: bool = False):
    """한국 시장 장중 사이클"""
    # 휴장 체크
    if not _check_market_open(db, "KR"):
        return {"buy_plans": [], "sell_plans": [], "skipped": [], "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0}, "message": "시장 휴장"}
    
    await _sync_profit_and_account(db, market="KR")
    eng = HybridTraderEngine(db, EngineConfig(market="KR", currency="KRW", test_mode=test_mode))
    return eng.run_intraday_cycle()

async def run_us_intraday(db: Session, test_mode: bool = False):
    """미국 시장 장중 사이클"""
    # 휴장 체크
    if not _check_market_open(db, "US"):
        return {"buy_plans": [], "sell_plans": [], "skipped": [], "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0}, "message": "시장 휴장"}
    
    await _sync_profit_and_account(db, market="US")
    eng = HybridTraderEngine(db, EngineConfig(market="US", currency="USD", test_mode=test_mode))
    return eng.run_intraday_cycle()
