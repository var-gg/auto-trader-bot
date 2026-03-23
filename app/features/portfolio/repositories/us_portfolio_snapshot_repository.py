from __future__ import annotations
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
# UUID 관련 import 제거
import json
import logging

from app.features.portfolio.models.portfolio_snapshot import (
    KISPortfolioSnapshot,
    KISPositionExecbasis,
    KISCurrencySummary,
    KISAccountTotals
)
from app.shared.models.ticker import Ticker
from app.core.symbol_normalizer import to_canonical_symbol, to_kis_symbol

class PortfolioSnapshotRepository:
    """포트폴리오 스냅샷 리포지토리"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _map_kis_exchange_to_finnhub(self, kis_exchange: str) -> str:
        """
        KIS 거래소 코드를 핀헙 거래소 코드로 매핑
        
        Args:
            kis_exchange: KIS 거래소 코드 (예: NASD, NYSE)
            
        Returns:
            핀헙 거래소 코드 (예: NMS, NYQ)
        """
        mapping = {
            "NASD": "NMS",  # 나스닥
            "NYSE": "NYQ",  # 뉴욕증권거래소
            "AMEX": "AMX",  # 아메리칸증권거래소
            # 필요에 따라 추가 매핑
        }
        return mapping.get(kis_exchange, kis_exchange)
    
    def _find_ticker_id(self, symbol: str, exchange_code: str) -> Optional[int]:
        """
        심볼과 거래소 코드로 ticker ID를 찾습니다.

        포맷 이질성 대응:
        - Yahoo canonical: BF-B
        - Dot style:       BF.B
        - KIS style:       BF/B
        """
        try:
            finnhub_exchange = self._map_kis_exchange_to_finnhub(exchange_code)

            candidates = [c for c in {
                symbol,
                to_canonical_symbol(symbol),
                to_kis_symbol(symbol),
            } if c]

            ticker = (
                self.db.query(Ticker)
                .filter(
                    Ticker.symbol.in_(candidates),
                    Ticker.exchange == finnhub_exchange
                )
                .first()
            )

            if ticker:
                return ticker.id
            else:
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Ticker not found: symbol={symbol}, candidates={candidates}, "
                    f"kis_exchange={exchange_code}, finnhub_exchange={finnhub_exchange}"
                )
                return None

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error finding ticker: {str(e)}")
            return None

    def save_portfolio_snapshot(
        self,
        account_id: str,
        venue_scope: str,
        raw_response: Dict[str, Any],
        asof_kst: Optional[datetime] = None
    ) -> int:
        """
        포트폴리오 스냅샷을 저장합니다.
        
        Args:
            account_id: 계좌 식별자
            venue_scope: 시장 범위 (US/KR/ALL)
            raw_response: KIS API 원본 응답
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            snapshot_id: 생성된 스냅샷 ID
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"=== Portfolio Snapshot Repository ===")
            logger.info(f"Input: account_id={account_id}, venue_scope={venue_scope}")
            
            if asof_kst is None:
                asof_kst = datetime.now(timezone.utc)
            
            # 1. 메인 스냅샷 저장 (auto-increment ID 사용)
            logger.info("Creating main snapshot record...")
            snapshot = KISPortfolioSnapshot(
                provider="KIS",
                account_id=account_id,
                venue_scope=venue_scope,
                asof_kst=asof_kst,
                raw_json=raw_response
            )
            self.db.add(snapshot)
            self.db.flush()  # 중간 커밋으로 메인 레코드를 먼저 저장하고 ID 획득
            snapshot_id = snapshot.snapshot_id
            logger.info(f"✅ Main snapshot record added and flushed to database. Generated snapshot_id: {snapshot_id}")
            
            # 2. output1 → 종목별 체결기준 현재 잔고 저장
            output1 = raw_response.get("output1", [])
            logger.info(f"Processing output1: {len(output1)} positions")
            
            for i, item in enumerate(output1):
                symbol_raw = item.get("pdno", "")
                symbol = to_canonical_symbol(symbol_raw) or symbol_raw
                exchange_code = item.get("ovrs_excg_cd", "")
                logger.info(f"Position {i+1}: symbol={symbol_raw} -> {symbol}, name={item.get('prdt_name')}, exchange={exchange_code}")

                # ticker_id 찾기
                ticker_id = self._find_ticker_id(symbol, exchange_code)
                if ticker_id:
                    logger.info(f"Found ticker_id: {ticker_id} for {symbol}")
                else:
                    logger.warning(f"No ticker_id found for {symbol} on {exchange_code}")
                
                position = KISPositionExecbasis(
                    snapshot_id=snapshot_id,
                    symbol=symbol,
                    name=item.get("prdt_name", ""),
                    exchange_code=exchange_code,
                    ticker_id=ticker_id,
                    ccy=item.get("buy_crcy_cd", ""),
                    pos_qty_exec=self._safe_numeric(item.get("ccld_qty_smtl1")),
                    orderable_qty=self._safe_numeric(item.get("ord_psbl_qty1")),
                    avg_cost_ccy=self._safe_numeric(item.get("avg_unpr3")),
                    last_price_ccy=self._safe_numeric(item.get("ovrs_now_pric1")),
                    purchase_amt_ccy=self._safe_numeric(item.get("frcr_pchs_amt")),
                    eval_amt_ccy=self._safe_numeric(item.get("frcr_evlu_amt2")),
                    pnl_ccy=self._safe_numeric(item.get("evlu_pfls_amt2")),
                    pnl_rate=self._safe_numeric(item.get("evlu_pfls_rt1")),
                    fx_rate_krw_per_ccy=self._safe_numeric(item.get("bass_exrt")),
                    std_pdno=item.get("std_pdno", ""),
                    product_type_cd=item.get("prdt_type_cd", ""),
                    security_type=item.get("scts_dvsn_name", "")
                )
                self.db.add(position)
            
            logger.info(f"✅ Added {len(output1)} position records")
            self.db.flush()  # 포지션 레코드들을 중간 커밋
            
            # 3. output2 → 통화별 요약 저장
            output2 = raw_response.get("output2", [])
            logger.info(f"Processing output2: {len(output2)} currencies")
            
            for i, item in enumerate(output2):
                logger.info(f"Currency {i+1}: ccy={item.get('crcy_cd')}")
                currency = KISCurrencySummary(
                    snapshot_id=snapshot_id,
                    ccy=item.get("crcy_cd", ""),
                    buy_amt_ccy_sum=self._safe_numeric(item.get("frcr_buy_amt_smtl")),
                    sell_amt_ccy_sum=self._safe_numeric(item.get("frcr_sll_amt_smtl")),
                    cash_ccy=self._safe_numeric(item.get("frcr_dncl_amt_2")),
                    locked_margin_ccy=self._safe_numeric(item.get("frcr_buy_mgn_amt")),
                    other_margin_ccy=self._safe_numeric(item.get("frcr_etc_mgna")),
                    withdrawable_ccy=self._safe_numeric(item.get("frcr_drwg_psbl_amt_1")),
                    portfolio_eval_ccy=self._safe_numeric(item.get("frcr_evlu_amt2")),
                    first_fx_krw_per_ccy=self._safe_numeric(item.get("frst_bltn_exrt")),
                    local_custody_flag=item.get("acpl_cstd_crcy_yn", ""),
                    nextday_withdrawable_ccy=self._safe_numeric(item.get("nxdy_frcr_drwg_psbl_amt"))
                )
                self.db.add(currency)
            
            logger.info(f"✅ Added {len(output2)} currency records")
            self.db.flush()  # 통화 레코드들을 중간 커밋
            
            # 4. output3 → 계좌 전체 요약 저장
            output3 = raw_response.get("output3", {})
            logger.info(f"Processing output3: {'exists' if output3 else 'missing'}")
            
            if output3:
                logger.info("Creating account totals record...")
                totals = KISAccountTotals(
                    snapshot_id=snapshot_id,
                    total_assets_krw=self._safe_numeric(output3.get("tot_asst_amt")),
                    total_deposit_krw=self._safe_numeric(output3.get("tot_dncl_amt")),
                    withdrawable_total_krw=self._safe_numeric(output3.get("wdrw_psbl_tot_amt")),
                    usable_fx_total_ccy=self._safe_numeric(output3.get("frcr_use_psbl_amt")),
                    locked_margin_krw=self._safe_numeric(output3.get("buy_mgn_amt")),
                    unsettled_buy_krw=self._safe_numeric(output3.get("ustl_buy_amt_smtl")),
                    unsettled_sell_krw=self._safe_numeric(output3.get("ustl_sll_amt_smtl")),
                    fx_balance_total_ccy=self._safe_numeric(output3.get("tot_frcr_cblc_smtl")),
                    total_eval_pnl_krw=self._safe_numeric(output3.get("tot_evlu_pfls_amt"))
                )
                self.db.add(totals)
                logger.info("✅ Account totals record added")
            else:
                logger.warning("⚠️ output3 is missing, skipping account totals")
            
            # 최종 커밋
            logger.info("Committing final transaction...")
            self.db.commit()
            logger.info(f"✅ Transaction committed successfully. Snapshot ID: {snapshot_id}")
            
            return snapshot_id
            
        except Exception as e:
            logger.error(f"❌ Failed to save portfolio snapshot: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # 롤백
            try:
                self.db.rollback()
                logger.info("Transaction rolled back")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback: {str(rollback_error)}")
            
            raise

    def _safe_numeric(self, value: Any) -> Optional[float]:
        """안전하게 숫자로 변환"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_latest_snapshot(self, account_id: str) -> Optional[KISPortfolioSnapshot]:
        """최신 스냅샷 조회"""
        return (
            self.db.query(KISPortfolioSnapshot)
            .filter(KISPortfolioSnapshot.account_id == account_id)
            .order_by(KISPortfolioSnapshot.asof_kst.desc())
            .first()
        )
