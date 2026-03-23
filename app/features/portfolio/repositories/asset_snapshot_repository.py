from __future__ import annotations
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc
import logging

from app.features.portfolio.models.asset_snapshot import AccountSnapshot, PositionSnapshot, MarketType
from app.shared.models.ticker import Ticker
from app.core.symbol_normalizer import to_canonical_symbol, to_kis_symbol

class AssetSnapshotRepository:
    """자산 스냅샷 리포지토리"""
    
    def __init__(self, db: Session):
        self.db = db
        self.logger = logging.getLogger(__name__)
    
    def _map_kis_exchange_to_finnhub(self, kis_exchange: str) -> str:
        """
        KIS 거래소 코드를 핀헙 거래소 코드로 매핑
        
        Args:
            kis_exchange: KIS 거래소 코드 (예: NASD, NYSE, KOE)
            
        Returns:
            핀헙 거래소 코드 (예: NMS, NYQ, KRX)
        """
        mapping = {
            "NASD": "NMS",   # 나스닥
            "NYSE": "NYQ",   # 뉴욕증권거래소
            "AMEX": "AMX",   # 아메리칸증권거래소
            "KOE": "KOE",    # 한국거래소
            "KOSPI": "KOE",  # 코스피
            "KOSDAQ": "KOE", # 코스닥
            # 필요에 따라 추가 매핑
        }
        return mapping.get(kis_exchange, kis_exchange)
    
    def _find_ticker_id(self, symbol: str, exchange_code: str) -> Optional[int]:
        """
        심볼과 거래소 코드로 ticker ID를 찾습니다.
        
        Args:
            symbol: 종목 심볼 (예: AAPL, 005930)
            exchange_code: KIS 거래소 코드 (예: NASD, KOE)
            
        Returns:
            ticker ID 또는 None
        """
        try:
            # KIS 거래소 코드를 핀헙 코드로 변환
            finnhub_exchange = self._map_kis_exchange_to_finnhub(exchange_code)

            # 심볼 포맷 이질성 대응: canonical(Yahoo, '-') <-> KIS('/').
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
                self.logger.warning(
                    f"Ticker not found: symbol={symbol}, candidates={candidates}, "
                    f"kis_exchange={exchange_code}, finnhub_exchange={finnhub_exchange}"
                )
                return None

        except Exception as e:
            self.logger.error(f"Error finding ticker: {str(e)}")
            return None

    def save_kr_account_snapshot(
        self,
        account_uid: str,
        balance_data: Dict[str, Any],
        buying_power_data: Dict[str, Any],
        asof_kst: Optional[datetime] = None
    ) -> int:
        """
        국내 계좌 스냅샷을 저장합니다.
        
        Args:
            account_uid: 계좌 식별자 (CANO-ACNT_PRDT_CD)
            balance_data: 국내 잔고조회(006) 응답 데이터
            buying_power_data: 매수가능조회(007) 응답 데이터
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            snapshot_id: 생성된 스냅샷 ID
        """
        try:
            if asof_kst is None:
                asof_kst = datetime.now(timezone.utc)
            
            # output2에서 계좌 정보 추출
            output2_list = balance_data.get("output2", [])
            output2 = output2_list[0] if output2_list and len(output2_list) > 0 else {}
            
            # output1에서 포지션 정보 추출
            output1 = balance_data.get("output1", [])
            
            # 매수가능금액 추출 (007 API 응답)
            buying_power = self._extract_buying_power(buying_power_data)
            
            # 1. AccountSnapshot 저장
            account_snapshot = AccountSnapshot(
                asof_kst=asof_kst,
                market=MarketType.KR.value,
                account_uid=account_uid,
                base_ccy="KRW",
                cash_balance_ccy=self._safe_numeric(output2.get("dnca_tot_amt")),
                buying_power_ccy=buying_power,
                total_market_value_ccy=self._safe_numeric(output2.get("scts_evlu_amt")),
                total_equity_ccy=self._safe_numeric(output2.get("nass_amt")),
                pnl_amount_ccy=self._safe_numeric(output2.get("evlu_pfls_smtl_amt")),
                pnl_rate=self._calculate_pnl_rate(
                    self._safe_numeric(output2.get("evlu_pfls_smtl_amt")),
                    self._safe_numeric(output2.get("pchs_amt_smtl_amt"))
                )
            )
            
            self.db.add(account_snapshot)
            self.db.flush()
            snapshot_id = account_snapshot.snapshot_id
            
            # 2. PositionSnapshot 저장 (qty > 0인 것만)
            for position_data in output1:
                qty = self._safe_numeric(position_data.get("hldg_qty"))
                if qty and qty > 0:  # 보유수량이 0보다 큰 것만 저장
                    symbol = position_data.get("pdno", "")
                    ticker_id = self._find_ticker_id(symbol, "KOE")  # 국내는 KOE
                    
                    position = PositionSnapshot(
                        snapshot_id=snapshot_id,
                        ticker_id=ticker_id,
                        symbol=symbol,
                        exchange_code="KOE",
                        position_ccy="KRW",
                        qty=qty,
                        orderable_qty=self._safe_numeric(position_data.get("ord_psbl_qty")),
                        avg_cost_ccy=self._safe_numeric(position_data.get("pchs_avg_pric")),
                        last_price_ccy=self._safe_numeric(position_data.get("prpr")),
                        market_value_ccy=self._safe_numeric(position_data.get("evlu_amt")),
                        unrealized_pnl_ccy=self._safe_numeric(position_data.get("evlu_pfls_amt")),
                        pnl_rate=self._safe_numeric(position_data.get("evlu_pfls_rt"))
                    )
                    self.db.add(position)
            
            self.db.commit()
            self.logger.info(f"✅ KR account snapshot saved: snapshot_id={snapshot_id}")
            return snapshot_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save KR account snapshot: {str(e)}")
            self.db.rollback()
            raise

    def save_ovrs_account_snapshot(
        self,
        account_uid: str,
        balance_data: Dict[str, Any],
        asof_kst: Optional[datetime] = None
    ) -> int:
        """
        해외 계좌 스냅샷을 저장합니다.
        
        Args:
            account_uid: 계좌 식별자 (CANO-ACNT_PRDT_CD)
            balance_data: 해외 체결기준잔고(008) 응답 데이터
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            snapshot_id: 생성된 스냅샷 ID
        """
        try:
            if asof_kst is None:
                asof_kst = datetime.now(timezone.utc)
            
            # output1에서 포지션 정보 추출
            output1 = balance_data.get("output1", [])
            
            # output2에서 현금 정보 추출 (USD 기준)
            output2 = balance_data.get("output2", [])
            cash_data = output2[0] if output2 and len(output2) > 0 else {}
            
            # output3에서 수익률만 추출 (나머지는 KRW 합계라 무시)
            output3 = balance_data.get("output3", {})
            
            # output1과 output2 기반으로 직접 계산
            positions = output1
            
            # 공식: total_market_value_ccy + cash_balance_ccy = total_equity_ccy
            total_market_value_ccy = sum(self._safe_numeric(p.get("frcr_evlu_amt2")) for p in positions if self._safe_numeric(p.get("frcr_evlu_amt2")))
            cash_balance_ccy = self._safe_numeric(cash_data.get("frcr_drwg_psbl_amt_1"))  # 출금가능현금 = 매수가능현금
            total_equity_ccy = total_market_value_ccy + cash_balance_ccy  # 총자산
            buying_power_ccy = cash_balance_ccy  # 매수가능현금 = 출금가능현금 (체결대기 묶임)
            
            # 포지션 손익 계산
            total_pnl = sum(self._safe_numeric(p.get("evlu_pfls_amt2")) for p in positions if self._safe_numeric(p.get("evlu_pfls_amt2")))
            
            # 1. AccountSnapshot 저장
            account_snapshot = AccountSnapshot(
                asof_kst=asof_kst,
                market=MarketType.OVRS.value,
                account_uid=account_uid,
                base_ccy="USD",
                cash_balance_ccy=cash_balance_ccy,  # frcr_drwg_psbl_amt_1
                buying_power_ccy=buying_power_ccy,  # cash_balance_ccy와 동일
                total_market_value_ccy=total_market_value_ccy,  # sum(frcr_evlu_amt2)
                total_equity_ccy=total_equity_ccy,  # frcr_dncl_amt_2
                pnl_amount_ccy=total_pnl,
                pnl_rate=self._safe_numeric(output3.get("evlu_erng_rt1"))  # output3에서 수익률만 사용
            )
            
            self.db.add(account_snapshot)
            self.db.flush()
            snapshot_id = account_snapshot.snapshot_id
            
            # 2. PositionSnapshot 저장 (qty > 0인 것만)
            for position_data in output1:
                qty = self._safe_numeric(position_data.get("ccld_qty_smtl1"))
                if qty and qty > 0:  # 보유수량이 0보다 큰 것만 저장
                    symbol = position_data.get("pdno", "") or position_data.get("std_pdno", "")
                    canonical_symbol = to_canonical_symbol(symbol) or symbol
                    exchange_code = position_data.get("ovrs_excg_cd", "")
                    position_ccy = position_data.get("buy_crcy_cd", "USD")

                    ticker_id = self._find_ticker_id(canonical_symbol, exchange_code)

                    position = PositionSnapshot(
                        snapshot_id=snapshot_id,
                        ticker_id=ticker_id,
                        symbol=canonical_symbol,
                        exchange_code=exchange_code,
                        position_ccy=position_ccy,
                        qty=qty,
                        orderable_qty=self._safe_numeric(position_data.get("ord_psbl_qty1")),
                        avg_cost_ccy=self._safe_numeric(position_data.get("avg_unpr3")),
                        last_price_ccy=self._safe_numeric(position_data.get("ovrs_now_pric1")),
                        market_value_ccy=self._safe_numeric(position_data.get("frcr_evlu_amt2")),
                        unrealized_pnl_ccy=self._safe_numeric(position_data.get("evlu_pfls_amt2")),
                        pnl_rate=self._safe_numeric(position_data.get("evlu_pfls_rt1")),
                        fx_krw_per_ccy=self._safe_numeric(position_data.get("bass_exrt"))
                    )
                    self.db.add(position)
            
            self.db.commit()
            self.logger.info(f"✅ OVRS account snapshot saved: snapshot_id={snapshot_id}")
            return snapshot_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save OVRS account snapshot: {str(e)}")
            self.db.rollback()
            raise

    def _extract_buying_power(self, buying_power_data: Dict[str, Any]) -> Optional[float]:
        """매수가능금액 추출 (007 API 응답에서)"""
        try:
            output = buying_power_data.get("output")
            if output:
                # output이 배열인 경우
                if isinstance(output, list) and len(output) > 0:
                    output_item = output[0]
                # output이 객체인 경우
                elif isinstance(output, dict):
                    output_item = output
                else:
                    return None
                
                # 미수 미포함 기준으로 nrcvb_buy_amt 우선 사용
                return self._safe_numeric(output_item.get("ord_psbl_cash")) or \
                    self._safe_numeric(output_item.get("nrcvb_buy_amt")) or \
                    self._safe_numeric(output_item.get("max_buy_amt"))
            return None
        except Exception as e:
            self.logger.error(f"Error extracting buying power: {str(e)}")
            return None

    def _calculate_pnl_rate(self, pnl_amount: Optional[float], purchase_amount: Optional[float]) -> Optional[float]:
        """손익률 계산"""
        if pnl_amount is None or purchase_amount is None or purchase_amount == 0:
            return None
        try:
            return (pnl_amount / purchase_amount) * 100
        except (ZeroDivisionError, TypeError):
            return None

    def _safe_numeric(self, value: Any) -> Optional[float]:
        """안전하게 숫자로 변환"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_latest_snapshot(self, account_uid: str, market: MarketType) -> Optional[AccountSnapshot]:
        """최신 스냅샷 조회"""
        return (
            self.db.query(AccountSnapshot)
            .filter(
                and_(
                    AccountSnapshot.account_uid == account_uid,
                    AccountSnapshot.market == market.value
                )
            )
            .order_by(desc(AccountSnapshot.asof_kst))
            .first()
        )

    def get_snapshot_with_positions(self, snapshot_id: int) -> Optional[AccountSnapshot]:
        """스냅샷과 포지션 정보를 함께 조회"""
        return (
            self.db.query(AccountSnapshot)
            .filter(AccountSnapshot.snapshot_id == snapshot_id)
            .first()
        )

    def get_account_history(
        self, 
        account_uid: str, 
        market: MarketType, 
        limit: int = 30
    ) -> List[AccountSnapshot]:
        """계좌 히스토리 조회"""
        return (
            self.db.query(AccountSnapshot)
            .filter(
                and_(
                    AccountSnapshot.account_uid == account_uid,
                    AccountSnapshot.market == market.value
                )
            )
            .order_by(desc(AccountSnapshot.asof_kst))
            .limit(limit)
            .all()
        )
