from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import logging

from app.core.kis_client import KISClient
from app.features.portfolio.repositories.asset_snapshot_repository import AssetSnapshotRepository
from app.features.portfolio.models.asset_snapshot import MarketType
from app.core import config as settings

class AssetSnapshotService:
    """자산 스냅샷 수집 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.kis_client = KISClient(db)
        self.snapshot_repo = AssetSnapshotRepository(db)
        self.logger = logging.getLogger(__name__)
    
    def collect_kr_account_snapshot(
        self, 
        account_uid: Optional[str] = None,
        asof_kst: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        국내 계좌 스냅샷 수집
        
        Args:
            account_uid: 계좌 식별자 (기본값: 설정에서 가져옴)
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            수집 결과 정보
        """
        try:
            if account_uid is None or account_uid == "default_account":
                cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
                account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
            
            self.logger.info(f"=== KR Account Snapshot Collection ===")
            self.logger.info(f"Account UID: {account_uid}")
            
            # 1. 국내 잔고조회 (006) - 계좌 정보 + 포지션
            balance_result = self.kis_client.inquire_balance(
                CANO=account_uid.split('-')[0],
                ACNT_PRDT_CD=account_uid.split('-')[1],
                AFHR_FLPR_YN="N",  # 기본값
                OFL_YN="",  # 오프라인여부
                INQR_DVSN="02",  # 종목별 조회
                UNPR_DVSN="01",  # 기본값
                FUND_STTL_ICLD_YN="N",  # 펀드결제분 미포함
                FNCG_AMT_AUTO_RDPT_YN="N",  # 융자금액자동상환 미사용
                PRCS_DVSN="00",  # 전일매매 포함
                CTX_AREA_FK100="",  # 연속조회 없음
                CTX_AREA_NK100=""  # 연속조회 없음
            )
            
            if balance_result.get("rt_cd") != "0":
                raise Exception(f"국내 잔고조회 실패: {balance_result.get('msg1', 'Unknown error')}")
            
            # 2. 매수가능조회 (007) - 매수가능금액
            buying_power_result = self.kis_client.inquire_psbl_order(
                CANO=account_uid.split('-')[0],
                ACNT_PRDT_CD=account_uid.split('-')[1],
                PDNO="",  # 전체
                ORD_UNPR="",  # 전체
                ORD_DVSN="00",  # 전체
                CMA_EVLU_AMT_ICLD_YN="Y",  # CMA 평가금액 포함
                OVRS_ICLD_YN="N"  # 해외 미포함
            )
            
            if buying_power_result.get("rt_cd") != "0":
                self.logger.warning(f"매수가능조회 실패: {buying_power_result.get('msg1', 'Unknown error')}")
                buying_power_result = {"output": []}  # 빈 결과로 처리
            
            # 3. 스냅샷 저장
            snapshot_id = self.snapshot_repo.save_kr_account_snapshot(
                account_uid=account_uid,
                balance_data=balance_result,
                buying_power_data=buying_power_result,
                asof_kst=asof_kst
            )
            
            # 4. 결과 반환
            result = {
                "success": True,
                "snapshot_id": snapshot_id,
                "market": MarketType.KR.value,
                "account_uid": account_uid,
                "asof_kst": asof_kst or datetime.now(timezone.utc),
                "balance_api_success": True,
                "buying_power_api_success": buying_power_result.get("rt_cd") == "0",
                "positions_count": len(balance_result.get("output1", [])),
                "message": "국내 계좌 스냅샷 수집 완료"
            }
            
            self.logger.info(f"✅ KR account snapshot collected: {result}")
            return result
            
        except Exception as e:
            import traceback
            self.logger.error(f"❌ Failed to collect KR account snapshot: {str(e)}")
            self.logger.error(f"❌ Stack trace: {traceback.format_exc()}")
            return {
                "success": False,
                "market": MarketType.KR.value,
                "account_uid": account_uid,
                "error": str(e),
                "message": "국내 계좌 스냅샷 수집 실패"
            }

    def collect_ovrs_account_snapshot(
        self, 
        account_uid: Optional[str] = None,
        asof_kst: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        해외 계좌 스냅샷 수집
        
        Args:
            account_uid: 계좌 식별자 (기본값: 설정에서 가져옴)
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            수집 결과 정보
        """
        try:
            if account_uid is None or account_uid == "default_account":
                cano = settings.KIS_VIRTUAL_CANO if settings.KIS_VIRTUAL else settings.KIS_CANO
                account_uid = f"{cano}-{settings.KIS_ACNT_PRDT_CD}"
            
            self.logger.info(f"=== OVRS Account Snapshot Collection ===")
            self.logger.info(f"Account UID: {account_uid}")
            
            # 해외 체결기준잔고 조회 (008) - 해외 전용 매개변수
            balance_result = self.kis_client.present_balance(
                wcrc_frcr_dvsn_cd="02",  # 외화 기준 (해외 계좌용)
                natn_cd="840",           # 미국
                tr_mket_cd="00",         # 전체 거래시장
                inqr_dvsn_cd="00"        # 체결기준 조회
            )
            
            if balance_result.get("rt_cd") != "0":
                raise Exception(f"해외 잔고조회 실패: {balance_result.get('msg1', 'Unknown error')}")
            
            # 스냅샷 저장
            snapshot_id = self.snapshot_repo.save_ovrs_account_snapshot(
                account_uid=account_uid,
                balance_data=balance_result,
                asof_kst=asof_kst
            )
            
            # 결과 반환
            result = {
                "success": True,
                "snapshot_id": snapshot_id,
                "market": MarketType.OVRS.value,
                "account_uid": account_uid,
                "asof_kst": asof_kst or datetime.now(timezone.utc),
                "positions_count": len(balance_result.get("output1", [])),
                "currencies_count": len(balance_result.get("output2", [])),
                "message": "해외 계좌 스냅샷 수집 완료"
            }
            
            self.logger.info(f"✅ OVRS account snapshot collected: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Failed to collect OVRS account snapshot: {str(e)}")
            return {
                "success": False,
                "market": MarketType.OVRS.value,
                "account_uid": account_uid,
                "error": str(e),
                "message": "해외 계좌 스냅샷 수집 실패"
            }

    def collect_all_account_snapshots(
        self, 
        account_uid: Optional[str] = None,
        asof_kst: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        전체 계좌 스냅샷 수집 (국내 + 해외)
        
        Args:
            account_uid: 계좌 식별자 (기본값: 설정에서 가져옴)
            asof_kst: 조회 기준 시각 (기본값: 현재 시각)
            
        Returns:
            수집 결과 정보
        """
        try:
            self.logger.info(f"=== All Account Snapshots Collection ===")
            
            # 국내 스냅샷 수집
            kr_result = self.collect_kr_account_snapshot(account_uid, asof_kst)
            
            # 해외 스냅샷 수집
            ovrs_result = self.collect_ovrs_account_snapshot(account_uid, asof_kst)
            
            # 전체 결과
            result = {
                "success": kr_result.get("success", False) and ovrs_result.get("success", False),
                "kr_snapshot": kr_result,
                "ovrs_snapshot": ovrs_result,
                "message": "전체 계좌 스냅샷 수집 완료" if kr_result.get("success") and ovrs_result.get("success") else "일부 계좌 스냅샷 수집 실패"
            }
            
            self.logger.info(f"✅ All account snapshots collected: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Failed to collect all account snapshots: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": "전체 계좌 스냅샷 수집 실패"
            }

    def get_latest_snapshot(self, account_uid: str, market: MarketType) -> Optional[Dict[str, Any]]:
        """
        최신 스냅샷 조회
        
        Args:
            account_uid: 계좌 식별자
            market: 시장 구분
            
        Returns:
            스냅샷 정보 (포지션 포함)
        """
        try:
            snapshot = self.snapshot_repo.get_latest_snapshot(account_uid, market)
            if not snapshot:
                return None
            
            # 포지션 정보 포함하여 반환
            return {
                "snapshot_id": snapshot.snapshot_id,
                "asof_kst": snapshot.asof_kst,
                "market": snapshot.market,
                "account_uid": snapshot.account_uid,
                "base_ccy": snapshot.base_ccy,
                "cash_balance_ccy": float(snapshot.cash_balance_ccy) if snapshot.cash_balance_ccy else None,
                "buying_power_ccy": float(snapshot.buying_power_ccy) if snapshot.buying_power_ccy else None,
                "total_market_value_ccy": float(snapshot.total_market_value_ccy) if snapshot.total_market_value_ccy else None,
                "total_equity_ccy": float(snapshot.total_equity_ccy) if snapshot.total_equity_ccy else None,
                "pnl_amount_ccy": float(snapshot.pnl_amount_ccy) if snapshot.pnl_amount_ccy else None,
                "pnl_rate": float(snapshot.pnl_rate) if snapshot.pnl_rate else None,
                "positions": [
                    {
                        "id": pos.id,
                        "ticker_id": pos.ticker_id,
                        "symbol": pos.symbol,
                        "exchange_code": pos.exchange_code,
                        "position_ccy": pos.position_ccy,
                        "qty": float(pos.qty) if pos.qty else None,
                        "orderable_qty": float(pos.orderable_qty) if pos.orderable_qty else None,
                        "avg_cost_ccy": float(pos.avg_cost_ccy) if pos.avg_cost_ccy else None,
                        "last_price_ccy": float(pos.last_price_ccy) if pos.last_price_ccy else None,
                        "market_value_ccy": float(pos.market_value_ccy) if pos.market_value_ccy else None,
                        "unrealized_pnl_ccy": float(pos.unrealized_pnl_ccy) if pos.unrealized_pnl_ccy else None,
                        "pnl_rate": float(pos.pnl_rate) if pos.pnl_rate else None,
                        "fx_krw_per_ccy": float(pos.fx_krw_per_ccy) if pos.fx_krw_per_ccy else None,
                    }
                    for pos in snapshot.positions
                ],
                "created_at": snapshot.created_at
            }
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get latest snapshot: {str(e)}")
            return None

    def get_account_history(
        self, 
        account_uid: str, 
        market: MarketType, 
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        계좌 히스토리 조회
        
        Args:
            account_uid: 계좌 식별자
            market: 시장 구분
            limit: 조회 개수 제한
            
        Returns:
            히스토리 목록
        """
        try:
            snapshots = self.snapshot_repo.get_account_history(account_uid, market, limit)
            
            return [
                {
                    "snapshot_id": snapshot.snapshot_id,
                    "asof_kst": snapshot.asof_kst,
                    "market": snapshot.market,
                    "account_uid": snapshot.account_uid,
                    "base_ccy": snapshot.base_ccy,
                    "cash_balance_ccy": float(snapshot.cash_balance_ccy) if snapshot.cash_balance_ccy else None,
                    "buying_power_ccy": float(snapshot.buying_power_ccy) if snapshot.buying_power_ccy else None,
                    "total_market_value_ccy": float(snapshot.total_market_value_ccy) if snapshot.total_market_value_ccy else None,
                    "total_equity_ccy": float(snapshot.total_equity_ccy) if snapshot.total_equity_ccy else None,
                    "pnl_amount_ccy": float(snapshot.pnl_amount_ccy) if snapshot.pnl_amount_ccy else None,
                    "pnl_rate": float(snapshot.pnl_rate) if snapshot.pnl_rate else None,
                    "created_at": snapshot.created_at
                }
                for snapshot in snapshots
            ]
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get account history: {str(e)}")
            return []
