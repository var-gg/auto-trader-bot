from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import datetime, timezone
import logging

from app.core.db import get_db
from app.features.portfolio.services.asset_snapshot_service import AssetSnapshotService
from app.features.portfolio.models.asset_snapshot import MarketType

router = APIRouter(prefix="/asset-snapshots", tags=["asset-snapshots"])
logger = logging.getLogger(__name__)

@router.post("/collect/kr")
async def collect_kr_snapshot(
    account_uid: Optional[str] = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    국내 계좌 스냅샷 수집
    
    Args:
        account_uid: 계좌 식별자 (선택사항, 설정에서 기본값 사용)
        
    Returns:
        수집 결과 정보
    """
    try:
        service = AssetSnapshotService(db)
        result = service.collect_kr_account_snapshot(account_uid)
        
        if result.get("success"):
            return {
                "success": True,
                "data": result,
                "message": "국내 계좌 스냅샷 수집 완료"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"국내 계좌 스냅샷 수집 실패: {result.get('error', 'Unknown error')}"
            )
            
    except Exception as e:
        logger.error(f"❌ Failed to collect KR snapshot: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"국내 계좌 스냅샷 수집 중 오류 발생: {str(e)}"
        )

@router.post("/collect/ovrs")
async def collect_ovrs_snapshot(
    account_uid: Optional[str] = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    해외 계좌 스냅샷 수집
    
    Args:
        account_uid: 계좌 식별자 (선택사항, 설정에서 기본값 사용)
        
    Returns:
        수집 결과 정보
    """
    try:
        service = AssetSnapshotService(db)
        result = service.collect_ovrs_account_snapshot(account_uid)
        
        if result.get("success"):
            return {
                "success": True,
                "data": result,
                "message": "해외 계좌 스냅샷 수집 완료"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"해외 계좌 스냅샷 수집 실패: {result.get('error', 'Unknown error')}"
            )
            
    except Exception as e:
        logger.error(f"❌ Failed to collect OVRS snapshot: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"해외 계좌 스냅샷 수집 중 오류 발생: {str(e)}"
        )

@router.post("/collect/all")
async def collect_all_snapshots(
    account_uid: Optional[str] = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    전체 계좌 스냅샷 수집 (국내 + 해외)
    
    Args:
        account_uid: 계좌 식별자 (선택사항, 설정에서 기본값 사용)
        
    Returns:
        수집 결과 정보
    """
    try:
        service = AssetSnapshotService(db)
        result = service.collect_all_account_snapshots(account_uid)
        
        if result.get("success"):
            return {
                "success": True,
                "data": result,
                "message": "전체 계좌 스냅샷 수집 완료"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"전체 계좌 스냅샷 수집 실패: {result.get('error', 'Unknown error')}"
            )
            
    except Exception as e:
        logger.error(f"❌ Failed to collect all snapshots: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"전체 계좌 스냅샷 수집 중 오류 발생: {str(e)}"
        )

@router.get("/latest/{market}")
async def get_latest_snapshot(
    market: str,
    account_uid: str = Query(..., description="계좌 식별자"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    최신 스냅샷 조회
    
    Args:
        market: 시장 구분 (KR 또는 OVRS)
        account_uid: 계좌 식별자
        
    Returns:
        최신 스냅샷 정보 (포지션 포함)
    """
    try:
        # 시장 구분 검증
        try:
            market_type = MarketType(market.upper())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid market type: {market}. Must be 'KR' or 'OVRS'"
            )
        
        service = AssetSnapshotService(db)
        snapshot = service.get_latest_snapshot(account_uid, market_type)
        
        if snapshot:
            return {
                "success": True,
                "data": snapshot,
                "message": "최신 스냅샷 조회 완료"
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No snapshot found for account {account_uid} in market {market}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get latest snapshot: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"최신 스냅샷 조회 중 오류 발생: {str(e)}"
        )

@router.get("/history/{market}")
async def get_account_history(
    market: str,
    account_uid: str = Query(..., description="계좌 식별자"),
    limit: int = Query(30, ge=1, le=100, description="조회 개수 제한 (1-100)"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    계좌 히스토리 조회
    
    Args:
        market: 시장 구분 (KR 또는 OVRS)
        account_uid: 계좌 식별자
        limit: 조회 개수 제한
        
    Returns:
        계좌 히스토리 목록
    """
    try:
        # 시장 구분 검증
        try:
            market_type = MarketType(market.upper())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid market type: {market}. Must be 'KR' or 'OVRS'"
            )
        
        service = AssetSnapshotService(db)
        history = service.get_account_history(account_uid, market_type, limit)
        
        return {
            "success": True,
            "data": {
                "account_uid": account_uid,
                "market": market,
                "limit": limit,
                "count": len(history),
                "snapshots": history
            },
            "message": f"계좌 히스토리 조회 완료 ({len(history)}개)"
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to get account history: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"계좌 히스토리 조회 중 오류 발생: {str(e)}"
        )

@router.get("/summary")
async def get_account_summary(
    account_uid: str = Query(..., description="계좌 식별자"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    계좌 요약 정보 조회 (국내 + 해외 최신 스냅샷)
    
    Args:
        account_uid: 계좌 식별자
        
    Returns:
        계좌 요약 정보
    """
    try:
        service = AssetSnapshotService(db)
        
        # 국내 최신 스냅샷
        kr_snapshot = service.get_latest_snapshot(account_uid, MarketType.KR)
        
        # 해외 최신 스냅샷
        ovrs_snapshot = service.get_latest_snapshot(account_uid, MarketType.OVRS)
        
        summary = {
            "account_uid": account_uid,
            "kr_snapshot": kr_snapshot,
            "ovrs_snapshot": ovrs_snapshot,
            "has_kr_data": kr_snapshot is not None,
            "has_ovrs_data": ovrs_snapshot is not None,
            "total_positions": 0,
            "total_equity_krw": 0,
            "total_pnl_krw": 0
        }
        
        # 포지션 수 계산
        if kr_snapshot:
            summary["total_positions"] += len(kr_snapshot.get("positions", []))
        if ovrs_snapshot:
            summary["total_positions"] += len(ovrs_snapshot.get("positions", []))
        
        # 총 자산 및 손익 계산 (간단한 예시)
        # 실제로는 환율 변환 등을 고려해야 함
        if kr_snapshot and kr_snapshot.get("total_equity_ccy"):
            summary["total_equity_krw"] += kr_snapshot.get("total_equity_ccy", 0)
        if kr_snapshot and kr_snapshot.get("pnl_amount_ccy"):
            summary["total_pnl_krw"] += kr_snapshot.get("pnl_amount_ccy", 0)
        
        return {
            "success": True,
            "data": summary,
            "message": "계좌 요약 정보 조회 완료"
        }
            
    except Exception as e:
        logger.error(f"❌ Failed to get account summary: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"계좌 요약 정보 조회 중 오류 발생: {str(e)}"
        )
