# app/features/trading_hybrid/repositories/portfolio_repository.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc, text
from app.features.portfolio.models.asset_snapshot import AccountSnapshot, PositionSnapshot

def load_latest_account_snapshot(db: Session, market: str, currency: str) -> Dict[str, Any]:
    """
    최신 계좌 스냅샷 로드
    
    로직:
    - trading.account_snapshot에서 market, base_ccy 필터링
    - asof_kst 내림차순으로 최신 1건 조회
    - 없으면 기본값 반환 (buying_power=0 등)
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
        currency: "KRW" 또는 "USD"
    
    Returns:
        계좌 정보 딕셔너리
    """
    market_type = "KR" if market == "KR" else "OVRS"
    snapshot = (
        db.query(AccountSnapshot)
          .filter(AccountSnapshot.market == market_type,
                  AccountSnapshot.base_ccy == currency)
          .order_by(desc(AccountSnapshot.asof_kst))
          .first()
    )
    if not snapshot:
        return {
            "snapshot_id": None,
            "buying_power_ccy": 0.0,
            "total_equity_ccy": 0.0,
            "cash_balance_ccy": 0.0,
            "total_market_value_ccy": 0.0,
        }
    return {
        "snapshot_id": snapshot.snapshot_id,
        "buying_power_ccy": float(snapshot.buying_power_ccy or 0.0),
        "total_equity_ccy": float(snapshot.total_equity_ccy or 0.0),
        "cash_balance_ccy": float(snapshot.cash_balance_ccy or 0.0),
        "total_market_value_ccy": float(snapshot.total_market_value_ccy or 0.0),
    }


def load_latest_positions(db: Session, snapshot_id: int | None) -> List[Dict[str, Any]]:
    """
    최신 포지션 목록 로드
    
    Args:
        db: DB 세션
        snapshot_id: 스냅샷 ID (None이면 빈 리스트 반환)
    
    Returns:
        포지션 리스트 (signal_1d 포함)
    """
    if not snapshot_id:
        return []
    
    # pm_best_signal과 조인하여 signal_1d 정보 추가
    sql = """
    SELECT 
        ps.ticker_id,
        ps.symbol,
        ps.qty,
        ps.orderable_qty,
        ps.avg_cost_ccy,
        ps.last_price_ccy,
        ps.market_value_ccy,
        ps.unrealized_pnl_ccy,
        ps.pnl_rate,
        pbs.signal_1d
    FROM trading.position_snapshot ps
    LEFT JOIN trading.pm_best_signal pbs ON ps.ticker_id = pbs.ticker_id
    WHERE ps.snapshot_id = :snapshot_id
    """
    
    rows = db.execute(text(sql), {"snapshot_id": snapshot_id}).fetchall()
    
    res = []
    for row in rows:
        res.append({
            "ticker_id": row.ticker_id,
            "symbol": row.symbol,
            "qty": float(row.qty or 0),
            "orderable_qty": float(row.orderable_qty or 0),
            "avg_cost_ccy": float(row.avg_cost_ccy or 0),
            "last_price_ccy": float(row.last_price_ccy or 0),
            "market_value_ccy": float(row.market_value_ccy or 0),
            "unrealized_pnl_ccy": float(row.unrealized_pnl_ccy or 0),
            "pnl_rate": float(row.pnl_rate or 0),
            "signal_1d": float(row.signal_1d) if row.signal_1d is not None else None,
        })
    return res


def load_pending_orders(db: Session, market: str) -> List[Dict[str, Any]]:
    """
    미체결 주문 로드 (broker_order SUBMITTED + order_fill 없음 또는 UNFILLED)
    
    로직:
    - broker_order.status = 'SUBMITTED'
    - LEFT JOIN order_fill ... WHERE (order_fill.id IS NULL OR fill_status = 'UNFILLED')
    - 국가 필터링 (ticker.country)
    - ⚠️ 24시간 이내 제출된 주문만 (오래된 SUBMITTED는 문제 있음)
    
    Args:
        db: DB 세션
        market: "KR" 또는 "US"
    
    Returns:
        미체결 주문 리스트
    """
    country = "KR" if market == "KR" else "US"
    sql = """
    WITH latest_submitted_orders AS (
        SELECT DISTINCT ON (leg_id)
            id as broker_order_id,
            leg_id,
            order_number,
            submitted_at,
            status
        FROM trading.broker_order
        WHERE status = 'SUBMITTED'
          AND submitted_at >= NOW() - INTERVAL '24 hours'  -- ⚠️ 24시간 이내만
        ORDER BY leg_id, submitted_at DESC
    )
    SELECT
        lso.broker_order_id,
        ol.id as leg_id,
        ol.plan_id,
        op.ticker_id,
        op.symbol,
        ol.side,
        ol.quantity,
        ol.limit_price,
        ol.type as order_type,
        lso.order_number,
        lso.submitted_at,
        t.exchange,
        t.country
    FROM trading.order_leg ol
    JOIN latest_submitted_orders lso ON lso.leg_id = ol.id
    JOIN trading.order_plan op ON ol.plan_id = op.id
    JOIN trading.ticker t ON op.ticker_id = t.id
    LEFT JOIN trading.order_fill of ON of.broker_order_id = lso.broker_order_id
    WHERE (of.id IS NULL OR of.fill_status = 'UNFILLED')
      AND t.country = :country
    ORDER BY lso.submitted_at DESC
    """
    rows = db.execute(text(sql), {"country": country}).fetchall()
    return [r._mapping for r in rows]
