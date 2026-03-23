from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class UnfilledBackfillSummary:
    scanned: int
    updated: int
    unresolved: int


@dataclass
class TplusOutcomeSummary:
    scanned: int
    upserted: int
    skipped_missing_price: int


class PMHistoryBatchService:
    """PM decision history 후처리 배치 (안전한 스켈레톤 포함)."""

    HORIZONS = (1, 3, 5)
    FLAT_EPS_BPS = 1e-9

    def __init__(self, db: Session):
        self.db = db

    def backfill_unfilled_reasons(self, lookback_days: int = 7, limit: int = 2000) -> UnfilledBackfillSummary:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))

        rows = self.db.execute(text("""
            SELECT id, unfilled_reason_code, unfilled_reason_text, error_code, error_message
              FROM trading.pm_order_execution_history
             WHERE order_outcome_code = 'UNFILLED'
               AND executed_at >= :cutoff
               AND (unfilled_reason_code IS NULL OR unfilled_reason_text IS NULL)
             ORDER BY executed_at DESC, id DESC
             LIMIT :limit
        """), {"cutoff": cutoff, "limit": int(limit)}).fetchall()

        scanned = len(rows)
        updated = 0

        for row in rows:
            m = row._mapping
            inferred = self._infer_unfilled_reason(
                error_code=m.get("error_code"),
                error_message=m.get("error_message"),
            )
            if not inferred:
                continue

            self.db.execute(text("""
                UPDATE trading.pm_order_execution_history
                   SET unfilled_reason_code = COALESCE(unfilled_reason_code, :reason_code),
                       unfilled_reason_text = COALESCE(unfilled_reason_text, :reason_text)
                 WHERE id = :id
            """), {
                "id": int(m["id"]),
                "reason_code": inferred["reason_code"],
                "reason_text": inferred["reason_text"],
            })
            updated += 1

        self.db.commit()
        unresolved = scanned - updated

        logger.info(
            "PM unfilled reason backfill done: scanned=%s, updated=%s, unresolved=%s",
            scanned,
            updated,
            unresolved,
        )
        return UnfilledBackfillSummary(scanned=scanned, updated=updated, unresolved=unresolved)

    def compute_tplus_outcomes(self, lookback_days: int = 14, limit: int = 5000) -> TplusOutcomeSummary:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))

        rows = self.db.execute(text("""
            SELECT id, run_id, ticker_id, symbol,
                   COALESCE(avg_fill_price, submitted_price, intended_limit_price, intent_price) AS entry_price,
                   executed_at::date AS entry_date
              FROM trading.pm_order_execution_history
             WHERE action_code = 'BUY'
               AND executed_at >= :cutoff
             ORDER BY executed_at DESC, id DESC
             LIMIT :limit
        """), {"cutoff": cutoff, "limit": int(limit)}).fetchall()

        scanned = len(rows)
        upserted = 0
        skipped_missing_price = 0

        for row in rows:
            m = row._mapping
            entry_price = m.get("entry_price")
            if entry_price is None or float(entry_price) <= 0:
                skipped_missing_price += len(self.HORIZONS)
                continue

            run_id = int(m["run_id"])
            ticker_id = int(m["ticker_id"])
            symbol = m.get("symbol")
            entry_date = m.get("entry_date")

            for horizon in self.HORIZONS:
                horizon_price = self._load_tplus_close(ticker_id=ticker_id, entry_date=entry_date, horizon_days=horizon)
                if horizon_price is None or float(horizon_price) <= 0:
                    skipped_missing_price += 1
                    continue

                pnl_bps = ((float(horizon_price) - float(entry_price)) / float(entry_price)) * 10000.0
                label_code = self._label_from_pnl_bps(pnl_bps)

                self.db.execute(text("""
                    INSERT INTO trading.pm_outcome_tplus_history
                        (run_id, ticker_id, symbol, horizon_days, outcome_price, pnl_bps, label_code, evaluated_at)
                    VALUES
                        (:run_id, :ticker_id, :symbol, :horizon_days, :outcome_price, :pnl_bps, :label_code, NOW())
                    ON CONFLICT (run_id, ticker_id, horizon_days)
                    DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        outcome_price = EXCLUDED.outcome_price,
                        pnl_bps = EXCLUDED.pnl_bps,
                        label_code = EXCLUDED.label_code,
                        evaluated_at = NOW()
                """), {
                    "run_id": run_id,
                    "ticker_id": ticker_id,
                    "symbol": symbol,
                    "horizon_days": int(horizon),
                    "outcome_price": float(horizon_price),
                    "pnl_bps": float(pnl_bps),
                    "label_code": label_code,
                })
                upserted += 1

        self.db.commit()

        logger.info(
            "PM T+N outcome batch done: scanned=%s, upserted=%s, skipped_missing_price=%s",
            scanned,
            upserted,
            skipped_missing_price,
        )
        return TplusOutcomeSummary(
            scanned=scanned,
            upserted=upserted,
            skipped_missing_price=skipped_missing_price,
        )

    def run_postprocess(
        self,
        *,
        backfill_lookback_days: int = 7,
        backfill_limit: int = 2000,
        outcome_lookback_days: int = 14,
        outcome_limit: int = 5000,
    ) -> PMPostprocessSummary:
        """PM 사후처리 통합 실행.

        스케줄러를 쪼개지 않고도 장마감 후 1회 또는 세션 종료 후 1회 호출할 수 있도록
        미체결 사유 보강과 T+N 성과 계산을 하나의 진입점으로 묶는다.
        """
        unfilled = self.backfill_unfilled_reasons(
            lookback_days=backfill_lookback_days,
            limit=backfill_limit,
        )
        outcomes = self.compute_tplus_outcomes(
            lookback_days=outcome_lookback_days,
            limit=outcome_limit,
        )
        return PMPostprocessSummary(unfilled=unfilled, outcomes=outcomes)

    def _infer_unfilled_reason(self, *, error_code: Optional[str], error_message: Optional[str]) -> Optional[Dict[str, str]]:
        code = (error_code or "").strip()
        msg = (error_message or "").strip()
        msg_upper = msg.upper()

        # TODO: broker(KIS/거래소)별 상세 매핑 테이블 확장
        if not code and not msg:
            return None

        if "TIME" in msg_upper and "OUT" in msg_upper:
            return {"reason_code": "UNFILLED_TIMEOUT", "reason_text": msg}
        if "PRICE" in msg_upper and ("LIMIT" in msg_upper or "BAND" in msg_upper):
            return {"reason_code": "UNFILLED_PRICE_CONSTRAINT", "reason_text": msg}
        if "QTY" in msg_upper or "QUANTITY" in msg_upper:
            return {"reason_code": "UNFILLED_QUANTITY", "reason_text": msg}

        # 코드가 있으면 최소한 코드만 안전하게 보존
        if code:
            return {"reason_code": code[:32], "reason_text": msg or code}
        return None

    def _load_tplus_close(self, *, ticker_id: int, entry_date, horizon_days: int) -> Optional[float]:
        row = self.db.execute(text("""
            WITH ranked AS (
                SELECT close,
                       ROW_NUMBER() OVER (ORDER BY trade_date ASC) AS rn
                  FROM trading.ohlcv_daily
                 WHERE ticker_id = :ticker_id
                   AND trade_date > :entry_date
                   AND close IS NOT NULL
            )
            SELECT close FROM ranked WHERE rn = :horizon_days
        """), {
            "ticker_id": ticker_id,
            "entry_date": entry_date,
            "horizon_days": int(horizon_days),
        }).fetchone()
        if not row:
            return None
        return float(row._mapping["close"])

    def _label_from_pnl_bps(self, pnl_bps: float) -> str:
        if pnl_bps > self.FLAT_EPS_BPS:
            return "WIN"
        if pnl_bps < -self.FLAT_EPS_BPS:
            return "LOSS"
        return "FLAT"
