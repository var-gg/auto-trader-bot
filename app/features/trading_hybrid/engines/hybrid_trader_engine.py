from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone
import logging

# === Service Imports ===
from app.features.trading_hybrid.services.intraday_session_service import (
    plan_intraday_actions, apply_rebalancing_rules
)
from app.features.trading_hybrid.services.risk_controller import (
    enforce_intraday_stops, near_close_cleanup, block_daily_loss_symbols,
    close_negative_signal_positions, cancel_negative_signal_pending_orders
)
from app.features.trading_hybrid.services.executor_service import persist_batch_and_execute

# === Repository Imports ===
from app.features.trading_hybrid.repositories.portfolio_repository import (
    load_latest_account_snapshot, load_latest_positions, load_pending_orders
)
from app.features.trading_hybrid.repositories.intraday_signal_repository import (
    predict_5min_window
)
from app.features.trading_hybrid.repositories.order_repository import (
    compute_bucket_caps, log_cycle_note, get_blocked_symbols_today
)

# === Utils & Policy ===
from app.features.trading_hybrid.utils.timebars import (
    is_near_close, market_now_kst, is_within_first_hour, 
    is_before_last_hour, is_kr_after_hours_regular,
    is_kr_after_hours_single, is_us_after_market
)
from app.features.trading_hybrid.policy.tuning import Tuning


# -----------------------------------------------------------------------------
# 엔진 설정 구조체
# -----------------------------------------------------------------------------
@dataclass
class EngineConfig:
    market: str            # "KR" | "US"
    currency: str          # "KRW" | "USD"
    test_mode: bool = False  # True일 경우 KIS API 호출 skip
    active_set_max: int = 5
    intraday_bucket_ratio: float = 0.25  # of BP
    swing_bucket_ratio: float = 0.70     # of BP
    cash_buffer_ratio: float = 0.12      # >=10% 유지
    # 장초 탐욕 레그용
    open_leg_count_range: Tuple[int, int] = (2, 4)
    open_leg_min_gap_pct: float = 0.003


# -----------------------------------------------------------------------------
# 하이브리드 트레이더 엔진
# -----------------------------------------------------------------------------
class HybridTraderEngine:
    """
    스윙 후보군을 기반으로 장초 '탐욕 레그' + 장중 5/10분 루프(단타·리밸런싱·손절·익절)를 수행.
    - 상태 연속성: order_batch.notes(JSON)에 캡/액티브셋/사이클 메타를 기록(Phase-1)
    """
    def __init__(self, db_sess, cfg: EngineConfig, tuning: Tuning | None = None):
        self.db = db_sess
        self.cfg = cfg
        self.tuning = tuning or Tuning.default_for_market(cfg.market)

    # -------------------------------------------------------------------------
    # 장초 탐욕 매수
    # -------------------------------------------------------------------------
    def run_open_greedy(self) -> Dict[str, Any]:
        logger = logging.getLogger(__name__)
        logger.info(f"🚀 OPEN_GREEDY 시작: market={self.cfg.market}, currency={self.cfg.currency}, test_mode={self.cfg.test_mode}")

        now = market_now_kst(self.cfg.market)
        acct = load_latest_account_snapshot(self.db, self.cfg.market, self.cfg.currency)
        positions = load_latest_positions(self.db, acct["snapshot_id"])
        pending = load_pending_orders(self.db, self.cfg.market)

        # 버킷 계산
        caps = compute_bucket_caps(
            buying_power=acct["buying_power_ccy"],
            total_equity=acct["total_equity_ccy"],
            swing_ratio=self.cfg.swing_bucket_ratio,
            intraday_ratio=self.cfg.intraday_bucket_ratio,
            cash_buffer_ratio=self.tuning.CASH_BUFFER_RATIO
        )

        # =======================================================================
        # PM 신호 기반 장초 로직
        # =======================================================================
        from app.features.premarket.services.pm_open_session_service import (
            plan_pm_open_buy_orders, plan_pm_take_profit_orders
        )
        
        logger.info("🎯 PM 신호 기반 장초 로직 사용")
        
        # 국가 매핑
        country = "KR" if self.cfg.market == "KR" else "US"
        
        # PM 매수 플랜
        buy_plans, sell_plans, skipped = plan_pm_open_buy_orders(
            db=self.db,
            now_kst=now,
            market=self.cfg.market,
            currency=self.cfg.currency,
            account=acct,
            positions=positions,
            caps=caps,
            country=country,
            min_signal=0.5,  # signal_1d ≥ 0.5인 종목만
            limit=self.cfg.active_set_max or 10
        )
        logger.info(f"📊 PM 장초 매수: BUY={len(buy_plans)}, SKIP={len(skipped)}")
        
        # PM 익절 플랜
        tp_sells, skip_tp = plan_pm_take_profit_orders(
            db=self.db,
            now_kst=now,
            market=self.cfg.market,
            currency=self.cfg.currency,
            positions=positions,
            pending=pending,
            country=country
        )
        logger.info(f"🎯 PM 익절 사다리: SELL={len(tp_sells)}, SKIP={len(skip_tp)}")
        
        sell_plans += tp_sells
        skipped += skip_tp
        
        batch_meta = {
            "phase": "OPEN_GREEDY_PM",
            "market": self.cfg.market,
            "caps": caps,
            "pm_mode": True
        }
        
        # =======================================================================
        # 공통: 배치 저장 및 실행
        # =======================================================================

        result = persist_batch_and_execute(
            self.db, now, self.cfg.currency,
            buy_plans, sell_plans, skipped,
            batch_meta=batch_meta,
            test_mode=self.cfg.test_mode
        )

        log_cycle_note(self.db, now, self.cfg.market, "open_greedy_done")
        logger.info("✅ OPEN_GREEDY 완료")
        return result

    # -------------------------------------------------------------------------
    # 장중 사이클
    # -------------------------------------------------------------------------
    def run_intraday_cycle(self) -> Dict[str, Any]:
        logger = logging.getLogger(__name__)
        logger.info(f"🚀 INTRADAY_CYCLE 시작: market={self.cfg.market}, currency={self.cfg.currency}, test_mode={self.cfg.test_mode}")

        now = market_now_kst(self.cfg.market)
        country = "KR" if self.cfg.market == "KR" else "US"
        
        acct = load_latest_account_snapshot(self.db, self.cfg.market, self.cfg.currency)
        positions = load_latest_positions(self.db, acct["snapshot_id"])
        pending = load_pending_orders(self.db, self.cfg.market)
        
        # 버킷 계산 (시간외 거래에서도 사용)
        caps = compute_bucket_caps(
            buying_power=acct["buying_power_ccy"],
            total_equity=acct["total_equity_ccy"],
            swing_ratio=self.cfg.swing_bucket_ratio,
            intraday_ratio=self.cfg.intraday_bucket_ratio,
            cash_buffer_ratio=self.tuning.CASH_BUFFER_RATIO
        )
        
        # =======================================================================
        # 🌙 시간외 거래 체크 (가장 먼저) - 정규장 로직 스킵
        # =======================================================================
        from app.features.trading_hybrid.services.intraday_session_service import (
            plan_kr_after_hours_orders, plan_us_after_market_orders
        )
        
        # 🧹 시간외 거래 전 펜딩 오더 정리 (역추세/SHORT 추천 취소)
        pending_cleanup_result = {"cancelled_count": 0, "cancelled_orders": []}
        
        if is_kr_after_hours_regular(now) or is_kr_after_hours_single(now) or is_us_after_market(now):
            try:
                logger.info("🧹 시간외 거래 전: 역추세 펜딩 오더 정리")
                pending_cleanup_result = cancel_negative_signal_pending_orders(self.db, self.cfg.market)
                logger.info(f"✅ 펜딩 정리: {pending_cleanup_result['cancelled_count']}건 취소")
            except Exception as e:
                logger.error(f"❌ 펜딩 정리 실패: {e}", exc_info=True)
        
        # 국장 15:30~16:00 (06 장후 시간외)
        if self.cfg.market == "KR" and is_kr_after_hours_regular(now):
            logger.info("🌙 국장 06 장후 시간외 (15:30~16:00)")
            try:
                buy_plans, skipped, revised_count = plan_kr_after_hours_orders(
                    db=self.db,
                    now_kst=now,
                    market=self.cfg.market,
                    currency=self.cfg.currency,
                    positions=positions,
                    country=country,
                    swing_cap_cash=caps.get("swing_cap_cash", 0.0),
                    order_type="06",
                    pending=None  # 함수 내부에서 조회
                )
                
                if buy_plans:
                    batch_meta = {"phase": "KR_AFTER_HOURS_06", "market": self.cfg.market, "pending_cleanup": pending_cleanup_result, "revised_count": revised_count}
                    result = persist_batch_and_execute(
                        self.db, now, self.cfg.currency, buy_plans, [], skipped,
                        batch_meta=batch_meta, test_mode=self.cfg.test_mode
                    )
                    result["pending_cleanup"] = pending_cleanup_result
                    result["revised_count"] = revised_count
                    logger.info(f"✅ 국장 06 장후 시간외 완료: 신규={len(buy_plans)}건, 정정={revised_count}건")
                    return result
                else:
                    logger.info(f"⚠️ 국장 06 장후 시간외: 신규=0건, 정정={revised_count}건")
                    return {"buy_plans": [], "sell_plans": [], "skipped": skipped, "pending_cleanup": pending_cleanup_result, "revised_count": revised_count, "summary": {"buy_count": 0, "sell_count": 0, "skip_count": len(skipped)}}
            except Exception as e:
                logger.error(f"❌ 국장 06 장후 시간외 실패: {e}", exc_info=True)
                return {"error": str(e)}
        
        # 국장 16:00~18:00 (07 시간외 단일가)
        if self.cfg.market == "KR" and is_kr_after_hours_single(now):
            logger.info("🌙 국장 07 시간외 단일가 (16:00~18:00)")
            try:
                buy_plans, skipped, revised_count = plan_kr_after_hours_orders(
                    db=self.db,
                    now_kst=now,
                    market=self.cfg.market,
                    currency=self.cfg.currency,
                    positions=positions,
                    country=country,
                    swing_cap_cash=caps.get("swing_cap_cash", 0.0),
                    order_type="07",
                    pending=None  # 함수 내부에서 조회
                )
                
                if buy_plans:
                    batch_meta = {"phase": "KR_AFTER_HOURS_07", "market": self.cfg.market, "pending_cleanup": pending_cleanup_result, "revised_count": revised_count}
                    result = persist_batch_and_execute(
                        self.db, now, self.cfg.currency, buy_plans, [], skipped,
                        batch_meta=batch_meta, test_mode=self.cfg.test_mode
                    )
                    result["pending_cleanup"] = pending_cleanup_result
                    result["revised_count"] = revised_count
                    logger.info(f"✅ 국장 07 시간외 단일가 완료: 신규={len(buy_plans)}건, 정정={revised_count}건")
                    return result
                else:
                    logger.info(f"⚠️ 국장 07 시간외 단일가: 신규=0건, 정정={revised_count}건")
                    return {"buy_plans": [], "sell_plans": [], "skipped": skipped, "pending_cleanup": pending_cleanup_result, "revised_count": revised_count, "summary": {"buy_count": 0, "sell_count": 0, "skip_count": len(skipped)}}
            except Exception as e:
                logger.error(f"❌ 국장 07 시간외 단일가 실패: {e}", exc_info=True)
                return {"error": str(e)}
        
        # 미장 애프터마켓 (16:00~20:00 ET)
        if self.cfg.market == "US" and is_us_after_market(now):
            logger.info("🌙 미장 애프터마켓 (16:00~20:00 ET)")
            try:
                buy_plans, skipped = plan_us_after_market_orders(
                    db=self.db,
                    now_kst=now,
                    market=self.cfg.market,
                    currency=self.cfg.currency,
                    positions=positions,
                    country=country,
                    swing_cap_cash=caps.get("swing_cap_cash", 0.0)
                )
                
                if buy_plans:
                    batch_meta = {"phase": "US_AFTER_MARKET", "market": self.cfg.market, "pending_cleanup": pending_cleanup_result}
                    result = persist_batch_and_execute(
                        self.db, now, self.cfg.currency, buy_plans, [], skipped,
                        batch_meta=batch_meta, test_mode=self.cfg.test_mode
                    )
                    result["pending_cleanup"] = pending_cleanup_result
                    logger.info(f"✅ 미장 애프터마켓 완료: {len(buy_plans)}건")
                    return result
                else:
                    logger.info("⚠️ 미장 애프터마켓 대상 없음")
                    return {"buy_plans": [], "sell_plans": [], "skipped": skipped, "pending_cleanup": pending_cleanup_result, "summary": {"buy_count": 0, "sell_count": 0, "skip_count": len(skipped)}}
            except Exception as e:
                logger.error(f"❌ 미장 애프터마켓 실패: {e}", exc_info=True)
                return {"error": str(e)}
        
        # =======================================================================
        # 정규장 로직 (시간외가 아닐 때만 실행)
        # =======================================================================
        logger.info("📊 정규장 모드 - 단타/리밸런싱 시작")

        # =======================================================================
        # 🔥 1단계: 장마감 직전 역추세(signal_1d < 0) 포지션 정리
        # =======================================================================
        # 5분봉 예측/리밸런싱 전에 먼저 처리하여 음수 포지션 제외
        negative_signal_closed = []
        try:
            negative_signal_closed = close_negative_signal_positions(self.db, now, self.cfg.market, positions, test_mode=self.cfg.test_mode)
        except Exception as e:
            logger.error(f"❌ 음수 신호 포지션 정리 실패: {e}", exc_info=True)
        
        # 음수 청산 후 나머지 포지션만 리밸런싱 대상으로
        positions_for_rebalancing = [
            p for p in positions 
            if p.get("signal_1d") is None or float(p.get("signal_1d", 0)) >= 0
        ]
        logger.info(f"📊 리밸런싱 대상 포지션: {len(positions_for_rebalancing)}개 (음수 제외)")

        # =======================================================================
        # ✅ PM 신호 기반 하이브리드 장중 로직
        # =======================================================================
        from app.features.premarket.services.pm_intraday_session_service import (
            get_pm_intraday_active_set
        )
        
        logger.info("🎯 PM 하이브리드 장중: PM(액티브셋) + 5분봉(가격+리밸런싱)")
        
        # 손실 차단 갱신
        try:
            blocked_today = block_daily_loss_symbols(self.db, self.cfg.market, limit_pct=self.tuning.DAILY_LOSS_BLOCK_PCT)
        except Exception as e:
            logger.error(f"❌ 차단 갱신 실패: {e}", exc_info=True)
            blocked_today = set()
        
        # 2) PM으로 액티브 셋 선정 (오늘 매수 대상 풀만)
        pm_candidates = get_pm_intraday_active_set(
            db=self.db,
            country=country,
            min_signal=0.3,
            limit=10
        )
        
        # PM 후보를 기존 active 형식으로 변환
        active = []
        for cand in pm_candidates:
            active.append({
                "ticker_id": cand.get("ticker_id"),
                "symbol": cand.get("symbol"),
                "current_price": cand.get("current_price"),
                "atr_pct": cand.get("atr_pct", 0.05),
                "pm_signal": cand.get("signal_1d"),
                "pm_strength": cand.get("signal_strength")
            })
        
        logger.info(f"🎯 PM 액티브 셋: {len(active)}개 (SHORT 제외, signal > 0.3)")
        
        # 3) 5분봉 예측 (PM 후보 + 리밸런싱 대상 보유 종목만)
        preds: Dict[str, Dict[str, Any]] = {}
        
        for item in active:
            sym = item["symbol"]
            tid = item["ticker_id"]
            try:
                pred = predict_5min_window(self.db, tid, lookback=10, window=15)
                preds[sym] = pred
            except Exception as e:
                logger.error(f"❌ [{sym}] 5분봉 예측 실패: {e}", exc_info=True)
                preds[sym] = {"ticker_id": tid, "symbol": sym, "dir": "FLAT", "prob": 0.0, "exp_move_pct": 0.0, "current_price": 0.0}
        
        # 보유 종목도 5분봉 예측 추가 (음수 제외된 리밸런싱 대상만)
        for p in positions_for_rebalancing:
            sym = p.get("symbol")
            tid = p.get("ticker_id")
            if sym and tid and sym not in preds:
                try:
                    pred = predict_5min_window(self.db, tid, lookback=10, window=15)
                    preds[sym] = pred
                except Exception as e:
                    logger.debug(f"[{sym}] 5분봉 예측 실패 (보유): {e}")
                    preds[sym] = {"ticker_id": tid, "symbol": sym, "dir": "FLAT", "prob": 0.0, "exp_move_pct": 0.0, "current_price": 0.0}
        
        logger.info(f"📊 5분봉 예측: {len(preds)}개 (PM 후보 + 보유, 음수제외)")
        
        # 4) 5분봉 기반 리밸런싱 (음수 청산 완료 후 나머지만)
        pos_map = {p["symbol"]: p for p in positions_for_rebalancing}
        ratchet_summary = {"buy_ratcheted": [], "sell_ratcheted": [], "cancelled": []}
        for sym, pred in preds.items():
            try:
                # 🆕 보유 포지션 수량 전달
                position_qty = float(pos_map.get(sym, {}).get("qty", 0) or 0)
                ratchet_result = apply_rebalancing_rules(
                    self.db, self.cfg.market, sym, pred, self.tuning, 
                    test_mode=self.cfg.test_mode,
                    position_qty=position_qty
                )
                if ratchet_result:
                    ratchet_summary["buy_ratcheted"].extend(ratchet_result.get("buy_ratcheted", []))
                    ratchet_summary["sell_ratcheted"].extend(ratchet_result.get("sell_ratcheted", []))
                    ratchet_summary["cancelled"].extend(ratchet_result.get("cancelled", []))
            except Exception as e:
                logger.error(f"❌ 리밸런싱 실패 [{sym}]: {e}", exc_info=True)
        
        logger.info(f"🔄 리밸런싱 완료: BUY래칫={len(ratchet_summary['buy_ratcheted'])}, "
                   f"SELL래칫={len(ratchet_summary['sell_ratcheted'])}, "
                   f"취소={len(ratchet_summary['cancelled'])}")
        
        # 5) 5분봉 기반 장중 플랜 생성 (단타는 개장 후 1시간 + 폐장 전 1시간만)
        buy_plans, sell_plans, skipped = [], [], []
        
        # ✅ 단타 시간대 체크 (개장 후 1시간 OR 폐장 전 1시간)
        is_first_hour = is_within_first_hour(self.cfg.market, now)
        is_last_hour = is_before_last_hour(self.cfg.market, now)
        is_intraday_trading_time = is_first_hour or is_last_hour
        
        if is_intraday_trading_time:
            logger.info(f"⏰ 단타 시간대: 개장후1시간={is_first_hour}, 폐장전1시간={is_last_hour}")
            try:
                buy_plans, sell_plans, skipped = plan_intraday_actions(
                    now_kst=now,
                    market=self.cfg.market,
                    currency=self.cfg.currency,
                    preds=preds,
                    account=acct,
                    positions=positions_for_rebalancing,
                    pending=pending,
                    caps=caps,
                    tuning=self.tuning,
                    blocked_symbols=blocked_today
                )
                logger.info(f"📊 5분봉 Intraday: BUY={len(buy_plans)}, SELL={len(sell_plans)}, SKIP={len(skipped)}")
            except Exception as e:
                logger.error(f"❌ 장중 플랜 생성 실패: {e}", exc_info=True)
        else:
            logger.info("⏰ 단타 시간대 아님 (개장 1시간 후 ~ 폐장 1시간 전) → 단타 스킵, 리밸런싱만 수행")
        
        batch_meta = {
            "phase": "INTRADAY_PM_HYBRID",
            "market": self.cfg.market,
            "caps": caps,
            "pm_mode": "hybrid",
            "active_set_size": len(active),
            "pred_count": len(preds),
            "ratchet_summary": ratchet_summary,
            "negative_signal_closed": negative_signal_closed  # 🆕 음수 청산 내역
        }
        # =======================================================================
        # 공통: 배치 저장 및 실행
        # =======================================================================
        
        result = persist_batch_and_execute(
            self.db, now, self.cfg.currency,
            buy_plans, sell_plans, skipped,
            batch_meta=batch_meta,
            test_mode=self.cfg.test_mode
        )

        # =======================================================================
        # 6단계: 리스크컷 + 마감 청소
        # =======================================================================
        try:
            enforce_intraday_stops(self.db, self.cfg.market, positions, self.tuning, test_mode=self.cfg.test_mode)
        except Exception as e:
            logger.error(f"❌ 손절 집행 실패: {e}", exc_info=True)

        if is_near_close(self.cfg.market, minutes=self.tuning.NEAR_CLOSE_MIN, now_kst=now):
            try:
                near_close_cleanup(self.db, now, self.cfg.market, cleanup_enabled=self.tuning.NEAR_CLOSE_CLEANUP_ENABLED, test_mode=self.cfg.test_mode)
            except Exception as e:
                logger.error(f"❌ 마감 청소 실패: {e}", exc_info=True)

        log_cycle_note(self.db, now, self.cfg.market, "intraday_cycle_done")
        logger.info("✅ INTRADAY_CYCLE 완료")
        return result
