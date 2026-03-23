# app/features/trading_hybrid/repositories/intraday_signal_repository.py
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.features.signals.services.intraday_signal_service import IntradaySignalService
from app.features.signals.models.similarity_models import IntradaySimilaritySearchRequest
from app.features.signals.models.signal_models import AlgorithmVersion
from app.shared.models.ticker import Ticker
import logging, os, numpy as np, traceback

logger = logging.getLogger(__name__)

# ====== [A] SQLAlchemy 에러 훅 ======
_SQLA_HOOK_INSTALLED = False
def _install_sqlalchemy_error_hook(db: Session):
    global _SQLA_HOOK_INSTALLED
    if _SQLA_HOOK_INSTALLED:
        return
    try:
        from sqlalchemy import event
        from sqlalchemy.engine import Engine
        eng = db.get_bind()
        @event.listens_for(Engine, "handle_error")
        def _sa_handle_error(exception_context):
            logger.error(
                "🧨 SQL ERROR CAUGHT (handle_error)\norig=%r\nstmt=\n%s\nparams=%r\nis_disconnect=%s",
                exception_context.original_exception,
                exception_context.statement,
                exception_context.parameters,
                exception_context.is_disconnect,
            )
        _SQLA_HOOK_INSTALLED = True
        logger.debug("✅ SQLAlchemy handle_error hook installed")
    except Exception as e:
        logger.warning(f"⚠️ failed to install sqlalchemy error hook: {e}")

# ====== [B] psycopg2 세션 상태 ======
def _pg_state(db: Session, tag: str = "") -> str:
    try:
        from psycopg2 import extensions
        conn = db.connection().connection
        st = conn.get_transaction_status()
        m = {
            extensions.TRANSACTION_STATUS_IDLE: "IDLE",
            extensions.TRANSACTION_STATUS_ACTIVE: "ACTIVE",
            extensions.TRANSACTION_STATUS_INTRANS: "INTRANS",
            extensions.TRANSACTION_STATUS_INERROR: "INERROR",
            extensions.TRANSACTION_STATUS_UNKNOWN: "UNKNOWN",
        }
        s = m.get(st, str(st))
        logger.debug(f"{tag} psycopg2_state={s}")
        return s
    except Exception as e:
        logger.debug(f"{tag} psycopg2_state=ERROR ({e})")
        return "ERROR"

# ====== [C] 진입 가드 ======
def _entry_session_guard(db: Session, who: str):
    _install_sqlalchemy_error_hook(db)
    state = _pg_state(db, f"[ENTRY:{who}]")
    if os.getenv("FORCE_FORENSIC_ABORT_FAIL", "0") == "1" and state == "INERROR":
        raise RuntimeError(f"[FORCED_FORENSIC] Session already INERROR before {who}. Look earlier in logs.")
    if os.getenv("FORCE_RESCUE_ROLLBACK", "1") == "1" and state == "INERROR":
        logger.warning(f"[RESCUE] Session INERROR before {who} → doing rollback now.")
        try:
            db.rollback()
            _pg_state(db, f"[AFTER-ROLLBACK:{who}]")
        except Exception as e:
            logger.error(f"[RESCUE] rollback failed: {e}")

def _get_current_price_for_intraday(db: Session, ticker: Ticker, market: str) -> float:
    try:
        _entry_session_guard(db, f"price:{ticker.symbol}")
        from app.core.kis_client import KISClient
        kis = KISClient(db)
        if market == "KR":
            output = kis.kr_current_price(ticker.symbol).get("output", {})
            return float(output.get("stck_prpr", 0))
        else:
            output = kis.price_detail(ticker.symbol, ticker.exchange).get("output", {})
            return float(output.get("last", 0))
    except Exception as e:
        logger.warning(f"가격조회 실패 {ticker.symbol}: {e}")
        _pg_state(db, f"[PRICE:EXCEPT:{ticker.symbol}]")
        return 0.0

def predict_5min_window(db: Session, ticker_id: int, lookback: int = 10, window: int = 15) -> Dict[str, Any]:
    _entry_session_guard(db, f"predict:{ticker_id}")
    try:
        logger.debug(f"🔍 5분봉 예측 시작: ticker_id={ticker_id}, lookback={lookback}")
        ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            logger.warning(f"⚠️ 티커 조회 실패: ticker_id={ticker_id}")
            return {"ticker_id": ticker_id, "symbol": f"UNKNOWN_{ticker_id}",
                    "dir": "FLAT", "prob": 0.0, "exp_move_pct": 0.0, "current_price": 0.0}

        market = "KR" if ticker.country == "KR" else "US"

        # === 유사도 기반 시뮬러 호출 ===
        try:
            service = IntradaySignalService(db)
            req = IntradaySimilaritySearchRequest(
                ticker_id=ticker_id,
                reference_datetime=None,
                lookback=lookback,
                top_k=10,  # 더 안정적 가중을 위해 10개 사용
                direction_filter=None,
                version=AlgorithmVersion.V3,
            )
            resp = service.search_intraday_similar_signals(req)
        except Exception as svc_err:
            logger.error(f"🧨 search_intraday_similar_signals 실패: {svc_err}")
            logger.error("📋 svc traceback:\n" + "".join(traceback.format_exception(type(svc_err), svc_err, svc_err.__traceback__)))
            _pg_state(db, f"[SERVICE:EXCEPT:{ticker.symbol}]")
            try:
                db.rollback()
            except Exception as rb:
                logger.error(f"rollback 실패: {rb}")
            raise

        # 유사 시그널 없으면 FLAT
        if not resp.similar_signals:
            logger.debug(f"  ⚠️ [{ticker.symbol}] 유사 시그널 없음 → FLAT")
            current_price = _get_current_price_for_intraday(db, ticker, market)
            return {"ticker_id": ticker_id, "symbol": ticker.symbol,
                    "dir": "FLAT", "prob": 0.0, "exp_move_pct": 0.0,
                    "current_price": current_price, "p_up": 0.0, "p_down": 0.0,
                    "exp_up": 0.0, "exp_down": 0.0, "net_strength": 0.0, "s": 0.0,
                    "atr5m_pct": float(getattr(resp, "atr5m_pct", 0.0) or 0.0)}

        # 가중/변화율 집계
        score_up = score_down = 0.0
        ups, downs = [], []
        for s in resp.similar_signals[:10]:
            if s.change_7_24d is None:
                continue
            mv = float(s.change_7_24d)   # +상승, -하락(절대값은 아래에서 처리)
            sim = float(getattr(s, "similarity", 1.0))
            sim = max(0.0, min(1.0, sim))
            # 유사도가 0~1일 때 로그 스케일 가중
            w = float(np.log1p(sim * 10))
            if getattr(s, "direction", "UP") == "UP" and mv > 0:
                score_up += w * mv
                ups.append((w, mv))
            else:
                score_down += w * abs(mv)
                downs.append((w, abs(mv)))

        # 확률/기대변화율 근사
        eps = 1e-9
        exp_up = float(sum(w*m for w, m in ups) / (sum(w for w, _ in ups) + eps)) if ups else 0.0
        exp_down = float(sum(w*m for w, m in downs) / (sum(w for w, _ in downs) + eps)) if downs else 0.0
        up_lin = float(score_up)
        down_lin = float(score_down)
        p_up = up_lin / (up_lin + down_lin + eps)
        p_down = 1.0 - p_up

        # 방향/강도
        if p_up > p_down:
            mc, prob, avg_mv = "UP", p_up, exp_up
        elif p_down > p_up:
            mc, prob, avg_mv = "DOWN", p_down, exp_down
        else:
            mc, prob, avg_mv = "FLAT", 0.5, 0.0

        # net_strength / s (정규화는 상위 모듈 튜닝 스케일을 사용할 수 있게 여기선 원값도 제공)
        net_strength = (p_up * exp_up) - (p_down * exp_down)  # [+]상승 기대, [-]하락 기대
        current_price = _get_current_price_for_intraday(db, ticker, market)
        atr5m_pct = float(getattr(resp, "atr5m_pct", 0.0) or 0.0)

        _pg_state(db, f"[EXIT:{ticker.symbol}]")
        return {
            "ticker_id": ticker_id,
            "symbol": ticker.symbol,
            "dir": mc,
            "prob": float(prob),
            "exp_move_pct": float(avg_mv),  # 하위 호환
            "current_price": current_price,

            # 신규 필드(통합 사다리/리밸런싱용)
            "p_up": float(p_up),
            "p_down": float(p_down),
            "exp_up": float(exp_up),
            "exp_down": float(exp_down),
            "net_strength": float(net_strength),
            # s는 소비측(tuning.ADAPTIVE_STRENGTH_SCALE)에 의존 → 소비측에서 재계산하기도 함
            "s": 0.0,  # 소비측에서 덮어씀; placeholder
            "atr5m_pct": atr5m_pct,
        }

    except Exception as e:
        logger.error(f"❌ 예측 중 예외: ticker_id={ticker_id}, error={e}")
        logger.error("📋 traceback:\n" + "".join(traceback.format_exception(type(e), e, e.__traceback__)))
        _pg_state(db, f"[EXCEPT:{ticker_id}]")
        try:
            db.rollback()
            logger.debug("🔁 rollback 완료")
        except Exception as rb:
            logger.error(f"rollback 실패: {rb}")
        return {
            "ticker_id": ticker_id,
            "symbol": f"ERR_{ticker_id}",
            "dir": "FLAT",
            "prob": 0.0,
            "exp_move_pct": 0.0,
            "current_price": 0.0,
            "p_up": 0.0, "p_down": 0.0, "exp_up": 0.0, "exp_down": 0.0,
            "net_strength": 0.0, "s": 0.0, "atr5m_pct": 0.0,
        }
