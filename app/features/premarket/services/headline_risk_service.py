from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import (
    NEWS_BULL_MAX_MULTIPLIER,
    NEWS_RISK_DEFAULT_TTL_MIN,
    NEWS_RISK_GLOBAL_BLEND_WEIGHT,
    NEWS_RISK_MAX_MULTIPLIER,
    NEWS_RISK_MIN_HEADLINES,
    NEWS_RISK_MODEL,
)
from app.core.gpt_client import responses_json

logger = logging.getLogger(__name__)


class HeadlineRiskService:
    """Headline 기반 시장 체제(regime) 스냅샷 생성/조회 서비스"""

    VALID_SCOPE = {"KR", "US", "GLOBAL"}
    VALID_SHOCK = {"war", "policy", "rates", "commodity", "credit", "liquidity", "earnings", "other"}
    DEFAULT_REFRESH_WINDOW_MINUTES = 720
    STALE_REFRESH_GRACE_MINUTES = 30

    def __init__(self, db: Session):
        self.db = db

    def refresh_snapshot(self, scope: str, window_minutes: int = 720) -> Dict[str, Any]:
        scope = (scope or "GLOBAL").upper()
        if scope not in self.VALID_SCOPE:
            raise ValueError(f"invalid scope: {scope}")

        primary = self._fetch_headlines(scope=scope, window_minutes=window_minutes, limit=40)
        used = list(primary)
        blend_applied = False

        if scope != "GLOBAL" and len(primary) < NEWS_RISK_MIN_HEADLINES:
            global_h = self._fetch_headlines(scope="GLOBAL", window_minutes=window_minutes, limit=30)
            take_n = int(max(0, min(len(global_h), round(len(global_h) * NEWS_RISK_GLOBAL_BLEND_WEIGHT))))
            if take_n > 0:
                used.extend(global_h[:take_n])
                blend_applied = True

        scored = self._score_headlines(scope=scope, headlines=used)
        normalized = self._normalize(scored)

        now_utc = datetime.now(timezone.utc)
        ttl_min = int(normalized.get("ttl_minutes") or NEWS_RISK_DEFAULT_TTL_MIN)
        expires_at = now_utc + timedelta(minutes=ttl_min)

        snapshot_id = self._insert_snapshot(
            scope=scope,
            as_of_at=now_utc,
            window_minutes=window_minutes,
            risk_score=normalized["risk_score"],
            confidence=normalized["confidence"],
            shock_type=normalized["shock_type"],
            severity_band=normalized["severity_band"],
            discount_multiplier=normalized["discount_multiplier"],
            sell_markup_multiplier=normalized["sell_markup_multiplier"],
            regime_score=normalized["regime_score"],
            ttl_minutes=ttl_min,
            expires_at=expires_at,
            source_provider="openai",
            model_name=NEWS_RISK_MODEL,
            raw_response=normalized.get("raw_response"),
            reason_short=normalized.get("reason_short"),
        )

        return {
            "snapshot_id": snapshot_id,
            "scope": scope,
            **normalized,
            "expires_at": expires_at.isoformat(),
            "headline_count_primary": len(primary),
            "headline_count_used": len(used),
            "blend_applied": blend_applied,
        }

    def get_latest_active_snapshot(self, scope: str) -> Optional[Dict[str, Any]]:
        scope = (scope or "GLOBAL").upper()
        row = self.db.execute(text("""
            SELECT id, market_scope, as_of_at, risk_score, confidence, shock_type,
                   severity_band, discount_multiplier, sell_markup_multiplier, regime_score,
                   ttl_minutes, expires_at, source_provider, model_name, reason_short
            FROM trading.market_headline_risk_snapshot
            WHERE market_scope = :scope
              AND expires_at > NOW()
            ORDER BY as_of_at DESC
            LIMIT 1
        """), {"scope": scope}).mappings().first()
        return dict(row) if row else None

    def get_latest_snapshot(self, scope: str) -> Optional[Dict[str, Any]]:
        scope = (scope or "GLOBAL").upper()
        row = self.db.execute(text("""
            SELECT id, market_scope, as_of_at, risk_score, confidence, shock_type,
                   severity_band, discount_multiplier, sell_markup_multiplier, regime_score,
                   ttl_minutes, expires_at, source_provider, model_name, reason_short
            FROM trading.market_headline_risk_snapshot
            WHERE market_scope = :scope
            ORDER BY as_of_at DESC
            LIMIT 1
        """), {"scope": scope}).mappings().first()
        return dict(row) if row else None

    def ensure_active_snapshot(self, scope: str, *, max_staleness_minutes: int | None = None) -> Optional[Dict[str, Any]]:
        scope = (scope or "GLOBAL").upper()
        snap = self.get_latest_active_snapshot(scope)
        if snap:
            return snap

        latest = self.get_latest_snapshot(scope)
        if latest:
            try:
                expires_at = latest.get("expires_at")
                if expires_at is not None:
                    now_utc = datetime.now(timezone.utc)
                    age_min = max(0.0, (now_utc - expires_at).total_seconds() / 60.0)
                    grace = float(max_staleness_minutes or self.STALE_REFRESH_GRACE_MINUTES)
                    if age_min <= grace:
                        latest = dict(latest)
                        latest["reason"] = "recently_expired_snapshot"
                        return latest
            except Exception:
                pass

        try:
            refreshed = self.refresh_snapshot(scope=scope, window_minutes=self.DEFAULT_REFRESH_WINDOW_MINUTES)
            snap = self.get_latest_active_snapshot(scope)
            if snap:
                snap = dict(snap)
                snap["reason"] = "auto_refreshed_snapshot"
                snap["refresh_snapshot_id"] = refreshed.get("snapshot_id")
                return snap
        except Exception as e:
            logger.warning("headline snapshot auto-refresh failed for %s: %s", scope, e)

        if latest:
            latest = dict(latest)
            latest["reason"] = "stale_snapshot_fallback"
            return latest
        return None

    def get_discount_multiplier(self, scope: str) -> tuple[float, Optional[int], Dict[str, Any]]:
        snap = self.ensure_active_snapshot(scope)
        if not snap:
            return 1.0, None, {"reason": "no_snapshot_available"}
        mult = float(snap.get("discount_multiplier") or 1.0)
        mult = max(1.0, min(float(NEWS_RISK_MAX_MULTIPLIER), mult))
        return mult, int(snap["id"]), snap

    def get_sell_markup_multiplier(self, scope: str) -> tuple[float, Optional[int], Dict[str, Any]]:
        snap = self.ensure_active_snapshot(scope)
        if not snap:
            return 1.0, None, {"reason": "no_snapshot_available"}
        mult = float(snap.get("sell_markup_multiplier") or 1.0)
        mult = max(1.0, min(float(NEWS_BULL_MAX_MULTIPLIER), mult))
        return mult, int(snap["id"]), snap

    def _fetch_headlines(self, scope: str, window_minutes: int, limit: int) -> List[Dict[str, Any]]:
        source_type = None
        if scope == "US":
            source_type = "overseas"
        elif scope == "KR":
            source_type = "domestic"

        params: Dict[str, Any] = {
            "window_minutes": int(window_minutes),
            "limit": int(limit),
        }
        if source_type:
            params["source_type"] = source_type
            sql = """
                SELECT title, COALESCE(raw_json->>'content', '') AS content, published_at
                FROM trading.kis_news
                WHERE published_at >= NOW() - (:window_minutes || ' minutes')::interval
                  AND source_type = :source_type
                ORDER BY published_at DESC
                LIMIT :limit
            """
        else:
            sql = """
                SELECT title, COALESCE(raw_json->>'content', '') AS content, published_at
                FROM trading.kis_news
                WHERE published_at >= NOW() - (:window_minutes || ' minutes')::interval
                ORDER BY published_at DESC
                LIMIT :limit
            """

        rows = self.db.execute(text(sql), params).mappings().all()
        return [
            {
                "title": r.get("title") or "",
                "content": (r.get("content") or "")[:320],
                "published_at": r.get("published_at").isoformat() if r.get("published_at") else None,
            }
            for r in rows
            if r.get("title")
        ]

    def _score_headlines(self, scope: str, headlines: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not headlines:
            return self._fallback("no_headlines")

        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "regime_score": {"type": "integer", "minimum": -100, "maximum": 100},
                "risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "shock_type": {"type": "string"},
                "severity_band": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "EXTREME"]},
                "ttl_minutes": {"type": "integer", "minimum": 30, "maximum": 480},
                "reason_short": {"type": "string"},
            },
            "required": [
                "regime_score", "risk_score", "confidence", "shock_type", "severity_band", "ttl_minutes", "reason_short"
            ],
        }

        prompt = (
            f"You are market regime scorer. Scope={scope}. "
            "Return JSON only. regime_score is -100(risk-off) to +100(risk-on). "
            "risk_score is downside tail risk 0..100. "
            "Risk-off: war escalation, sanctions, oil shock, credit stress, severe tightening. "
            "Risk-on: peace deal, broad disinflation + supportive policy pivot, strong synchronized growth.\n\n"
            f"Headlines:\n{headlines[:30]}"
        )

        try:
            obj = responses_json(
                model=NEWS_RISK_MODEL,
                schema_name="headline_market_regime",
                schema=schema,
                user_text=prompt,
                temperature=0.0,
                task="premarket_market_regime",
                request_delay=0.0,
            )
            obj["raw_response"] = obj.copy()
            return obj
        except Exception as e:
            logger.warning("headline regime llm scoring failed: %s", e)
            return self._fallback("llm_failed")

    def _normalize(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        regime_score = int(max(-100, min(100, int(obj.get("regime_score", 0)))))
        risk_score = int(max(0, min(100, int(obj.get("risk_score", 0)))))
        confidence = float(max(0.0, min(1.0, float(obj.get("confidence", 0.0)))))
        shock_type = str(obj.get("shock_type", "other")).lower().strip()
        if shock_type not in self.VALID_SHOCK:
            shock_type = "other"

        severity = str(obj.get("severity_band", "LOW")).upper()
        if severity not in {"LOW", "MEDIUM", "HIGH", "EXTREME"}:
            severity = "LOW"

        buy_mult = self._buy_multiplier(regime_score=regime_score, risk_score=risk_score, confidence=confidence)
        sell_mult = self._sell_multiplier(regime_score=regime_score, confidence=confidence)

        ttl = int(obj.get("ttl_minutes") or NEWS_RISK_DEFAULT_TTL_MIN)
        ttl = max(30, min(480, ttl))

        return {
            "regime_score": regime_score,
            "risk_score": risk_score,
            "confidence": confidence,
            "shock_type": shock_type,
            "severity_band": severity,
            "discount_multiplier": buy_mult,
            "sell_markup_multiplier": sell_mult,
            "ttl_minutes": ttl,
            "reason_short": str(obj.get("reason_short") or "market-regime-mvp"),
            "raw_response": obj.get("raw_response") or obj,
        }

    def _buy_multiplier(self, regime_score: int, risk_score: int, confidence: float) -> float:
        """Smooth downside-risk mapping.

        Goal:
        - avoid wide 1.0 dead-zones for mildly negative / soso headline regimes
        - still keep multiplier close to 1.0 when both regime and risk are benign
        - ramp continuously as either downside tail risk or risk-off regime worsens
        """
        risk_component = max(0.0, min(1.0, risk_score / 100.0))
        regime_component = max(0.0, min(1.0, (-regime_score) / 100.0))

        # Risk score drives most of the move; regime score adds directional context.
        raw = 0.7 * risk_component + 0.3 * regime_component
        # Convexity: preserve low-end sensitivity while still widening under true shock.
        shaped = 0.55 * raw + 0.45 * (raw ** 1.6)
        conf_adj = 0.55 + 0.45 * confidence

        max_extra = max(0.0, float(NEWS_RISK_MAX_MULTIPLIER) - 1.0)
        m = 1.0 + max_extra * shaped * conf_adj
        return round(max(1.0, min(float(NEWS_RISK_MAX_MULTIPLIER), m)), 4)

    def _sell_multiplier(self, regime_score: int, confidence: float) -> float:
        if regime_score <= 20:
            return 1.0
        if regime_score <= 45:
            base = 1.12
        elif regime_score <= 65:
            base = 1.28
        elif regime_score <= 80:
            base = 1.45
        else:
            base = 1.6

        conf_adj = 0.7 + 0.3 * confidence
        m = 1.0 + (base - 1.0) * conf_adj
        return round(max(1.0, min(float(NEWS_BULL_MAX_MULTIPLIER), m)), 4)

    def _fallback(self, reason: str) -> Dict[str, Any]:
        return {
            "regime_score": 0,
            "risk_score": 0,
            "confidence": 0.0,
            "shock_type": "other",
            "severity_band": "LOW",
            "ttl_minutes": NEWS_RISK_DEFAULT_TTL_MIN,
            "reason_short": f"fallback:{reason}",
            "discount_multiplier": 1.0,
            "sell_markup_multiplier": 1.0,
            "raw_response": {"fallback": reason},
        }

    def _insert_snapshot(
        self,
        scope: str,
        as_of_at: datetime,
        window_minutes: int,
        risk_score: int,
        confidence: float,
        shock_type: str,
        severity_band: str,
        discount_multiplier: float,
        sell_markup_multiplier: float,
        regime_score: int,
        ttl_minutes: int,
        expires_at: datetime,
        source_provider: str,
        model_name: str,
        raw_response: Dict[str, Any],
        reason_short: str,
    ) -> int:
        row = self.db.execute(text("""
            INSERT INTO trading.market_headline_risk_snapshot
                (market_scope, as_of_at, window_minutes, risk_score, confidence,
                 shock_type, severity_band, discount_multiplier, sell_markup_multiplier, regime_score,
                 ttl_minutes, expires_at, source_provider, model_name, raw_response, reason_short)
            VALUES
                (:market_scope, :as_of_at, :window_minutes, :risk_score, :confidence,
                 :shock_type, :severity_band, :discount_multiplier, :sell_markup_multiplier, :regime_score,
                 :ttl_minutes, :expires_at, :source_provider, :model_name, CAST(:raw_response AS JSONB), :reason_short)
            RETURNING id
        """), {
            "market_scope": scope,
            "as_of_at": as_of_at,
            "window_minutes": window_minutes,
            "risk_score": risk_score,
            "confidence": confidence,
            "shock_type": shock_type,
            "severity_band": severity_band,
            "discount_multiplier": discount_multiplier,
            "sell_markup_multiplier": sell_markup_multiplier,
            "regime_score": regime_score,
            "ttl_minutes": ttl_minutes,
            "expires_at": expires_at,
            "source_provider": source_provider,
            "model_name": model_name,
            "raw_response": json.dumps(raw_response, default=str),
            "reason_short": reason_short,
        }).fetchone()
        self.db.commit()
        return int(row[0])
