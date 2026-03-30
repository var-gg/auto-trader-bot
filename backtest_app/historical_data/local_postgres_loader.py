from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Iterable, List

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.research.pipeline import generate_similarity_candidates, generate_similarity_candidates_rolling
from shared.domain.models import MarketCode, MarketSnapshot, OutcomeLabel, Side, SignalCandidate

from .features import CTX_SERIES, compute_bar_features
from .models import HistoricalBar, HistoricalSlice

WARMUP_DAYS = 120
MACRO_SERIES_NAME_TO_CANONICAL = {
    "vix": "vix",
    "rate": "rate",
    "dollar": "dollar",
    "oil": "oil",
    "breadth": "breadth",
    "CBOE Volatility Index: VIX": "vix",
    "Federal Funds Effective Rate": "rate",
    "Nominal Broad U.S. Dollar Index": "dollar",
    "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma": "oil",
}
CANONICAL_MACRO_SOURCE_NAMES = sorted(MACRO_SERIES_NAME_TO_CANONICAL.keys())


def _canonical_macro_series_name(raw_name: str | None) -> str | None:
    if raw_name is None:
        return None
    return MACRO_SERIES_NAME_TO_CANONICAL.get(str(raw_name).strip())


def _canonicalize_macro_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for row in rows:
        canonical_name = _canonical_macro_series_name(str(row.get("name") or ""))
        if not canonical_name or row.get("value") is None:
            continue
        out.append(
            {
                **row,
                "name": canonical_name,
                "raw_name": str(row.get("name") or ""),
                "value": float(row["value"]),
            }
        )
    return out


class LocalPostgresLoader:
    def __init__(self, session_factory: sessionmaker[Session], *, schema: str = "trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, *, scenario_id: str, market: str, start_date: str, end_date: str, symbols: Iterable[str], strategy_mode: str = "legacy_event_window", research_spec: ResearchExperimentSpec | None = None, train_end: str | None = None, decision_dates: Iterable[str] | None = None, metadata: Dict[str, str] | None = None, progress_callback=None) -> HistoricalSlice:
        symbols = [s for s in symbols if s]
        if not symbols:
            raise ValueError("symbols required")
        spec = research_spec or ResearchExperimentSpec()
        bars_end_date = train_end or end_date
        bars_by_symbol = self._load_bars(start_date=start_date, end_date=bars_end_date, symbols=symbols, warmup_days=max(WARMUP_DAYS, spec.feature_window_bars * 2))
        sector_map = self._load_sector_map(symbols)
        features_by_symbol: Dict[str, Dict[str, float]] = {symbol: compute_bar_features(bars) for symbol, bars in bars_by_symbol.items()}

        if strategy_mode == "research_similarity_v1":
            macro_payload = self._load_macro_payload(as_of=end_date)
            candidates, research_diag = generate_similarity_candidates(bars_by_symbol=bars_by_symbol, market=market, macro_payload=macro_payload, sector_map=sector_map, spec=spec)
            snapshot_ts = self._resolve_snapshot_ts(bars_by_symbol)
            enriched = self._enrich_candidates(candidates, features_by_symbol)
            snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False, macro_state=macro_payload)
            return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "research_spec": spec.to_dict(), "diagnostics": research_diag, "sector_map": sector_map})

        if strategy_mode == "research_similarity_v2":
            macro_history = self._load_macro_history(start_date=start_date, end_date=end_date, prewarm_days=max(WARMUP_DAYS, spec.feature_window_bars * 2))
            resolved_metadata = dict(metadata or {})
            abstain_margin = float(resolved_metadata.get("abstain_margin", 0.05) or 0.05)
            candidates, research_diag = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, sector_map=sector_map, spec=spec, abstain_margin=abstain_margin, metadata=resolved_metadata, progress_callback=progress_callback)
            snapshot_ts = self._resolve_snapshot_ts(bars_by_symbol)
            enriched = self._enrich_candidates(candidates, features_by_symbol)
            snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False)
            if decision_dates:
                allowed_dates = {str(d)[:10] for d in decision_dates}
                enriched = [c for c in enriched if str(c.reference_date)[:10] in allowed_dates]
            return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "research_spec": spec.to_dict(), "train_end": train_end, "decision_dates": [str(d)[:10] for d in decision_dates] if decision_dates else None, "diagnostics": research_diag, "signal_panel_artifact": research_diag.get("signal_panel", []), "memory_snapshot_artifact": research_diag.get("artifacts", {}), "macro_history_by_date": macro_history, "sector_map": sector_map})

        candidates, snapshot_ts = self._load_candidates(scenario_id=scenario_id, market=market, start_date=start_date, end_date=end_date, symbols=symbols)
        enriched = self._enrich_candidates(candidates, features_by_symbol)
        snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False)
        return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "sector_map": sector_map})

    def _enrich_candidates(self, candidates: List[SignalCandidate], features_by_symbol: Dict[str, Dict[str, float]]) -> List[SignalCandidate]:
        enriched: List[SignalCandidate] = []
        for candidate in candidates:
            prov = dict(candidate.provenance)
            prov["derived_bar_features"] = features_by_symbol.get(candidate.symbol, {})
            enriched.append(SignalCandidate(symbol=candidate.symbol, ticker_id=candidate.ticker_id, market=candidate.market, side_bias=candidate.side_bias, signal_strength=candidate.signal_strength, confidence=candidate.confidence, anchor_date=candidate.anchor_date, reference_date=candidate.reference_date, current_price=candidate.current_price, atr_pct=candidate.atr_pct, target_return_pct=candidate.target_return_pct, max_reverse_pct=candidate.max_reverse_pct, expected_horizon_days=candidate.expected_horizon_days, outcome_label=candidate.outcome_label, reverse_breach_day=candidate.reverse_breach_day, provenance=prov, diagnostics=dict(candidate.diagnostics), notes=list(candidate.notes)))
        return enriched

    def _resolve_snapshot_ts(self, bars_by_symbol: Dict[str, List[HistoricalBar]]) -> datetime:
        timestamps = [bar.timestamp for bars in bars_by_symbol.values() for bar in bars]
        if not timestamps:
            return datetime.utcnow()
        raw = max(timestamps)
        return raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))

    def _load_macro_payload(self, *, as_of: str) -> Dict[str, float]:
        sql = text(f"""
            SELECT latest.name, latest.value
            FROM (
                SELECT s.name, v.value, ROW_NUMBER() OVER (PARTITION BY v.series_id ORDER BY v.obs_date DESC, v.id DESC) AS rn
                  FROM {self.schema}.macro_data_series_value v
                  JOIN {self.schema}.macro_data_series s ON s.id = v.series_id
                 WHERE v.obs_date <= CAST(:as_of AS date)
                   AND s.name = ANY(:series_names)
            ) latest
            WHERE latest.rn = 1
            """)
        try:
            with self.session_factory() as session:
                rows = [dict(r._mapping) for r in session.execute(sql, {"as_of": as_of, "series_names": CANONICAL_MACRO_SOURCE_NAMES})]
            canonical_rows = _canonicalize_macro_rows(rows)
            return {str(r["name"]): float(r["value"]) for r in canonical_rows if str(r["name"]) in CTX_SERIES}
        except Exception:
            return {}

    def _load_macro_history(self, *, start_date: str, end_date: str, prewarm_days: int = 0) -> Dict[str, Dict[str, float]]:
        sql = text(f"""
            SELECT v.obs_date, s.name, v.value
              FROM {self.schema}.macro_data_series_value v
              JOIN {self.schema}.macro_data_series s ON s.id = v.series_id
             WHERE v.obs_date BETWEEN CAST(:prewarm_start AS date) AND CAST(:end_date AS date)
               AND s.name = ANY(:series_names)
             ORDER BY v.obs_date, s.name
            """)
        by_date: Dict[str, Dict[str, float]] = {}
        last_seen: Dict[str, float] = {}
        try:
            prewarm_start = (datetime.fromisoformat(start_date) - timedelta(days=prewarm_days)).date().isoformat()
            with self.session_factory() as session:
                rows = [dict(r._mapping) for r in session.execute(sql, {"prewarm_start": prewarm_start, "end_date": end_date, "series_names": CANONICAL_MACRO_SOURCE_NAMES})]
            for row in _canonicalize_macro_rows(rows):
                d = str(row["obs_date"])
                by_date.setdefault(d, dict(last_seen))
                last_seen[str(row["name"])] = float(row["value"])
                by_date[d][str(row["name"])] = float(row["value"])
            cursor = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
            while cursor <= end:
                d = cursor.date().isoformat()
                by_date.setdefault(d, dict(last_seen))
                last_seen = dict(by_date[d])
                cursor += timedelta(days=1)
            return by_date
        except Exception:
            return {}

    def _load_sector_map(self, symbols: List[str]) -> Dict[str, str]:
        sql = text(f"""
        SELECT t.symbol, s.code AS sector_code
          FROM {self.schema}.bt_mirror_ticker t
          JOIN {self.schema}.bt_mirror_ticker_industry ti ON ti.ticker_id = t.ticker_id
          JOIN {self.schema}.bt_mirror_industry i ON i.industry_id = ti.industry_id
          JOIN {self.schema}.bt_mirror_sector s ON s.sector_id = i.sector_id
         WHERE t.symbol = ANY(:symbols)
        """)
        with self.session_factory() as session:
            rows = [dict(r._mapping) for r in session.execute(sql, {"symbols": symbols})]
        return {str(r["symbol"]): str(r["sector_code"]) for r in rows if r.get("sector_code")}

    def _load_candidates(self, *, scenario_id: str, market: str, start_date: str, end_date: str, symbols: List[str]):
        sql = text(f"""
        SELECT scenario_id, market, symbol, ticker_id, event_time, anchor_date, reference_date,
               signal_strength, confidence, current_price, atr_pct, target_return_pct,
               max_reverse_pct, expected_horizon_days, reverse_breach_day, side_bias,
               outcome_label, provenance, diagnostics, notes
          FROM {self.schema}.bt_event_window
         WHERE scenario_id = :scenario_id
           AND market = :market
           AND symbol = ANY(:symbols)
           AND reference_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
         ORDER BY event_time, symbol
        """)
        with self.session_factory() as session:
            rows = [dict(r._mapping) for r in session.execute(sql, {"scenario_id": scenario_id, "market": market, "symbols": symbols, "start_date": start_date, "end_date": end_date})]
        if not rows:
            raise ValueError(f"No bt_event_window rows found for scenario_id={scenario_id}")
        snapshot_ts = rows[0]["event_time"]
        candidates = [SignalCandidate(symbol=row["symbol"], ticker_id=row["ticker_id"], market=MarketCode(row["market"]), side_bias=Side(row["side_bias"]), signal_strength=float(row["signal_strength"]), confidence=float(row["confidence"]) if row["confidence"] is not None else None, anchor_date=row["anchor_date"], reference_date=row["reference_date"], current_price=float(row["current_price"]) if row["current_price"] is not None else None, atr_pct=float(row["atr_pct"]) if row["atr_pct"] is not None else None, target_return_pct=float(row["target_return_pct"]) if row["target_return_pct"] is not None else None, max_reverse_pct=float(row["max_reverse_pct"]) if row["max_reverse_pct"] is not None else None, expected_horizon_days=row["expected_horizon_days"], reverse_breach_day=row["reverse_breach_day"], outcome_label=OutcomeLabel(row["outcome_label"]) if row.get("outcome_label") else OutcomeLabel.UNKNOWN, provenance=dict(row.get("provenance") or {}), diagnostics=dict(row.get("diagnostics") or {}), notes=list(row.get("notes") or [])) for row in rows]
        return candidates, snapshot_ts

    def _load_bars(self, *, start_date: str, end_date: str, symbols: List[str], warmup_days: int = 0) -> Dict[str, List[HistoricalBar]]:
        sql = text(f"""
        SELECT symbol, trade_date, open, high, low, close, volume
          FROM {self.schema}.bt_mirror_ohlcv_daily
         WHERE symbol = ANY(:symbols)
           AND trade_date BETWEEN CAST(:warmup_start AS date) AND CAST(:end_date AS date)
         ORDER BY symbol, trade_date
        """)
        out: Dict[str, List[HistoricalBar]] = {symbol: [] for symbol in symbols}
        warmup_start = (datetime.fromisoformat(start_date) - timedelta(days=warmup_days)).date().isoformat()
        with self.session_factory() as session:
            for row in session.execute(sql, {"symbols": symbols, "warmup_start": warmup_start, "end_date": end_date}):
                m = row._mapping
                out[m["symbol"]].append(HistoricalBar(symbol=m["symbol"], timestamp=str(m["trade_date"]), open=float(m["open"]), high=float(m["high"]), low=float(m["low"]), close=float(m["close"]), volume=float(m["volume"] or 0)))
        return out
