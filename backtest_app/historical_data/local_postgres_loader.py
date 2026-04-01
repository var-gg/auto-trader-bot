from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.research.pipeline import generate_similarity_candidates, generate_similarity_candidates_rolling
from shared.domain.models import MarketCode, MarketSnapshot, OutcomeLabel, Side, SignalCandidate

from .features import CTX_SERIES, compute_bar_features
from .models import HistoricalBar, HistoricalSlice, SymbolSessionMetadata
from .session_alignment import build_symbol_session_metadata, session_metadata_to_dict

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
MACRO_SOURCE_CONFIG = {
    "vix": {"exchange_tz": "America/New_York", "session_close_local_time": "16:00"},
    "rate": {"exchange_tz": "America/New_York", "session_close_local_time": "16:00"},
    "dollar": {"exchange_tz": "America/New_York", "session_close_local_time": "16:00"},
    "oil": {"exchange_tz": "America/New_York", "session_close_local_time": "16:00"},
}


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


def _derived_macro_source_ts_utc(*, obs_date: str | date, series_name: str) -> str | None:
    cfg = MACRO_SOURCE_CONFIG.get(str(series_name))
    if cfg is None:
        return None
    local_date = obs_date if isinstance(obs_date, date) else datetime.fromisoformat(str(obs_date)[:10]).date()
    local_dt = datetime.combine(
        local_date,
        time.fromisoformat(str(cfg["session_close_local_time"])),
        tzinfo=ZoneInfo(str(cfg["exchange_tz"])),
    )
    return local_dt.astimezone(timezone.utc).isoformat()


def _macro_history_by_obs_date(rows: List[Dict[str, Any]], *, start_date: str, end_date: str, prewarm_days: int) -> Dict[str, Dict[str, float]]:
    by_date: Dict[str, Dict[str, float]] = {}
    last_seen: Dict[str, float] = {}
    prewarm_start = (datetime.fromisoformat(start_date) - timedelta(days=prewarm_days)).date().isoformat()
    filtered = [row for row in rows if prewarm_start <= str(row.get("obs_date")) <= end_date]
    for row in filtered:
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


class LocalPostgresLoader:
    def __init__(self, session_factory: sessionmaker[Session], *, schema: str = "trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_research_context(
        self,
        *,
        start_date: str,
        end_date: str,
        symbols: Iterable[str],
        research_spec: ResearchExperimentSpec | None = None,
    ) -> Dict[str, Any]:
        symbols = [str(symbol) for symbol in symbols if symbol]
        if not symbols:
            raise ValueError("symbols required")
        spec = research_spec or ResearchExperimentSpec()
        prewarm_days = max(WARMUP_DAYS, spec.feature_window_bars * 2)
        bars_by_symbol = self._load_bars(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            warmup_days=prewarm_days,
        )
        sector_map = self._load_sector_map(symbols)
        session_metadata_by_symbol, missing_session_metadata_symbols = self._load_session_metadata(symbols)
        macro_series_history = self._load_macro_series_history(
            start_date=start_date,
            end_date=end_date,
            prewarm_days=prewarm_days,
        )
        macro_history_by_date = _macro_history_by_obs_date(
            macro_series_history,
            start_date=start_date,
            end_date=end_date,
            prewarm_days=prewarm_days,
        )
        return {
            "bars_by_symbol": bars_by_symbol,
            "sector_map": sector_map,
            "session_metadata_by_symbol": session_metadata_by_symbol,
            "missing_session_metadata_symbols": missing_session_metadata_symbols,
            "macro_series_history": macro_series_history,
            "macro_history_by_date": macro_history_by_date,
            "prewarm_days": prewarm_days,
        }

    def load_for_scenario(self, *, scenario_id: str, market: str, start_date: str, end_date: str, symbols: Iterable[str], strategy_mode: str = "legacy_event_window", research_spec: ResearchExperimentSpec | None = None, train_end: str | None = None, decision_dates: Iterable[str] | None = None, metadata: Dict[str, str] | None = None, progress_callback=None) -> HistoricalSlice:
        symbols = [s for s in symbols if s]
        if not symbols:
            raise ValueError("symbols required")
        spec = research_spec or ResearchExperimentSpec()
        bars_end_date = train_end or end_date
        bars_by_symbol = self._load_bars(start_date=start_date, end_date=bars_end_date, symbols=symbols, warmup_days=max(WARMUP_DAYS, spec.feature_window_bars * 2))
        sector_map = self._load_sector_map(symbols)
        session_metadata_by_symbol, missing_session_metadata_symbols = self._load_session_metadata(symbols)
        features_by_symbol: Dict[str, Dict[str, float]] = {symbol: compute_bar_features(bars) for symbol, bars in bars_by_symbol.items()}

        if strategy_mode == "research_similarity_v1":
            macro_payload = self._load_macro_payload(as_of=end_date)
            candidates, research_diag = generate_similarity_candidates(bars_by_symbol=bars_by_symbol, market=market, macro_payload=macro_payload, sector_map=sector_map, spec=spec)
            snapshot_ts = self._resolve_snapshot_ts(bars_by_symbol)
            enriched = self._enrich_candidates(candidates, features_by_symbol)
            snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False, macro_state=macro_payload)
            return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, session_metadata_by_symbol=session_metadata_by_symbol, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "research_spec": spec.to_dict(), "diagnostics": research_diag, "sector_map": sector_map, "session_metadata_by_symbol": {symbol: session_metadata_to_dict(meta) for symbol, meta in session_metadata_by_symbol.items()}, "missing_session_metadata_symbols": missing_session_metadata_symbols})

        if strategy_mode == "research_similarity_v2":
            prewarm_days = max(WARMUP_DAYS, spec.feature_window_bars * 2)
            macro_series_history = self._load_macro_series_history(start_date=start_date, end_date=end_date, prewarm_days=prewarm_days)
            macro_history = _macro_history_by_obs_date(macro_series_history, start_date=start_date, end_date=end_date, prewarm_days=prewarm_days)
            resolved_metadata = dict(metadata or {})
            abstain_margin = float(resolved_metadata.get("abstain_margin", 0.05) or 0.05)
            candidates, research_diag = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, macro_series_history=macro_series_history, sector_map=sector_map, session_metadata_by_symbol=session_metadata_by_symbol, spec=spec, abstain_margin=abstain_margin, metadata=resolved_metadata, progress_callback=progress_callback)
            snapshot_ts = self._resolve_snapshot_ts(bars_by_symbol)
            enriched = self._enrich_candidates(candidates, features_by_symbol)
            snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False)
            if decision_dates:
                allowed_dates = {str(d)[:10] for d in decision_dates}
                enriched = [c for c in enriched if str(c.reference_date)[:10] in allowed_dates]
            return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, session_metadata_by_symbol=session_metadata_by_symbol, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "research_spec": spec.to_dict(), "train_end": train_end, "decision_dates": [str(d)[:10] for d in decision_dates] if decision_dates else None, "diagnostics": research_diag, "signal_panel_artifact": research_diag.get("signal_panel", []), "memory_snapshot_artifact": research_diag.get("artifacts", {}), "macro_history_by_date": macro_history, "macro_series_history": macro_series_history, "sector_map": sector_map, "session_metadata_by_symbol": {symbol: session_metadata_to_dict(meta) for symbol, meta in session_metadata_by_symbol.items()}, "missing_session_metadata_symbols": missing_session_metadata_symbols})

        candidates, snapshot_ts = self._load_candidates(scenario_id=scenario_id, market=market, start_date=start_date, end_date=end_date, symbols=symbols)
        enriched = self._enrich_candidates(candidates, features_by_symbol)
        snapshot = MarketSnapshot(as_of=snapshot_ts, market=MarketCode(market), session_label="BACKTEST", is_open=False)
        return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, session_metadata_by_symbol=session_metadata_by_symbol, metadata={"source": "local-db", "scenario_id": scenario_id, "strategy_mode": strategy_mode, "sector_map": sector_map, "session_metadata_by_symbol": {symbol: session_metadata_to_dict(meta) for symbol, meta in session_metadata_by_symbol.items()}, "missing_session_metadata_symbols": missing_session_metadata_symbols})

    def list_tradable_symbols(self, *, market: str | None = None) -> List[str]:
        sql = text(f"""
        SELECT DISTINCT t.symbol
          FROM {self.schema}.bt_mirror_ticker t
          JOIN {self.schema}.bt_mirror_ohlcv_daily o ON o.symbol = t.symbol
         WHERE t.symbol IS NOT NULL
           AND t.symbol <> ''
           AND (
                :market = ''
                OR (:market = 'US' AND COALESCE(t.country, '') <> 'KR' AND COALESCE(t.exchange, '') NOT IN ('KRX', 'KOSPI', 'KOSDAQ', 'KOE'))
                OR (:market = 'KR' AND (COALESCE(t.country, '') = 'KR' OR COALESCE(t.exchange, '') IN ('KRX', 'KOSPI', 'KOSDAQ', 'KOE')))
           )
         ORDER BY t.symbol
        """)
        normalized_market = str(market or "").upper()
        with self.session_factory() as session:
            rows = [str(row._mapping["symbol"]) for row in session.execute(sql, {"market": normalized_market})]
        return rows

    def available_date_range(self, *, symbols: Iterable[str] | None = None) -> tuple[str | None, str | None]:
        symbol_list = [str(symbol) for symbol in (symbols or []) if symbol]
        if symbol_list:
            sql = text(f"""
            SELECT MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date
              FROM {self.schema}.bt_mirror_ohlcv_daily
             WHERE symbol = ANY(:symbols)
            """)
            params = {"symbols": symbol_list}
        else:
            sql = text(f"""
            SELECT MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date
              FROM {self.schema}.bt_mirror_ohlcv_daily
            """)
            params = {}
        with self.session_factory() as session:
            row = session.execute(sql, params).one()._mapping
        min_trade_date = row.get("min_trade_date")
        max_trade_date = row.get("max_trade_date")
        return (
            str(min_trade_date) if min_trade_date is not None else None,
            str(max_trade_date) if max_trade_date is not None else None,
        )

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
        rows = self._load_macro_series_history(start_date=start_date, end_date=end_date, prewarm_days=prewarm_days)
        return _macro_history_by_obs_date(rows, start_date=start_date, end_date=end_date, prewarm_days=prewarm_days)

    def _load_macro_series_history(self, *, start_date: str, end_date: str, prewarm_days: int = 0) -> List[Dict[str, Any]]:
        sql = text(f"""
            SELECT v.obs_date, s.name, v.value
              FROM {self.schema}.macro_data_series_value v
              JOIN {self.schema}.macro_data_series s ON s.id = v.series_id
             WHERE v.obs_date BETWEEN CAST(:prewarm_start AS date) AND CAST(:end_date AS date)
               AND s.name = ANY(:series_names)
             ORDER BY v.obs_date, s.name
            """)
        try:
            prewarm_start = (datetime.fromisoformat(start_date) - timedelta(days=prewarm_days)).date().isoformat()
            with self.session_factory() as session:
                rows = [dict(r._mapping) for r in session.execute(sql, {"prewarm_start": prewarm_start, "end_date": end_date, "series_names": CANONICAL_MACRO_SOURCE_NAMES})]
            canonical_rows = _canonicalize_macro_rows(rows)
            enriched_rows: List[Dict[str, Any]] = []
            for row in canonical_rows:
                series_name = str(row["name"])
                source_ts_utc = _derived_macro_source_ts_utc(obs_date=str(row["obs_date"]), series_name=series_name)
                enriched_rows.append(
                    {
                        "obs_date": str(row["obs_date"]),
                        "name": series_name,
                        "raw_name": str(row.get("raw_name") or ""),
                        "value": float(row["value"]),
                        "source_ts_utc": source_ts_utc,
                        "source_ts_is_derived": source_ts_utc is not None,
                    }
                )
            return enriched_rows
        except Exception:
            return []

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

    def _load_session_metadata(self, symbols: List[str]) -> tuple[Dict[str, SymbolSessionMetadata], List[str]]:
        sql = text(f"""
        SELECT symbol, exchange, country
          FROM {self.schema}.bt_mirror_ticker
         WHERE symbol = ANY(:symbols)
        """)
        session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] = {}
        missing: List[str] = []
        with self.session_factory() as session:
            rows = [dict(r._mapping) for r in session.execute(sql, {"symbols": symbols})]
        by_symbol = {str(row["symbol"]): row for row in rows}
        for symbol in symbols:
            row = by_symbol.get(symbol)
            metadata = build_symbol_session_metadata(
                symbol=symbol,
                exchange_code=(row or {}).get("exchange"),
                country_code=(row or {}).get("country"),
            )
            if metadata is None:
                missing.append(symbol)
                continue
            session_metadata_by_symbol[symbol] = metadata
        return session_metadata_by_symbol, sorted(set(missing))

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
