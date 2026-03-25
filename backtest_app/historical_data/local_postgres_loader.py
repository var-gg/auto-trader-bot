from __future__ import annotations

from typing import Dict, Iterable, List

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from shared.domain.models import MarketCode, MarketSnapshot, OutcomeLabel, Side, SignalCandidate

from .features import compute_bar_features
from .models import HistoricalBar, HistoricalSlice


class LocalPostgresLoader:
    """Read-only local Postgres loader for backtest_app.

    Uses backtest_app-only session wiring; never reuses live FastAPI session setup.
    """

    def __init__(self, session_factory: sessionmaker[Session], *, schema: str = "trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, *, scenario_id: str, market: str, start_date: str, end_date: str, symbols: Iterable[str]) -> HistoricalSlice:
        symbols = [s for s in symbols if s]
        if not symbols:
            raise ValueError("symbols required")
        bars_by_symbol = self._load_bars(start_date=start_date, end_date=end_date, symbols=symbols)
        candidates, snapshot_ts = self._load_candidates(scenario_id=scenario_id, market=market, start_date=start_date, end_date=end_date, symbols=symbols)
        features_by_symbol: Dict[str, Dict[str, float]] = {symbol: compute_bar_features(bars) for symbol, bars in bars_by_symbol.items()}
        enriched: List[SignalCandidate] = []
        for candidate in candidates:
            prov = dict(candidate.provenance)
            prov["derived_bar_features"] = features_by_symbol.get(candidate.symbol, {})
            enriched.append(
                SignalCandidate(
                    symbol=candidate.symbol,
                    ticker_id=candidate.ticker_id,
                    market=candidate.market,
                    side_bias=candidate.side_bias,
                    signal_strength=candidate.signal_strength,
                    confidence=candidate.confidence,
                    anchor_date=candidate.anchor_date,
                    reference_date=candidate.reference_date,
                    current_price=candidate.current_price,
                    atr_pct=candidate.atr_pct,
                    target_return_pct=candidate.target_return_pct,
                    max_reverse_pct=candidate.max_reverse_pct,
                    expected_horizon_days=candidate.expected_horizon_days,
                    outcome_label=candidate.outcome_label,
                    reverse_breach_day=candidate.reverse_breach_day,
                    provenance=prov,
                    diagnostics=dict(candidate.diagnostics),
                    notes=list(candidate.notes),
                )
            )
        snapshot = MarketSnapshot(as_of=snapshot_ts, market=market)
        return HistoricalSlice(market_snapshot=snapshot, bars_by_symbol=bars_by_symbol, candidates=enriched, metadata={"source": "local-db", "scenario_id": scenario_id})

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
           AND reference_date BETWEEN :start_date::date AND :end_date::date
         ORDER BY event_time, symbol
        """)
        with self.session_factory() as session:
            rows = [dict(r._mapping) for r in session.execute(sql, {"scenario_id": scenario_id, "market": market, "symbols": symbols, "start_date": start_date, "end_date": end_date})]
        if not rows:
            raise ValueError(f"No bt_event_window rows found for scenario_id={scenario_id}")
        snapshot_ts = rows[0]["event_time"]
        candidates = [
            SignalCandidate(
                symbol=row["symbol"],
                ticker_id=row["ticker_id"],
                market=MarketCode(row["market"]),
                side_bias=Side(row["side_bias"]),
                signal_strength=float(row["signal_strength"]),
                confidence=float(row["confidence"]) if row["confidence"] is not None else None,
                anchor_date=row["anchor_date"],
                reference_date=row["reference_date"],
                current_price=float(row["current_price"]) if row["current_price"] is not None else None,
                atr_pct=float(row["atr_pct"]) if row["atr_pct"] is not None else None,
                target_return_pct=float(row["target_return_pct"]) if row["target_return_pct"] is not None else None,
                max_reverse_pct=float(row["max_reverse_pct"]) if row["max_reverse_pct"] is not None else None,
                expected_horizon_days=row["expected_horizon_days"],
                reverse_breach_day=row["reverse_breach_day"],
                outcome_label=OutcomeLabel(row["outcome_label"]) if row.get("outcome_label") else OutcomeLabel.UNKNOWN,
                provenance=dict(row.get("provenance") or {}),
                diagnostics=dict(row.get("diagnostics") or {}),
                notes=list(row.get("notes") or []),
            )
            for row in rows
        ]
        return candidates, snapshot_ts

    def _load_bars(self, *, start_date: str, end_date: str, symbols: List[str]) -> Dict[str, List[HistoricalBar]]:
        sql = text(f"""
        SELECT symbol, trade_date, open, high, low, close, volume
          FROM {self.schema}.bt_mirror_ohlcv_daily
         WHERE symbol = ANY(:symbols)
           AND trade_date BETWEEN :start_date::date AND :end_date::date
         ORDER BY symbol, trade_date
        """)
        out: Dict[str, List[HistoricalBar]] = {symbol: [] for symbol in symbols}
        with self.session_factory() as session:
            for row in session.execute(sql, {"symbols": symbols, "start_date": start_date, "end_date": end_date}):
                m = row._mapping
                out[m["symbol"]].append(HistoricalBar(trade_date=m["trade_date"], open=float(m["open"]), high=float(m["high"]), low=float(m["low"]), close=float(m["close"]), volume=int(m["volume"] or 0)))
        return out
