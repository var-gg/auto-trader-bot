"""Anchor table builder: generates bt_result.anchor_vector from bar + macro data.

Usage:
    from backtest_app.research_runtime.anchor_table_builder import build_anchor_table
    result = build_anchor_table()
"""
from __future__ import annotations

import io
import math
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import mean, pstdev
from typing import Any, Dict, List, Mapping, Sequence

import numpy as np
import psycopg2

from backtest_app.historical_data.features import (
    SIMILARITY_CTX_SERIES,
    _context_series_features,
    _safe_div,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PAST_TRADING_DAYS = 10
FUTURE_TRADING_DAYS = 10
MIN_HISTORY_DAYS = 30  # need 20 for zscore/vol/beta + 10 for candle shape
BATCH_SIZE = 50  # symbols per batch (OOM safety)
SIM_VECTOR_VERSION = "anchor_v1"

DB_URL = "postgresql://postgres:change_me@127.0.0.1:5433/auto_trader_backtest"

MACRO_SERIES_IDS = {
    307: "vix",    # CBOE Volatility Index: VIX
    304: "oil",    # Crude Oil Prices: WTI
    4: "rate",     # Federal Funds Effective Rate
    306: "dollar",  # Nominal Broad U.S. Dollar Index
}


# ---------------------------------------------------------------------------
# Candle shape features (per bar, all relative)
# ---------------------------------------------------------------------------

def _candle_features(o: float, h: float, l: float, c: float) -> list[float]:
    rng = max(h - l, 1e-8)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    return [
        (c - o) / rng,            # body_pct (signed: positive=bullish)
        upper / rng,              # upper_wick_pct
        lower / rng,              # lower_wick_pct
        (c - l) / rng,            # close_location
    ]


# ---------------------------------------------------------------------------
# Macro snapshot: zscore_20, pct_change_5, slope_5 for each series
# ---------------------------------------------------------------------------

def _macro_snapshot_from_history(
    macro_by_date: Dict[str, Dict[str, float]],
    anchor_date_str: str,
) -> list[float]:
    """Compute 12-dim macro snapshot using data strictly before anchor_date."""
    result = []
    for series_name in ("vix", "oil", "rate", "dollar"):
        # Collect available values up to the day before anchor_date
        ordered_vals: list[float] = []
        for d in sorted(macro_by_date.keys()):
            if d >= anchor_date_str:
                break
            val = macro_by_date[d].get(series_name)
            if val is not None:
                ordered_vals.append(val)

        if len(ordered_vals) < 2:
            result.extend([0.0, 0.0, 0.0])
            continue

        # zscore_20
        window = ordered_vals[-20:] if len(ordered_vals) >= 20 else ordered_vals
        level = window[-1]
        mu = mean(window)
        std = pstdev(window) if len(window) > 1 else 0.0
        zscore = _safe_div(level - mu, std) if std > 1e-12 else 0.0

        # pct_change_5
        if len(ordered_vals) > 5:
            base = ordered_vals[-6]
            pct_chg = _safe_div(ordered_vals[-1] - base, abs(base)) if abs(base) > 1e-12 else 0.0
        else:
            pct_chg = 0.0

        # slope_5
        slope_window = ordered_vals[-5:] if len(ordered_vals) >= 5 else ordered_vals
        if len(slope_window) > 1:
            slope = (slope_window[-1] - slope_window[0]) / (len(slope_window) - 1)
        else:
            slope = 0.0

        result.extend([zscore, pct_chg, slope])

    return result


# ---------------------------------------------------------------------------
# Tech indicators (from bars ending at prev day)
# ---------------------------------------------------------------------------

def _tech_indicators(
    bars: list[dict],
    market_bars: list[dict],
) -> list[float]:
    """Compute 10-dim tech indicator vector from bars (up to prev day)."""
    if len(bars) < 2:
        return [0.0] * 10

    closes = [b["close"] for b in bars]
    rets = [(closes[i] / closes[i - 1]) - 1.0 for i in range(1, len(closes)) if closes[i - 1] > 0]

    # realized_vol_20
    vol_window = rets[-20:] if len(rets) >= 20 else rets
    if len(vol_window) > 1:
        mu = mean(vol_window)
        rv20 = math.sqrt(sum((r - mu) ** 2 for r in vol_window) / len(vol_window))
    else:
        rv20 = 0.0

    # atr_pct_14
    trs = []
    for i in range(max(1, len(bars) - 14), len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr14 = _safe_div(mean(trs), closes[-1]) if trs else 0.0

    # drawdown_20
    dd_closes = closes[-20:]
    peak = dd_closes[0] if dd_closes else 0.0
    dd = 0.0
    for c in dd_closes:
        peak = max(peak, c)
        dd = min(dd, _safe_div(c - peak, peak))

    # market relative returns
    mkt_closes = [b["close"] for b in market_bars] if market_bars else []

    def _ret(arr, n):
        if len(arr) < n + 1 or arr[-n - 1] == 0:
            return 0.0
        return (arr[-1] / arr[-n - 1]) - 1.0

    own_ret_1 = _ret(closes, 1)
    own_ret_5 = _ret(closes, 5)
    own_ret_20 = _ret(closes, 20)
    mkt_ret_1 = _ret(mkt_closes, 1) if len(mkt_closes) > 1 else 0.0
    mkt_ret_5 = _ret(mkt_closes, 5) if len(mkt_closes) > 5 else 0.0
    mkt_ret_20 = _ret(mkt_closes, 20) if len(mkt_closes) > 20 else 0.0

    # relative_volume, adv_percentile
    volumes = [b.get("volume", 0) or 0 for b in bars]
    curr_vol = volumes[-1]
    trail_vol = volumes[:-1] or volumes
    rel_vol = _safe_div(curr_vol, mean(trail_vol)) if trail_vol else 0.0
    dvs = [bars[i]["close"] * (volumes[i] or 0) for i in range(len(bars))]
    curr_dv = dvs[-1]
    trail_dv = dvs[:-1] or dvs
    adv_pctile = _safe_div(sum(1 for v in trail_dv if v <= curr_dv), len(trail_dv))

    # beta_20
    mkt_rets = [(mkt_closes[i] / mkt_closes[i - 1]) - 1.0 for i in range(1, len(mkt_closes)) if mkt_closes[i - 1] > 0] if len(mkt_closes) > 1 else []
    own_r20 = rets[-20:]
    mkt_r20 = mkt_rets[-20:] if mkt_rets else []
    min_len = min(len(own_r20), len(mkt_r20))
    if min_len > 1:
        num = sum(own_r20[-min_len + i] * mkt_r20[-min_len + i] for i in range(min_len))
        den = sum(m * m for m in mkt_r20[-min_len:])
        beta = _safe_div(num, den)
    else:
        beta = 0.0

    # gap_pct
    if len(bars) >= 2:
        gap = _safe_div(bars[-1]["open"] - bars[-2]["close"], bars[-2]["close"])
    else:
        gap = 0.0

    return [
        rv20,
        atr14,
        dd,
        own_ret_1 - mkt_ret_1,   # mkt_rel_ret_1
        own_ret_5 - mkt_ret_5,   # mkt_rel_ret_5
        own_ret_20 - mkt_ret_20, # mkt_rel_ret_20
        rel_vol,
        adv_pctile,
        beta,
        gap,
    ]


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_anchor_table(
    *,
    markets: list[str] | None = None,
    db_url: str = DB_URL,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Build and populate bt_result.anchor_vector."""
    if markets is None:
        markets = ["US"]

    started = time.time()
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    # --- Load macro history (one-time) ---
    print("[anchor] Loading macro history...")
    t0 = time.time()
    cur.execute("""
        SELECT v.obs_date, s.name, v.value
        FROM trading.macro_data_series_value v
        JOIN trading.macro_data_series s ON s.id = v.series_id
        WHERE v.series_id IN %s
        ORDER BY v.obs_date
    """, (tuple(MACRO_SERIES_IDS.keys()),))
    macro_rows = cur.fetchall()

    # Build forward-filled macro history: {date_str: {series: value}}
    macro_by_date: Dict[str, Dict[str, float]] = {}
    last_seen: Dict[str, float] = {}
    for obs_date, name, value in macro_rows:
        d = str(obs_date)
        canonical = MACRO_SERIES_IDS.get(None)  # need to map by name
        for sid, cname in MACRO_SERIES_IDS.items():
            if cname in name.lower() or name in (
                "CBOE Volatility Index: VIX",
                "Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma",
                "Federal Funds Effective Rate",
                "Nominal Broad U.S. Dollar Index",
            ):
                pass
        # Simpler: map by series_id which we already have
    # Redo: load with series_id
    macro_by_date = {}
    last_seen = {}
    cur.execute("""
        SELECT v.obs_date, v.series_id, v.value
        FROM trading.macro_data_series_value v
        WHERE v.series_id IN %s
        ORDER BY v.obs_date, v.id
    """, (tuple(MACRO_SERIES_IDS.keys()),))
    for obs_date, series_id, value in cur.fetchall():
        if value is None:
            continue
        d = str(obs_date)
        cname = MACRO_SERIES_IDS[series_id]
        last_seen[cname] = float(value)
        if d not in macro_by_date:
            macro_by_date[d] = dict(last_seen)
        else:
            macro_by_date[d][cname] = float(value)

    # Forward-fill: ensure every calendar date between min and max has values
    if macro_by_date:
        all_dates = sorted(macro_by_date.keys())
        cursor_date = datetime.fromisoformat(all_dates[0]).date()
        end_date = datetime.fromisoformat(all_dates[-1]).date()
        fill_state: Dict[str, float] = {}
        while cursor_date <= end_date:
            d = cursor_date.isoformat()
            if d in macro_by_date:
                fill_state.update(macro_by_date[d])
                macro_by_date[d] = dict(fill_state)
            else:
                macro_by_date[d] = dict(fill_state)
            cursor_date += timedelta(days=1)

    print(f"[anchor] Macro loaded: {len(macro_by_date)} dates in {time.time()-t0:.1f}s")

    # --- Load symbols by market ---
    for market in markets:
        print(f"\n[anchor] Processing market: {market}")

        if market == "KR":
            market_filter = "country = 'KR' OR exchange IN ('KRX','KOSPI','KOSDAQ','KOE')"
            market_proxy_symbol = None  # TODO: KOSPI index
        else:
            market_filter = "country != 'KR' AND exchange NOT IN ('KRX','KOSPI','KOSDAQ','KOE')"
            market_proxy_symbol = "SPY"

        cur.execute(f"""
            SELECT DISTINCT t.symbol
            FROM trading.bt_mirror_ticker t
            JOIN trading.bt_mirror_ohlcv_daily d ON d.ticker_id = t.ticker_id
            WHERE ({market_filter})
            ORDER BY t.symbol
        """)
        symbols = [row[0] for row in cur.fetchall()]
        print(f"[anchor] {market}: {len(symbols)} symbols")

        # Load market proxy bars
        market_proxy_bars: list[dict] = []
        if market_proxy_symbol:
            cur.execute("""
                SELECT trade_date, open, high, low, close, volume
                FROM trading.bt_mirror_ohlcv_daily
                WHERE symbol = %s
                ORDER BY trade_date
            """, (market_proxy_symbol,))
            market_proxy_bars = [
                {"date": str(r[0]), "open": float(r[1]), "high": float(r[2]),
                 "low": float(r[3]), "close": float(r[4]), "volume": int(r[5] or 0)}
                for r in cur.fetchall()
            ]
        mkt_date_idx = {b["date"]: i for i, b in enumerate(market_proxy_bars)}

        total_rows = 0
        total_valid = 0

        # Process in batches
        for batch_start in range(0, len(symbols), batch_size):
            batch_symbols = symbols[batch_start:batch_start + batch_size]

            # Bulk load bars for batch
            cur.execute("""
                SELECT symbol, trade_date, open, high, low, close, volume
                FROM trading.bt_mirror_ohlcv_daily
                WHERE symbol IN %s
                ORDER BY symbol, trade_date
            """, (tuple(batch_symbols),))

            bars_by_symbol: Dict[str, list[dict]] = defaultdict(list)
            for sym, td, o, h, l, c, v in cur.fetchall():
                bars_by_symbol[sym].append({
                    "date": str(td), "open": float(o), "high": float(h),
                    "low": float(l), "close": float(c), "volume": int(v or 0),
                })

            # Build anchor rows
            batch_rows: list[tuple] = []

            for sym in batch_symbols:
                bars = bars_by_symbol.get(sym, [])
                if len(bars) < MIN_HISTORY_DAYS + FUTURE_TRADING_DAYS:
                    continue

                for i in range(MIN_HISTORY_DAYS, len(bars) - FUTURE_TRADING_DAYS):
                    anchor_bar = bars[i]
                    anchor_date_str = anchor_bar["date"]
                    anchor_open = anchor_bar["open"]

                    if anchor_open <= 0:
                        continue

                    # Past bars: 10 trading days before anchor (indices i-10..i-1)
                    past_start = max(0, i - PAST_TRADING_DAYS)
                    past_bars = bars[past_start:i]  # excludes anchor day

                    if len(past_bars) < PAST_TRADING_DAYS:
                        continue

                    # --- candle_shape[40] ---
                    candle_shape = []
                    for pb in past_bars[-PAST_TRADING_DAYS:]:
                        candle_shape.extend(_candle_features(pb["open"], pb["high"], pb["low"], pb["close"]))

                    # --- return_series[10] ---
                    return_series = []
                    # Need one extra bar before past window for first return
                    ret_start = max(0, i - PAST_TRADING_DAYS - 1)
                    ret_bars = bars[ret_start:i]
                    for j in range(1, len(ret_bars)):
                        pc = ret_bars[j - 1]["close"]
                        if pc > 0:
                            return_series.append((ret_bars[j]["close"] / pc) - 1.0)
                        else:
                            return_series.append(0.0)
                    # Take last 10
                    return_series = return_series[-PAST_TRADING_DAYS:]
                    if len(return_series) < PAST_TRADING_DAYS:
                        return_series = [0.0] * (PAST_TRADING_DAYS - len(return_series)) + return_series

                    # --- macro_snapshot[12] ---
                    macro_snap = _macro_snapshot_from_history(macro_by_date, anchor_date_str)

                    # --- tech_indicators[10] ---
                    # Use bars up to and including prev day (bars[:i] = up to index i-1)
                    tech_bars = bars[max(0, i - MIN_HISTORY_DAYS):i]
                    # Market proxy bars aligned to same dates
                    prev_date = bars[i - 1]["date"]
                    mkt_end = mkt_date_idx.get(prev_date)
                    if mkt_end is not None and mkt_end >= 20:
                        mkt_window = market_proxy_bars[max(0, mkt_end - MIN_HISTORY_DAYS + 1):mkt_end + 1]
                    else:
                        mkt_window = []
                    tech = _tech_indicators(tech_bars, mkt_window)

                    # --- sim_vector[72] = concat + L2 normalize ---
                    raw_vec = candle_shape + return_series + macro_snap + tech
                    arr = np.array(raw_vec, dtype=np.float64)
                    norm = np.linalg.norm(arr)
                    if norm > 1e-12:
                        arr = arr / norm
                    sim_vector = arr.tolist()

                    # --- future bars (anchor day + next 9 trading days) ---
                    future_bars = bars[i:i + FUTURE_TRADING_DAYS]
                    fut_count = len(future_bars)
                    fut_h = [(b["high"] / anchor_open) for b in future_bars]
                    fut_l = [(b["low"] / anchor_open) for b in future_bars]
                    fut_c = [(b["close"] / anchor_open) for b in future_bars]

                    # Pad to 10 if needed
                    while len(fut_h) < FUTURE_TRADING_DAYS:
                        fut_h.append(0.0)
                        fut_l.append(0.0)
                        fut_c.append(0.0)

                    # Validity check
                    is_valid = (
                        len(candle_shape) == 40
                        and len(return_series) == 10
                        and len(sim_vector) == 72
                        and fut_count >= 1
                        and not any(math.isnan(v) for v in sim_vector)
                    )

                    # Sector code (not available in bar data, leave NULL for now)
                    sector_code = None

                    batch_rows.append((
                        sym, anchor_date_str, market, sector_code, anchor_open,
                        sim_vector, SIM_VECTOR_VERSION,
                        candle_shape, return_series, macro_snap, tech,
                        fut_h, fut_l, fut_c, fut_count,
                        is_valid,
                    ))
                    total_rows += 1
                    if is_valid:
                        total_valid += 1

            # Bulk insert batch via COPY
            if batch_rows:
                buf = io.StringIO()
                for row in batch_rows:
                    fields = []
                    for val in row:
                        if val is None:
                            fields.append("\\N")
                        elif isinstance(val, bool):
                            fields.append("t" if val else "f")
                        elif isinstance(val, list):
                            fields.append("{" + ",".join(str(v) for v in val) + "}")
                        else:
                            fields.append(str(val))
                    buf.write("\t".join(fields) + "\n")
                buf.seek(0)
                cur.execute("SET search_path TO bt_result, trading, public")
                cur.copy_from(
                    buf,
                    "anchor_vector",
                    columns=(
                        "symbol", "anchor_date", "market", "sector_code", "anchor_open",
                        "sim_vector", "sim_vector_version",
                        "candle_shape", "return_series", "macro_snapshot", "tech_indicators",
                        "future_high_ratios", "future_low_ratios", "future_close_ratios",
                        "future_bar_count", "is_valid",
                    ),
                )
                conn.commit()

            elapsed = time.time() - started
            print(f"[anchor] {market}: {batch_start + len(batch_symbols)}/{len(symbols)} symbols, "
                  f"{total_rows} rows ({total_valid} valid), {elapsed:.0f}s")

    conn.close()
    total_elapsed = time.time() - started
    print(f"\n[anchor] Done: {total_rows} rows ({total_valid} valid) in {total_elapsed:.1f}s")
    return {"total_rows": total_rows, "valid_rows": total_valid, "elapsed_seconds": total_elapsed}


if __name__ == "__main__":
    build_anchor_table(markets=["US"])
