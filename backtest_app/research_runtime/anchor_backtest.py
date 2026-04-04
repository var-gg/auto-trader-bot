"""Anchor-table-based backtest engine for Optuna trials.

Option C: full numpy load + date-batch matmul + pre-cached futures + test sampling.
pgvector HNSW index exists for live trading; Optuna uses numpy for batch speed.

Usage:
    engine = AnchorBacktestEngine.from_db()
    engine.precompute_similarities(test_start_date="2024-01-01")
    result = engine.evaluate_params(params, initial_capital=100_000)
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from typing import Any

import numpy as np
import psycopg2

DB_URL = "postgresql://postgres:change_me@127.0.0.1:5433/auto_trader_backtest"


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    sv = values[order]
    sw = weights[order]
    cdf = np.cumsum(sw)
    cdf /= cdf[-1]
    idx = int(np.searchsorted(cdf, q))
    return float(sv[min(idx, len(sv) - 1)])


class AnchorBacktestEngine:

    def __init__(
        self,
        symbols: np.ndarray,
        dates: np.ndarray,
        markets: np.ndarray,
        anchor_opens: np.ndarray,
        sim_matrix: np.ndarray,
        future_h: np.ndarray,
        future_l: np.ndarray,
        future_c: np.ndarray,
        future_counts: np.ndarray,
    ):
        self.symbols = symbols
        self.dates = dates
        self.markets = markets
        self.anchor_opens = anchor_opens
        self.sim_matrix = sim_matrix       # (N, 72) L2-normalized
        self.future_h = future_h           # (N, 10)
        self.future_l = future_l           # (N, 10)
        self.future_c = future_c           # (N, 10)
        self.future_counts = future_counts # (N,)
        self.n = len(symbols)

        # Filled by precompute_similarities
        self.folds: list[dict] = []
        # Per-anchor cache: sim indices, scores, AND similar anchors' future arrays
        self.sim_cache: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
        # Sampled test indices for Optuna (subset of full test)
        self.sampled_test_indices: dict[int, np.ndarray] = {}  # fold_idx -> sampled indices

    @classmethod
    def from_db(cls, db_url: str = DB_URL, market: str | None = None) -> "AnchorBacktestEngine":
        t0 = time.time()
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        where = "WHERE is_valid = TRUE"
        params: tuple = ()
        if market:
            where += " AND market = %s"
            params = (market,)

        cur.execute(f"""
            SELECT symbol, anchor_date, market, anchor_open,
                   sim_vector,
                   future_high_ratios, future_low_ratios, future_close_ratios,
                   future_bar_count
            FROM bt_result.anchor_vector
            {where}
            ORDER BY anchor_date, symbol
        """, params)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        n = len(rows)
        symbols = np.array([r[0] for r in rows], dtype=object)
        dates = np.array([str(r[1]) for r in rows], dtype=object)
        markets = np.array([r[2] for r in rows], dtype=object)
        anchor_opens = np.array([float(r[3]) for r in rows], dtype=np.float64)

        sim_matrix = np.zeros((n, 72), dtype=np.float64)
        future_h = np.zeros((n, 10), dtype=np.float64)
        future_l = np.zeros((n, 10), dtype=np.float64)
        future_c = np.zeros((n, 10), dtype=np.float64)
        future_counts = np.zeros(n, dtype=np.int32)

        for i, r in enumerate(rows):
            sv = r[4]
            if sv is not None:
                if isinstance(sv, str):
                    sv = [float(x) for x in sv.strip("[]").split(",")]
                if hasattr(sv, '__len__') and len(sv) >= 72:
                    sim_matrix[i, :72] = sv[:72]
            fh, fl, fc = r[5], r[6], r[7]
            fc_count = int(r[8] or 0)
            future_counts[i] = fc_count
            if fh:
                future_h[i, :min(len(fh), 10)] = fh[:10]
            if fl:
                future_l[i, :min(len(fl), 10)] = fl[:10]
            if fc:
                future_c[i, :min(len(fc), 10)] = fc[:10]

        print(f"[anchor-bt] Loaded {n} anchors in {time.time()-t0:.1f}s")
        return cls(symbols, dates, markets, anchor_opens, sim_matrix,
                   future_h, future_l, future_c, future_counts)

    def precompute_similarities(
        self,
        test_start_date: str = "2024-01-01",
        n_folds: int = 3,
        top_k_cache: int = 50,
        sample_ratio: float = 0.2,
        sample_seed: int = 42,
    ):
        """Precompute top-50 similar anchors + their future arrays. Sample 20% for Optuna speed."""
        t0 = time.time()

        train_mask = self.dates < test_start_date
        test_mask = (self.dates >= test_start_date) & (self.future_counts >= 2)
        train_idx = np.where(train_mask)[0]
        test_all_idx = np.where(test_mask)[0]

        if len(train_idx) == 0 or len(test_all_idx) == 0:
            print("[anchor-bt] No train or test anchors.")
            return

        # Split test into folds by date
        test_dates_unique = sorted(set(self.dates[test_all_idx].tolist()))
        fold_size = len(test_dates_unique) // n_folds

        self.folds = []
        for f in range(n_folds):
            s = f * fold_size
            e = (f + 1) * fold_size if f < n_folds - 1 else len(test_dates_unique)
            fold_dates = set(test_dates_unique[s:e])
            fold_test_mask = np.array([d in fold_dates for d in self.dates]) & test_mask
            fold_test_idx = np.where(fold_test_mask)[0]

            fold_first_date = test_dates_unique[s]
            fold_train_idx = np.where(self.dates < fold_first_date)[0]

            self.folds.append({
                "fold_idx": f,
                "test_indices": fold_test_idx,
                "train_indices": fold_train_idx,
            })

        # Sample test dates (not individual anchors) to keep date diversity
        rng = np.random.RandomState(sample_seed)
        for fold in self.folds:
            f_test = fold["test_indices"]
            test_dates_in_fold = sorted(set(self.dates[f_test].tolist()))
            n_sample_dates = max(1, int(len(test_dates_in_fold) * sample_ratio))
            sampled_dates = set(rng.choice(test_dates_in_fold, size=n_sample_dates, replace=False).tolist())
            sampled_mask = np.array([self.dates[i] in sampled_dates for i in f_test])
            self.sampled_test_indices[fold["fold_idx"]] = f_test[sampled_mask]

        # Precompute similarities + cache future arrays
        self.sim_cache = {}
        total_cached = 0
        total_dates_processed = 0

        for fold in self.folds:
            f_train = fold["train_indices"]
            f_test = self.sampled_test_indices[fold["fold_idx"]]  # sampled only
            if len(f_train) == 0 or len(f_test) == 0:
                continue

            train_vecs = self.sim_matrix[f_train]

            date_to_test: dict[str, list[int]] = defaultdict(list)
            for idx in f_test:
                date_to_test[self.dates[idx]].append(idx)

            n_dates = len(date_to_test)
            for d_idx, (dt, test_idx_list) in enumerate(sorted(date_to_test.items())):
                test_idx_arr = np.array(test_idx_list)
                test_vecs = self.sim_matrix[test_idx_arr]

                sims = test_vecs @ train_vecs.T  # (B, T) cosine

                k = min(top_k_cache, sims.shape[1])
                for j, tidx in enumerate(test_idx_arr):
                    row_sims = sims[j]
                    top_local = np.argpartition(row_sims, -k)[-k:]
                    top_global = f_train[top_local]
                    top_scores = row_sims[top_local]
                    order = np.argsort(-top_scores)

                    sorted_global = top_global[order]
                    sorted_scores = top_scores[order]

                    # Pre-cache similar anchors' future arrays (avoid lookup during trial)
                    cached_fut_l = self.future_l[sorted_global]  # (50, 10)
                    cached_fut_h = self.future_h[sorted_global]  # (50, 10)

                    self.sim_cache[int(tidx)] = (
                        sorted_global,
                        sorted_scores,
                        cached_fut_l,
                        cached_fut_h,
                    )
                    total_cached += 1

                total_dates_processed += 1
                if (d_idx + 1) % 20 == 0 or d_idx == n_dates - 1:
                    elapsed = time.time() - t0
                    print(f"[anchor-bt] fold{fold['fold_idx']}: {d_idx+1}/{n_dates} dates, "
                          f"{total_cached} cached, {elapsed:.0f}s")

        elapsed = time.time() - t0
        total_sampled = sum(len(v) for v in self.sampled_test_indices.values())
        print(f"[anchor-bt] Precomputed: train={len(train_idx)}, "
              f"test_full={len(test_all_idx)}, test_sampled={total_sampled}, "
              f"cached={total_cached}, {elapsed:.1f}s")

    def evaluate_params(
        self,
        params: dict[str, Any],
        *,
        initial_capital: float = 100_000.0,
    ) -> dict[str, Any]:
        top_k = int(params.get("top_k", 30))
        min_sim = float(params.get("min_similarity", 0.4))
        temperature = float(params.get("temperature", 10.0))
        max_hold = int(params.get("max_holding_days", 5))
        max_new_buys = int(params.get("max_new_buys", 3))
        per_name_cap = float(params.get("per_name_cap_fraction", 0.15))
        buy_blend = float(params.get("buy_dist_blend", 0.5))
        sell_blend = float(params.get("sell_dist_blend", 0.5))
        fallback_sell = float(params.get("fallback_min_sell_markup", 0.003))

        use_skew = params.get("use_skew_adjust") in (True, "true", "True")
        skew_damp = float(params.get("skew_dampener", 0.2))
        use_ess = params.get("use_ess_tightening") in (True, "true", "True")
        ess_cap_val = float(params.get("ess_cap", 20.0))
        tighten_r = float(params.get("tighten_ratio", 0.3))
        use_sell_skew = params.get("use_sell_skew_adjust") in (True, "true", "True")
        sell_skew_fl = float(params.get("sell_skew_floor", 0.5))
        sell_skew_cl = float(params.get("sell_skew_ceil", 1.5))
        use_unc = params.get("use_uncertainty_discount") in (True, "true", "True")
        sell_unc_w = float(params.get("sell_unc_weight", 0.5))

        max_hold_clamp = min(max_hold, 10)
        fold_results = []

        for fold in self.folds:
            test_indices = self.sampled_test_indices.get(fold["fold_idx"], np.array([], dtype=int))
            if len(test_indices) == 0:
                continue

            date_groups: dict[str, list[int]] = defaultdict(list)
            for idx in test_indices:
                date_groups[self.dates[idx]].append(idx)

            capital = initial_capital
            positions: dict[str, dict] = {}
            trade_count = 0
            sell_count = 0
            equity_peak = capital

            for test_date in sorted(date_groups.keys()):
                anchors_today = date_groups[test_date]

                # --- Age + sell/exit ---
                to_remove = []
                for sym, pos in positions.items():
                    pos["days_held"] = pos.get("days_held", 0) + 1
                    d = pos["days_held"] - 1
                    aidx = pos["anchor_idx"]
                    fc = int(self.future_counts[aidx])

                    if d >= min(max_hold_clamp, fc):
                        exit_d = min(d - 1, fc - 1, 9)
                        exit_price = pos["ref_price"] * float(self.future_c[aidx, max(exit_d, 0)])
                        pnl = (exit_price - pos["buy_price"]) * pos["qty"]
                        capital += pos["cost"] + pnl
                        sell_count += 1
                        to_remove.append(sym)
                        continue

                    if d < 10 and d < fc:
                        if float(self.future_h[aidx, d]) >= pos["sell_ratio"]:
                            exit_price = pos["ref_price"] * pos["sell_ratio"]
                            pnl = (exit_price - pos["buy_price"]) * pos["qty"]
                            capital += pos["cost"] + pnl
                            sell_count += 1
                            to_remove.append(sym)

                for sym in to_remove:
                    del positions[sym]

                # --- Score candidates ---
                candidates = []
                held_syms = set(positions.keys())

                for idx in anchors_today:
                    sym = self.symbols[idx]
                    if sym in held_syms or idx not in self.sim_cache:
                        continue

                    sim_idx, sim_scores, cached_fl, cached_fh = self.sim_cache[idx]

                    # Filter by min_sim + top_k
                    mask = sim_scores >= min_sim
                    n_sel = min(int(mask.sum()), top_k)
                    if n_sel < 3:
                        continue

                    sel_scores = sim_scores[mask][:n_sel]
                    sel_fl = cached_fl[mask][:n_sel]  # (k, 10)
                    sel_fh = cached_fh[mask][:n_sel]  # (k, 10)

                    w = np.exp(temperature * (sel_scores - 1.0))
                    w_sum = w.sum()
                    if w_sum < 1e-12:
                        continue
                    w /= w_sum

                    # Vectorized: cumulative min/max through max_hold days
                    cum_min_lows = sel_fl[:, :max_hold_clamp].min(axis=1)   # (k,)
                    cum_max_highs = sel_fh[:, :max_hold_clamp].max(axis=1)  # (k,)

                    q10_low = _weighted_quantile(cum_min_lows, w, 0.10)
                    q50_low = _weighted_quantile(cum_min_lows, w, 0.50)
                    q90_high = _weighted_quantile(cum_max_highs, w, 0.90)
                    q50_high = _weighted_quantile(cum_max_highs, w, 0.50)

                    ess = float(1.0 / np.sum(w ** 2))
                    mean_low = float(np.dot(w, cum_min_lows))
                    disp_low = float(np.sqrt(np.dot(w, (cum_min_lows - mean_low) ** 2)))
                    lb_low = q10_low - disp_low / max(math.sqrt(ess), 1.0)

                    # Buy ratio
                    raw_buy = buy_blend * q10_low + (1.0 - buy_blend) * lb_low

                    if use_skew:
                        lower_sp = q50_low - q10_low
                        interval_w = max(q90_high - q10_low, 1e-6)
                        raw_buy *= (1.0 + skew_damp * (lower_sp / interval_w - 0.5))

                    if use_ess:
                        conf = min(math.log1p(max(ess, 0)) / max(math.log1p(ess_cap_val), 1e-6), 1.0)
                        raw_buy *= (1.0 - conf * tighten_r)

                    buy_ratio = max(min(raw_buy, 0.999), 0.85)

                    # Sell ratio
                    raw_sell = sell_blend * q90_high + (1.0 - sell_blend) * q50_high
                    if raw_sell <= 1.0:
                        raw_sell = 1.0 + fallback_sell

                    if use_sell_skew:
                        lower_sp2 = max(q50_low - q10_low, 1e-6)
                        upper_sp = max(q90_high - q50_high, 1e-6)
                        skew_r = upper_sp / lower_sp2
                        raw_sell = 1.0 + (raw_sell - 1.0) * max(min(skew_r, sell_skew_cl), sell_skew_fl)

                    if use_unc:
                        mean_high = float(np.dot(w, cum_max_highs))
                        disp_high = float(np.sqrt(np.dot(w, (cum_max_highs - mean_high) ** 2)))
                        unc = disp_high / max(math.sqrt(ess), 1.0)
                        raw_sell -= sell_unc_w * unc

                    sell_ratio = max(raw_sell, 1.0 + fallback_sell)

                    profit = sell_ratio - buy_ratio
                    if profit <= 0:
                        continue

                    candidates.append((profit, idx, buy_ratio, sell_ratio))

                candidates.sort(key=lambda x: -x[0])
                new_buys = 0

                for _, idx, buy_ratio, sell_ratio in candidates:
                    if new_buys >= max_new_buys:
                        break
                    sym = self.symbols[idx]
                    if sym in positions:
                        continue

                    ref_price = float(self.anchor_opens[idx])
                    buy_price = ref_price * buy_ratio
                    if buy_price <= 0:
                        continue
                    name_budget = capital * per_name_cap
                    qty = int(name_budget // buy_price)
                    if qty <= 0:
                        continue

                    # Check buy fill
                    fc = int(self.future_counts[idx])
                    filled_day = -1
                    for d in range(min(max_hold_clamp, fc, 10)):
                        if float(self.future_l[idx, d]) <= buy_ratio:
                            filled_day = d
                            break
                    if filled_day < 0:
                        continue

                    cost = buy_price * qty
                    if cost > capital * 0.95:
                        continue
                    capital -= cost
                    positions[sym] = {
                        "anchor_idx": idx,
                        "buy_price": buy_price,
                        "ref_price": ref_price,
                        "qty": qty,
                        "cost": cost,
                        "sell_ratio": sell_ratio,
                        "days_held": filled_day + 1,
                    }
                    trade_count += 1
                    new_buys += 1

                # Track equity
                total_value = capital
                for pos in positions.values():
                    d = min(pos.get("days_held", 1) - 1, 9,
                            int(self.future_counts[pos["anchor_idx"]]) - 1)
                    mark = pos["ref_price"] * float(self.future_c[pos["anchor_idx"], max(d, 0)])
                    total_value += mark * pos["qty"]
                equity_peak = max(equity_peak, total_value)

            # Close remaining
            for pos in positions.values():
                d = min(pos.get("days_held", 1) - 1, 9,
                        int(self.future_counts[pos["anchor_idx"]]) - 1)
                exit_price = pos["ref_price"] * float(self.future_c[pos["anchor_idx"], max(d, 0)])
                pnl = (exit_price - pos["buy_price"]) * pos["qty"]
                capital += pos["cost"] + pnl
                sell_count += 1

            fold_results.append({
                "fold_idx": fold["fold_idx"],
                "initial_capital": initial_capital,
                "final_equity": capital,
                "equity_ratio": capital / initial_capital,
                "trade_count": trade_count,
                "sell_count": sell_count,
                "max_drawdown": (equity_peak - capital) / max(equity_peak, 1.0),
            })

        if not fold_results:
            return {"objective": -1e9, "feasible": False,
                    "aggregate": {"equity_ratio_mean": 0, "trade_count": 0}, "folds": []}

        eq_ratios = [f["equity_ratio"] for f in fold_results]
        total_trades = sum(f["trade_count"] for f in fold_results)
        total_sells = sum(f["sell_count"] for f in fold_results)
        mean_eq = sum(eq_ratios) / len(eq_ratios)
        feasible = total_trades >= 10 and total_sells >= 5

        return {
            "objective": mean_eq if feasible else -1e9,
            "feasible": feasible,
            "aggregate": {
                "equity_ratio_mean": mean_eq,
                "trade_count": total_trades,
                "sell_count": total_sells,
                "fold_count": len(fold_results),
            },
            "folds": fold_results,
        }
