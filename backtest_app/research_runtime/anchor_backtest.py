"""Anchor-table-based backtest engine for Optuna trials.

Option C: full numpy load + date-batch matmul + pre-cached futures + test sampling.
US/KR separate capital. Composite buy scoring. Ladder buy support.

Usage:
    engine = AnchorBacktestEngine.from_db()
    engine.precompute_similarities(test_start_date="2024-01-01")
    result = engine.evaluate_params(params)
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from typing import Any

import numpy as np
import psycopg2

DB_URL = "postgresql://postgres:change_me@127.0.0.1:5433/auto_trader_backtest"
KRW_PER_USD = 1500.0


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    sv = values[order]
    sw = weights[order]
    cdf = np.cumsum(sw)
    cdf /= cdf[-1]
    idx = int(np.searchsorted(cdf, q))
    return float(sv[min(idx, len(sv) - 1)])


class AnchorBacktestEngine:

    def __init__(self, symbols, dates, markets, anchor_opens, sim_matrix,
                 future_h, future_l, future_c, future_counts):
        self.symbols = symbols
        self.dates = dates
        self.markets = markets
        self.anchor_opens = anchor_opens
        self.sim_matrix = sim_matrix
        self.future_h = future_h
        self.future_l = future_l
        self.future_c = future_c
        self.future_counts = future_counts
        self.n = len(symbols)
        self.folds: list[dict] = []
        self.sim_cache: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
        self.sampled_test_indices: dict[int, np.ndarray] = {}

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
            SELECT symbol, anchor_date, market, anchor_open, sim_vector,
                   future_high_ratios, future_low_ratios, future_close_ratios, future_bar_count
            FROM bt_result.anchor_vector {where}
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

    def precompute_similarities(self, test_start_date="2024-01-01", n_folds=3,
                                 top_k_cache=50, sample_ratio=0.2, sample_seed=42):
        t0 = time.time()
        train_mask = self.dates < test_start_date
        test_mask = (self.dates >= test_start_date) & (self.future_counts >= 2)
        train_idx = np.where(train_mask)[0]
        test_all_idx = np.where(test_mask)[0]
        if len(train_idx) == 0 or len(test_all_idx) == 0:
            print("[anchor-bt] No train or test anchors.")
            return
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
            self.folds.append({"fold_idx": f, "test_indices": fold_test_idx, "train_indices": fold_train_idx})
        rng = np.random.RandomState(sample_seed)
        for fold in self.folds:
            f_test = fold["test_indices"]
            test_dates_in_fold = sorted(set(self.dates[f_test].tolist()))
            n_sample = max(1, int(len(test_dates_in_fold) * sample_ratio))
            sampled_dates = set(rng.choice(test_dates_in_fold, size=n_sample, replace=False).tolist())
            sampled_mask = np.array([self.dates[i] in sampled_dates for i in f_test])
            self.sampled_test_indices[fold["fold_idx"]] = f_test[sampled_mask]
        self.sim_cache = {}
        total_cached = 0
        for fold in self.folds:
            f_train = fold["train_indices"]
            f_test = self.sampled_test_indices[fold["fold_idx"]]
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
                sims = test_vecs @ train_vecs.T
                k = min(top_k_cache, sims.shape[1])
                for j, tidx in enumerate(test_idx_arr):
                    row_sims = sims[j]
                    top_local = np.argpartition(row_sims, -k)[-k:]
                    top_global = f_train[top_local]
                    top_scores = row_sims[top_local]
                    order = np.argsort(-top_scores)
                    sorted_global = top_global[order]
                    sorted_scores = top_scores[order]
                    self.sim_cache[int(tidx)] = (
                        sorted_global, sorted_scores,
                        self.future_l[sorted_global],
                        self.future_h[sorted_global],
                    )
                    total_cached += 1
                if (d_idx + 1) % 20 == 0 or d_idx == n_dates - 1:
                    elapsed = time.time() - t0
                    print(f"[anchor-bt] fold{fold['fold_idx']}: {d_idx+1}/{n_dates} dates, "
                          f"{total_cached} cached, {elapsed:.0f}s")
        elapsed = time.time() - t0
        total_sampled = sum(len(v) for v in self.sampled_test_indices.values())
        print(f"[anchor-bt] Precomputed: train={len(train_idx)}, test_full={len(test_all_idx)}, "
              f"test_sampled={total_sampled}, cached={total_cached}, {elapsed:.1f}s")

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    def evaluate_params(
        self,
        params: dict[str, Any],
        *,
        us_capital: float = 6667.0,
        kr_capital: float = 10_000_000.0,
    ) -> dict[str, Any]:
        # --- Parse params ---
        top_k = int(params.get("top_k", 30))
        min_sim = float(params.get("min_similarity", 0.4))
        temperature = float(params.get("temperature", 10.0))
        max_hold = min(int(params.get("max_holding_days", 5)), 10)
        max_new_buys = int(params.get("max_new_buys", 3))
        per_name_cap = float(params.get("per_name_cap_fraction", 0.15))
        buy_blend = float(params.get("buy_dist_blend", 0.5))
        sell_blend = float(params.get("sell_dist_blend", 0.5))
        fallback_sell = float(params.get("fallback_min_sell_markup", 0.003))
        sell_skew_fl = float(params.get("sell_skew_floor", 0.5))
        sell_skew_cl = float(params.get("sell_skew_ceil", 1.5))
        sell_unc_w = float(params.get("sell_unc_weight", 0.5))

        # Candidate scoring weights
        w_profit = float(params.get("w_profit", 1.0))
        w_sim = float(params.get("w_sim", 1.0))
        w_ess_score = float(params.get("w_ess_score", 0.5))
        w_width_penalty = float(params.get("w_width_penalty", 0.5))
        min_buy_score = float(params.get("min_buy_score", 0.0))

        # Ladder
        use_ladder = params.get("use_ladder_buy") in (True, "true", "True")
        ladder_legs = int(params.get("ladder_leg_count", 2))
        ladder_spread = float(params.get("ladder_spread", 0.01))

        # KR search pool
        kr_pool = str(params.get("kr_search_pool", "KR_ONLY"))

        fold_results = []

        for fold in self.folds:
            test_indices = self.sampled_test_indices.get(fold["fold_idx"], np.array([], dtype=int))
            if len(test_indices) == 0:
                continue

            date_groups: dict[str, list[int]] = defaultdict(list)
            for idx in test_indices:
                date_groups[self.dates[idx]].append(idx)

            # Separate capital and positions per market
            cap = {"US": us_capital, "KR": kr_capital}
            positions: dict[str, dict[str, dict]] = {"US": {}, "KR": {}}
            trade_pnls: dict[str, list[tuple[str, float]]] = {"US": [], "KR": []}
            trade_counts = {"US": 0, "KR": 0}
            sell_counts = {"US": 0, "KR": 0}
            equity_peaks = {"US": us_capital, "KR": kr_capital}
            # Equity snapshots for yearly returns
            equity_by_date: dict[str, dict[str, float]] = {}  # {date: {US: val, KR: val}}

            for test_date in sorted(date_groups.keys()):
                anchors_today = date_groups[test_date]

                for mkt in ("US", "KR"):
                    # --- Age + sell/exit ---
                    to_remove = []
                    for sym, pos in positions[mkt].items():
                        pos["days_held"] = pos.get("days_held", 0) + 1
                        d = pos["days_held"] - 1
                        aidx = pos["anchor_idx"]
                        fc = int(self.future_counts[aidx])

                        if d >= min(max_hold, fc):
                            exit_d = min(d - 1, fc - 1, 9)
                            exit_price = pos["ref_price"] * float(self.future_c[aidx, max(exit_d, 0)])
                            pnl = (exit_price - pos["avg_buy_price"]) * pos["qty"]
                            cap[mkt] += pos["cost"] + pnl
                            sell_counts[mkt] += 1
                            pnl_pct = (exit_price / pos["avg_buy_price"] - 1.0) if pos["avg_buy_price"] > 0 else 0.0
                            trade_pnls[mkt].append((test_date, pnl_pct))
                            to_remove.append(sym)
                            continue

                        if d < 10 and d < fc:
                            if float(self.future_h[aidx, d]) >= pos["sell_ratio"]:
                                exit_price = pos["ref_price"] * pos["sell_ratio"]
                                pnl = (exit_price - pos["avg_buy_price"]) * pos["qty"]
                                cap[mkt] += pos["cost"] + pnl
                                sell_counts[mkt] += 1
                                pnl_pct = (exit_price / pos["avg_buy_price"] - 1.0) if pos["avg_buy_price"] > 0 else 0.0
                                trade_pnls[mkt].append((test_date, pnl_pct))
                                to_remove.append(sym)

                    for sym in to_remove:
                        del positions[mkt][sym]

                    # --- Score candidates ---
                    mkt_anchors = [i for i in anchors_today if self.markets[i] == mkt]
                    candidates = []
                    held_syms = set(positions[mkt].keys())

                    for idx in mkt_anchors:
                        sym = self.symbols[idx]
                        if sym in held_syms or idx not in self.sim_cache:
                            continue

                        # KR search pool filtering
                        sim_idx, sim_scores, cached_fl, cached_fh = self.sim_cache[idx]

                        if mkt == "KR" and kr_pool == "KR_ONLY":
                            kr_mask = np.array([self.markets[si] == "KR" for si in sim_idx])
                            sim_idx = sim_idx[kr_mask]
                            sim_scores = sim_scores[kr_mask]
                            cached_fl = cached_fl[kr_mask]
                            cached_fh = cached_fh[kr_mask]

                        mask = sim_scores >= min_sim
                        n_sel = min(int(mask.sum()), top_k)
                        if n_sel < 3:
                            continue

                        sel_scores = sim_scores[mask][:n_sel]
                        sel_fl = cached_fl[mask][:n_sel]
                        sel_fh = cached_fh[mask][:n_sel]

                        w = np.exp(temperature * (sel_scores - 1.0))
                        w_sum = w.sum()
                        if w_sum < 1e-12:
                            continue
                        w /= w_sum

                        cum_min_lows = sel_fl[:, :max_hold].min(axis=1)
                        cum_max_highs = sel_fh[:, :max_hold].max(axis=1)

                        q10_low = _weighted_quantile(cum_min_lows, w, 0.10)
                        q50_low = _weighted_quantile(cum_min_lows, w, 0.50)
                        q90_high = _weighted_quantile(cum_max_highs, w, 0.90)
                        q50_high = _weighted_quantile(cum_max_highs, w, 0.50)

                        ess = float(1.0 / np.sum(w ** 2))
                        mean_low = float(np.dot(w, cum_min_lows))
                        disp_low = float(np.sqrt(np.dot(w, (cum_min_lows - mean_low) ** 2)))
                        lb_low = q10_low - disp_low / max(math.sqrt(ess), 1.0)
                        interval_width = max(q90_high - q10_low, 1e-6)

                        # Buy ratio
                        raw_buy = buy_blend * q10_low + (1.0 - buy_blend) * lb_low
                        buy_ratio = max(min(raw_buy, 0.999), 0.85)

                        # Sell ratio (FLAG C+D always ON)
                        raw_sell = sell_blend * q90_high + (1.0 - sell_blend) * q50_high
                        if raw_sell <= 1.0:
                            raw_sell = 1.0 + fallback_sell
                        lower_sp = max(q50_low - q10_low, 1e-6)
                        upper_sp = max(q90_high - q50_high, 1e-6)
                        skew_r = upper_sp / lower_sp
                        raw_sell = 1.0 + (raw_sell - 1.0) * max(min(skew_r, sell_skew_cl), sell_skew_fl)
                        mean_high = float(np.dot(w, cum_max_highs))
                        disp_high = float(np.sqrt(np.dot(w, (cum_max_highs - mean_high) ** 2)))
                        unc = disp_high / max(math.sqrt(ess), 1.0)
                        raw_sell -= sell_unc_w * unc
                        sell_ratio = max(raw_sell, 1.0 + fallback_sell)

                        expected_profit = sell_ratio - buy_ratio
                        if expected_profit <= 0:
                            continue

                        # Composite buy score
                        top_sim = float(sel_scores[0])
                        ess_norm = min(math.log1p(ess) / math.log1p(50.0), 1.0)
                        width_norm = min(interval_width / 0.3, 1.0)
                        profit_norm = min(expected_profit / 0.1, 1.0)

                        buy_score = (
                            w_profit * profit_norm
                            + w_sim * top_sim
                            + w_ess_score * ess_norm
                            - w_width_penalty * width_norm
                        )

                        if buy_score < min_buy_score:
                            continue

                        candidates.append({
                            "score": buy_score,
                            "idx": idx,
                            "buy_ratio": buy_ratio,
                            "sell_ratio": sell_ratio,
                        })

                    candidates.sort(key=lambda x: -x["score"])
                    new_buys = 0

                    for cand in candidates:
                        if new_buys >= max_new_buys:
                            break
                        idx = cand["idx"]
                        buy_ratio = cand["buy_ratio"]
                        sell_ratio = cand["sell_ratio"]
                        sym = self.symbols[idx]
                        if sym in positions[mkt]:
                            continue

                        ref_price = float(self.anchor_opens[idx])
                        if ref_price <= 0:
                            continue
                        name_budget = cap[mkt] * per_name_cap

                        fc = int(self.future_counts[idx])

                        if use_ladder:
                            # Ladder: multiple legs at decreasing prices
                            legs = []
                            total_qty = 0
                            total_cost = 0.0
                            weight_sum = sum(1.0 / (leg + 1) for leg in range(ladder_legs))
                            for leg in range(ladder_legs):
                                leg_ratio = buy_ratio - leg * ladder_spread
                                leg_price = ref_price * leg_ratio
                                if leg_price <= 0:
                                    continue
                                leg_fraction = (1.0 / (leg + 1)) / weight_sum
                                leg_budget = name_budget * leg_fraction
                                leg_qty = int(leg_budget // leg_price)
                                if leg_qty <= 0:
                                    continue
                                # Check fill
                                filled_day = -1
                                for d in range(min(max_hold, fc, 10)):
                                    if float(self.future_l[idx, d]) <= leg_ratio:
                                        filled_day = d
                                        break
                                if filled_day >= 0:
                                    legs.append({"qty": leg_qty, "price": leg_price, "day": filled_day})
                                    total_qty += leg_qty
                                    total_cost += leg_price * leg_qty

                            if total_qty <= 0:
                                continue
                            if total_cost > cap[mkt] * 0.95:
                                continue
                            avg_price = total_cost / total_qty
                            cap[mkt] -= total_cost
                            max_fill_day = max(leg["day"] for leg in legs)
                            positions[mkt][sym] = {
                                "anchor_idx": idx, "avg_buy_price": avg_price,
                                "ref_price": ref_price, "qty": total_qty,
                                "cost": total_cost, "sell_ratio": sell_ratio,
                                "days_held": max_fill_day + 1,
                            }
                        else:
                            # Single price buy
                            buy_price = ref_price * buy_ratio
                            if buy_price <= 0:
                                continue
                            qty = int(name_budget // buy_price)
                            if qty <= 0:
                                continue
                            filled_day = -1
                            for d in range(min(max_hold, fc, 10)):
                                if float(self.future_l[idx, d]) <= buy_ratio:
                                    filled_day = d
                                    break
                            if filled_day < 0:
                                continue
                            cost = buy_price * qty
                            if cost > cap[mkt] * 0.95:
                                continue
                            cap[mkt] -= cost
                            positions[mkt][sym] = {
                                "anchor_idx": idx, "avg_buy_price": buy_price,
                                "ref_price": ref_price, "qty": qty,
                                "cost": cost, "sell_ratio": sell_ratio,
                                "days_held": filled_day + 1,
                            }

                        trade_counts[mkt] += 1
                        new_buys += 1

                    # Track equity per market
                    day_equity = {}
                    for m in ("US", "KR"):
                        tv = cap[m]
                        for pos in positions[m].values():
                            d = min(pos.get("days_held", 1) - 1, 9,
                                    int(self.future_counts[pos["anchor_idx"]]) - 1)
                            mark = pos["ref_price"] * float(self.future_c[pos["anchor_idx"], max(d, 0)])
                            tv += mark * pos["qty"]
                        equity_peaks[m] = max(equity_peaks[m], tv)
                        day_equity[m] = tv
                    equity_by_date[test_date] = day_equity

            # Close remaining
            last_date = sorted(date_groups.keys())[-1] if date_groups else ""
            for mkt in ("US", "KR"):
                for pos in positions[mkt].values():
                    d = min(pos.get("days_held", 1) - 1, 9,
                            int(self.future_counts[pos["anchor_idx"]]) - 1)
                    exit_price = pos["ref_price"] * float(self.future_c[pos["anchor_idx"], max(d, 0)])
                    pnl = (exit_price - pos["avg_buy_price"]) * pos["qty"]
                    cap[mkt] += pos["cost"] + pnl
                    sell_counts[mkt] += 1
                    pnl_pct = (exit_price / pos["avg_buy_price"] - 1.0) if pos["avg_buy_price"] > 0 else 0.0
                    trade_pnls[mkt].append((last_date, pnl_pct))

            # Compute per-market stats
            mkt_stats = {}
            for mkt, init_cap in [("US", us_capital), ("KR", kr_capital)]:
                pnls = trade_pnls[mkt]
                wins = sum(1 for _, p in pnls if p > 0)
                win_rate = wins / len(pnls) if pnls else 0.0
                avg_trade_pnl = sum(p for _, p in pnls) / len(pnls) if pnls else 0.0

                # Yearly portfolio returns from equity snapshots
                yearly_returns = {}
                sorted_dates = sorted(equity_by_date.keys())
                if sorted_dates:
                    years = sorted(set(d[:4] for d in sorted_dates))
                    for yr in years:
                        yr_dates = [d for d in sorted_dates if d[:4] == yr]
                        if not yr_dates:
                            continue
                        # Year start: last equity before this year, or init_cap
                        prev_dates = [d for d in sorted_dates if d < yr_dates[0]]
                        start_eq = equity_by_date[prev_dates[-1]][mkt] if prev_dates else init_cap
                        end_eq = equity_by_date[yr_dates[-1]][mkt]
                        if start_eq > 0:
                            yearly_returns[yr] = (end_eq / start_eq) - 1.0

                mkt_stats[mkt] = {
                    "initial_capital": init_cap,
                    "final_equity": cap[mkt],
                    "equity_ratio": cap[mkt] / init_cap if init_cap > 0 else 0.0,
                    "trade_count": trade_counts[mkt],
                    "sell_count": sell_counts[mkt],
                    "win_rate": win_rate,
                    "avg_trade_pnl": avg_trade_pnl,
                    "yearly_returns": yearly_returns,
                    "max_drawdown": (equity_peaks[mkt] - cap[mkt]) / max(equity_peaks[mkt], 1.0),
                }

            # Combined equity (KRW)
            us_krw = cap["US"] * KRW_PER_USD
            kr_krw = cap["KR"]
            total_krw = us_krw + kr_krw
            init_krw = us_capital * KRW_PER_USD + kr_capital

            fold_results.append({
                "fold_idx": fold["fold_idx"],
                "us": mkt_stats["US"],
                "kr": mkt_stats["KR"],
                "combined_krw": {
                    "initial": init_krw,
                    "final": total_krw,
                    "equity_ratio": total_krw / init_krw if init_krw > 0 else 0.0,
                },
            })

        if not fold_results:
            return {"objective": -1e9, "feasible": False,
                    "aggregate": {}, "folds": []}

        # Aggregate across folds
        combined_ratios = [f["combined_krw"]["equity_ratio"] for f in fold_results]
        mean_combined = sum(combined_ratios) / len(combined_ratios)

        us_ratios = [f["us"]["equity_ratio"] for f in fold_results]
        kr_ratios = [f["kr"]["equity_ratio"] for f in fold_results]
        total_trades = sum(f["us"]["trade_count"] + f["kr"]["trade_count"] for f in fold_results)
        total_sells = sum(f["us"]["sell_count"] + f["kr"]["sell_count"] for f in fold_results)

        us_win_rates = [f["us"]["win_rate"] for f in fold_results if f["us"]["trade_count"] > 0]
        kr_win_rates = [f["kr"]["win_rate"] for f in fold_results if f["kr"]["trade_count"] > 0]
        all_win_rates = us_win_rates + kr_win_rates
        mean_win_rate = sum(all_win_rates) / len(all_win_rates) if all_win_rates else 0.0

        # Merge yearly
        all_yearly_us: dict[str, list[float]] = defaultdict(list)
        all_yearly_kr: dict[str, list[float]] = defaultdict(list)
        for f in fold_results:
            for yr, v in f["us"].get("yearly_returns", {}).items():
                all_yearly_us[yr].append(v)
            for yr, v in f["kr"].get("yearly_returns", {}).items():
                all_yearly_kr[yr].append(v)

        feasible = total_trades >= 10 and total_sells >= 5

        return {
            "objective": mean_combined if feasible else -1e9,
            "feasible": feasible,
            "aggregate": {
                "combined_equity_ratio": mean_combined,
                "us_equity_ratio": sum(us_ratios) / len(us_ratios) if us_ratios else 0,
                "kr_equity_ratio": sum(kr_ratios) / len(kr_ratios) if kr_ratios else 0,
                "trade_count": total_trades,
                "sell_count": total_sells,
                "win_rate": mean_win_rate,
                "us_yearly": {yr: sum(vs)/len(vs) for yr, vs in sorted(all_yearly_us.items())},
                "kr_yearly": {yr: sum(vs)/len(vs) for yr, vs in sorted(all_yearly_kr.items())},
                "fold_count": len(fold_results),
            },
            "folds": fold_results,
        }
