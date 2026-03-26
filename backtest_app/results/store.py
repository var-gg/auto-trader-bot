from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from backtest_app.results.sql_models import BacktestMetricRecord, BacktestRunRecord, BacktestTradeRecord
from shared.domain.models import FillOutcome, FillStatus, OrderPlan


@dataclass
class JsonResultStore:
    output_dir: str

    def save_run(self, *, run_id: str, plans: Iterable[OrderPlan], fills: Iterable[FillOutcome], summary: Mapping[str, object], diagnostics: Mapping[str, object] | None = None) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{run_id}.json"
        payload = {
            "run_id": run_id,
            "plans": [p.to_dict() for p in plans],
            "fills": [f.to_dict() for f in fills],
            "summary": dict(summary),
            "diagnostics": dict(diagnostics or {}),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)


@dataclass
class SqlResultStore:
    db_url: str

    def __post_init__(self):
        self.engine = create_engine(self.db_url, future=True)
        self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)

    def save_run(
        self,
        *,
        run_key: str,
        scenario_id: str,
        strategy_id: str,
        strategy_mode: str,
        market: str,
        data_source: str,
        config_version: str,
        label_version: str,
        vector_version: str,
        initial_capital: float,
        params: Mapping[str, object],
        summary: Mapping[str, object],
        diagnostics: Mapping[str, object],
        plans: Iterable[OrderPlan],
        fills: Iterable[FillOutcome],
        snapshot_info: Mapping[str, object],
    ) -> int:
        plans = list(plans)
        fills = list(fills)
        with self.session_factory() as session:
            run = BacktestRunRecord(
                run_key=run_key,
                scenario_id=scenario_id,
                strategy_id=strategy_id,
                market=market,
                config_version=config_version,
                data_source=data_source,
                status="RUNNING",
                initial_capital=initial_capital,
                params_json={
                    **dict(params),
                    "strategy_mode": strategy_mode,
                    "config_version": config_version,
                    "label_version": label_version,
                    "vector_version": vector_version,
                    "snapshot_info": dict(snapshot_info),
                    "scenario_id": scenario_id,
                    "git_commit": _git_commit(),
                },
                summary_json={"status": "RUNNING"},
                notes="started",
            )
            session.add(run)
            session.flush()
            run_id = int(run.id)
            try:
                for plan in plans:
                    matching_fills = [f for f in fills if f.plan_id == plan.plan_id]
                    total_qty = sum(float(f.filled_quantity or 0) for f in matching_fills)
                    avg_entry = _weighted_average([(float(f.average_fill_price or 0), float(f.filled_quantity or 0)) for f in matching_fills])
                    status = _collapse_fill_status(matching_fills)
                    session.add(
                        BacktestTradeRecord(
                            run_id=run_id,
                            plan_key=plan.plan_id,
                            ticker_id=plan.ticker_id,
                            symbol=plan.symbol,
                            side=plan.side.value,
                            opened_at=plan.generated_at,
                            closed_at=max((f.event_time for f in matching_fills), default=plan.generated_at),
                            quantity=total_qty or float(plan.requested_quantity or 0),
                            entry_price=avg_entry,
                            exit_price=None,
                            gross_pnl=None,
                            net_pnl=None,
                            return_pct=None,
                            fill_status=status,
                            trade_payload={
                                "plan": plan.to_dict(),
                                "fills": [f.to_dict() for f in matching_fills],
                            },
                        )
                    )

                metrics = _build_metrics(
                    run_id=run_id,
                    config_version=config_version,
                    summary=summary,
                    diagnostics=diagnostics,
                    strategy_mode=strategy_mode,
                    label_version=label_version,
                    vector_version=vector_version,
                    snapshot_info=snapshot_info,
                )
                session.add_all(metrics)

                run.status = "COMPLETED"
                run.finished_at = datetime.now(timezone.utc)
                run.summary_json = {**dict(summary), "diagnostics_present": bool(diagnostics), "trade_count": len(plans)}
                run.notes = "completed"
                session.commit()
                return run_id
            except Exception as exc:
                session.rollback()
                with self.session_factory() as fail_session:
                    failed = fail_session.get(BacktestRunRecord, run_id)
                    if failed is not None:
                        failed.status = "FAILED"
                        failed.finished_at = datetime.now(timezone.utc)
                        failed.summary_json = {**dict(summary), "error": str(exc)}
                        failed.notes = str(exc)
                        fail_session.commit()
                raise


def _collapse_fill_status(fills: list[FillOutcome]) -> str:
    if not fills:
        return FillStatus.UNFILLED.value
    statuses = {f.fill_status.value for f in fills}
    if FillStatus.FULL.value in statuses:
        return FillStatus.FULL.value
    if FillStatus.PARTIAL.value in statuses:
        return FillStatus.PARTIAL.value
    if FillStatus.REJECTED.value in statuses:
        return FillStatus.REJECTED.value
    if FillStatus.CANCELLED.value in statuses:
        return FillStatus.CANCELLED.value
    return FillStatus.UNFILLED.value


def _weighted_average(pairs: list[tuple[float, float]]) -> float | None:
    total_weight = sum(weight for _value, weight in pairs if weight > 0)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in pairs if weight > 0) / total_weight


def _git_commit() -> str | None:
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
        return proc.stdout.strip() or None
    except Exception:
        return None


def _build_metrics(
    *,
    run_id: int,
    config_version: str,
    summary: Mapping[str, object],
    diagnostics: Mapping[str, object],
    strategy_mode: str,
    label_version: str,
    vector_version: str,
    snapshot_info: Mapping[str, object],
) -> list[BacktestMetricRecord]:
    metrics: list[BacktestMetricRecord] = []
    for key, value in summary.items():
        metric_value = float(value) if isinstance(value, (int, float)) else None
        metric_text = None if metric_value is not None else json.dumps(value, ensure_ascii=False, default=str)
        metrics.append(
            BacktestMetricRecord(
                run_id=run_id,
                metric_group="summary",
                metric_name=str(key),
                metric_value=metric_value,
                metric_text=metric_text,
                config_version=config_version,
                metric_payload={"strategy_mode": strategy_mode},
            )
        )

    metrics.extend(
        [
            BacktestMetricRecord(
                run_id=run_id,
                metric_group="run_meta",
                metric_name="strategy_mode",
                metric_text=strategy_mode,
                config_version=config_version,
                metric_payload={"label_version": label_version, "vector_version": vector_version},
            ),
            BacktestMetricRecord(
                run_id=run_id,
                metric_group="run_meta",
                metric_name="snapshot_info",
                metric_text=json.dumps(dict(snapshot_info), ensure_ascii=False, default=str),
                config_version=config_version,
                metric_payload=dict(snapshot_info),
            ),
            BacktestMetricRecord(
                run_id=run_id,
                metric_group="diagnostics",
                metric_name="top_k_prototype_matches",
                metric_text="stored",
                config_version=config_version,
                metric_payload=dict(diagnostics),
            ),
        ]
    )
    return metrics
