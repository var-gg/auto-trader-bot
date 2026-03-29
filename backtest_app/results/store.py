from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backtest_app.results.sql_models import LiveMetricRecord, LiveRunRecord, LiveTradeRecord, ResearchMetricRecord, ResearchRunRecord, ResearchTradeRecord
from shared.domain.models import FillOutcome, FillStatus, OrderPlan


@dataclass
class JsonResultStore:
    output_dir: str
    namespace: str = "research"

    def save_blob(self, *, name: str, payload: Mapping[str, object]) -> str:
        out_dir = Path(self.output_dir) / self.namespace
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

    def save_run(self, *, run_id: str, plans: Iterable[OrderPlan], fills: Iterable[FillOutcome], summary: Mapping[str, object], diagnostics: Mapping[str, object] | None = None, manifest: Mapping[str, object] | None = None) -> str:
        resolved_diagnostics = dict(diagnostics or {})
        return self.save_blob(name=run_id, payload={"run_id": run_id, "namespace": self.namespace, "manifest": dict(manifest or {}), "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills], "summary": dict(summary), "diagnostics": resolved_diagnostics, "reproducibility": resolved_diagnostics.get("reproducibility")})


@dataclass
class SqlResultStore:
    db_url: str
    namespace: str = "research"

    def __post_init__(self):
        self.engine = create_engine(self.db_url, future=True)
        self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        if self.namespace == "live":
            self.RunRecord, self.TradeRecord, self.MetricRecord = LiveRunRecord, LiveTradeRecord, LiveMetricRecord
        else:
            self.RunRecord, self.TradeRecord, self.MetricRecord = ResearchRunRecord, ResearchTradeRecord, ResearchMetricRecord

    def save_run(self, *, run_key: str, scenario_id: str, strategy_id: str, strategy_mode: str, market: str, data_source: str, config_version: str, label_version: str, vector_version: str, initial_capital: float, params: Mapping[str, object], summary: Mapping[str, object], diagnostics: Mapping[str, object], plans: Iterable[OrderPlan], fills: Iterable[FillOutcome], snapshot_info: Mapping[str, object], manifest: Mapping[str, object] | None = None) -> int:
        plans = list(plans)
        fills = list(fills)
        with self.session_factory() as session:
            run = self.RunRecord(run_key=run_key, scenario_id=scenario_id, strategy_id=strategy_id, market=market, config_version=config_version, data_source=data_source, status="RUNNING", initial_capital=initial_capital, params_json={**dict(params), "namespace": self.namespace, "manifest": dict(manifest or {})}, summary_json={"status": "RUNNING", "namespace": self.namespace}, notes="started")
            session.add(run)
            session.flush()
            run_id = int(run.id)
            for plan in plans:
                matching_fills = [f for f in fills if f.plan_id == plan.plan_id]
                total_qty = sum(float(f.filled_quantity or 0) for f in matching_fills)
                avg_entry = _weighted_average([(float(f.average_fill_price or 0), float(f.filled_quantity or 0)) for f in matching_fills])
                session.add(self.TradeRecord(run_id=run_id, plan_key=plan.plan_id, ticker_id=plan.ticker_id, symbol=plan.symbol, side=plan.side.value, opened_at=plan.generated_at, closed_at=max((f.event_time for f in matching_fills), default=plan.generated_at), quantity=total_qty or float(plan.requested_quantity or 0), entry_price=avg_entry, exit_price=None, gross_pnl=None, net_pnl=None, return_pct=None, fill_status=_collapse_fill_status(matching_fills), trade_payload={"namespace": self.namespace, "manifest": dict(manifest or {}), "plan": plan.to_dict(), "fills": [f.to_dict() for f in matching_fills]}))
            session.add_all(_build_metrics(run_id=run_id, config_version=config_version, summary=summary, diagnostics=diagnostics, strategy_mode=strategy_mode, label_version=label_version, vector_version=vector_version, snapshot_info=snapshot_info, namespace=self.namespace, metric_cls=self.MetricRecord, manifest=manifest))
            run.status = "COMPLETED"
            run.finished_at = datetime.now(timezone.utc)
            run.summary_json = {**dict(summary), "namespace": self.namespace, "manifest": dict(manifest or {})}
            run.notes = "completed"
            session.commit()
            return run_id


def _collapse_fill_status(fills: list[FillOutcome]) -> str:
    if not fills:
        return FillStatus.UNFILLED.value
    statuses = {f.fill_status.value for f in fills}
    if FillStatus.FULL.value in statuses:
        return FillStatus.FULL.value
    if FillStatus.PARTIAL.value in statuses:
        return FillStatus.PARTIAL.value
    return next(iter(statuses), FillStatus.UNFILLED.value)


def _weighted_average(pairs: list[tuple[float, float]]) -> float | None:
    total_weight = sum(weight for _value, weight in pairs if weight > 0)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in pairs if weight > 0) / total_weight


def _build_metrics(*, run_id: int, config_version: str, summary: Mapping[str, object], diagnostics: Mapping[str, object], strategy_mode: str, label_version: str, vector_version: str, snapshot_info: Mapping[str, object], namespace: str, metric_cls, manifest: Mapping[str, object] | None = None) -> list:
    metrics = []
    for key, value in summary.items():
        metric_value = float(value) if isinstance(value, (int, float)) else None
        metric_text = None if metric_value is not None else json.dumps(value, ensure_ascii=False, default=str)
        metrics.append(metric_cls(run_id=run_id, metric_group="summary", metric_name=str(key), metric_value=metric_value, metric_text=metric_text, config_version=config_version, metric_payload={"namespace": namespace, "strategy_mode": strategy_mode, "manifest": dict(manifest or {})}))
    metrics.append(metric_cls(run_id=run_id, metric_group="run_meta", metric_name="snapshot_info", metric_text=json.dumps(dict(snapshot_info), ensure_ascii=False, default=str), config_version=config_version, metric_payload={"namespace": namespace, "label_version": label_version, "vector_version": vector_version, "manifest": dict(manifest or {})}))
    metrics.append(metric_cls(run_id=run_id, metric_group="diagnostics", metric_name="payload", metric_text="stored", config_version=config_version, metric_payload={"namespace": namespace, **dict(diagnostics)}))
    return metrics
