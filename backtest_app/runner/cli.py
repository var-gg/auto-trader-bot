from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaConstraintConfig, OptunaObjectiveConfig, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest
from backtest_app.db.local_session import create_backtest_session_factory
from backtest_app.db.local_write_session import create_backtest_write_session_factory
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.portfolio import build_portfolio_decisions
from backtest_app.quote_policy import compare_policy_ab
from backtest_app.research_runtime.calibration_cache import (
    build_study_cache_from_materialized_bundle,
    create_or_resume_bundle_run,
    derive_chunk_timeouts,
    export_materialized_bundle_artifacts,
    list_chunk_runs,
    materialize_query_feature_cache,
    materialize_train_snapshots,
    materialize_calibration_chunk,
)
from backtest_app.research_runtime.frozen_seed import (
    CALIBRATION_UNIVERSE_SEED_PROFILE,
    build_study_cache,
    build_preopen_signal_snapshot,
    load_optuna_replay_seed,
    write_preopen_signal_snapshot_artifacts,
)
from backtest_app.research_runtime import engine as research_engine
from backtest_app.research_runtime.service import execute_research_backtest, execute_research_study
from backtest_app.simulated_broker.engine import SimulatedBroker
from shared.domain.execution import build_order_plan_from_candidate

STRATEGY_MODES = ["legacy_event_window", "research_similarity_v1", "research_similarity_v2"]

_load_historical = research_engine.load_historical


def run_backtest(*args, **kwargs):
    research_engine.load_historical = _load_historical
    research_engine.create_backtest_session_factory = create_backtest_session_factory
    research_engine.LocalPostgresLoader = LocalPostgresLoader
    research_engine.build_portfolio_decisions = build_portfolio_decisions
    research_engine.compare_policy_ab = compare_policy_ab
    research_engine.build_order_plan_from_candidate = build_order_plan_from_candidate
    research_engine.SimulatedBroker = SimulatedBroker
    return research_engine.run_backtest(*args, **kwargs)


def _parse_symbols(raw: str) -> list[str]:
    return [s.strip() for s in str(raw or "").split(",") if s.strip()]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _append_arg(argv: list[str], flag: str, value) -> None:
    if value is None:
        return
    text = str(value)
    if text == "":
        return
    argv.extend([flag, text])


def _build_chunk_backtest_command(*, args, chunk_request: RunnerRequest, chunk_output_dir: Path, chunk_id: int = 0) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "backtest_app.runner.cli",
        "--mode",
        "build-calibration-chunk",
        "--scenario-id",
        chunk_request.scenario.scenario_id,
        "--market",
        chunk_request.scenario.market,
        "--start-date",
        chunk_request.scenario.start_date,
        "--end-date",
        chunk_request.scenario.end_date,
        "--symbols",
        ",".join(chunk_request.scenario.symbols),
        "--data-source",
        args.data_source,
        "--strategy-mode",
        args.strategy_mode,
        "--initial-capital",
        str(chunk_request.config.initial_capital),
        "--results-dir",
        str(chunk_output_dir),
        "--output",
        str(chunk_output_dir / "result.json"),
        "--calibration-bundle-run-id",
        str(getattr(args, "calibration_bundle_run_id", 0) or 0),
        "--calibration-bundle-key",
        str(getattr(args, "calibration_bundle_key", "") or ""),
        "--calibration-chunk-id",
        str(int(chunk_id or 0)),
    ]
    _append_arg(command, "--data", getattr(args, "data", ""))
    _append_arg(command, "--research-spec-json", getattr(args, "research_spec_json", ""))
    _append_arg(command, "--metadata-json", getattr(args, "metadata_json", ""))
    optional_pairs = [
        ("--feature-window-bars", getattr(args, "feature_window_bars", None)),
        ("--lookback-horizons", getattr(args, "lookback_horizons", "")),
        ("--horizon-days", getattr(args, "horizon_days", None)),
        ("--target-return-pct", getattr(args, "target_return_pct", None)),
        ("--stop-return-pct", getattr(args, "stop_return_pct", None)),
        ("--research-fee-bps", getattr(args, "research_fee_bps", None)),
        ("--research-slippage-bps", getattr(args, "research_slippage_bps", None)),
        ("--flat-return-band-pct", getattr(args, "flat_return_band_pct", None)),
        ("--feature-version", getattr(args, "feature_version", "")),
        ("--label-version", getattr(args, "label_version", "")),
        ("--memory-version", getattr(args, "memory_version", "")),
    ]
    for flag, value in optional_pairs:
        _append_arg(command, flag, value)
    return command


def _run_chunk_backtest_child(*, args, chunk_request: RunnerRequest, chunk_output_dir: Path, chunk_id: int = 0, soft_timeout_seconds: int = 20 * 60, hard_timeout_seconds: int = 60 * 60) -> dict[str, object]:
    chunk_output_dir.mkdir(parents=True, exist_ok=True)
    status_path = chunk_output_dir / "chunk_status.json"
    stdout_path = chunk_output_dir / "chunk_stdout.log"
    stderr_path = chunk_output_dir / "chunk_stderr.log"
    command = _build_chunk_backtest_command(args=args, chunk_request=chunk_request, chunk_output_dir=chunk_output_dir, chunk_id=chunk_id)
    started_at = _utcnow_iso()
    _write_json(
        status_path,
        {
            "status": "starting",
            "started_at": started_at,
            "scenario_id": chunk_request.scenario.scenario_id,
            "symbol_count": len(chunk_request.scenario.symbols),
            "symbols": list(chunk_request.scenario.symbols),
            "soft_timeout_seconds": soft_timeout_seconds,
            "hard_timeout_seconds": hard_timeout_seconds,
            "command": command,
        },
    )
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(command, cwd=str(Path(__file__).resolve().parents[2]), stdout=stdout_handle, stderr=stderr_handle)
        started_monotonic = time.monotonic()
        _write_json(
            status_path,
            {
                "status": "running",
                "started_at": started_at,
                "pid": process.pid,
                "scenario_id": chunk_request.scenario.scenario_id,
                "symbol_count": len(chunk_request.scenario.symbols),
                "symbols": list(chunk_request.scenario.symbols),
                "soft_timeout_seconds": soft_timeout_seconds,
                "hard_timeout_seconds": hard_timeout_seconds,
                "command": command,
            },
        )
        soft_warned = False
        while True:
            return_code = process.poll()
            if return_code is not None:
                status = "ok" if return_code == 0 else "failed"
                _write_json(
                    status_path,
                    {
                        "status": status,
                        "started_at": started_at,
                        "completed_at": _utcnow_iso(),
                        "pid": process.pid,
                        "return_code": return_code,
                        "scenario_id": chunk_request.scenario.scenario_id,
                        "symbol_count": len(chunk_request.scenario.symbols),
                        "symbols": list(chunk_request.scenario.symbols),
                        "soft_timeout_seconds": soft_timeout_seconds,
                        "hard_timeout_seconds": hard_timeout_seconds,
                        "command": command,
                    },
                )
                if return_code != 0:
                    raise RuntimeError(f"chunk child exited with return code {return_code}")
                break
            elapsed_seconds = time.monotonic() - started_monotonic
            if elapsed_seconds >= hard_timeout_seconds:
                process.kill()
                process.wait(timeout=10)
                _write_json(
                    status_path,
                    {
                        "status": "failed",
                        "started_at": started_at,
                        "completed_at": _utcnow_iso(),
                        "pid": process.pid,
                        "scenario_id": chunk_request.scenario.scenario_id,
                        "symbol_count": len(chunk_request.scenario.symbols),
                        "symbols": list(chunk_request.scenario.symbols),
                        "soft_timeout_seconds": soft_timeout_seconds,
                        "hard_timeout_seconds": hard_timeout_seconds,
                        "elapsed_seconds": elapsed_seconds,
                        "error": "chunk_hard_timeout",
                        "command": command,
                    },
                )
                raise TimeoutError(f"chunk hard timeout after {hard_timeout_seconds} seconds")
            _write_json(
                status_path,
                {
                    "status": "running",
                    "started_at": started_at,
                    "last_checked_at": _utcnow_iso(),
                    "pid": process.pid,
                    "scenario_id": chunk_request.scenario.scenario_id,
                    "symbol_count": len(chunk_request.scenario.symbols),
                    "symbols": list(chunk_request.scenario.symbols),
                    "soft_timeout_seconds": soft_timeout_seconds,
                    "hard_timeout_seconds": hard_timeout_seconds,
                    "elapsed_seconds": elapsed_seconds,
                    "soft_timeout_exceeded": soft_warned,
                    "command": command,
                },
            )
            if elapsed_seconds >= soft_timeout_seconds and not soft_warned:
                soft_warned = True
                _write_json(
                    status_path,
                    {
                        "status": "running",
                        "started_at": started_at,
                        "last_checked_at": _utcnow_iso(),
                        "pid": process.pid,
                        "scenario_id": chunk_request.scenario.scenario_id,
                        "symbol_count": len(chunk_request.scenario.symbols),
                        "symbols": list(chunk_request.scenario.symbols),
                        "soft_timeout_seconds": soft_timeout_seconds,
                        "hard_timeout_seconds": hard_timeout_seconds,
                        "elapsed_seconds": elapsed_seconds,
                        "soft_timeout_exceeded": True,
                        "command": command,
                    },
                )
            time.sleep(5)
    return {
        "status_path": str(status_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }


def _build_request(args) -> RunnerRequest:
    spec_payload = json.loads(args.research_spec_json) if args.research_spec_json else {}
    metadata_payload = json.loads(args.metadata_json) if args.metadata_json else {}
    if args.feature_window_bars is not None:
        spec_payload["feature_window_bars"] = args.feature_window_bars
    if args.lookback_horizons:
        spec_payload["lookback_horizons"] = [int(x.strip()) for x in args.lookback_horizons.split(",") if x.strip()]
    if args.horizon_days is not None:
        spec_payload["horizon_days"] = args.horizon_days
    if args.target_return_pct is not None:
        spec_payload["target_return_pct"] = args.target_return_pct
    if args.stop_return_pct is not None:
        spec_payload["stop_return_pct"] = args.stop_return_pct
    if args.research_fee_bps is not None:
        spec_payload["fee_bps"] = args.research_fee_bps
    if args.research_slippage_bps is not None:
        spec_payload["slippage_bps"] = args.research_slippage_bps
    if args.flat_return_band_pct is not None:
        spec_payload["flat_return_band_pct"] = args.flat_return_band_pct
    if args.feature_version:
        spec_payload["feature_version"] = args.feature_version
    if args.label_version:
        spec_payload["label_version"] = args.label_version
    if args.memory_version:
        spec_payload["memory_version"] = args.memory_version
    research_spec = ResearchExperimentSpec(**spec_payload) if spec_payload else None
    optuna_payload = json.loads(args.optuna_json) if args.optuna_json else {}
    if args.optuna_discovery_start:
        optuna_payload["discovery_start_date"] = args.optuna_discovery_start
    if args.optuna_discovery_end:
        optuna_payload["discovery_end_date"] = args.optuna_discovery_end
    if args.optuna_holdout_start:
        optuna_payload["holdout_start_date"] = args.optuna_holdout_start
    if args.optuna_holdout_end:
        optuna_payload["holdout_end_date"] = args.optuna_holdout_end
    if args.optuna_n_trials is not None:
        optuna_payload["n_trials"] = args.optuna_n_trials
    if args.optuna_pruner:
        optuna_payload["pruner"] = args.optuna_pruner
    if args.optuna_search_space_json:
        optuna_payload["search_space"] = json.loads(args.optuna_search_space_json)
    if args.optuna_search_mode:
        optuna_payload["mode"] = args.optuna_search_mode
    if args.seed_artifact_root:
        optuna_payload["seed_artifact_root"] = args.seed_artifact_root
    if args.optuna_policy_scope:
        optuna_payload["policy_scope"] = args.optuna_policy_scope
    if getattr(args, "optuna_seed_profile", ""):
        optuna_payload["seed_profile"] = args.optuna_seed_profile
    if args.optuna_seed_filter:
        optuna_payload["seed_filter"] = args.optuna_seed_filter
    if args.optuna_objective_metric:
        optuna_payload["objective_metric"] = args.optuna_objective_metric
    if getattr(args, "snapshot_cadence", ""):
        optuna_payload["snapshot_cadence"] = args.snapshot_cadence
    if getattr(args, "model_version", ""):
        optuna_payload["model_version"] = args.model_version
    if optuna_payload and "n_trials" not in optuna_payload and str(optuna_payload.get("mode") or "") == "frozen_seed_v1":
        optuna_payload["n_trials"] = 32
    if optuna_payload and "experiment_id" not in optuna_payload:
        optuna_payload["experiment_id"] = args.scenario_id
    if isinstance(optuna_payload.get("constraints"), dict):
        optuna_payload["constraints"] = OptunaConstraintConfig(**optuna_payload["constraints"])
    if isinstance(optuna_payload.get("objective"), dict):
        optuna_payload["objective"] = OptunaObjectiveConfig(**optuna_payload["objective"])
    optuna_cfg = OptunaSearchConfig(**optuna_payload) if optuna_payload else None
    return RunnerRequest(
        scenario=BacktestScenario(
            scenario_id=args.scenario_id,
            market=args.market,
            start_date=args.start_date,
            end_date=args.end_date,
            symbols=_parse_symbols(args.symbols),
        ),
        config=BacktestConfig(
            initial_capital=args.initial_capital,
            research_spec=research_spec,
            optuna=optuna_cfg,
            metadata=metadata_payload,
        ),
        output_path=args.output or None,
    )


def _raise_missing_legacy_snapshot(args) -> None:
    guidance = (
        "local-db + legacy_event_window requires a pre-materialized bt_event_window snapshot for the requested "
        f"scenario-id ({args.scenario_id}).\n"
        "Resolution:\n"
        "  1) If you want the mirror-only TOBE path, rerun with --strategy-mode research_similarity_v2.\n"
        "  2) If you need legacy parity, materialize the snapshot first, for example:\n"
        "     python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery --phase discovery --source-json runs\\legacy_discovery.json\n"
        "     python scripts/materialize_bt_event_window.py --scenario-id legacy_holdout --phase holdout --source-json runs\\legacy_holdout.json\n"
        "  3) Then rerun using one of those scenario ids.\n"
        "See docs/local-backtest-postgres.md and docs/research_run_protocol.md for the two supported local-db paths."
    )
    raise SystemExit(guidance)


def _load_json_payload(*, raw: str = "", path: str = "", default):
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    if raw:
        return json.loads(raw)
    return default


def _default_calibration_root(args) -> Path:
    if args.results_dir:
        return Path(args.results_dir)
    if args.output:
        return Path(args.output).parent
    return Path("runs") / "diagnostics" / "calibration_bundle" / args.scenario_id


def _resolve_calibration_bundle_context(args) -> dict:
    session_factory = create_backtest_session_factory()
    write_session_factory = create_backtest_write_session_factory()
    loader = LocalPostgresLoader(session_factory)
    requested_symbols = _parse_symbols(args.symbols)
    if not requested_symbols or requested_symbols == ["ALL"] or requested_symbols == ["*"]:
        requested_symbols = loader.list_tradable_symbols(market=args.market)
    start_date = args.start_date or None
    end_date = args.end_date or None
    if not start_date or not end_date:
        available_start, available_end = loader.available_date_range(symbols=requested_symbols)
        start_date = start_date or available_start
        end_date = end_date or available_end
    if not requested_symbols:
        raise SystemExit("No tradable symbols found for calibration workflow")
    if not start_date or not end_date:
        raise SystemExit("Unable to resolve calibration date range from local mirror")
    args.start_date = start_date
    args.end_date = end_date
    args.symbols = ",".join(requested_symbols)
    request = _build_request(args)
    output_root = _default_calibration_root(args)
    output_root.mkdir(parents=True, exist_ok=True)
    policy_scope = args.optuna_policy_scope or "directional_wide_only"
    snapshot_cadence = str(getattr(args, "snapshot_cadence", "") or "daily").strip().lower() or "daily"
    model_version = str(getattr(args, "model_version", "") or "").strip()
    bundle_key = str(getattr(args, "calibration_bundle_key", "") or "").strip() or (
        f"{args.scenario_id}_{args.market}_{start_date}_{end_date}_{policy_scope}_{snapshot_cadence}"
    )
    bundle_run = create_or_resume_bundle_run(
        session_factory=write_session_factory,
        bundle_key=bundle_key,
        market=args.market,
        strategy_mode=args.strategy_mode,
        policy_scope=policy_scope,
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
        proof_reference_run=getattr(args, "proof_reference_run", ""),
        start_date=start_date,
        end_date=end_date,
        chunk_size=max(1, int(getattr(args, "calibration_chunk_size", 10) or 10)),
        worker_count=max(1, int(getattr(args, "calibration_worker_count", 4) or 4)),
        universe_symbol_count=len(requested_symbols),
        snapshot_cadence=snapshot_cadence,
        model_version=model_version,
    )
    args.calibration_bundle_key = bundle_key
    args.calibration_bundle_run_id = int(bundle_run["bundle_run_id"])
    return {
        "session_factory": session_factory,
        "write_session_factory": write_session_factory,
        "request": request,
        "start_date": start_date,
        "end_date": end_date,
        "symbols": requested_symbols,
        "policy_scope": policy_scope,
        "bundle_key": bundle_key,
        "bundle_run": bundle_run,
        "output_root": output_root,
        "snapshot_cadence": snapshot_cadence,
        "model_version": model_version,
    }


def _run_build_query_feature_cache_mode(args) -> dict:
    if args.data_source != "local-db":
        raise SystemExit("build-query-feature-cache requires --data-source local-db")
    if args.strategy_mode != "research_similarity_v2":
        raise SystemExit("build-query-feature-cache currently supports --strategy-mode research_similarity_v2 only")
    context = _resolve_calibration_bundle_context(args)
    request = context["request"]
    result = materialize_query_feature_cache(
        write_session_factory=context["write_session_factory"],
        bundle_run_id=int(context["bundle_run"]["bundle_run_id"]),
        market=args.market,
        start_date=context["start_date"],
        end_date=context["end_date"],
        symbols=context["symbols"],
        research_spec=request.config.research_spec,
        metadata=request.config.metadata,
    )
    return {
        "mode": "build-query-feature-cache",
        "status": result.get("status", "ok"),
        "bundle_run_id": int(context["bundle_run"]["bundle_run_id"]),
        "bundle_key": context["bundle_key"],
        "snapshot_cadence": context["snapshot_cadence"],
        "model_version": context["model_version"] or "daily_reuse_v1",
        **result,
    }


def _run_build_train_snapshots_mode(args) -> dict:
    if args.data_source != "local-db":
        raise SystemExit("build-train-snapshots requires --data-source local-db")
    if args.strategy_mode != "research_similarity_v2":
        raise SystemExit("build-train-snapshots currently supports --strategy-mode research_similarity_v2 only")
    context = _resolve_calibration_bundle_context(args)
    request = context["request"]
    snapshot_root = context["output_root"] / "train_snapshots"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    result = materialize_train_snapshots(
        write_session_factory=context["write_session_factory"],
        bundle_run_id=int(context["bundle_run"]["bundle_run_id"]),
        bundle_key=context["bundle_key"],
        market=args.market,
        start_date=context["start_date"],
        end_date=context["end_date"],
        symbols=context["symbols"],
        research_spec=request.config.research_spec,
        metadata=request.config.metadata,
        output_dir=str(snapshot_root),
        snapshot_cadence=context["snapshot_cadence"],
        model_version=context["model_version"],
    )
    return {
        "mode": "build-train-snapshots",
        "status": result.get("status", "ok"),
        "bundle_run_id": int(context["bundle_run"]["bundle_run_id"]),
        "bundle_key": context["bundle_key"],
        "snapshot_root": str(snapshot_root),
        **result,
    }


def _run_preopen_snapshot_mode(args) -> dict:
    seed_bundle = load_optuna_replay_seed(args.seed_artifact_root)
    rows = list(seed_bundle.get("rows") or [])
    symbols = set(_parse_symbols(args.symbols))
    if symbols:
        rows = [row for row in rows if str(row.get("symbol") or "") in symbols]
    policy_params = _load_json_payload(raw=args.policy_params_json, path=args.policy_params_path, default={})
    holdings = _load_json_payload(raw=args.holdings_json, path=args.holdings_path, default=[])
    as_of_date = args.as_of_date or args.start_date
    snapshot_payload = build_preopen_signal_snapshot(
        seed_rows=rows,
        as_of_date=as_of_date,
        policy_params=policy_params,
        available_cash=args.available_cash if args.available_cash is not None else args.initial_capital,
        holdings=holdings,
        policy_scope=args.optuna_policy_scope or "directional_wide_only",
        seed_profile=args.optuna_seed_profile or CALIBRATION_UNIVERSE_SEED_PROFILE,
        seed_filter=args.optuna_seed_filter,
    )
    artifact_root = args.results_dir or (str(Path(args.output).parent) if args.output else ".")
    artifact_paths = write_preopen_signal_snapshot_artifacts(output_dir=artifact_root, snapshot_payload=snapshot_payload)
    return {
        "mode": "preopen-snapshot",
        "as_of_date": as_of_date,
        "seed_artifact_root": args.seed_artifact_root,
        "snapshot": snapshot_payload,
        "artifacts": artifact_paths,
    }


def _run_build_calibration_chunk_mode(args) -> dict:
    if args.data_source != "local-db":
        raise SystemExit("build-calibration-chunk requires --data-source local-db")
    if args.strategy_mode != "research_similarity_v2":
        raise SystemExit("build-calibration-chunk currently supports --strategy-mode research_similarity_v2 only")
    if int(getattr(args, "calibration_bundle_run_id", 0) or 0) <= 0:
        raise SystemExit("build-calibration-chunk requires --calibration-bundle-run-id")
    request = _build_request(args)
    result = materialize_calibration_chunk(
        write_session_factory=create_backtest_write_session_factory(),
        bundle_run_id=int(args.calibration_bundle_run_id),
        chunk_id=int(getattr(args, "calibration_chunk_id", 0) or 0),
        market=args.market,
        scenario_id=request.scenario.scenario_id,
        start_date=request.scenario.start_date,
        end_date=request.scenario.end_date,
        symbols=list(request.scenario.symbols),
        strategy_mode=args.strategy_mode,
        policy_scope=args.optuna_policy_scope or "directional_wide_only",
        research_spec=request.config.research_spec,
        metadata=request.config.metadata,
        chunk_output_dir=args.results_dir or "",
    )
    return {
        "mode": "build-calibration-chunk",
        "status": result.get("status", "ok"),
        "bundle_run_id": int(args.calibration_bundle_run_id),
        "chunk_id": int(getattr(args, "calibration_chunk_id", 0) or 0),
        **result,
    }


def _run_build_calibration_bundle_mode(args) -> dict:
    if args.data_source != "local-db":
        raise SystemExit("build-calibration-bundle requires --data-source local-db")
    if args.strategy_mode != "research_similarity_v2":
        raise SystemExit("build-calibration-bundle currently supports --strategy-mode research_similarity_v2 only")
    context = _resolve_calibration_bundle_context(args)
    write_session_factory = context["write_session_factory"]
    template_request = context["request"]
    requested_symbols = list(context["symbols"])
    start_date = str(context["start_date"])
    end_date = str(context["end_date"])
    bundle_key = str(context["bundle_key"])
    bundle_run = dict(context["bundle_run"])
    policy_scope = str(context["policy_scope"])
    output_root = Path(context["output_root"])
    chunk_size = max(1, int(getattr(args, "calibration_chunk_size", 10) or 10))
    worker_count = max(1, int(getattr(args, "calibration_worker_count", 4) or 4))
    chunk_root = output_root / "chunks"
    output_root.mkdir(parents=True, exist_ok=True)
    chunk_root.mkdir(parents=True, exist_ok=True)
    live_source_chunks_path = output_root / "source_chunks.json"
    progress_path = output_root / "bundle_progress.json"
    source_chunks: list[dict[str, object]] = []
    total_chunks = (len(requested_symbols) + chunk_size - 1) // chunk_size if requested_symbols else 0
    existing_chunks = {int(row.get("chunk_id") or 0): row for row in list_chunk_runs(session_factory=write_session_factory, bundle_run_id=int(bundle_run["bundle_run_id"]))}

    def _persist_progress(*, status: str, phase: str, current_chunk_index: int = 0, current_symbols: list[str] | None = None, current_output_dir: str = "", last_error: str = "") -> None:
        payload = {
            "status": status,
            "phase": phase,
            "updated_at": _utcnow_iso(),
            "market": args.market,
            "strategy_mode": args.strategy_mode,
            "start_date": start_date,
            "end_date": end_date,
            "chunk_size": chunk_size,
            "worker_count": worker_count,
            "bundle_key": bundle_key,
            "bundle_run_id": int(bundle_run["bundle_run_id"]),
            "snapshot_cadence": str(context["snapshot_cadence"]),
            "model_version": str(context["model_version"] or "daily_reuse_v1"),
            "total_chunks": total_chunks,
            "completed_chunks": sum(1 for item in source_chunks if item.get("status") in {"ok", "reused"}),
            "failed_chunks": sum(1 for item in source_chunks if item.get("status") == "failed"),
            "current_chunk_index": current_chunk_index,
            "current_symbols": list(current_symbols or []),
            "current_output_dir": current_output_dir,
            "last_error": last_error,
            "proof_reference_run": getattr(args, "proof_reference_run", ""),
        }
        _write_json(progress_path, payload)
        _write_json(live_source_chunks_path, source_chunks)

    _persist_progress(status="running", phase="prepare")
    print(
        f"[build-calibration-bundle] start universe={len(requested_symbols)} "
        f"range={start_date}..{end_date} chunk_size={chunk_size} workers={worker_count} total_chunks={total_chunks}",
        flush=True,
    )
    timeout_cfg = derive_chunk_timeouts(
        session_factory=write_session_factory,
        bundle_run_id=int(bundle_run["bundle_run_id"]),
        fallback_soft_timeout_seconds=int(getattr(args, "calibration_soft_timeout_seconds", 10 * 60) or 10 * 60),
        fallback_hard_timeout_seconds=int(getattr(args, "calibration_hard_timeout_seconds", 30 * 60) or 30 * 60),
    )
    soft_timeout_seconds = int(timeout_cfg["soft_timeout_seconds"])
    hard_timeout_seconds = int(timeout_cfg["hard_timeout_seconds"])
    print(
        f"[build-calibration-bundle] timeout soft={soft_timeout_seconds}s hard={hard_timeout_seconds}s "
        f"bundle_run_id={int(bundle_run['bundle_run_id'])}",
        flush=True,
    )
    pending_jobs: list[dict[str, object]] = []
    for start_idx in range(0, len(requested_symbols), chunk_size):
        chunk_symbols = requested_symbols[start_idx : start_idx + chunk_size]
        chunk_index = (start_idx // chunk_size) + 1
        chunk_output_dir = chunk_root / f"chunk_{chunk_index:03d}"
        chunk_output_dir.mkdir(parents=True, exist_ok=True)
        chunk_request = RunnerRequest(
            scenario=replace(
                template_request.scenario,
                scenario_id=f"{args.scenario_id}_chunk_{chunk_index:03d}",
                start_date=start_date,
                end_date=end_date,
                symbols=list(chunk_symbols),
            ),
            config=template_request.config,
            output_path=None,
        )
        existing_chunk = existing_chunks.get(chunk_index) or {}
        if str(existing_chunk.get("status") or "") in {"ok", "reused"}:
            source_chunks.append(
                {
                    "chunk_index": chunk_index,
                    "status": "reused",
                    "scenario_id": chunk_request.scenario.scenario_id,
                    "symbol_count": len(chunk_symbols),
                    "symbols": list(chunk_symbols),
                    "start_date": start_date,
                    "end_date": end_date,
                    "output_dir": str(chunk_output_dir),
                    "row_count": int(existing_chunk.get("seed_row_count") or 0),
                    "replay_bar_count": int(existing_chunk.get("replay_bar_count") or 0),
                    "chunk_status_path": str(chunk_output_dir / "chunk_status.json"),
                    "chunk_stdout_path": str(chunk_output_dir / "chunk_stdout.log"),
                    "chunk_stderr_path": str(chunk_output_dir / "chunk_stderr.log"),
                    "reused_existing_output": True,
                    "completed_at": _utcnow_iso(),
                }
            )
            _persist_progress(
                status="running",
                phase="reuse_chunk",
                current_chunk_index=chunk_index,
                current_symbols=list(chunk_symbols),
                current_output_dir=str(chunk_output_dir),
            )
            print(
                f"[build-calibration-bundle] reused chunk {chunk_index}/{total_chunks} "
                f"rows={int(existing_chunk.get('seed_row_count') or 0)}",
                flush=True,
            )
            continue
        pending_jobs.append(
            {
                "chunk_index": chunk_index,
                "chunk_symbols": list(chunk_symbols),
                "chunk_output_dir": chunk_output_dir,
                "chunk_request": chunk_request,
            }
        )
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {}
        for job in pending_jobs:
            chunk_index = int(job["chunk_index"])
            chunk_symbols = list(job["chunk_symbols"])
            chunk_output_dir = Path(job["chunk_output_dir"])
            chunk_request = job["chunk_request"]
            _persist_progress(
                status="running",
                phase="run_chunk",
                current_chunk_index=chunk_index,
                current_symbols=chunk_symbols,
                current_output_dir=str(chunk_output_dir),
            )
            print(
                f"[build-calibration-bundle] chunk {chunk_index}/{total_chunks} start "
                f"symbols={len(chunk_symbols)} output={chunk_output_dir}",
                flush=True,
            )
            future = executor.submit(
                _run_chunk_backtest_child,
                args=args,
                chunk_request=chunk_request,
                chunk_output_dir=chunk_output_dir,
                chunk_id=chunk_index,
                soft_timeout_seconds=soft_timeout_seconds,
                hard_timeout_seconds=hard_timeout_seconds,
            )
            future_map[future] = job
        for future in as_completed(future_map):
            job = future_map[future]
            chunk_index = int(job["chunk_index"])
            chunk_symbols = list(job["chunk_symbols"])
            chunk_output_dir = Path(job["chunk_output_dir"])
            try:
                child_artifacts = future.result()
                result_path = chunk_output_dir / "result.json"
                chunk_result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
                source_chunks.append(
                    {
                        "chunk_index": chunk_index,
                        "status": "ok",
                        "scenario_id": job["chunk_request"].scenario.scenario_id,
                        "symbol_count": len(chunk_symbols),
                        "symbols": list(chunk_symbols),
                        "start_date": start_date,
                        "end_date": end_date,
                        "output_dir": str(chunk_output_dir),
                        "row_count": int(chunk_result.get("seed_row_count") or 0),
                        "replay_bar_count": int(chunk_result.get("replay_bar_count") or 0),
                        "chunk_status_path": child_artifacts.get("status_path"),
                        "chunk_stdout_path": child_artifacts.get("stdout_path"),
                        "chunk_stderr_path": child_artifacts.get("stderr_path"),
                        "completed_at": _utcnow_iso(),
                    }
                )
                _persist_progress(
                    status="running",
                    phase="chunk_complete",
                    current_chunk_index=chunk_index,
                    current_symbols=list(chunk_symbols),
                    current_output_dir=str(chunk_output_dir),
                )
                print(
                    f"[build-calibration-bundle] chunk {chunk_index}/{total_chunks} complete "
                    f"rows={int(chunk_result.get('seed_row_count') or 0)}",
                    flush=True,
                )
            except Exception as exc:
                source_chunks.append(
                    {
                        "chunk_index": chunk_index,
                        "status": "failed",
                        "scenario_id": f"{args.scenario_id}_chunk_{chunk_index:03d}",
                        "symbol_count": len(chunk_symbols),
                        "symbols": list(chunk_symbols),
                        "start_date": start_date,
                        "end_date": end_date,
                        "output_dir": str(chunk_output_dir),
                        "error": str(exc),
                        "chunk_status_path": str(chunk_output_dir / "chunk_status.json"),
                        "chunk_stdout_path": str(chunk_output_dir / "chunk_stdout.log"),
                        "chunk_stderr_path": str(chunk_output_dir / "chunk_stderr.log"),
                        "failed_at": _utcnow_iso(),
                    }
                )
                _persist_progress(
                    status="running",
                    phase="chunk_failed",
                    current_chunk_index=chunk_index,
                    current_symbols=chunk_symbols,
                    current_output_dir=str(chunk_output_dir),
                    last_error=str(exc),
                )
                print(
                    f"[build-calibration-bundle] chunk {chunk_index}/{total_chunks} failed error={exc}",
                    flush=True,
                )
    artifacts = export_materialized_bundle_artifacts(
        session_factory=write_session_factory,
        bundle_run_id=int(bundle_run["bundle_run_id"]),
        output_dir=str(output_root),
        policy_scope=policy_scope,
    )
    failed_chunk_count = sum(1 for item in source_chunks if item.get("status") == "failed")
    status = str(artifacts.get("status") or ("partial" if failed_chunk_count > 0 else "ok"))
    _persist_progress(status=status, phase="complete" if status != "failed" else "failed")
    print(
        f"[build-calibration-bundle] complete status={status} "
        f"failed_chunks={failed_chunk_count}",
        flush=True,
    )
    return {
        "mode": "build-calibration-bundle",
        "status": status,
        "market": args.market,
        "strategy_mode": args.strategy_mode,
        "start_date": start_date,
        "end_date": end_date,
        "universe_symbol_count": len(requested_symbols),
        "source_chunk_count": len(source_chunks),
        "failed_chunk_count": failed_chunk_count,
        "bundle_key": bundle_key,
        "bundle_run_id": int(bundle_run["bundle_run_id"]),
        "proof_reference_run": getattr(args, "proof_reference_run", ""),
        "artifacts": artifacts,
        "progress_path": str(progress_path),
    }


def _run_build_study_cache_mode(args) -> dict:
    cache_root = args.results_dir or ""
    if int(getattr(args, "calibration_bundle_run_id", 0) or 0) > 0 or str(getattr(args, "calibration_bundle_key", "") or "").strip():
        if not cache_root:
            cache_root = str(Path(args.seed_artifact_root or ".") / "study_cache") if args.seed_artifact_root else str(Path("study_cache"))
        artifacts = build_study_cache_from_materialized_bundle(
            session_factory=create_backtest_session_factory(),
            bundle_run_id=int(getattr(args, "calibration_bundle_run_id", 0) or 0) or None,
            bundle_key=str(getattr(args, "calibration_bundle_key", "") or ""),
            output_dir=cache_root,
            policy_scope=args.optuna_policy_scope or "directional_wide_only",
            seed_profile=args.optuna_seed_profile or CALIBRATION_UNIVERSE_SEED_PROFILE,
        )
    else:
        if not args.seed_artifact_root:
            raise SystemExit("build-study-cache requires --seed-artifact-root")
        artifacts = build_study_cache(
            seed_artifact_root=args.seed_artifact_root,
            output_dir=cache_root,
            policy_scope=args.optuna_policy_scope or "directional_wide_only",
            seed_profile=args.optuna_seed_profile or CALIBRATION_UNIVERSE_SEED_PROFILE,
            seed_filter=args.optuna_seed_filter,
        )
    manifest = dict(artifacts.get("study_cache_manifest") or {})
    return {
        "mode": "build-study-cache",
        "status": "ok",
        "seed_artifact_root": args.seed_artifact_root,
        "bundle_run_id": int(getattr(args, "calibration_bundle_run_id", 0) or 0),
        "bundle_key": str(getattr(args, "calibration_bundle_key", "") or ""),
        "policy_scope": args.optuna_policy_scope or "directional_wide_only",
        "seed_profile": args.optuna_seed_profile or CALIBRATION_UNIVERSE_SEED_PROFILE,
        "artifacts": artifacts,
        "row_count": int(manifest.get("row_count") or 0),
        "fold_count": len(manifest.get("folds") or []),
    }


def _validate_args(args) -> None:
    if not args.scenario_id:
        raise SystemExit("--scenario-id is required")
    if not args.market:
        raise SystemExit("--market is required")
    if args.mode in {"backtest", "optuna", "build-calibration-chunk"}:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date are required for backtest/optuna modes")
        if not _parse_symbols(args.symbols):
            raise SystemExit("--symbols is required for backtest/optuna modes")
    if args.mode == "preopen-snapshot" and not (args.as_of_date or args.start_date):
        raise SystemExit("--as-of-date or --start-date is required for preopen-snapshot mode")
    if args.mode == "build-study-cache" and not args.seed_artifact_root and int(getattr(args, "calibration_bundle_run_id", 0) or 0) <= 0 and not str(getattr(args, "calibration_bundle_key", "") or "").strip():
        raise SystemExit("--seed-artifact-root is required for build-study-cache mode")



def main() -> int:
    parser = argparse.ArgumentParser(description="Run backtest_app research runtime")
    parser.add_argument("--mode", choices=["backtest", "optuna", "preopen-snapshot", "build-query-feature-cache", "build-train-snapshots", "build-calibration-bundle", "build-calibration-chunk", "build-study-cache"], default="backtest")
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--symbols", default="", help="comma-separated or ALL")
    parser.add_argument("--data", default="")
    parser.add_argument("--data-source", choices=["json", "local-db"], default="json")
    parser.add_argument("--strategy-mode", choices=STRATEGY_MODES, default="legacy_event_window")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--output", default="")
    parser.add_argument("--results-dir", default="")
    parser.add_argument("--results-db-url", default="")
    parser.add_argument("--no-json-artifact", action="store_true")
    parser.add_argument("--research-spec-json", default="")
    parser.add_argument("--metadata-json", default="", help="JSON object for BacktestConfig.metadata (e.g. TOBE quote/portfolio overrides)")
    parser.add_argument("--feature-window-bars", type=int, default=None)
    parser.add_argument("--lookback-horizons", default="")
    parser.add_argument("--horizon-days", type=int, default=None)
    parser.add_argument("--target-return-pct", type=float, default=None)
    parser.add_argument("--stop-return-pct", type=float, default=None)
    parser.add_argument("--research-fee-bps", type=float, default=None)
    parser.add_argument("--research-slippage-bps", type=float, default=None)
    parser.add_argument("--flat-return-band-pct", type=float, default=None)
    parser.add_argument("--feature-version", default="")
    parser.add_argument("--label-version", default="")
    parser.add_argument("--memory-version", default="")
    parser.add_argument("--optuna-json", default="")
    parser.add_argument("--optuna-discovery-start", default="")
    parser.add_argument("--optuna-discovery-end", default="")
    parser.add_argument("--optuna-holdout-start", default="")
    parser.add_argument("--optuna-holdout-end", default="")
    parser.add_argument("--optuna-n-trials", type=int, default=None)
    parser.add_argument("--optuna-pruner", default="")
    parser.add_argument("--optuna-search-space-json", default="")
    parser.add_argument("--optuna-search-mode", default="")
    parser.add_argument("--optuna-policy-scope", default="")
    parser.add_argument("--optuna-seed-profile", default="")
    parser.add_argument("--optuna-seed-filter", default="")
    parser.add_argument("--optuna-objective-metric", default="")
    parser.add_argument("--seed-artifact-root", default="")
    parser.add_argument("--policy-params-json", default="")
    parser.add_argument("--policy-params-path", default="")
    parser.add_argument("--holdings-json", default="")
    parser.add_argument("--holdings-path", default="")
    parser.add_argument("--available-cash", type=float, default=None)
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--calibration-chunk-size", type=int, default=10)
    parser.add_argument("--calibration-worker-count", type=int, default=4)
    parser.add_argument("--calibration-bundle-key", default="")
    parser.add_argument("--calibration-bundle-run-id", type=int, default=0)
    parser.add_argument("--calibration-chunk-id", type=int, default=0)
    parser.add_argument("--calibration-soft-timeout-seconds", type=int, default=10 * 60)
    parser.add_argument("--calibration-hard-timeout-seconds", type=int, default=30 * 60)
    parser.add_argument("--proof-reference-run", default="")
    parser.add_argument("--snapshot-cadence", choices=["daily", "monthly"], default="daily")
    parser.add_argument("--model-version", default="")
    args = parser.parse_args()
    _validate_args(args)
    output_path = args.output or None
    try:
        if args.mode == "optuna":
            request = _build_request(args)
            output_path = request.output_path
            result = execute_research_study(request=request, data_path=args.data or None, output_dir=args.results_dir or None, data_source=args.data_source, strategy_mode=args.strategy_mode)
        elif args.mode == "preopen-snapshot":
            result = _run_preopen_snapshot_mode(args)
        elif args.mode == "build-query-feature-cache":
            result = _run_build_query_feature_cache_mode(args)
        elif args.mode == "build-train-snapshots":
            result = _run_build_train_snapshots_mode(args)
        elif args.mode == "build-calibration-bundle":
            result = _run_build_calibration_bundle_mode(args)
        elif args.mode == "build-calibration-chunk":
            result = _run_build_calibration_chunk_mode(args)
        elif args.mode == "build-study-cache":
            result = _run_build_study_cache_mode(args)
        else:
            request = _build_request(args)
            output_path = request.output_path
            result = execute_research_backtest(request, args.data or None, output_dir=args.results_dir or None, save_json=not args.no_json_artifact, sql_db_url=args.results_db_url or None, data_source=args.data_source, scenario_id=args.scenario_id, strategy_mode=args.strategy_mode)
    except ValueError as exc:
        message = str(exc)
        if args.data_source == "local-db" and args.strategy_mode == "legacy_event_window" and "No bt_event_window rows found for scenario_id=" in message:
            _raise_missing_legacy_snapshot(args)
        raise
    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if output_path:
        Path(output_path).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
