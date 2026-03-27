from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_runtime_does_not_import_research_validation_or_simulated_broker_or_research_runtime():
    root = REPO_ROOT / "live_runtime"
    banned = ["backtest_app.validation", "backtest_app.simulated_broker", "backtest_app.research_runtime"]
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in banned:
            assert token not in text, f"{path} imports banned module {token}"


def test_research_runtime_engine_does_not_import_cli():
    engine_path = REPO_ROOT / "backtest_app" / "research_runtime" / "engine.py"
    text = engine_path.read_text(encoding="utf-8")
    assert "backtest_app.runner.cli" not in text


def test_research_runtime_has_no_legacy_back_imports_to_cli():
    for rel in [Path("backtest_app/research_runtime/service.py"), Path("backtest_app/research_runtime/backtest_runner.py")]:
        text = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "backtest_app.runner" not in text
        assert "legacy_cli" not in text


def test_research_and_live_json_roots_are_separate(tmp_path):
    from backtest_app.results.store import JsonResultStore
    rp = JsonResultStore(str(tmp_path), namespace="research").save_blob(name="r1", payload={"kind": "research"})
    lp = JsonResultStore(str(tmp_path), namespace="live").save_blob(name="l1", payload={"kind": "live"})
    assert "/research/" in rp.replace("\\", "/")
    assert "/live/" in lp.replace("\\", "/")
