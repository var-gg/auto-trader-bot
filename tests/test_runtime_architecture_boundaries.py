from pathlib import Path


def test_live_runtime_does_not_import_research_validation_or_simulated_broker():
    root = Path("A:/vargg-workspace/30_trading/auto-trader-bot/live_runtime")
    banned = ["backtest_app.validation", "backtest_app.simulated_broker", "backtest_app.research_runtime", "backtest_app.research"]
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in banned:
            assert token not in text, f"{path} imports banned module {token}"


def test_research_and_live_json_roots_are_separate(tmp_path):
    from backtest_app.results.store import JsonResultStore
    rp = JsonResultStore(str(tmp_path), namespace="research").save_blob(name="r1", payload={"kind": "research"})
    lp = JsonResultStore(str(tmp_path), namespace="live").save_blob(name="l1", payload={"kind": "live"})
    assert "/research/" in rp.replace("\\", "/")
    assert "/live/" in lp.replace("\\", "/")
