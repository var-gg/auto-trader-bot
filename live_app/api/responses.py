from __future__ import annotations


def wrap_trading_result(result, *, test_mode: bool, success_message: str):
    if not result:
        raise RuntimeError(f"{success_message} returned empty result")
    if result.get("message") == "시장 휴장":
        return {
            "status": "skipped",
            "message": "Market closed - no trading executed",
            "test_mode": test_mode,
            "data": result,
        }
    return {
        "status": "success",
        "message": success_message,
        "test_mode": test_mode,
        "data": result,
    }
