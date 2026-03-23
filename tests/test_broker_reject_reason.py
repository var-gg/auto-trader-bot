from app.features.trading_hybrid.repositories.order_repository import extract_reject_reason


def test_extract_reject_reason_from_kis_reject_payload():
    response = {
        "rt_cd": "1",
        "msg_cd": "OPSQ2001",
        "msg1": "주문가능수량이 부족합니다.",
        "output": {},
    }

    code, message = extract_reject_reason(response)

    assert code == "OPSQ2001"
    assert message == "주문가능수량이 부족합니다."


def test_extract_reject_reason_from_exception_like_payload():
    response = {"error": "KIS timeout"}

    code, message = extract_reject_reason(response, RuntimeError("KIS timeout"))

    assert code in {"EXCEPTION", "UNKNOWN_REJECT"}
    assert "timeout" in message.lower()
