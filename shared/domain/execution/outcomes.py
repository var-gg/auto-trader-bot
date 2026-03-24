from __future__ import annotations

from shared.domain.models import OutcomeLabel

FLAT_EPS_BPS = 1e-9


def label_outcome_from_pnl_bps(pnl_bps: float) -> OutcomeLabel:
    if pnl_bps > FLAT_EPS_BPS:
        return OutcomeLabel.WIN
    if pnl_bps < -FLAT_EPS_BPS:
        return OutcomeLabel.LOSS
    return OutcomeLabel.FLAT


def classify_unfilled_reason(error_code: str | None, error_message: str | None) -> dict[str, str] | None:
    code = (error_code or "").strip()
    msg = (error_message or "").strip()
    msg_upper = msg.upper()
    if not code and not msg:
        return None
    if "TIME" in msg_upper and "OUT" in msg_upper:
        return {"reason_code": "UNFILLED_TIMEOUT", "reason_text": msg}
    if "PRICE" in msg_upper and ("LIMIT" in msg_upper or "BAND" in msg_upper):
        return {"reason_code": "UNFILLED_PRICE_CONSTRAINT", "reason_text": msg}
    if "QTY" in msg_upper or "QUANTITY" in msg_upper:
        return {"reason_code": "UNFILLED_QUANTITY", "reason_text": msg}
    if code:
        return {"reason_code": code[:32], "reason_text": msg or code}
    return None
