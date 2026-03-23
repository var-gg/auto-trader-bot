from app.features.premarket.services.headline_risk_service import HeadlineRiskService


def test_buy_multiplier_increases_with_risk_and_confidence():
    svc = HeadlineRiskService.__new__(HeadlineRiskService)
    low = svc._buy_multiplier(regime_score=-10, risk_score=20, confidence=0.9)
    high = svc._buy_multiplier(regime_score=-80, risk_score=90, confidence=0.9)
    assert low >= 1.0
    assert high > low


def test_sell_multiplier_increases_when_regime_is_positive():
    svc = HeadlineRiskService.__new__(HeadlineRiskService)
    neutral = svc._sell_multiplier(regime_score=10, confidence=0.9)
    bull = svc._sell_multiplier(regime_score=85, confidence=0.9)
    assert neutral == 1.0
    assert bull > neutral


def test_fallback_is_neutral():
    svc = HeadlineRiskService.__new__(HeadlineRiskService)
    out = svc._fallback("llm_failed")
    assert out["risk_score"] == 0
    assert out["discount_multiplier"] == 1.0
    assert out["shock_type"] == "other"


def test_normalize_clamps_values():
    svc = HeadlineRiskService.__new__(HeadlineRiskService)
    out = svc._normalize({
        "regime_score": 999,
        "risk_score": 999,
        "confidence": 3.0,
        "shock_type": "WAR",
        "severity_band": "EXTREME",
        "ttl_minutes": 9999,
        "reason_short": "x",
    })
    assert out["regime_score"] == 100
    assert out["risk_score"] == 100
    assert 0.0 <= out["confidence"] <= 1.0
    assert out["shock_type"] == "war"
    assert out["discount_multiplier"] >= 1.0
    assert out["sell_markup_multiplier"] >= 1.0
