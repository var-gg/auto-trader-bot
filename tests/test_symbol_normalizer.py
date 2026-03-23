from app.core.symbol_normalizer import to_canonical_symbol, to_kis_symbol


def test_to_kis_symbol_hyphen_class_share():
    assert to_kis_symbol("BF-B") == "BF/B"
    assert to_kis_symbol("BRK-B") == "BRK/B"


def test_to_kis_symbol_dot_class_share():
    assert to_kis_symbol("BF.B") == "BF/B"
    assert to_kis_symbol("BRK.B") == "BRK/B"


def test_to_canonical_symbol_slash_class_share():
    assert to_canonical_symbol("BF/B") == "BF-B"
    assert to_canonical_symbol("BRK/B") == "BRK-B"


def test_roundtrip_slash_style_symbol():
    # slash style from broker -> canonical -> broker should be stable
    original = "RDS/A"
    canonical = to_canonical_symbol(original)
    assert canonical == "RDS-A"
    assert to_kis_symbol(canonical) == original
