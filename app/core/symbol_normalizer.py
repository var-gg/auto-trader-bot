"""Symbol normalization helpers for Yahoo/KIS format interoperability.

Canonical internal format in this codebase is Yahoo-style (e.g. ``BF-B``),
while KIS API expects slash style for some classes (e.g. ``BF/B``).
"""

from __future__ import annotations


def to_kis_symbol(symbol: str | None) -> str | None:
    """Convert internal symbol to KIS style.

    Interop rules:
    - canonical Yahoo class-share: BF-B / BRK-B
    - dot style class-share:      BF.B / BRK.B
    - KIS style class-share:      BF/B / BRK/B

    KIS expects slash style for class shares.
    """
    if symbol is None:
        return None
    s = symbol.strip()
    # dot/slash/hyphen class-share formats -> slash style
    return s.replace(".", "/").replace("-", "/")


def to_canonical_symbol(symbol: str | None) -> str | None:
    """Convert external symbol to canonical Yahoo-style.

    Examples:
    - BF/B -> BF-B
    - BF.B -> BF-B
    - BRK/B -> BRK-B
    - BRK.B -> BRK-B
    - AAPL -> AAPL
    """
    if symbol is None:
        return None
    s = symbol.strip()
    return s.replace("/", "-").replace(".", "-")
