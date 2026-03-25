from __future__ import annotations

import json
import os
import sys
from typing import Dict, List

PLACEHOLDER_ACCOUNT_VALUES = {"", "00000000", "00000000-00", "CHANGE_ME", "your_account", "your_account_number"}


def _is_placeholder(value: str) -> bool:
    value = (value or "").strip()
    return (not value) or value in PLACEHOLDER_ACCOUNT_VALUES or value.lower().startswith("your_")


def main() -> int:
    fields = {
        "KIS_APPKEY": os.getenv("KIS_APPKEY", "").strip(),
        "KIS_APPSECRET": os.getenv("KIS_APPSECRET", "").strip(),
        "KIS_CANO": os.getenv("KIS_CANO", "").strip(),
        "KIS_ACNT_PRDT_CD": os.getenv("KIS_ACNT_PRDT_CD", "").strip(),
        "KIS_VIRTUAL": os.getenv("KIS_VIRTUAL", "false").strip().lower(),
        "KIS_VIRTUAL_CANO": os.getenv("KIS_VIRTUAL_CANO", "").strip(),
    }

    errors: List[str] = []
    required = ["KIS_APPKEY", "KIS_APPSECRET", "KIS_CANO", "KIS_ACNT_PRDT_CD"]
    for key in required:
        if _is_placeholder(fields[key]):
            errors.append(f"{key} is missing or placeholder")

    if fields["KIS_VIRTUAL"] == "true" and _is_placeholder(fields["KIS_VIRTUAL_CANO"]):
        errors.append("KIS_VIRTUAL_CANO is missing or placeholder while KIS_VIRTUAL=true")

    report: Dict[str, object] = {
        "ok": not errors,
        "virtual": fields["KIS_VIRTUAL"] == "true",
        "checks": {
            "KIS_APPKEY": not _is_placeholder(fields["KIS_APPKEY"]),
            "KIS_APPSECRET": not _is_placeholder(fields["KIS_APPSECRET"]),
            "KIS_CANO": not _is_placeholder(fields["KIS_CANO"]),
            "KIS_ACNT_PRDT_CD": not _is_placeholder(fields["KIS_ACNT_PRDT_CD"]),
            "KIS_VIRTUAL_CANO": (not _is_placeholder(fields["KIS_VIRTUAL_CANO"])) if fields["KIS_VIRTUAL"] == "true" else None,
        },
        "errors": errors,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
