from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


_MANIFEST_PATH = Path(__file__).with_name("schedule_manifest.json")


@dataclass(frozen=True)
class ScheduleManifest:
    raw: Dict[str, Any]

    @property
    def version(self) -> str:
        return str(self.raw.get("version") or "unknown")

    @property
    def slots(self) -> Dict[str, Any]:
        return dict(self.raw.get("slots") or {})

    @property
    def active_path(self) -> Dict[str, Any]:
        return dict(self.raw.get("active_path") or {})

    @property
    def lineage(self) -> Dict[str, Any]:
        return dict(self.raw.get("lineage") or {})

    def slot(self, slot_name: str) -> Dict[str, Any]:
        return dict(self.slots.get(slot_name) or {})


def load_schedule_manifest() -> ScheduleManifest:
    return ScheduleManifest(json.loads(_MANIFEST_PATH.read_text(encoding="utf-8")))
