from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SqlAlchemySessionAdapter:
    session: Any


@dataclass
class ServiceAdapter:
    service: Any


@dataclass
class BrokerAdapter:
    client: Any
