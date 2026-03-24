from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Type, TypeVar, Union, get_args, get_origin, get_type_hints

JsonScalar = Union[str, int, float, bool, None]
JsonValue = Union[JsonScalar, List["JsonValue"], Dict[str, "JsonValue"]]

T = TypeVar("T", bound="DomainModel")


def _encode(value: Any) -> JsonValue:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {k: _encode(v) for k, v in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(k): _encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    return value


def _strip_optional(tp: Any) -> Any:
    origin = get_origin(tp)
    if origin is Union:
        args = [a for a in get_args(tp) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return tp


def _decode(tp: Any, value: Any) -> Any:
    if value is None:
        return None

    tp = _strip_optional(tp)
    origin = get_origin(tp)

    if origin in (list, List):
        inner = get_args(tp)[0]
        return [_decode(inner, item) for item in value]

    if origin in (dict, Dict):
        key_tp, val_tp = get_args(tp)
        return {str(k) if key_tp is str else k: _decode(val_tp, v) for k, v in value.items()}

    if isinstance(tp, type) and issubclass(tp, Enum):
        return tp(value)

    if tp is datetime:
        return datetime.fromisoformat(value)

    if tp is date:
        return date.fromisoformat(value)

    if isinstance(tp, type) and is_dataclass(tp) and issubclass(tp, DomainModel):
        return tp.from_dict(value)

    return value


class DomainModel:
    """Serializable dataclass base for canonical domain types.

    Constraints:
    - no ORM / HTTP / broker payload coupling
    - JSON-serializable output only
    - pure Python reconstruction via from_dict
    """

    def to_dict(self) -> Dict[str, JsonValue]:
        if not is_dataclass(self):
            raise TypeError(f"{type(self).__name__} must be a dataclass")
        return {f.name: _encode(getattr(self, f.name)) for f in fields(self)}

    @classmethod
    def from_dict(cls: Type[T], data: Mapping[str, Any]) -> T:
        kwargs: Dict[str, Any] = {}
        type_hints = get_type_hints(cls)
        for f in fields(cls):
            field_type = type_hints.get(f.name, f.type)
            kwargs[f.name] = _decode(field_type, data.get(f.name))
        return cls(**kwargs)
