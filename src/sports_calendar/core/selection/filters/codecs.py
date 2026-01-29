from enum import Enum
from typing import Protocol, TypeVar, Any

T = TypeVar("T")

class FieldCodec(Protocol[T]):
    def to_dict(self, value: T) -> Any: ...
    def from_dict(self, value: Any) -> T: ...


class IdentityCodec:
    def to_dict(self, value: Any) -> Any:
        return value

    def from_dict(self, value: Any) -> Any:
        return value


class IntCodec:
    def to_dict(self, value: int) -> Any:
        return value

    def from_dict(self, value: Any) -> int:
        return int(value)


class EnumCodec:
    def __init__(self, enum_cls: type[Enum]):
        self.enum_cls = enum_cls

    def to_dict(self, value: Enum) -> Any:
        return value.value if value is not None else None

    def from_dict(self, value: Any) -> Enum | None:
        return self.enum_cls(value) if value is not None else None
