from __future__ import annotations
from enum import IntEnum


class DataStage(IntEnum):
    LANDING = 0
    INTERMEDIATE = 1
    STAGING = 2
    # PRODUCTION = 3

    def __str__(self) -> str:
        return self.name.lower()

    def __repr__(self) -> str:
        return f"<DataStage.{self.name}: {self.value}>"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            try:
                return cls[value.strip().upper()]
            except KeyError:
                pass
        return super()._missing_(value)

    @classmethod
    def from_str(cls, name: str) -> DataStage:
        if isinstance(name, DataStage):
            return name
        try:
            return cls[name.strip().upper()]
        except KeyError:
            raise ValueError(f"Invalid DataStage name: {name}")

    @classmethod
    def as_list(cls) -> list[str]:
        return [stage.name.lower() for stage in cls]

    @classmethod
    def as_str(cls) -> str:
        return ", ".join(cls.as_list())

    @classmethod
    def is_valid_stage(cls, stage: str) -> bool:
        return stage.strip().upper() in cls.__members__

    @classmethod
    def instances(cls) -> list[DataStage]:
        return list(cls)
