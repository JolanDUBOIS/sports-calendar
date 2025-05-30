from __future__ import annotations
from enum import IntEnum


class DataStage(IntEnum):
    LANDING = 0
    INTERMEDIATE = 1
    STAGING = 2
    PRODUCTION = 3

    @classmethod
    def from_str(cls, name: str) -> DataStage:
        try:
            return cls[name.strip().upper()]
        except KeyError:
            raise ValueError(f"Invalid DataStage name: {name}")
