from __future__ import annotations
from enum import IntEnum

from .selection import logger


class CompetitionStage(IntEnum):
    NULL = 0

    QUALIFYING_PLAY_IN = 1
    
    GROUP_STAGE = 2
    LEAGUE_STAGE = 2

    SECOND_ROUND = 3
    THIRD_ROUND = 4
    FOURTH_ROUND = 5
    FIFTH_ROUND = 6

    KNOCKOUT_ROUND_PLAYOFFS = 7

    ROUND_OF_64 = 8
    ROUND_OF_32 = 9
    ROUND_OF_16 = 10

    QUARTERFINALS = 11
    SEMIFINALS = 12
    THIRD_PLACE_MATCH = 13
    FINAL = 14

    @classmethod
    def from_str(cls, stage_str: str | None) -> CompetitionStage:
        if stage_str is None:
            return cls.NULL

        normalized = (
            stage_str.lower()
            .replace("phase", "stage")
            .replace("3rd", "third")
            .replace("-", "_")
            .replace(" ", "_")
            .upper()
        )
        try:
            return cls[normalized]
        except KeyError:
            return cls.NULL
