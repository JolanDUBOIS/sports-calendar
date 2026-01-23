from __future__ import annotations
from typing import Optional, Union
from dataclasses import dataclass, field

from . import logger


@dataclass
class EmptyFilterDTO:
    @classmethod
    def from_payload(self, payload: dict) -> EmptyFilterDTO:
        return EmptyFilterDTO()


@dataclass
class MinRankingFilterDTO:
    rule: Optional[str] = None
    ranking: Optional[int] = None
    competition_ids: list[int] = field(default_factory=list)
    reference_team: Optional[str] = None

    @classmethod
    def from_payload(self, payload: dict) -> MinRankingFilterDTO:
        return MinRankingFilterDTO(
            rule=payload.get("rule"),
            ranking=payload.get("ranking"),
            competition_ids=payload.get("competition_ids", []),
            reference_team=payload.get("reference_team"),
        )


@dataclass
class StageFilterDTO:
    stage: Optional[int] = None
    competition_ids: list[int] = field(default_factory=list)

    @classmethod
    def from_payload(self, payload: dict) -> StageFilterDTO:
        return StageFilterDTO(
            stage=payload.get("stage"),
            competition_ids=payload.get("competition_ids", []),
        )


@dataclass
class TeamsFilterDTO:
    team_ids: list[int] = field(default_factory=list)
    rule: Optional[str] = None

    @classmethod
    def from_payload(self, payload: dict) -> TeamsFilterDTO:
        return TeamsFilterDTO(
            team_ids=payload.get("team_ids", []),
            rule=payload.get("rule"),
        )


@dataclass
class CompetitionsFilterDTO:
    competition_ids: list[int] = field(default_factory=list)

    @classmethod
    def from_payload(self, payload: dict) -> CompetitionsFilterDTO:
        return CompetitionsFilterDTO(
            competition_ids=payload.get("competition_ids", []),
        )


@dataclass
class SessionFilterDTO:
    sessions: list[str] = field(default_factory=list)

    @classmethod
    def from_payload(self, payload: dict) -> SessionFilterDTO:
        return SessionFilterDTO(
            sessions=payload.get("sessions", []),
        )

FilterDTO = Union[
    EmptyFilterDTO,
    MinRankingFilterDTO,
    StageFilterDTO,
    TeamsFilterDTO,
    CompetitionsFilterDTO,
    SessionFilterDTO,
]

class FilterDTOFactory:
    mapping = {
        "empty": EmptyFilterDTO,
        "min_ranking": MinRankingFilterDTO,
        "stage": StageFilterDTO,
        "teams": TeamsFilterDTO,
        "competitions": CompetitionsFilterDTO,
        "session": SessionFilterDTO,
    }
    
    @staticmethod
    def from_payload(filter_type: str, payload: dict) -> FilterDTO:
        logger.debug(f"Creating DTO for filter type: {filter_type} with payload: {payload}")
        dto_class = FilterDTOFactory.mapping.get(filter_type)
        if not dto_class:
            logger.error(f"Unknown filter type for DTO: {filter_type}")
            raise ValueError(f"Unknown filter type: {filter_type}")
        try:
            return dto_class.from_payload(payload)
        except Exception:
            logger.exception(f"Error creating DTO for filter type: {filter_type} with payload: {payload}")
            raise
