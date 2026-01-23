from abc import ABC, abstractmethod

from ..dtos import FilterDTO, EmptyFilterDTO, MinRankingFilterDTO, StageFilterDTO, TeamsFilterDTO, CompetitionsFilterDTO, SessionFilterDTO
from sports_calendar.core.competition_stages import CompetitionStage
from sports_calendar.core.selection import SelectionFilter, EmptyFilter, MinRankingFilter, StageFilter, TeamsFilter, CompetitionsFilter, SessionFilter


class FilterMapper(ABC):
    @classmethod
    @abstractmethod
    def from_dto(cls, dto: FilterDTO, original: SelectionFilter) -> SelectionFilter:
        """ Maps an original SelectionFilter to a new SelectionFilter based on the modifications provided in the FilterDTO. """
        raise NotImplementedError

class EmptyFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: EmptyFilterDTO, original: EmptyFilter) -> EmptyFilter:
        return EmptyFilter(
            sport=original.sport
        )

class MinRankingFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: MinRankingFilterDTO, original: MinRankingFilter) -> MinRankingFilter:
        return MinRankingFilter(
            sport=original.sport,
            rule=dto.rule or original.rule,
            ranking=dto.ranking or original.ranking,
            competition_ids=dto.competition_ids or original.competition_ids,
            reference_team=dto.reference_team or original.reference_team,
        )

class StageFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: StageFilterDTO, original: StageFilter) -> StageFilter:
        return StageFilter(
            sport=original.sport,
            stage=CompetitionStage(dto.stage) if dto.stage is not None else original.stage,
            competition_ids=dto.competition_ids or original.competition_ids,
        )

class TeamsFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: TeamsFilterDTO, original: TeamsFilter) -> TeamsFilter:
        return TeamsFilter(
            sport=original.sport,
            team_ids=dto.team_ids or original.team_ids,
            rule=dto.rule or original.rule,
        )

class CompetitionsFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: CompetitionsFilterDTO, original: CompetitionsFilter) -> CompetitionsFilter:
        return CompetitionsFilter(
            sport=original.sport,
            competition_ids=dto.competition_ids or original.competition_ids,
        )

class SessionFilterMapper(FilterMapper):
    @classmethod
    def from_dto(cls, dto: SessionFilterDTO, original: SessionFilter) -> SessionFilter:
        return SessionFilter(
            sport=original.sport,
            sessions=dto.sessions or original.sessions,
        )
