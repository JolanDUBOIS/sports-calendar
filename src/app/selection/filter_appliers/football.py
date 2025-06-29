from typing import Type

import pandas as pd

from . import logger
from .base import FilterApplier
from ...models.football import FootballStandingsTable
from ..filters import (
    SelectionFilter,
    MinRankingFilter,
    StageFilter,
    TeamsFilter,
    CompetitionsFilter
)
from ...competition_stages import CompetitionStage


class FootballFilterApplier(FilterApplier):
    """ Applies football-specific filters. """

    def _get_dispatch_map(self) -> dict[Type[SelectionFilter], callable]:
        """ Returns a mapping of filter types to their respective handler methods. """
        return {
            MinRankingFilter: self._apply_min_ranking_filter,
            StageFilter: self._apply_stage_filter,
            TeamsFilter: self._apply_teams_filter,
            CompetitionsFilter: self._apply_competitions_filter,
        }

    # Min Ranking Filter
    def _apply_min_ranking_filter(self, filter: MinRankingFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the MinRankingFilter to the DataFrame. """
        return getattr(self, f'_apply_min_ranking_{filter.rule}')(filter, matches)

    def _apply_min_ranking_both(self, filter: MinRankingFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the MinRankingFilter with 'both' rule to the DataFrame. """
        valid_teams = self._get_ranking_valid_teams(filter)
        return matches[(matches['home_team_id'].isin(valid_teams)) & (matches['away_team_id'].isin(valid_teams))]

    def _apply_min_ranking_any(self, filter: MinRankingFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the MinRankingFilter with 'any' rule to the DataFrame. """
        valid_teams = self._get_ranking_valid_teams(filter)
        return matches[(matches['home_team_id'].isin(valid_teams)) | (matches['away_team_id'].isin(valid_teams))]

    def _apply_min_ranking_opponent(self, filter: MinRankingFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the MinRankingFilter with 'opponent' rule to the DataFrame. """
        valid_teams = self._get_ranking_valid_teams(filter)
        return matches[
            ((matches['home_team_id'] == filter.reference_team) & matches['away_team_id'].isin(valid_teams)) |
            ((matches['away_team_id'] == filter.reference_team) & matches['home_team_id'].isin(valid_teams))
        ]

    def _get_ranking_valid_teams(self, filter: MinRankingFilter) -> pd.DataFrame:
        """ Retrieves valid teams based on the minimum ranking from standings. """
        standings = FootballStandingsTable.query(competition_ids=filter.competition_ids)
        if standings.empty:
            logger.error(f"No standings found for competition IDs: {filter.competition_ids}")
            raise ValueError(f"No standings found for competition IDs: {filter.competition_ids}")
        if standings.duplicated(subset=['team_id']).any():
            logger.error("Standings contain duplicate team IDs.")
            raise ValueError("Standings contain duplicate team IDs.")
        return standings[standings['ranking'] <= filter.ranking]['team_id'].unique()

    # Stage Filter
    def _apply_stage_filter(self, filter: StageFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the StageFilter to the DataFrame. """
        return matches[matches["stage"].apply(lambda s: CompetitionStage.from_str(s) >= filter.stage)]

    # Teams Filter
    def _apply_teams_filter(self, filter: TeamsFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the TeamsFilter to the DataFrame. """
        return getattr(self, f'_apply_teams_{filter.rule}')(filter, matches)

    def _apply_teams_both(self, filter: TeamsFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the TeamsFilter with 'both' rule to the DataFrame. """
        return matches[matches['home_team_id'].isin(filter.team_ids) & matches['away_team_id'].isin(filter.team_ids)]

    def _apply_teams_any(self, filter: TeamsFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the TeamsFilter with 'any' rule to the DataFrame. """
        return matches[matches['home_team_id'].isin(filter.team_ids) | matches['away_team_id'].isin(filter.team_ids)]

    # Competitions Filter
    def _apply_competitions_filter(self, filter: CompetitionsFilter, matches: pd.DataFrame) -> pd.DataFrame:
        """ Applies the CompetitionsFilter to the DataFrame. """
        return matches[matches['competition_id'].isin(filter.competition_ids)]
