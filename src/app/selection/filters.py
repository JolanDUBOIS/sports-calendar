from abc import ABC, abstractmethod
from typing import Type

import pandas as pd

from . import logger
from ..data_access import StandingsTable
from ..competition_stages import CompetitionStage


class SelectionFilter(ABC):
    """ Base class for selection filters. """

    @abstractmethod
    def apply(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the filter to the matches DataFrame. """
        raise NotImplementedError("Subclasses must implement the apply method.")

class MinRankingFilter(SelectionFilter):
    """ Filter for minimum ranking. """

    def __init__(self, competition_ids: list[int], rule: str, ranking: int, reference_team: str | None = None):
        """ Initialize the MinRankingFilter with competition IDs, rule, and ranking. """
        self.competition_ids = competition_ids
        self.rule = rule
        self.ranking = ranking
        self.reference_team = reference_team

        valid_rules = ['both', 'any', 'opponent']
        if self.rule not in valid_rules:
            logger.error(f"Invalid rule '{self.rule}' for MinRankingFilter. Must be one of {valid_rules}.")
            raise ValueError(f"Invalid rule '{self.rule}' for MinRankingFilter. Must be one of {valid_rules}.")

    def apply(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the minimum ranking filter to the matches DataFrame. """
        return getattr(self, f'_apply_{self.rule}')(matches)

    def _apply_both(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the 'both' rule for minimum ranking. """
        valid_teams = self._get_valid_teams()
        return matches[(matches['home_team_id'].isin(valid_teams)) & (matches['away_team_id'].isin(valid_teams))]

    def _apply_any(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the 'any' rule for minimum ranking. """
        valid_teams = self._get_valid_teams()
        return matches[(matches['home_team_id'].isin(valid_teams)) | (matches['away_team_id'].isin(valid_teams))]

    def _apply_opponent(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the 'opponent' rule for minimum ranking. """
        if self.reference_team is None:
            logger.error("Reference team not provided in 'min_ranking' filter for opponent rule.")
            raise ValueError("Reference team not provided in 'min_ranking' filter for opponent rule.")
        valid_teams = self._get_valid_teams()
        return matches[((matches['home_team_id'] == self.reference_team) & matches['away_team_id'].isin(valid_teams)) |
                       ((matches['away_team_id'] == self.reference_team) & matches['home_team_id'].isin(valid_teams))]

    def _get_valid_teams(self) -> pd.DataFrame:
        """ Retrieve valid teams based on the minimum ranking from standings. """
        standings = StandingsTable.query(competition_ids=self.competition_ids)
        if standings.empty:
            logger.error(f"No standings found for competitions {self.competition_ids}.")
            raise ValueError(f"No standings found for competitions {self.competition_ids}.")
        if standings.duplicated(subset=['team_id']).any():
            logger.error("Standings contain duplicate team entries.")
            raise ValueError("Standings contain duplicate team entries.")
        logger.debug(f"Columns in standings: {standings.columns.tolist()}")
        return standings[standings['position'] <= self.ranking]['team_id'].unique()

class StageFilter(SelectionFilter):
    """ Filter for stages. """

    def __init__(self, stage: str):
        """ Initialize the StageFilter with a specific stage. """
        if stage is None:
            logger.error("Stage cannot be None in StageFilter.")
            raise ValueError("Stage cannot be None in StageFilter.")
        self.stage = CompetitionStage.from_str(stage)

    def apply(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the stage filter to the matches DataFrame. """
        logger.debug(f"Competition: {matches['competition_id'].unique()}")
        return matches[
            matches["stage"].apply(lambda s: CompetitionStage.from_str(s) >= self.stage)
        ]

class TeamsFilter(SelectionFilter):
    """ Filter for teams. """

    def __init__(self, team_ids: list[int], rule: str):
        """ Initialize the TeamsFilter with team IDs and a rule. """
        self.team_ids = team_ids
        self.rule = rule

        valid_rules = ['both', 'any']
        if self.rule not in valid_rules:
            logger.error(f"Invalid rule '{self.rule}' for TeamsFilter. Must be one of {valid_rules}.")
            raise ValueError(f"Invalid rule '{self.rule}' for TeamsFilter. Must be one of {valid_rules}.")

    def apply(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the teams filter to the matches DataFrame. """
        if self.rule == 'both':
            return matches[matches['home_team_id'].isin(self.team_ids) & matches['away_team_id'].isin(self.team_ids)]
        elif self.rule == 'any':
            return matches[matches['home_team_id'].isin(self.team_ids) | matches['away_team_id'].isin(self.team_ids)]
        else:
            logger.error(f"Unknown rule '{self.rule}' in TeamsFilter.")
            raise ValueError(f"Unknown rule '{self.rule}' in TeamsFilter.")

class CompetitionsFilter(SelectionFilter):
    """ Filter for competitions. """

    def __init__(self, competition_ids: list[int]):
        """ Initialize the CompetitionsFilter with competition IDs. """
        self.competition_ids = competition_ids

    def apply(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ Apply the competitions filter to the matches DataFrame. """
        return matches[matches['competition_id'].isin(self.competition_ids)]

class SelectionFilterFactory:
    """ Factory class to create selection filters based on type. """

    filter_mapping = {
        'min_ranking': MinRankingFilter,
        'stage': StageFilter,
        'teams': TeamsFilter,
        'competitions': CompetitionsFilter
    }

    @classmethod
    def create_filter(cls, filter_type: str, **kwargs) -> Type[SelectionFilter]:
        """ Create a filter based on the provided type and parameters. """
        logger.debug(f"Creating filter of type '{filter_type}'")
        if filter_type not in cls.filter_mapping:
            logger.error(f"Unknown filter type '{filter_type}'. Available types: {list(cls.filter_mapping.keys())}.")
            raise ValueError(f"Unknown filter type '{filter_type}'. Available types: {list(cls.filter_mapping.keys())}.")
        return cls.filter_mapping[filter_type](**kwargs)
