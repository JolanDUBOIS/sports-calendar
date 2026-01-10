from __future__ import annotations
from typing import Callable, Type

import pandas as pd

from . import logger
from .specs import (
    SelectionSpec,
    SelectionItemSpec,
    SelectionFilterSpec,
    MinRankingFilterSpec,
    StageFilterSpec,
    TeamsFilterSpec,
    CompetitionsFilterSpec,
    SessionFilterSpec
)
from sports_calendar.core.db import (
    FootballMatchesTable,
    FootballStandingsTable,
    FootballTeamsTable,
    FootballCompetitionsTable,
    F1EventsTable,
    Filter,
    BaseTable,
    TableView
)
from sports_calendar.core.utils import validate


SPORT_TABLES: dict[str, dict[str, type[BaseTable]]] = {
    "football": {
        "events": FootballMatchesTable,
        "standings": FootballStandingsTable,
        "teams": FootballTeamsTable,
        "competitions": FootballCompetitionsTable,
    },
    "f1": {
        "events": F1EventsTable,
    }
}

class SelectionItemApplier:
    """ Base class for applying selection filters to retrieve sports data. """
    _DISPATCH: dict[
        Type[SelectionFilterSpec],
        Callable[..., TableView]
    ] = {}

    @classmethod
    def apply(cls, item: SelectionItemSpec) -> TableView:
        """ TODO """
        logger.debug(f"Applying selection for sport: {item.sport} with items: {item}")
        validate(item.sport in SPORT_TABLES, f"Unsupported sport: {item.sport}", logger)

        table = SPORT_TABLES[item.sport]["events"].view()
        for filter_spec in item.filters:
            apply_method = cls._DISPATCH.get(type(filter_spec))
            validate(apply_method is not None, f"No apply method for filter type: {type(filter_spec)}", logger)

            logger.debug(f"Applying filter: {filter_spec} using method: {apply_method.__name__}")
            table = apply_method(filter_spec, table, sport=item.sport)
            logger.debug(f"Table shape after applying filter: {table._df.shape}")

        return table

    # Min Ranking Filter

    @classmethod
    def _apply_min_ranking_filter(cls, filter_spec: MinRankingFilterSpec, table: TableView, sport: str, **kwargs) -> TableView:
        standings_table = SPORT_TABLES[sport]["standings"].view()
        valid_teams = standings_table.query(
            (Filter(col="competition_id", op="in", value=filter_spec.competition_ids) &
            Filter(col="ranking", op="<=", value=filter_spec.ranking))
        ).values("team_id")
        return getattr(cls, f'_apply_min_ranking_{filter_spec.rule}')(filter_spec, table, valid_teams)

    @staticmethod
    def _apply_min_ranking_both(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        return table.query(
            (Filter(col="home_team_id", op="in", value=valid_teams.tolist()) &
             Filter(col="away_team_id", op="in", value=valid_teams.tolist()))
        )

    @staticmethod
    def _apply_min_ranking_any(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        return table.query(
            (Filter(col="home_team_id", op="in", value=valid_teams.tolist()) |
             Filter(col="away_team_id", op="in", value=valid_teams.tolist()))
        )

    @staticmethod
    def _apply_min_ranking_opponent(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        return table.query(
            ((Filter(col="home_team_id", op="==", value=filter_spec.reference_team) &
              Filter(col="away_team_id", op="in", value=valid_teams.tolist())) |
             (Filter(col="away_team_id", op="==", value=filter_spec.reference_team) &
              Filter(col="home_team_id", op="in", value=valid_teams.tolist())))
        )

    # Stage Filter

    @staticmethod
    def _apply_stage_filter(filter_spec: StageFilterSpec, table: TableView, **kwargs) -> TableView:
        return table.query(
            Filter(col="stage", op=">=", value=filter_spec.stage)
        )

    # Teams Filter

    @classmethod
    def _apply_teams_filter(cls, filter_spec: TeamsFilterSpec, table: TableView, **kwargs) -> TableView:
        return getattr(cls, f'_apply_teams_{filter_spec.rule}')(filter_spec, table)

    @staticmethod
    def _apply_teams_both(filter_spec: TeamsFilterSpec, table: TableView) -> TableView:
        return table.query(
            (Filter(col="home_team_id", op="in", value=filter_spec.team_ids) &
             Filter(col="away_team_id", op="in", value=filter_spec.team_ids))
        )

    @staticmethod
    def _apply_teams_any(filter_spec: TeamsFilterSpec, table: TableView) -> TableView:
        return table.query(
            (Filter(col="home_team_id", op="in", value=filter_spec.team_ids) |
             Filter(col="away_team_id", op="in", value=filter_spec.team_ids))
        )

    # Competitions Filter

    @staticmethod
    def _apply_competitions_filter(filter_spec: CompetitionsFilterSpec, table: TableView, **kwargs) -> TableView:
        return table.query(
            Filter(col="competition_id", op="in", value=filter_spec.competition_ids)
        )

    # Session Filter

    @staticmethod
    def _apply_session_filter(filter_spec: SessionFilterSpec, table: TableView, **kwargs) -> TableView:
        return table.query(
            Filter(col="session", op="in", value=filter_spec.sessions)
        )


SelectionItemApplier._DISPATCH = {
    MinRankingFilterSpec: SelectionItemApplier._apply_min_ranking_filter,
    StageFilterSpec: SelectionItemApplier._apply_stage_filter,
    TeamsFilterSpec: SelectionItemApplier._apply_teams_filter,
    CompetitionsFilterSpec: SelectionItemApplier._apply_competitions_filter,
    SessionFilterSpec: SelectionItemApplier._apply_session_filter,
}


class SelectionApplier:
    """ Class to apply selection specifications to retrieve sports data. """

    @classmethod
    def apply(cls, spec: SelectionSpec) -> dict[str, TableView]:
        """ Apply the selection specification and return the resulting data tables. """
        logger.info(f"Applying selection spec with {len(spec.items)} items.")
        result: dict[str, TableView] = {}

        for item in spec.items:
            logger.info(f"Processing selection item for sport: {item.sport}")
            table_view = SelectionItemApplier.apply(item)
            result[item.sport] = table_view
            logger.info(f"Retrieved table for sport: {item.sport} with shape: {table_view._df.shape}")

        return result
