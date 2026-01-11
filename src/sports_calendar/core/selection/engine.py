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
    TableView,
    Filter,
    EventTableView,
    SportSchema,
    SPORT_SCHEMAS
)
from sports_calendar.core.utils import validate



class SelectionItemApplier:
    """ Base class for applying selection filters to retrieve sports data. """
    _DISPATCH: dict[
        Type[SelectionFilterSpec],
        Callable[..., TableView]
    ] = {}

    @classmethod
    def apply(cls, item: SelectionItemSpec) -> EventTableView:
        """ TODO """
        logger.debug(f"Applying selection for sport: {item.sport} with items: {item}")
        validate(item.sport in SPORT_SCHEMAS, f"Unsupported sport: {item.sport}", logger)
        schema = SPORT_SCHEMAS[item.sport]

        table_view = schema.events.view()
        logger.debug(f"SelectionItemApplier initial table view: {table_view}")
        for filter_spec in item.filters:
            apply_method = cls._DISPATCH.get(type(filter_spec))
            validate(apply_method is not None, f"No apply method for filter type: {type(filter_spec)}", logger)

            logger.debug(f"Applying filter: {filter_spec} using method: {apply_method.__name__}")
            table_view = apply_method(filter_spec, table_view, schema=schema)
            logger.debug(f"Table shape after applying filter: {table_view._df.shape}")
        
        return EventTableView(
            sport=item.sport,
            schema=schema,
            view=table_view
        )

    # Min Ranking Filter

    @classmethod
    def _apply_min_ranking_filter(cls, filter_spec: MinRankingFilterSpec, table: TableView, schema: SportSchema, **kwargs) -> TableView:
        logger.debug(f"Applying MinRankingFilterSpec: {filter_spec} on table: {table}")
        validate(schema.standings is not None, f"Standings table not defined for sport {schema.sport}", logger)
        # TODO - Maybe use the table competitions to check if the competition has standings (field has_standings)
        standings_table = schema.standings.view()
        valid_teams = standings_table.query(
            (Filter(col="competition_id", op="in", value=filter_spec.competition_ids) &
            Filter(col="position", op="<=", value=filter_spec.ranking))
        ).values("team_id")
        return getattr(cls, f'_apply_min_ranking_{filter_spec.rule}')(filter_spec, table, valid_teams)

    @staticmethod
    def _apply_min_ranking_both(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        logger.debug(f"Applying MinRankingFilterSpec BOTH with valid teams: {valid_teams.tolist()}")
        return table.query(
            (Filter(col="home_team_id", op="in", value=valid_teams.tolist()) &
             Filter(col="away_team_id", op="in", value=valid_teams.tolist()))
        )

    @staticmethod
    def _apply_min_ranking_any(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        logger.debug(f"Applying MinRankingFilterSpec ANY with valid teams: {valid_teams.tolist()}")
        return table.query(
            (Filter(col="home_team_id", op="in", value=valid_teams.tolist()) |
             Filter(col="away_team_id", op="in", value=valid_teams.tolist()))
        )

    @staticmethod
    def _apply_min_ranking_opponent(filter_spec: MinRankingFilterSpec, table: TableView, valid_teams: pd.Series) -> TableView:
        logger.debug(f"Applying MinRankingFilterSpec OPPONENT with valid teams: {valid_teams.tolist()}")
        return table.query(
            ((Filter(col="home_team_id", op="==", value=filter_spec.reference_team) &
              Filter(col="away_team_id", op="in", value=valid_teams.tolist())) |
             (Filter(col="away_team_id", op="==", value=filter_spec.reference_team) &
              Filter(col="home_team_id", op="in", value=valid_teams.tolist())))
        )

    # Stage Filter

    @staticmethod
    def _apply_stage_filter(filter_spec: StageFilterSpec, table: TableView, **kwargs) -> TableView:
        logger.debug(f"Applying StageFilterSpec: {filter_spec} on table: {table}")
        return table.query(
            Filter(col="stage", op=">=", value=filter_spec.stage)
        )

    # Teams Filter

    @classmethod
    def _apply_teams_filter(cls, filter_spec: TeamsFilterSpec, table: TableView, **kwargs) -> TableView:
        logger.debug(f"Applying TeamsFilterSpec: {filter_spec} on table: {table}")
        return getattr(cls, f'_apply_teams_{filter_spec.rule}')(filter_spec, table)

    @staticmethod
    def _apply_teams_both(filter_spec: TeamsFilterSpec, table: TableView) -> TableView:
        logger.debug(f"Applying TeamsFilterSpec BOTH with team IDs: {filter_spec.team_ids}")
        return table.query(
            (Filter(col="home_team_id", op="in", value=filter_spec.team_ids) &
             Filter(col="away_team_id", op="in", value=filter_spec.team_ids))
        )

    @staticmethod
    def _apply_teams_any(filter_spec: TeamsFilterSpec, table: TableView) -> TableView:
        logger.debug(f"Applying TeamsFilterSpec ANY with team IDs: {filter_spec.team_ids}")
        return table.query(
            (Filter(col="home_team_id", op="in", value=filter_spec.team_ids) |
             Filter(col="away_team_id", op="in", value=filter_spec.team_ids))
        )

    # Competitions Filter

    @staticmethod
    def _apply_competitions_filter(filter_spec: CompetitionsFilterSpec, table: TableView, **kwargs) -> TableView:
        logger.debug(f"Applying CompetitionsFilterSpec: {filter_spec} on table: {table}")
        return table.query(
            Filter(col="competition_id", op="in", value=filter_spec.competition_ids)
        )

    # Session Filter

    @staticmethod
    def _apply_session_filter(filter_spec: SessionFilterSpec, table: TableView, **kwargs) -> TableView:
        logger.debug(f"Applying SessionFilterSpec: {filter_spec} on table: {table}")
        return table.query(
            Filter(col="session_id", op="in", value=filter_spec.sessions)
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
    def apply(cls, spec: SelectionSpec) -> list[TableView]:
        """ Apply the selection specification and return the resulting data tables. """
        logger.info(f"Applying selection spec with {len(spec.items)} items.")
        result: list[TableView] = []

        for item in spec.items:
            logger.debug(f"Processing selection item: {item}")
            table_view = SelectionItemApplier.apply(item)
            result.append(table_view)
            logger.debug(f"Retrieved event table view with view: {table_view}")

        return result
