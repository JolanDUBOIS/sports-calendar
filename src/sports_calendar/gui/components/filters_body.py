from nicegui import ui

from . import logger
from sports_calendar.core.db import SPORT_SCHEMAS, Filter
from sports_calendar.core.selection import (
    SelectionFilter,
    EmptyFilter,
    MinRankingFilter,
    StageFilter,
    TeamsFilter,
    CompetitionsFilter,
    SessionFilter,
)


def filter_body(filter: SelectionFilter):
    """ Render the appropriate UI for the specific filter. """
    render_function = DISPATCH_FILTER_BODY.get(filter.filter_type)
    if render_function:
        try:
            render_function(filter)
        except Exception:
            logger.exception(f"Error rendering filter body for filter type: {filter.filter_type}")
            with ui.column():
                ui.label('Error rendering filter details')
    else:
        with ui.column():
            ui.label('Unknown filter type')

def _empty_filter_body(filter: EmptyFilter):
    with ui.column():
        ui.label('Empty Filter')

def _min_ranking_filter_body(filter: MinRankingFilter):
    comp_df = SPORT_SCHEMAS[filter.sport].competitions.query(
        Filter(col="id", op="in", value=filter.competition_ids)
    ).select("id", "short_name").get()
    competitions = [comp["short_name"] for comp in comp_df.to_dict(orient="records")]

    if filter.reference_team is not None:
        team_df = SPORT_SCHEMAS[filter.sport].teams.query(
            Filter(col="id", op="==", value=filter.reference_team)
        ).select("id", "short_display_name").get()
        team_name = team_df.iloc[0]["short_display_name"] if not team_df.empty else None

    with ui.column():
        # Line 1 - Min Ranking Filter with Rule: {rule}, Ranking: {ranking}
        ui.label(f'Min Ranking Filter with Rule: {filter.rule}, Ranking: {filter.ranking}')
        # Line 2 - Competitions: {comp1}, {comp2}, ... # Max 5 competitions, then "and X more"
        if competitions:
            ui.label(f'Competitions: {_format_list_with_limit(competitions)}')
        else:
            ui.label('Competitions: None')
        # Line 3 - Reference Team: {team} (if applicable)
        if filter.reference_team is not None and team_name is not None:
            ui.label(f'Reference Team: {team_name}')

def _stage_filter_body(filter: StageFilter):
    comp_df = SPORT_SCHEMAS[filter.sport].competitions.query(
        Filter(col="id", op="in", value=filter.competition_ids)
    ).select("id", "short_name").get()
    competitions = [comp["short_name"] for comp in comp_df.to_dict(orient="records")]

    with ui.column():
        # Line 1 - Stage Filter with Stage: {stage.name}
        ui.label(f'Stage Filter with Stage: {filter.stage.name}')

        # Line 2 - Competitions: {comp1}, {comp2}, ... # Max 5 competitions, then "and X more"
        if competitions:
            ui.label(f'Competitions: {_format_list_with_limit(competitions)}')
        else:
            ui.label('Competitions: None')

def _teams_filter_body(filter: TeamsFilter):
    team_df = SPORT_SCHEMAS[filter.sport].teams.query(
        Filter(col="id", op="in", value=filter.team_ids)
    ).select("id", "short_display_name").get()
    teams = [team["short_display_name"] for team in team_df.to_dict(orient="records")]

    with ui.column():
        # Line 1 - Teams Filter with Rule: {rule}
        ui.label(f'Teams Filter with Rule: {filter.rule}')

        # Line 2 - Teams: {team1}, {team2}, ... # Max 5 teams, then "and X more"
        if teams:
            ui.label(f'Teams: {_format_list_with_limit(teams)}')
        else:
            ui.label('Teams: None')

def _competitions_filter_body(filter: CompetitionsFilter):
    comp_df = SPORT_SCHEMAS[filter.sport].competitions.query(
        Filter(col="id", op="in", value=filter.competition_ids)
    ).select("id", "short_name").get()
    competitions = [comp["short_name"] for comp in comp_df.to_dict(orient="records")]

    with ui.column():
        # Line 1 - Competitions Filter
        ui.label('Competitions Filter')

        # Line 2 - Competitions: {comp1}, {comp2}, ... # Max 5 competitions, then "and X more"
        if competitions:
            ui.label(f'Competitions: {_format_list_with_limit(competitions)}')
        else:
            ui.label('Competitions: None')

def _session_filter_body(filter: SessionFilter):
    with ui.column():
        # Line 1 - Session Filter with Sessions: {session1}, {session2}, ... # Max 5 sessions, then "and X more"
        ui.label(f'Session Filter with Sessions: {_format_list_with_limit(filter.sessions)}')

# Helpers

def _format_list_with_limit(items: list[str], limit: int = 5) -> str:
    if len(items) > limit:
        displayed_items = items[:limit]
        remaining = len(items) - limit
        return f'{", ".join(displayed_items)} and {remaining} more'
    else:
        return ', '.join(items)

# Dispatch dictionary

DISPATCH_FILTER_BODY = {
    "empty": _empty_filter_body,
    "min_ranking": _min_ranking_filter_body,
    "stage": _stage_filter_body,
    "teams": _teams_filter_body,
    "competitions": _competitions_filter_body,
    "session": _session_filter_body,
}