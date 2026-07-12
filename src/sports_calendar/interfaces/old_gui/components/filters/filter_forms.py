from types import SimpleNamespace

from sports_calendar.core import CompetitionStage
from sports_calendar.core.selection import FilterType, SelectionFilter

from .fields import (
    BaseField,
    Choice,
    MultipleSelectField,
    NumberField,
    SearchableMultipleSelectField,
    SearchableSelectField,
    SelectField,
    TextField,
)

# Main function

def get_fields_for_filter(filter: SelectionFilter | None, filter_meta: dict | None = None) -> list[BaseField]:
    """
    Returns fields for a filter form, including the filter_type selector.
    
    Args:
        filter: Existing filter object (for modification)
        filter_meta: Dict with 'sport' and optionally 'filter_type' (for creation or type switching)
    """
    # Determine current filter type and sport
    if filter is not None:
        current_type = filter.filter_type
        sport = filter.sport
    elif filter_meta:
        current_type = FilterType(filter_meta["filter_type"]) if "filter_type" in filter_meta else FilterType.EMPTY
        sport = filter_meta["sport"]
    else:
        raise ValueError("Either filter or filter_meta must be provided")

    # Create the filter_type selector field
    filter_type_field = SelectField(
        key="filter_type",
        label="Filter Type",
        options=[
            Choice(label="Empty", value=FilterType.EMPTY.value),
            Choice(label="Minimum Ranking", value=FilterType.MIN_RANKING.value),
            Choice(label="Teams", value=FilterType.TEAMS.value),
            Choice(label="Competitions", value=FilterType.COMPETITIONS.value),
            Choice(label="Session", value=FilterType.SESSION.value),
        ],
        default=current_type.value
    )

    # Get the specific fields for the current filter type
    if filter is None or filter.filter_type != current_type:
        filter = SimpleNamespace(sport=sport, filter_type=current_type)

    render_function = DISPATCH_FILTER_FIELDS.get(current_type)
    specific_fields = render_function(filter)

    return [filter_type_field] + specific_fields

# Specific filter field getters

def _get_fields_empty_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
    return []

def _get_fields_min_ranking_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
    if isinstance(filter, SimpleNamespace):
        filter.rule = "both"
        filter.ranking = 20
        filter.competition_ids = []
        filter.reference_team = None

    competitions_df = SPORT_SCHEMAS[filter.sport].competitions.select("id", "short_name").get().dropna(subset=["short_name"])
    teams_df = SPORT_SCHEMAS[filter.sport].teams.select("id", "short_display_name").get().dropna(subset=["short_display_name"])

    return [
        SelectField(
            key="rule",
            label="Rule",
            options=[
                Choice(label=r, value=r)
                for r in sorted(FilterType.MIN_RANKING.valid_values("rule") or [])
            ],
            default=filter.rule
        ),
        NumberField(
            key="ranking",
            label="Minimum Ranking",
            default=filter.ranking
        ),
        SearchableMultipleSelectField(
            key="competition_ids",
            label="Competitions",
            options=[
                Choice(label=comp["short_name"], value=comp["id"])
                for comp in competitions_df.to_dict(orient="records")
            ],
            default=filter.competition_ids
        ),
        SearchableSelectField(
            key="reference_team",
            label="Reference Team",
            options=[
                Choice(label=team["short_display_name"], value=team["id"])
                for team in teams_df.to_dict(orient="records")
            ],
            default=filter.reference_team
        )
    ]


# def _get_fields_stage_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
#     if isinstance(filter, SimpleNamespace):
#         filter.stage = CompetitionStage.NULL
#         filter.competition_ids = []

#     competitions_df = SPORT_SCHEMAS[filter.sport].competitions.select("id", "short_name").get().dropna(subset=["short_name"])

#     return [
#         SelectField(
#             key="stage",
#             label="Stage",
#             options=[
#                 Choice(label=stage.name, value=stage)
#                 for stage in CompetitionStage
#             ],
#             default=filter.stage
#         ),
#         SearchableMultipleSelectField(
#             key="competition_ids",
#             label="Competitions",
#             options=[
#                 Choice(label=comp["short_name"], value=comp["id"])
#                 for comp in competitions_df.to_dict(orient="records")
#             ],
#             default=filter.competition_ids
#         )
#     ]

def _get_fields_teams_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
    if isinstance(filter, SimpleNamespace):
        filter.rule = "any"
        filter.team_ids = []

    teams_df = SPORT_SCHEMAS[filter.sport].teams.select("id", "short_display_name").get().dropna(subset=["short_display_name"])

    return [
        SelectField(
            key="rule",
            label="Rule",
            options=[
                Choice(label=r, value=r)
                for r in FilterType.TEAMS.valid_values("rule") or []
            ],
            default=filter.rule
        ),
        SearchableMultipleSelectField(
            key="team_ids",
            label="Teams",
            options=[
                Choice(label=team["short_display_name"], value=team["id"])
                for team in teams_df.to_dict(orient="records")
            ],
            default=filter.team_ids
        )
    ]

def _get_fields_competitions_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
    if isinstance(filter, SimpleNamespace):
        filter.competition_ids = []

    competitions_df = SPORT_SCHEMAS[filter.sport].competitions.select("id", "short_name").get().dropna(subset=["short_name"])

    return [
        SearchableMultipleSelectField(
            key="competition_ids",
            label="Competitions",
            options=[
                Choice(label=comp["short_name"], value=comp["id"])
                for comp in competitions_df.to_dict(orient="records")
            ],
            default=filter.competition_ids
        )
    ]

def _get_fields_sessions_filter(filter: SelectionFilter | SimpleNamespace) -> list[BaseField]:
    if isinstance(filter, SimpleNamespace):
        filter.sessions = []

    sessions_df = SPORT_SCHEMAS[filter.sport].events.select("session_type").get().drop_duplicates().dropna(subset=["session_type"])

    return [
        MultipleSelectField(
            key="sessions",
            label="Sessions",
            options=[
                Choice(label=session["session_type"], value=session["session_type"])
                for session in sessions_df.to_dict(orient="records")
            ],
            default=filter.sessions
        )
    ]


# Dispatch dictionary

DISPATCH_FILTER_FIELDS = {
    FilterType.EMPTY: _get_fields_empty_filter,
    FilterType.MIN_RANKING: _get_fields_min_ranking_filter,
    FilterType.TEAMS: _get_fields_teams_filter,
    FilterType.COMPETITIONS: _get_fields_competitions_filter,
    FilterType.SESSIONS: _get_fields_sessions_filter,
}
