from . import logger
from .utils import FieldDescriptor, SelectorOption
from sports_calendar.core.utils import deep_asdict
from sports_calendar.core.db import SPORT_SCHEMAS, Filter
from sports_calendar.core.competition_stages import CompetitionStage
from sports_calendar.core.selection import (
    SelectionFilter,
    MinRankingFilter,
    StageFilter,
    TeamsFilter,
    CompetitionsFilter,
    SessionFilter
)


class FilterPresenter:
    DETAILED_DISPATCH = {
        MinRankingFilter: "_min_ranking_detailed",
        StageFilter: "_stage_detailed",
        TeamsFilter: "_teams_detailed",
        CompetitionsFilter: "_competitions_detailed",
        SessionFilter: "_session_detailed",
    }

    @staticmethod
    def summary(flt: SelectionFilter) -> dict:
        """ Minimal info for list views. """
        # TODO - Add the name in summary as well
        return {
            "uid": flt.uid,
            "sport": flt.sport,
            "filter_type": flt.filter_type
        }

    @staticmethod
    def detailed(flt: SelectionFilter) -> dict:
        """ Full info for detailed views. """
        method_name = FilterPresenter.DETAILED_DISPATCH.get(type(flt))
        try:
            method = getattr(FilterPresenter, method_name)
            return method(flt)
        except Exception:
            logger.exception(f"Error generating detailed filter data for filter {flt.uid}")
            return {}

    # Detailed helper methods

    @staticmethod
    def _min_ranking_detailed(flt: MinRankingFilter) -> dict:
        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        if flt.reference_team is not None:
            team_df = SPORT_SCHEMAS[flt.sport].teams.query(
                Filter(col="id", op="==", value=flt.reference_team)
            ).select("id", "short_display_name").get()

        result = {
            "name": "Min Ranking Filter",
            "sport": flt.sport,
            "uid": flt.uid,
            "filter_type": flt.filter_type,
            "fields": {
                "rule": FieldDescriptor(
                    label="Rule",
                    path="rule",
                    current_value=[flt.rule],
                    current_display=[flt.rule],
                    input_type="select",
                    select_options=[SelectorOption(value=r, display=r) for r in sorted(flt.valid_rules())]
                ),
                "ranking": FieldDescriptor(
                    label="Minimum Ranking",
                    path="ranking",
                    current_value=[flt.ranking],
                    current_display=[flt.ranking],
                    input_type="number"
                ),
                "competitions": FieldDescriptor(
                    label="Competitions",
                    path="competition_ids",
                    current_value=[cid for cid in flt.competition_ids],
                    current_display=[comp["short_name"] for comp in comp_df.to_dict(orient="records")],
                    input_type="multilookup",
                    lookup_endpoint=f"/lookups/{flt.sport}/competitions"
                ),
                "reference_team": FieldDescriptor(
                    label="Reference Team",
                    path="reference_team",
                    current_value=[flt.reference_team] if flt.reference_team is not None else [],
                    current_display=[team_df.to_dict(orient="records")[0]["short_display_name"]] if flt.reference_team is not None else [],
                    input_type="lookup",
                    lookup_endpoint=f"/lookups/{flt.sport}/teams"
                ),
            }
        }

        return deep_asdict(result)

    @staticmethod
    def _stage_detailed(flt: StageFilter) -> dict:
        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        result = {
            "name": "Stage Filter",
            "sport": flt.sport,
            "uid": flt.uid,
            "filter_type": flt.filter_type,
            "fields": {
                "stage": FieldDescriptor(
                    label="Stage",
                    path="stage",
                    current_value=[flt.stage.value],
                    current_display=[flt.stage.name], # TODO - better display?
                    input_type="select",
                    select_options=[SelectorOption(value=stage.value, display=stage.name) for stage in CompetitionStage if stage != CompetitionStage.NULL]
                ),
                "competitions": FieldDescriptor(
                    label="Competitions",
                    path="competition_ids",
                    current_value=[cid for cid in flt.competition_ids],
                    current_display=[comp["short_name"] for comp in comp_df.to_dict(orient="records")],
                    input_type="multilookup",
                    lookup_endpoint=f"/lookups/{flt.sport}/competitions"
                )
            }
        }

        return deep_asdict(result)

    @staticmethod
    def _teams_detailed(flt: TeamsFilter) -> dict:
        team_df = SPORT_SCHEMAS[flt.sport].teams.query(
            Filter(col="id", op="in", value=flt.team_ids)
        ).select("id", "short_display_name").get()

        result = {
            "name": "Teams Filter",
            "sport": flt.sport,
            "uid": flt.uid,
            "filter_type": flt.filter_type,
            "fields": {
                "rule": FieldDescriptor(
                    label="Rule",
                    path="rule",
                    current_value=[flt.rule],
                    current_display=[flt.rule],
                    input_type="select",
                    select_options=[SelectorOption(value=r, display=r) for r in sorted(flt.valid_rules())]
                ),
                "teams": FieldDescriptor(
                    label="Teams",
                    path="team_ids",
                    current_value=[tid for tid in flt.team_ids],
                    current_display=[team["short_display_name"] for team in team_df.to_dict(orient="records")],
                    input_type="multilookup",
                    lookup_endpoint=f"/lookups/{flt.sport}/teams"
                )
            }
        }

        return deep_asdict(result)

    @staticmethod
    def _competitions_detailed(flt: CompetitionsFilter) -> dict:
        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        result = {
            "name": "Competitions Filter",
            "sport": flt.sport,
            "uid": flt.uid,
            "filter_type": flt.filter_type,
            "fields": {
                "competitions": FieldDescriptor(
                    label="Competitions",
                    path="competition_ids",
                    current_value=[cid for cid in flt.competition_ids],
                    current_display=[comp["short_name"] for comp in comp_df.to_dict(orient="records")],
                    input_type="multilookup",
                    lookup_endpoint=f"/lookups/{flt.sport}/competitions"
                )
            }
        }

        return deep_asdict(result)

    @staticmethod
    def _session_detailed(flt: SessionFilter) -> dict:
        result = {
            "name": "Session Filter",
            "sport": flt.sport,
            "uid": flt.uid,
            "filter_type": flt.filter_type,
            "fields": {
                "sessions": FieldDescriptor(
                    label="Sessions",
                    path="sessions",
                    current_value=[s for s in flt.sessions],
                    current_display=[s for s in flt.sessions],
                    input_type="multiselect",
                    select_options=[SelectorOption(value=s, display=s) for s in sorted(flt.sessions)]
                )
            }
        }

        return deep_asdict(result)
