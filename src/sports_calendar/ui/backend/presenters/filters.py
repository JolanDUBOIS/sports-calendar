from types import SimpleNamespace

from . import logger
from .utils import FieldDescriptor, SelectorOption
from sports_calendar.core.utils import deep_asdict, validate
from sports_calendar.core.db import SPORT_SCHEMAS, Filter
from sports_calendar.core.competition_stages import CompetitionStage
from sports_calendar.core.selection import (
    SelectionFilter,
    EmptyFilter,
    MinRankingFilter,
    StageFilter,
    TeamsFilter,
    CompetitionsFilter,
    SessionFilter,
    FILTER_TYPE_MAP
)


FILTERS_TYPE_NAME_MAP = {
    "empty": "Empty Filter",
    "min_ranking": "Min Ranking Filter",
    "stage": "Stage Filter",
    "teams": "Teams Filter",
    "competitions": "Competitions Filter",
    "session": "Session Filter",
}


class FilterPresenter:
    FIELDS_DISPATCH = {
        EmptyFilter: "_empty_fields",
        MinRankingFilter: "_min_ranking_fields",
        StageFilter: "_stage_fields",
        TeamsFilter: "_teams_fields",
        CompetitionsFilter: "_competitions_fields",
        SessionFilter: "_session_fields",
    }

    @staticmethod
    def summary(flt: SelectionFilter) -> dict:
        """ Minimal info for list views. """
        return {
            "name": FILTERS_TYPE_NAME_MAP.get(flt.filter_type, "Unknown Filter"),
            "uid": flt.uid,
            "sport": flt.sport,
            "filter_type": flt.filter_type,
            "payload_type": "summary_filter",
        }

    @staticmethod
    def detailed(flt: SelectionFilter | None = None, filter_meta: dict | None = None) -> dict:
        """ Full info for detailed views. """
        validate(flt is not None or filter_meta is not None, "Either flt or filter_meta must be provided", logger)
        validate(not (flt is not None and filter_meta is not None), "Only one of flt or filter_meta should be provided", logger)

        if flt is None:
            method_name = FilterPresenter.FIELDS_DISPATCH.get(FILTER_TYPE_MAP.get(filter_meta["filter_type"]))
        else:
            method_name = FilterPresenter.FIELDS_DISPATCH.get(type(flt))
            filter_type = flt.filter_meta["filter_type"]

        try:
            result = {
                "payload_type": "detailed_filter",
                "name": FILTERS_TYPE_NAME_MAP[filter_type],
                "sport": flt.sport if flt is not None else filter_meta["sport"],
                "uid": flt.uid if flt is not None else None,
                "filter_type": filter_type,
            }

            method = getattr(FilterPresenter, method_name)
            fields = method(flt, sport=None if flt is not None else filter_meta["sport"])

            result["fields"] = fields

            result["fields"]["filter_type"] = FieldDescriptor(
                label="Filter Type",
                path="filter_type",
                current_value=[filter_type],
                current_display=[filter_type],
                input_type="select",
                select_options=[SelectorOption(value=ft, display=name) for ft, name in FILTERS_TYPE_NAME_MAP.items()]
            )

            return result
        except Exception:
            if flt is not None:
                logger.exception(f"Error generating detailed filter data for filter {flt.uid}")
            else:
                logger.exception(f"Error generating detailed filter data for filter type {filter_type}")
            return {}

    # Detailed helper methods

    @staticmethod
    def _empty_fields(flt: EmptyFilter | None = None, sport: str | None = None) -> dict:
        return {}

    @staticmethod
    def _min_ranking_fields(flt: MinRankingFilter | None = None, sport: str | None = None) -> dict:
        if flt is None:
            flt = SimpleNamespace()
            flt.rule = ""
            flt.ranking = 20
            flt.competition_ids = []
            flt.reference_team = None
            flt.sport = sport

        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        if flt.reference_team is not None:
            team_df = SPORT_SCHEMAS[flt.sport].teams.query(
                Filter(col="id", op="==", value=flt.reference_team)
            ).select("id", "short_display_name").get()

        result = {
            "rule": FieldDescriptor(
                label="Rule",
                path="rule",
                current_value=[flt.rule],
                current_display=[flt.rule],
                input_type="select",
                select_options=[SelectorOption(value=r, display=r) for r in sorted(MinRankingFilter.valid_rules())]
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
        return deep_asdict(result)

    @staticmethod
    def _stage_fields(flt: StageFilter | None = None, sport: str | None = None) -> dict:
        if flt is None:
            flt = SimpleNamespace()
            flt.stage = CompetitionStage.NULL
            flt.competition_ids = []
            flt.sport = sport

        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        result = {
            "stage": FieldDescriptor(
                label="Stage",
                path="stage",
                current_value=[flt.stage.value],
                current_display=[flt.stage.name], # TODO - better display?
                input_type="select",
                select_options=[SelectorOption(value=stage.value, display=stage.name) for stage in CompetitionStage]
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
        return deep_asdict(result)

    @staticmethod
    def _teams_fields(flt: TeamsFilter | None = None, sport: str | None = None) -> dict:
        if flt is None:
            flt = SimpleNamespace()
            flt.team_ids = []
            flt.rule = None
            flt.sport = sport

        team_df = SPORT_SCHEMAS[flt.sport].teams.query(
            Filter(col="id", op="in", value=flt.team_ids)
        ).select("id", "short_display_name").get()

        result = {
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
        return deep_asdict(result)

    @staticmethod
    def _competitions_fields(flt: CompetitionsFilter | None = None, sport: str | None = None) -> dict:
        if flt is None:
            flt = SimpleNamespace()
            flt.competition_ids = []
            flt.sport = sport

        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "short_name").get()

        result = {
            "competitions": FieldDescriptor(
                label="Competitions",
                path="competition_ids",
                current_value=[cid for cid in flt.competition_ids],
                current_display=[comp["short_name"] for comp in comp_df.to_dict(orient="records")],
                input_type="multilookup",
                lookup_endpoint=f"/lookups/{flt.sport}/competitions"
            )
        }
        return deep_asdict(result)

    @staticmethod
    def _session_fields(flt: SessionFilter | None = None, sport: str | None = None) -> dict:
        if flt is None:
            flt = SimpleNamespace()
            flt.sessions = []
            flt.sport = sport

        result = {
            "sessions": FieldDescriptor(
                label="Sessions",
                path="sessions",
                current_value=[s for s in flt.sessions],
                current_display=[s for s in flt.sessions],
                input_type="multiselect",
                select_options=[SelectorOption(value=s, display=s) for s in sorted(flt.sessions)]
            )
        }
        return deep_asdict(result)
