from . import logger
from sports_calendar.core.selection import (
    SelectionFilterSpec,
    MinRankingFilterSpec,
    StageFilterSpec,
    TeamsFilterSpec,
    CompetitionsFilterSpec,
    SessionFilterSpec
)
from sports_calendar.core.db import SPORT_SCHEMAS, Filter


class FilterPresenter:
    DETAILED_DISPATCH = {
        MinRankingFilterSpec: "_min_ranking_detailed",
        StageFilterSpec: "_stage_detailed",
        TeamsFilterSpec: "_teams_detailed",
        CompetitionsFilterSpec: "_competitions_detailed",
        SessionFilterSpec: "_session_detailed",
    }

    @staticmethod
    def summary(flt: SelectionFilterSpec) -> dict:
        """ Minimal info for list views. """
        # TODO - Add the name in summary as well
        return {
            "uid": flt.uid,
            "sport": flt.sport,
            "filter_type": flt.filter_type
        }

    @staticmethod
    def detailed(flt: SelectionFilterSpec) -> dict:
        """ Full info for detailed views. """
        method_name = FilterPresenter.DETAILED_DISPATCH.get(type(flt))
        try:
            method = getattr(FilterPresenter, method_name)
            detailed_data = method(flt)
        except Exception:
            logger.exception(f"Error generating detailed filter data for filter {flt.uid}")
            detailed_data = {}

        return {
            **flt.to_dict(),
            **detailed_data
        }

    # Detailed helper methods

    @staticmethod
    def _min_ranking_detailed(flt: MinRankingFilterSpec) -> dict:
        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "name", "abbreviation", "short_name").get()

        if flt.reference_team is not None:
            team_df = SPORT_SCHEMAS[flt.sport].teams.query(
                Filter(col="id", op="==", value=flt.reference_team)
            ).select("id", "name", "abbreviation", "display_name", "short_display_name", "location").get()

        return {
            "name": "Min Ranking Filter",
            "competitions": comp_df.to_dict(orient="records"),
            "reference_team": team_df.to_dict(orient="records")[0] if flt.reference_team is not None else None
        }

    @staticmethod
    def _stage_detailed(flt: StageFilterSpec) -> dict:
        return {"name": "Stage Filter"}

    @staticmethod
    def _teams_detailed(flt: TeamsFilterSpec) -> dict:
        team_df = SPORT_SCHEMAS[flt.sport].teams.query(
            Filter(col="id", op="in", value=flt.team_ids)
        ).select("id", "name", "abbreviation", "display_name", "short_display_name", "location").get()

        return {
            "name": "Teams Filter",
            "teams": team_df.to_dict(orient="records")
        }

    @staticmethod
    def _competitions_detailed(flt: CompetitionsFilterSpec) -> dict:
        comp_df = SPORT_SCHEMAS[flt.sport].competitions.query(
            Filter(col="id", op="in", value=flt.competition_ids)
        ).select("id", "name", "abbreviation", "short_name").get()

        return {
            "name": "Competitions Filter",
            "competitions": comp_df.to_dict(orient="records")
        }

    @staticmethod
    def _session_detailed(flt: SessionFilterSpec) -> dict:
        return {"name": "Session Filter"}
