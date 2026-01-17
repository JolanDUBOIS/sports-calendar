from .utils import SelectorOption
from sports_calendar.core.utils import deep_asdict
from sports_calendar.core.db import SPORT_SCHEMAS, Filter


class LookupPresenter:
    @staticmethod
    def teams(sport: str, query: str) -> dict:
        table = SPORT_SCHEMAS[sport].teams
        df = (
            table.query(Filter(col="short_display_name", op="contains", value=query))
                 .select("id", "short_display_name")
                 .get()
        )

        options = [
            deep_asdict(SelectorOption(value=row["id"], display=row["short_display_name"]))
            for row in df.to_dict(orient="records")
        ]

        return {"teams": options}

    @staticmethod
    def competitions(sport: str, query: str) -> dict:
        table = SPORT_SCHEMAS[sport].competitions
        df = (
            table.query(Filter(col="short_name", op="contains", value=query))
                 .select("id", "short_name")
                 .get()
        )

        options = [
            deep_asdict(SelectorOption(value=row["id"], display=row["short_name"]))
            for row in df.to_dict(orient="records")
        ]

        return {"competitions": options}
