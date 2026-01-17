import pandas as pd

from . import logger
from .base import TableView
from .schemas import SportSchema
from sports_calendar.core.utils import validate


class EventTableView:
    def __init__(self, *, sport: str, schema: SportSchema, view: TableView):
        self.sport = sport
        self.schema = schema
        self.view = view

        self._validate()

    def get(self) -> pd.DataFrame:
        """ Get the underlying DataFrame of the EventTableView. Calls TableView.get(). """
        return self.view.get()

    def _validate(self) -> None:
        expected = set(self.schema.events.columns())
        actual = set(self.view.columns())
        validate(
            actual == expected,
            f"Invalid events table for sport={self.sport}",
            logger,
        )

    def __str__(self) -> str:
        return f"<EventTableView(sport={self.sport}, schema={self.schema.__str__()}, view={self.view.__str__()})>"

    def __repr__(self) -> str:
        return f"<EventTableView(sport={self.sport}, schema={self.schema.__repr__()}, view={self.view.__repr__()})>"
