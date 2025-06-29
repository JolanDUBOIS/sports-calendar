from abc import ABC, abstractmethod

import pandas as pd

from ..selection import SelectionItem
from ..filter_appliers import FilterApplier


class SelectionResolver(ABC):
    """ Base class for selection resolvers. """

    def __init__(self, filter_applier: FilterApplier):
        """ Initialize the resolver with a filter applier. """
        self.filter_applier = filter_applier

    def get_events(self, selection_item: SelectionItem, date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
        """ Get events for a selection item within a date range. """
        events = self.get_all_events(selection_item, date_from, date_to)
        if events.empty:
            return pd.DataFrame()
        for filter in selection_item.filters:
            events = self.filter_applier.apply(filter, events)
        return events

    @abstractmethod
    def get_all_events(self, selection_item: SelectionItem, date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
        """Fetch all possible events for this selection item and date range."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_all_events.")
