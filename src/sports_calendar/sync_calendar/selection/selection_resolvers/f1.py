import pandas as pd

from .base import SelectionResolver
from ..filter_appliers import F1FilterApplier
from ..selection import SelectionItem
from ...models import F1EventsTable


class F1SelectionResolver(SelectionResolver):
    """ Resolver for F1 selections. """

    def __init__(self, filter_applier: F1FilterApplier):
        """ Initialize the resolver with an F1-specific filter applier. """
        super().__init__(filter_applier)

    def get_all_events(self, selection_item: SelectionItem, date_from=None, date_to=None) -> pd.DataFrame:
        """ Fetch all possible F1 events for this selection item and date range. """
        return F1EventsTable.query(
            date_from=date_from,
            date_to=date_to
        )
