import pandas as pd

from .base import SelectionResolver
from ..filter_appliers import FootballFilterApplier
from ..selection import SelectionItem
from ...models import FootballMatchesManager


class FootballSelectionResolver(SelectionResolver):
    """ Resolver for football selections. """

    def __init__(self, filter_applier: FootballFilterApplier):
        """ Initialize the resolver with a football-specific filter applier. """
        super().__init__(filter_applier)

    def get_all_events(self, selection_item: SelectionItem, date_from=None, date_to=None) -> pd.DataFrame:
        """ Fetch all possible football events for this selection item and date range. """
        team_ids = [selection_item.entity_id] if selection_item.entity == 'team' else None
        competition_ids = [selection_item.entity_id] if selection_item.entity == 'competition' else None
        return FootballMatchesManager.query(
            team_ids=team_ids,
            competition_ids=competition_ids,
            date_from=date_from,
            date_to=date_to
        )
