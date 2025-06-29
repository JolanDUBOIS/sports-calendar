from __future__ import annotations
from typing import TYPE_CHECKING, Type

import pandas as pd

from .base import FilterApplier
from ..filters import SessionFilter
if TYPE_CHECKING:
    from ..filters import SelectionFilter


class F1FilterApplier(FilterApplier):
    """ Applies f1-specific filters. """

    def _get_dispatch_map(self) -> dict[Type[SelectionFilter], callable]:
        """ Returns a mapping of filter types to their respective handler methods. """
        return {
            SessionFilter: self._apply_session_filter,
        }

    # Session Filter
    def _apply_session_filter(self, filter: SessionFilter, events: pd.DataFrame):
        """ Applies the SessionFilter to the DataFrame. """
        return events[events['session'].isin(filter.sessions)]
