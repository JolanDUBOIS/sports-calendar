from __future__ import annotations
from dataclasses import dataclass, field

import pandas as pd

from . import logger
from .filters import SelectionFilterFactory, SelectionFilter
from ..data_access import MatchesTable


@dataclass
class SelectionItem:
    entity: str
    entity_id: int
    filters: list[SelectionFilter]

    def __post_init__(self):
        """ Validates the selection item after initialization. """
        valid_entities = ['team', 'competition']
        if self.entity not in valid_entities:
            logger.error(f"Invalid entity '{self.entity}' for selection item. Must be one of {valid_entities}.")
            raise ValueError(f"Invalid entity '{self.entity}' for selection item. Must be one of {valid_entities}.")
        if not all(isinstance(filter, SelectionFilter) for filter in self.filters):
            logger.error("All filters must be instances of SelectionFilter.")
            raise TypeError("All filters must be instances of SelectionFilter.")

    def get_matches(self, date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
        """ Returns matches for the selection item based on its filters. """
        matches = self._get_unfiltered_matches(date_from, date_to)
        if matches.empty:
            logger.debug(f"No matches found for {self.entity} with ID {self.entity_id}.")
            return pd.DataFrame()
        for filter in self.filters:
            matches = filter.apply(matches)
        return matches

    def _get_unfiltered_matches(self, date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
        """ TODO """
        team_ids = [self.entity_id] if self.entity == 'team' else None
        competition_ids = [self.entity_id] if self.entity == 'competition' else None
        return MatchesTable.query(
            team_ids=team_ids,
            competition_ids=competition_ids,
            date_from=date_from,
            date_to=date_to
        )

    @classmethod
    def from_dict(cls, d: dict) -> SelectionItem:
        """ Creates a SelectionItem instance from a dictionary. """
        entity = d['entity']
        entity_id = d['id']
        filters = [SelectionFilterFactory.create_filter(**filter_data) for filter_data in d.get('filters', [])]
        return cls(entity=entity, entity_id=entity_id, filters=filters)

@dataclass
class Selection:
    name: str
    items: list[SelectionItem] = field(default_factory=list)

    def get_matches(self, date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
        """ Returns matches for all items in the selection. """
        matches = pd.DataFrame()
        for item in self.items:
            item_matches = item.get_matches(date_from, date_to)
            if not item_matches.empty:
                matches = pd.concat([matches, item_matches], ignore_index=True)
        if matches.empty:
            logger.warning("No matches found for the selection.")
        return matches.drop_duplicates().reset_index(drop=True)

    @classmethod
    def from_dict(cls, d: dict) -> Selection:
        """ Creates a Selection instance from a dictionary. """
        name = d['name']
        items = [SelectionItem.from_dict(item) for item in d.get('items', [])]
        return cls(name=name, items=items)
