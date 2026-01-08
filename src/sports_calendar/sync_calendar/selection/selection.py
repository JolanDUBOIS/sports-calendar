from __future__ import annotations
from dataclasses import dataclass, field

import pandas as pd

from . import logger
from .filters import SelectionFilterFactory, SelectionFilter


@dataclass
class SelectionItem:
    sport: str
    entity: str | None = None
    entity_id: int | None = None
    filters: list[SelectionFilter] = field(default_factory=list)

    def __post_init__(self):
        valid_entities = ['team', 'competition']
        if self.entity is not None and self.entity not in valid_entities:
            logger.error(f"Invalid entity '{self.entity}' for selection item. Must be one of {valid_entities}.")
            raise ValueError(f"Invalid entity '{self.entity}' for selection item. Must be one of {valid_entities}.")
        if not all(isinstance(filter, SelectionFilter) for filter in self.filters):
            logger.error("All filters must be instances of SelectionFilter.")
            raise TypeError("All filters must be instances of SelectionFilter.")

    @classmethod
    def from_dict(cls, d: dict) -> SelectionItem:
        """ Creates a SelectionItem instance from a dictionary. """
        sport = d['sport']
        entity = d.get('entity')
        entity_id = d.get('id')
        filters = [SelectionFilterFactory.create_filter(sport=sport, **filter_data) for filter_data in d.get('filters', [])]
        return cls(sport=sport, entity=entity, entity_id=entity_id, filters=filters)

@dataclass
class Selection:
    name: str
    items: list[SelectionItem] = field(default_factory=list)

    def __iter__(self):
        return iter(self.items)

    @classmethod
    def from_dict(cls, d: dict) -> Selection:
        """ Creates a Selection instance from a dictionary. """
        name = d['name']
        items = [SelectionItem.from_dict(item) for item in d.get('items', [])]
        return cls(name=name, items=items)
