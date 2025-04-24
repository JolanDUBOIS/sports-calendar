from typing import List

import pandas as pd

from src.utils import concatenate_unique_rows
from src.deprecated.models_deprecated import logger
from src.deprecated.models_deprecated.selection_items import Item
from src.deprecated.football_data_source_deprecated import FootballDataConnector


class Selection:
    """ TODO """
    
    def __init__(self, name: str, items: List[Item], api_connector: FootballDataConnector, **kwargs):
        """ TODO """
        self.name = name
        self.items = items
        self.api_connector = api_connector  # TODO - remove if not needed
    
    def get_matches(self) -> pd.DataFrame:
        """ TODO """
        matches = pd.DataFrame()
        for item in self.items:
            new_matches = item.get_matches()
            logger.debug(f"New matches: {new_matches}")
            matches = concatenate_unique_rows(matches, new_matches)
        return matches
        
    @classmethod
    def from_json(cls, json: dict, api_connector: FootballDataConnector) -> 'Selection':
        """ TODO """
        obj_items = []
        for item in json['items']:
            obj_items.append(Item.from_json(item, api_connector))
        return cls(
            name=json['name'],
            items=obj_items,
            api_connector=api_connector
        )
