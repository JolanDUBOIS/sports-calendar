import json
import logging
from pathlib import Path
import unicodedata

import yaml

from src.registry_manager import logger


# TODO - When an alias can't be mapped, the alias is saved somewhere to be latter assigned to a name

class AliasLogger:
    """ TODO """

    def __init__(self):
        """ TODO """
        self.alias_logger = logging.getLogger("aliases_logger")

    def alias_hook(self, func):
        """ Decorator to log the alias. """
        def wrapper(obj, entity_alias, *args, **kwargs):
            result = func(obj, entity_alias, *args, **kwargs)
            if result is None:
                self.log_alias(entity_alias)
            return result
        return wrapper

    def log_alias(self, entity_alias):
        """ Log the alias if not found. """
        self.alias_logger.debug(f"New alias: {entity_alias}")

alias_logger = AliasLogger()


class BaseRegistry:
    """ TODO """

    def __init__(self, data_source: Path, **kwargs):
        """ TODO """
        self.data_source = data_source
        self.data = self.load_data(data_source)
        self._invert_map_name = None
        self._invert_map_alias = None

    def load_data(self, data_source: Path) -> dict:
        """ TODO """
        if data_source.suffix == '.json':
            with open(data_source, 'r') as file:
                data = json.load(file, parse_int=str, parse_float=str)
        elif data_source.suffix == '.yml' or data_source.suffix == '.yaml':
            with open(data_source, 'r') as file:
                data = yaml.safe_load(file)
                # Convert all numbers to strings recursively
                def convert_to_str(obj):
                    if isinstance(obj, dict):
                        return {k: convert_to_str(v) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        return [convert_to_str(v) for v in obj]
                    elif isinstance(obj, (int, float)):
                        return str(obj)
                    return obj
                data = convert_to_str(data)
        else:
            logger.error(f"Unsupported file type: {data_source.suffix}")
            raise ValueError(f"Unsupported file type: {data_source.suffix}")
        return data

    # Property getters
    @property
    def invert_map_name(self) -> dict:
        """ TODO """
        if self._invert_map_name is None:
            self._invert_map_name = {}
            for key, value in self.data.items():
                name = value.get('name', None)
                if name is None:
                    logger.warning(f"No name found for {key}")
                    continue
                if name not in self._invert_map_name:
                    self._invert_map_name[name] = {'id': [key], 'aliases': [value.get('aliases')]}
                else:
                    logger.debug(f"Duplicate name found: {name} for {key} and {self._invert_map_name[name]}")
                    self._invert_map_name[name]['id'].append(key)
                    self._invert_map_name[name]['aliases'].append(value.get('aliases'))
        return self._invert_map_name

    @property
    def invert_map_alias(self) -> dict:
        """ TODO """
        if self._invert_map_alias is None:
            self._invert_map_alias = {}
            for key, value in self.data.items():
                aliases = value.get('aliases', None)
                if aliases is None:
                    logger.warning(f"No aliases found for {key}")
                    continue
                for alias in aliases:
                    if alias not in self._invert_map_alias:
                        self._invert_map_alias[alias] = {'id': [key], 'name': [value.get('name')]}
                    else:
                        logger.debug(f"Duplicate alias found: {alias} for {key} and {self._invert_map_alias[alias]}")
                        self._invert_map_alias[alias]['id'].append(key)
                        self._invert_map_alias[alias]['name'].append(value.get('name'))
        return self._invert_map_alias

    # Public getter methods
    def get_route(self, source: str, entity_id: str) -> str:
        """ TODO """
        return self.data.get(entity_id, {}).get('sources', {}).get(source, None)

    def get_name_by_id(self, entity_id: str) -> str:
        """ TODO """
        return self.data.get(entity_id, {}).get('name', None)

    @alias_logger.alias_hook
    def get_name_by_alias(self, entity_alias: str, first: bool = True) -> str:
        """ TODO """
        if first:
            # TODO - logger debug if several names are found
            return get_value_by_normalized_key(entity_alias, self.invert_map_alias, default=[{}])[0].get('name', [None])[0]
        else:
            raise NotImplementedError("get_name_by_alias with first=False is not implemented")

    @alias_logger.alias_hook
    def get_id_by_alias(self, entity_alias: str, first: bool = True) -> str:
        """ TODO """
        if first:
            # TODO - logger debug if several ids are found
            return get_value_by_normalized_key(entity_alias, self.invert_map_alias, default=[{}])[0].get('id', [None])[0]
        else:
            raise NotImplementedError("get_id_by_alias with first=False is not implemented")

    def get_id_by_name(self, entity_name: str, first: bool = True) -> str:
        """ TODO """
        if first:
            # TODO - logger debug if several ids are found
            return self.invert_map_name.get(entity_name, {}).get('id', [None])[0]
        else:
            raise NotImplementedError("get_id_by_name with first=False is not implemented")

# TODO - Make the last methods simpler (for that, remove the invert_map properties and make the logic easier, even if it means a bit more computation)


class CompetitionRegistry(BaseRegistry):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(Path(__file__).parent / 'config/competitions.yml', **kwargs)


class TeamRegistry(BaseRegistry):
    """ TODO """

    def __init__(self, **kwargs):
        super().__init__(Path(__file__).parent / 'config/teams.yml', **kwargs)


class RegionRegistry(BaseRegistry):
    """ TODO """

    def __init__(self, **kwargs):
        super().__init__(Path(__file__).parent / 'config/regions.yml', **kwargs)


def normalize_key(key: str) -> str:
    """ Normalize the key by removing accents and converting to lowercase. """
    normalized = unicodedata.normalize('NFD', key)
    without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    return without_accents.lower()


def get_value_by_normalized_key(key: str, dictionary: dict, default: any = None) -> list:
    """ Retrieve all values from the dictionary that match the normalized key. """
    normalized_key = normalize_key(key)
    matching_values = []
    for dict_key in dictionary.keys():
        if normalize_key(dict_key) == normalized_key:
            matching_values.append(dictionary[dict_key])
    return matching_values if matching_values else default
