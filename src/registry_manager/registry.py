import json
from pathlib import Path

import yaml

from src.registry_manager import logger


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
                data = json.load(file)
        elif data_source.suffix == '.yml' or data_source.suffix == '.yaml':
            with open(data_source, 'r') as file:
                data = yaml.safe_load(file)
        else:
            logger.error(f"Unsupported file type: {data_source.suffix}")
            raise ValueError(f"Unsupported file type: {data_source.suffix}")
        return data

    # We consider that names and aliases are unique across the registry but this actually is not true (for teams, e.g. PSG men and women)
    # TODO - find a solution to this problem
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
                    self._invert_map_name[name] = {'id': key, 'aliases': value.get('aliases')}
                else:
                    logger.error(f"Duplicate name found: {name} for {key} and {self._invert_map_name[name]}")
                    raise ValueError(f"Duplicate name found: {name} for {key} and {self._invert_map_name[name]}")
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
                        self._invert_map_alias[alias.lower()] = {'id': key, 'name': value.get('name')}
                    else:
                        logger.error(f"Duplicate alias found: {alias} for {key} and {self._invert_map_alias[alias]}")
                        raise ValueError(f"Duplicate alias found: {alias} for {key} and {self._invert_map_alias[alias]}")
        return self._invert_map_alias

    def get_route(self, source: str, entity_id: str) -> str:
        """ TODO """
        return self.data.get(entity_id, {}).get('sources', {}).get(source, None)

    def get_name_by_id(self, entity_id: str) -> str:
        """ TODO """
        return self.data.get(entity_id, {}).get('name', None)

    def get_name_by_alias(self, entity_alias: str) -> str:
        """ TODO """
        return self.invert_map_alias.get(entity_alias.lower(), {}).get('name', None)

    def get_id_by_name(self, entity_name: str) -> str:
        """ TODO """
        return self.invert_map_name.get(entity_name, {}).get('id', None)

    def get_id_by_alias(self, entity_alias: str) -> str:
        """ TODO """
        return self.invert_map_alias.get(entity_alias.lower(), {}).get('id', None)


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
