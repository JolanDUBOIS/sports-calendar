from pathlib import Path
from abc import ABC, abstractmethod

import yaml
import pandas as pd

from src.legacy.registry_manager import CompetitionRegistry, TeamRegistry


class BaseSourceClient(ABC):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        self.competition_registry = CompetitionRegistry()
        self.team_registry = TeamRegistry()

    @property
    @abstractmethod
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        pass

    @property
    @abstractmethod
    def source_name(self):
        """Getter for the source_name property."""
        pass

    @abstractmethod
    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        pass

    @abstractmethod
    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO """
        pass

    @staticmethod
    def read_yml(file_path: str) -> dict:
        """ TODO """
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
