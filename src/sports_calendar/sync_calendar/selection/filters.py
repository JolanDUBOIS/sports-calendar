from __future__ import annotations
from abc import ABC
from dataclasses import dataclass, field
from ..competition_stages import CompetitionStage

from . import logger


## Base class

@dataclass
class SelectionFilter(ABC):
    sport: str
    key: str = field(init=False)

    def __post_init__(self):
        """ Validates the filter after initialization. """
        if self.key is None:
            logger.error("Filter key cannot be None.")
            raise ValueError("Filter key cannot be None.")
        if self.sport is None: # TODO - add global config for valid sports
            logger.error("Sport cannot be None in SelectionFilter.")
            raise ValueError("Sport cannot be None in SelectionFilter.")

    @classmethod
    def create(cls, **kwargs) -> SelectionFilter:
        """ Factory method to create a SelectionFilter instance. """
        return cls(**kwargs)

## Concrete filter classes

@dataclass
class MinRankingFilter(SelectionFilter):
    rule: str
    ranking: int
    competition_ids: list[int] = field(default_factory=list)
    reference_team: str | None = None
    key: str = field(init=False, default='min_ranking', repr=False)

    def __post_init__(self):
        super().__post_init__()
        valid_rules = ['both', 'any', 'opponent']
        if self.rule not in valid_rules:
            logger.error(f"Invalid rule '{self.rule}' for MinRankingFilter. Must be one of {valid_rules}.")
            raise ValueError(f"Invalid rule '{self.rule}' for MinRankingFilter. Must be one of {valid_rules}.")
        if not isinstance(self.ranking, int) or self.ranking <= 0:
            logger.error("Ranking must be a strictly positive integer.")
            raise ValueError("Ranking must be a strictly positive integer.")
        if self.reference_team is not None and not isinstance(self.reference_team, str):
            logger.error("Reference team must be a string or None.")
            raise TypeError("Reference team must be a string or None.")
        if self.rule == 'opponent' and self.reference_team is None:
            logger.error("Reference team must be provided for 'opponent' rule in MinRankingFilter.")
            raise ValueError("Reference team must be provided for 'opponent' rule in MinRankingFilter.")

@dataclass
class StageFilter(SelectionFilter):
    stage: CompetitionStage
    key: str = field(init=False, default='stage', repr=False)

    def __post_init__(self):
        super().__post_init__()
        if self.stage is None:
            logger.error("Stage cannot be None in StageFilter.")
            raise ValueError("Stage cannot be None in StageFilter.")
        if not isinstance(self.stage, CompetitionStage):
            logger.error("Stage must be an instance of CompetitionStage.")
            raise TypeError("Stage must be an instance of CompetitionStage.")

    @classmethod
    def create(cls, stage: str, **kwargs) -> StageFilter:
        """ TODO """
        stage = CompetitionStage.from_str(stage)
        return cls(stage=stage, **kwargs)

@dataclass
class TeamsFilter(SelectionFilter):
    team_ids: list[int]
    rule: str
    key: str = field(init=False, default='teams', repr=False)

    def __post_init__(self):
        super().__post_init__()
        valid_rules = ['both', 'any']
        if self.rule not in valid_rules:
            logger.error(f"Invalid rule '{self.rule}' for TeamsFilter. Must be one of {valid_rules}.")
            raise ValueError(f"Invalid rule '{self.rule}' for TeamsFilter. Must be one of {valid_rules}.")
        if not isinstance(self.team_ids, list) or not all(isinstance(id, int) for id in self.team_ids):
            logger.error("Team IDs must be a list of integers.")
            raise TypeError("Team IDs must be a list of integers.")

@dataclass
class CompetitionsFilter(SelectionFilter):
    competition_ids: list[int]
    key: str = field(init=False, default='competitions', repr=False)

    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.competition_ids, list) or not all(isinstance(id, int) for id in self.competition_ids):
            logger.error("Competition IDs must be a list of integers.")
            raise TypeError("Competition IDs must be a list of integers.")

@dataclass
class SessionFilter(SelectionFilter):
    sessions: list[str]
    key: str = field(init=False, default='session', repr=False)

    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.sessions, list) or not all(isinstance(name, str) for name in self.sessions):
            logger.error("Session names must be a list of strings.")
            raise TypeError("Session names must be a list of strings.")
        if not self.sessions:
            logger.error("Session names cannot be an empty list.")
            raise ValueError("Session names cannot be an empty list.")


## Factory class

class SelectionFilterFactory:
    """ Factory class to create selection filters based on type. """

    _registry: dict[str, type[SelectionFilter]] = {}

    @classmethod
    def _build_registry(cls):
        """ Builds a registry of all subclasses of SelectionFilter using their `key` field. """
        if cls._registry:
            return  # already built

        for subclass in SelectionFilter.__subclasses__():
            try:
                key = getattr(subclass, 'key')
                if not isinstance(key, str):
                    logger.error(f"Invalid key type for {subclass.__name__}: {type(key)}")
                    raise TypeError(f"Invalid key type for {subclass.__name__}: {type(key)}")
                cls._registry[key] = subclass
            except AttributeError:
                logger.warning(f"No key defined on filter class {subclass.__name__}; skipping.")
            except Exception as e:
                logger.error(f"Failed to register filter class {subclass.__name__}: {e}")
                raise

    @classmethod
    def create_filter(cls, filter_type: str, **kwargs) -> SelectionFilter:
        """ Create a SelectionFilter based on the provided filter_type. """
        cls._build_registry()
        filter_cls = cls._registry.get(filter_type)
        if not filter_cls:
            logger.error(f"Unknown filter type: '{filter_type}'")
            raise ValueError(f"Unknown filter type: '{filter_type}'")
        return filter_cls.create(**kwargs)
