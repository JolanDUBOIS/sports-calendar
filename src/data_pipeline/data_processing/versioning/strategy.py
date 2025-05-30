from __future__ import annotations
from dataclasses import dataclass

from . import logger


@dataclass
class SourceVersioningStrategy:
    """ Defines how versioning is done for a source. """
    field: str
    mode: str
    
    mode_specs = {
        "newest": {"operator": ">="},
        "all": {"operator": None}
    }

    def __post_init__(self) -> None:
        """ Validate field and mode. """
        if not self.field or not self.mode:
            logger.error("field and mode must be provided.")
            raise ValueError("field and mode must be provided.")
        if not isinstance(self.field, str) or not isinstance(self.mode, str):
            logger.error("field and mode must be strings.")
            raise TypeError("field and mode must be strings.")

        if not self.mode in self.mode_specs:
            logger.error(f"Unknown mode '{self.mode}'. Valid modes are: {list(self.mode_specs.keys())}.")
            raise ValueError(f"Unknown mode '{self.mode}'. Valid modes are: {list(self.mode_specs.keys())}.")

    def get_operator(self) -> str | None:
        """ Get the operator for the current mode. """
        return self.mode_specs[self.mode]["operator"]

    @classmethod
    def from_dict(cls, d: dict) -> SourceVersioningStrategy:
        """ Create a SourceVersioningStrategy from a dictionary. """
        return cls(
            field=d["field"],
            mode=d["mode"]
        )
