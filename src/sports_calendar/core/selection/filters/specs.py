import logging
from dataclasses import dataclass, field
from typing import Callable, Any

from ...utils import validate
from ...competition_stages import CompetitionStage


MIN_RANKING_RULES = {"both", "any", "opponent"}
TEAM_RULES = {"both", "any"}

@dataclass
class FilterSpec:
    """ Specification for a filter type. """
    filter_type: str
    fields: list[str] = field(default_factory=list)
    validators: list[Callable[[dict[str, Any], logging.Logger], None]] = field(default_factory=list)
    valid_values: dict[str, set[Any]] | None = None

FILTER_SPECS = [
    FilterSpec(
        filter_type="empty",
    ),
    FilterSpec(
        filter_type="min_ranking",
        fields=["rule", "ranking", "competition_ids", "reference_team"],
        validators=[
            lambda data, logger: validate(data["rule"] in MIN_RANKING_RULES, 
                                 f"Invalid rule '{data['rule']}'. Must be one of {sorted(MIN_RANKING_RULES)}", 
                                 logger),
            lambda data, logger: validate(isinstance(data["ranking"], int) and data["ranking"] > 0,
                                 "ranking must be a positive integer", logger),
            lambda data, logger: validate(data["rule"] != "opponent" or data.get("reference_team"),
                                 "reference_team required for opponent rule", logger),
            lambda data, logger: validate(isinstance(data.get("competition_ids", []), list),
                                 "competition_ids must be a list", logger, TypeError),
            lambda data, logger: validate(all(isinstance(cid, int) for cid in data.get("competition_ids", [])),
                                 "competition_ids must contain only integers", logger, TypeError),
        ],
        valid_values={"rule": MIN_RANKING_RULES}
    ),
    FilterSpec(
        filter_type="stage",
        fields=["stage", "competition_ids"],
        validators=[
            lambda data, logger: validate(isinstance(data.get("stage"), CompetitionStage),
                                 "stage must be a CompetitionStage", logger, TypeError),
            lambda data, logger: validate(isinstance(data.get("competition_ids", []), list),
                                 "competition_ids must be a list", logger, TypeError),
            lambda data, logger: validate(all(isinstance(cid, int) for cid in data.get("competition_ids", [])),
                                 "competition_ids must contain only integers", logger, TypeError),
        ],
        valid_values={"stage": set(CompetitionStage)}
    ),
    FilterSpec(
        filter_type="teams",
        fields=["rule", "team_ids"],
        validators=[
            lambda data, logger: validate(data["rule"] in TEAM_RULES, 
                                 f"Invalid rule '{data['rule']}'. Must be one of {sorted(TEAM_RULES)}", 
                                 logger),
            lambda data, logger: validate(isinstance(data.get("team_ids", []), list),
                                 "team_ids must be a list", logger, TypeError),
            lambda data, logger: validate(all(isinstance(tid, int) for tid in data.get("team_ids", [])),
                                 "team_ids must contain only integers", logger, TypeError),
        ],
        valid_values={"rule": TEAM_RULES}
    ),
    FilterSpec(
        filter_type="competitions",
        fields=["competition_ids"],
        validators=[
            lambda data, logger: validate(isinstance(data.get("competition_ids", []), list),
                                 "competition_ids must be a list", logger, TypeError),
            lambda data, logger: validate(all(isinstance(cid, int) for cid in data.get("competition_ids", [])),
                                 "competition_ids must contain only integers", logger, TypeError),
        ],
    ),
    FilterSpec(
        filter_type="session",
        fields=["sessions"],
        validators=[
            lambda data, logger: validate(isinstance(data.get("sessions", []), list),
                                 "sessions must be a list", logger, TypeError),
            lambda data, logger: validate(all(isinstance(s, str) and bool(s) for s in data.get("sessions", [])),
                                 "sessions must contain only non-empty strings", logger, TypeError),
        ],
    ),
]