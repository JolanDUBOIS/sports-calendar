from .base import SportsEvent
from .match import MatchEvent
from .stage import StageEvent


EVENT_TYPE_MAP: dict[int, type[SportsEvent]] = {
    1: MatchEvent,
    11: StageEvent
}