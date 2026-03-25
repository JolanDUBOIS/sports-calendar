from .base import SportsEvent
from .f1 import F1Event
from .football import FootballEvent
from sports_calendar.core import SportType

EVENT_TYPE_MAP: dict[SportType, type[SportsEvent]] = {
    SportType.F1: F1Event,
    SportType.FOOTBALL: FootballEvent
}