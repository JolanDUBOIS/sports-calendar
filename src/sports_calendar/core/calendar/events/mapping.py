from .base import SportsEvent
from .match import MatchEvent
from .stage import StageEvent

# sport-index sport id -> the event shape that sport uses.
#
# Neither event class carries sport-specific logic: MatchEvent reads whatever
# sportindex.MatchEvent gives it (two competitors, a competition, a round) and
# StageEvent reads a parent event plus a session. So supporting a new sport is
# a question of which of the two shapes its fixtures come back as, not of new
# code — opposition sports are matches, staged sports are stages.
#
# This map is the single source of truth for "the app supports this sport":
# core/sports.py derives the supported set from its keys, and the UI only offers
# sports listed here. A sport missing from this map makes build_calendar raise.
EVENT_TYPE_MAP: dict[int, type[SportsEvent]] = {
    1: MatchEvent,    # football
    2: MatchEvent,    # basketball
    5: MatchEvent,    # tennis
    6: MatchEvent,    # handball
    11: StageEvent,   # motorsport
    12: MatchEvent,   # rugby
    65: StageEvent,   # cycling
}
