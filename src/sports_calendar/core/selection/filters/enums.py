from enum import Enum


# === Filter Type Enum ====

class FilterType(Enum):
    EMPTY = "empty"
    MIN_RANKING = "min_ranking"
    COMPETITIONS = "competitions"
    TEAMS = "teams"
    SESSIONS = "session"


# === Filter Target Enum ====
class FilterTarget(Enum):
    TEAM = "team"
    COMPETITION = "competition"
    SESSION = "session"
    NONE = "none"
    # More to be added as needed
