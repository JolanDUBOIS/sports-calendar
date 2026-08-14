from enum import Enum

# === Filter Type Enum ====

class FilterType(Enum):
    EMPTY = "empty"
    MIN_RANKING = "min_ranking"
    COMPETITIONS = "competitions"
    COMPETITORS = "competitors"
    SESSIONS = "sessions"
    # Distinct from MIN_RANKING: that one reads a season's league table, this
    # one reads a governing body's standing order (ATP, FIFA). Sports with no
    # league table, tennis above all, only have the latter.
    WORLD_RANKING = "world_ranking"


# === Filter Target Enum ====
class FilterTarget(Enum):
    COMPETITOR = "competitor"
    COMPETITION = "competition"
    SESSION = "session"
    NONE = "none"
    # More to be added as needed
