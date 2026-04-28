from enum import Enum

# === Filter Type Enum ====

class FilterType(Enum):
    EMPTY = "empty"
    MIN_RANKING = "min_ranking"
    COMPETITIONS = "competitions"
    COMPETITORS = "competitors"
    SESSIONS = "sessions"


# === Filter Target Enum ====
class FilterTarget(Enum):
    COMPETITOR = "competitor"
    COMPETITION = "competition"
    SESSION = "session"
    NONE = "none"
    # More to be added as needed
