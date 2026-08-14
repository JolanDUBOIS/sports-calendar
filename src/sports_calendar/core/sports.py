""" Which sports the app supports, and what can meaningfully be followed in each.

Two separate questions live here.

**Is the sport supported at all?** Answered by `EVENT_TYPE_MAP`: a sport is
supported exactly when its fixtures can be turned into calendar events. Anything
else makes `build_calendar` raise, so the UI must never offer it.

**Which filters make sense for it?** Not every filter works for every sport, and
the ones that don't fail *silently* — they return nothing, or log a warning and
skip. That is worse than an error, because the calendar just comes back empty
with no explanation. The table below is derived from what each executor actually
requires:

- `COMPETITIONS` / `COMPETITORS` read season fixtures and competitor ids. Both
  work for any sport.
- `SESSIONS` needs `StageEvent.substages` and skips anything else
  (`executors/sessions.py`), so it only means something for staged sports —
  motorsport race weekends, cycling stages.
- `MIN_RANKING` reads `competition.seasons[0].standings` and keeps everyone above
  a cut-off position (`executors/min_ranking.py`). That needs a *league table*,
  which only team leagues have.
- `EMPTY` is the unconfigured placeholder every new filter starts as, so it is
  always allowed regardless of sport.

A blank cell is a statement about today's executors, not about the sport. Tennis
is the clearest case: "top 20 ATP" is a perfectly good thing to want, and
sport-index already exposes it via `Sport.get_rankings()` — but that is a
*rankings* lookup, not a *standings* one, so it needs its own filter type and
executor before it can be offered here. Tracked as B2 in ROADMAP.md.
"""

from .calendar.events.mapping import EVENT_TYPE_MAP
from .selection.filters.enums import FilterType

# sport-index sport ids, named so the tables below read as domain rather than
# magic numbers. These match sportindex's SPORTS_REGISTRY.
FOOTBALL = 1
BASKETBALL = 2
TENNIS = 5
HANDBALL = 6
MOTORSPORT = 11
RUGBY = 12
CYCLING = 65

#: Sports the app can build a calendar for. Derived, not hand-written, so it can
#: never drift from the event map that actually does the work.
SUPPORTED_SPORT_IDS: frozenset[int] = frozenset(EVENT_TYPE_MAP)

# What every sport can do: follow a competition, or follow named competitors.
_BASE_FILTERS = frozenset({
    FilterType.COMPETITIONS,
    FilterType.COMPETITORS,
})

# Filters that need a league table (a "top N of this competition" question).
_LEAGUE_FILTERS = _BASE_FILTERS | {FilterType.MIN_RANKING}

# Filters for staged events, where one "event" is a weekend of sessions.
_STAGE_FILTERS = _BASE_FILTERS | {FilterType.SESSIONS}

# Sports with a governing body's standing order — ATP, FIFA, World Rugby.
# Keyed off SPORT_RANKINGS in sport-index: only football, tennis, rugby and MMA
# publish one, and of those we support the first three.
_WORLD_RANKING_FILTERS = frozenset({FilterType.WORLD_RANKING})

_ALLOWED_FILTER_TYPES: dict[int, frozenset[FilterType]] = {
    FOOTBALL: _LEAGUE_FILTERS | _WORLD_RANKING_FILTERS,
    BASKETBALL: _LEAGUE_FILTERS,
    HANDBALL: _LEAGUE_FILTERS,
    RUGBY: _LEAGUE_FILTERS | _WORLD_RANKING_FILTERS,
    TENNIS: _BASE_FILTERS | _WORLD_RANKING_FILTERS,
    MOTORSPORT: _STAGE_FILTERS,
    CYCLING: _STAGE_FILTERS,
}

# Which ranking tables each sport publishes, as (provider ranking id, label).
# The ids come from SPORT_RANKINGS in sport-index's domain/static.py. The live
# and UTR variants it also lists are deliberately left out: they say the same
# thing as the headline ranking with more noise.
#
# UEFA Countries (id 1) is also left out, for a stronger reason: it ranks
# *competitions*, not competitors — its top five are the Premier League, Serie
# A, LaLiga, Bundesliga and Ligue 1. Following it as a competitor filter can
# only ever produce nothing. It would make a good "top N leagues by
# coefficient" competitions filter, which does not exist yet.
SPORT_RANKING_CHOICES: dict[int, tuple[tuple[int, str], ...]] = {
    FOOTBALL: (
        (2, "FIFA World Ranking"),
        (9, "UEFA Clubs"),
    ),
    TENNIS: (
        (5, "ATP (men)"),
        (6, "WTA (women)"),
    ),
    RUGBY: (
        (3, "Rugby Union"),
        (4, "Rugby League"),
    ),
}


def ranking_choices(sport_id: int) -> tuple[tuple[int, str], ...]:
    """ The ranking tables a sport publishes, newest-relevant first. """
    return SPORT_RANKING_CHOICES.get(sport_id, ())

# Every filter starts life as EMPTY, so it is always on offer.
_ALWAYS_ALLOWED = frozenset({FilterType.EMPTY})

# The safe default for a sport with no row: offer only what works everywhere,
# rather than offering something that would quietly produce nothing.
_DEFAULT_FILTER_TYPES = _BASE_FILTERS


def is_supported(sport_id: int) -> bool:
    """ Whether the app can build calendar events for this sport. """
    return sport_id in SUPPORTED_SPORT_IDS


def allowed_filter_types(sport_id: int) -> frozenset[FilterType]:
    """ The filter types worth offering for a sport.

    Always includes EMPTY: a filter is created before it is configured, so the
    unconfigured state has to remain representable for every sport.
    """
    return _ALLOWED_FILTER_TYPES.get(sport_id, _DEFAULT_FILTER_TYPES) | _ALWAYS_ALLOWED
