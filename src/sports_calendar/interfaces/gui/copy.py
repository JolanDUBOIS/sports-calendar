""" User-facing wording for the GUI.

Kept here as plain data, deliberately separate from layout, so the guidance can
be written and reviewed without touching component code.

Every description below was written against the executor it describes
(`infra/engine/executors/`). If an executor's behaviour changes, change the
sentence here too — wrong help is worse than no help.
"""

from sportindex import StageTier

from sports_calendar.core.selection import FilterType, Rule

# ---- Filter types ---------------------------------------------------------

FILTER_TYPE_LABELS: dict[FilterType, str] = {
    FilterType.EMPTY: "Not set yet",
    FilterType.COMPETITIONS: "Specific competitions",
    FilterType.COMPETITORS: "Specific teams or players",
    FilterType.MIN_RANKING: "Top-ranked teams",
    FilterType.SESSIONS: "Race sessions",
}

FILTER_TYPE_HELP: dict[FilterType, str] = {
    FilterType.EMPTY:
        "A placeholder for a filter you haven't configured yet. On its own it "
        "adds no events — pick one of the other types above.",
    FilterType.COMPETITIONS:
        "Follow whole competitions: every fixture of the current season for each "
        "competition you pick. Good for \"all Champions League matches\".",
    FilterType.COMPETITORS:
        "Follow specific teams or players. Use the rule below to say whether you "
        "want every match they play, or only certain match-ups.",
    FilterType.MIN_RANKING:
        "Follow whoever is currently near the top. You pick the competitions and "
        "a cut-off position, and the teams are looked up from the live standings "
        "each time the calendar is built — so it keeps itself up to date.",
    FilterType.SESSIONS:
        "For motorsport and other staged events: pick a championship, then choose "
        "which session types you care about (the race only, or practice and "
        "qualifying too).",
}

# ---- Selection rules ------------------------------------------------------

RULE_LABELS: dict[Rule, str] = {
    Rule.ANY: "Any of them is playing",
    Rule.BOTH: "Two of them play each other",
    Rule.OPPONENT: "One specific team plays one of them",
}

RULE_HELP: dict[Rule, str] = {
    Rule.ANY:
        "Keep a match if at least one of the teams you picked is in it. This is "
        "the usual choice.",
    Rule.BOTH:
        "Keep a match only when both sides are teams you picked — the big "
        "match-ups between your teams, and nothing else.",
    Rule.OPPONENT:
        "Keep a match only when the reference team below is playing, and its "
        "opponent is one of the teams you picked.",
}

# ---- Sessions -------------------------------------------------------------

# StageTier also carries structural levels (SPORT, SEASON, EVENT, LAP) that are
# not sessions a user would ever pick. Only the meaningful ones are offered.
SESSION_LABELS: dict[StageTier, str] = {
    StageTier.PRACTICE: "Practice",
    StageTier.QUALIFYING: "Qualifying",
    StageTier.QUALIFYING_PART: "Qualifying (part)",
    StageTier.SPRINT_QUALIFYING: "Sprint qualifying",
    StageTier.SPRINT_RACE: "Sprint race",
    StageTier.RACE: "Race",
    StageTier.PROLOGUE: "Prologue",
    StageTier.STAGE: "Stage",
}

# ---- Search hints ---------------------------------------------------------

# Examples are written against a sport *slug*, never a raw id: slugs are
# readable, and several sports would otherwise share the placeholder id used for
# "not looked up yet" and silently overwrite each other in this table.
SEARCH_EXAMPLES: dict[tuple[str, str], tuple[str, ...]] = {
    ("competition", "football"): ("UEFA Champions League", "Ligue 1"),
    ("competitor", "football"): ("Paris Saint-Germain", "PSG", "Arsenal"),

    ("competition", "motorsport"): ("Formula 1", "MotoGP"),
    ("competitor", "motorsport"): ("Max Verstappen", "Ferrari"),

    ("competition", "tennis"): ("Roland Garros", "Wimbledon"),
    ("competitor", "tennis"): ("Carlos Alcaraz", "Iga Swiatek"),

    ("competition", "rugby"): ("Six Nations", "Top 14"),
    ("competitor", "rugby"): ("Stade Toulousain", "France"),

    ("competition", "basketball"): ("EuroLeague", "NBA"),
    ("competitor", "basketball"): ("ASVEL", "Boston Celtics"),

    ("competition", "handball"): ("EHF Champions League", "Liqui Moly Starligue"),
    ("competitor", "handball"): ("Paris Saint-Germain", "Montpellier"),

    ("competition", "cycling"): ("Tour de France", "Paris-Roubaix"),
    ("competitor", "cycling"): ("Tadej Pogacar", "Groupama-FDJ"),
}

# sport-index sport id -> slug, read off the live provider on 2026-08-11 via
# `sports-calendar devtools list-sports`. Re-run that command if the provider
# ever adds a sport; a slug with no SEARCH_EXAMPLES row simply falls back to the
# generic wording.
SPORT_SLUGS: dict[int, str] = {
    63: "american-football",
    31: "badminton",
    15: "bandy",
    2: "basketball",
    34: "beach-volley",
    62: "cricket",
    65: "cycling",
    22: "darts",
    72: "esports",
    1: "football",
    29: "futsal",
    6: "handball",
    4: "ice-hockey",
    109: "minifootball",
    76: "mma",
    11: "motorsport",
    12: "rugby",
    19: "snooker",
    20: "table-tennis",
    5: "tennis",
    23: "volleyball",
    26: "waterpolo",
}

# Used when a sport has no examples written yet — still better than "Search...".
GENERIC_SEARCH_SUBJECT: dict[str, str] = {
    "competition": "a competition or league",
    "competitor": "a team or player",
}


def search_hint(kind: str, sport_id: int) -> str:
    """ Placeholder for a search box: concrete examples where we have them.

    Falls back to generic wording when the sport's id has not been mapped yet,
    rather than showing another sport's examples.
    """
    slug = SPORT_SLUGS.get(sport_id)
    examples = SEARCH_EXAMPLES.get((kind, slug)) if slug else None
    if examples:
        return f"Search... (e.g. {', '.join(examples)})"

    subject = GENERIC_SEARCH_SUBJECT.get(kind)
    return f"Search for {subject}..." if subject else "Search..."


# ---- Field-level guidance -------------------------------------------------

FIELD_HELP: dict[str, str] = {
    "ranking":
        "How far down the table to go. 5 means the top five positions.",
    "competition_ids":
        "Type to search. Only competitions for this item's sport are shown.",
    "competition_id":
        "The championship whose sessions you want to follow.",
    "competitor_ids":
        "Type to search for teams or players in this item's sport.",
    "selection_rule":
        "Decides which matches are kept once your teams are known.",
    "selection_reference":
        "Only used by the \"one specific team plays one of them\" rule.",
    "sessions":
        "Which parts of a race weekend to put in the calendar.",
}

# ---- Vocabulary -----------------------------------------------------------
#
# The domain model says Selection / SelectionItem / SelectionFilter. Users see
# calendar / sport / rule, because that is what those things actually are — an
# "item" is exactly one sport, and a "selection" is exactly one calendar.
# Renaming here means most screens no longer need a paragraph explaining a word
# that only ever existed in the code. Nothing below this layer changes.

CALENDARS_PAGE_TITLE = "My calendars"
CALENDARS_PAGE_INTRO = "Each calendar is a set of sports events, synced to Google."

NEW_CALENDAR_TITLE = "New calendar"
NEW_CALENDAR_MESSAGE = (
    "One calendar of sports events, synced to Google. You'll add sports to it next."
)
CALENDAR_NAME_LABEL = "Name"

DELETE_CALENDAR_TITLE = "Delete this calendar?"
DELETE_CALENDAR_MESSAGE = (
    'Everything in "{name}" goes with it — the sports and what you follow in them.'
)

ADD_SPORT_TITLE = "Add a sport"
ADD_SPORT_MESSAGE = (
    "Each sport gets its own section. You then choose what to follow inside it."
)
SPORT_NAME_LABEL = "Name (optional)"

NO_SPORTS_YET = "No sports yet. Add one to start choosing what to follow."

DELETE_SPORT_TITLE = "Remove this sport?"
DELETE_SPORT_MESSAGE = "This also removes everything you follow in it."

FOLLOW_TITLE = "What do you want to follow?"
FOLLOW_TYPE_LABEL = "Choose one"

DELETE_RULE_TITLE = "Remove this?"
DELETE_RULE_MESSAGE = "It stops being included in the calendar."

# Notifications — plain and in the same vocabulary.
CALENDAR_CREATED = 'Calendar "{name}" created.'
CALENDAR_DELETED = 'Calendar "{name}" deleted.'
SPORT_ADDED = "Sport added."
SPORT_REMOVED = "Sport removed."
RULE_ADDED = "Added. Now choose what to follow."
RULE_REMOVED = "Removed."
RULE_SAVED = "Saved."


# ---- Concept explanations -------------------------------------------------

# The longer version, shown on hover behind an ⓘ. One line stays on screen; this
# is here for whoever wants it, so the default view never has to teach.

WHAT_IS_A_CALENDAR = (
    "A calendar collects everything you want to watch. You add sports to it, and "
    "within each sport you say what to follow — a competition, some teams, or "
    "the top of a table. Every matching event is synced to Google Calendar."
)

WHAT_IS_A_SPORT_SECTION = (
    "Sports are kept apart because they work differently: football has leagues "
    "and tables, motorsport has race weekends. Adding a sport lets the app offer "
    "you the right choices for it."
)

WHAT_IS_A_RULE = (
    "Each line describes a set of events to include. You can add several — follow "
    "a whole competition and a couple of extra teams, and you get both."
)

# ---- Preview --------------------------------------------------------------

PREVIEW_INTRO = (
    "The events this selection currently produces, in date order. This runs the "
    "same resolver the sync uses, so it is what would land in your calendar."
)

PREVIEW_STALE = "Nothing loaded yet — hit the button to build the preview."

PREVIEW_EMPTY = (
    "This selection produced no events. Check that its filters have competitions "
    "or teams chosen, and that a season is currently running."
)

# Placeholder examples, shown when naming things.
SELECTION_NAME_EXAMPLES = "e.g. My football season, Weekend sport, F1 2026"

ITEM_NAME_EXAMPLES: dict[int, str] = {
    1: "e.g. Premier League + PSG",
    11: "e.g. F1 race weekends",
}
DEFAULT_ITEM_NAME_EXAMPLE = "e.g. Everything I want to watch"


def filter_type_label(filter_type: FilterType) -> str:
    """ Human label for a filter type, falling back to its raw value. """
    return FILTER_TYPE_LABELS.get(filter_type, filter_type.value)


def item_name_example(sport_id: int) -> str:
    """ A naming example appropriate to the sport, when one is known. """
    return ITEM_NAME_EXAMPLES.get(sport_id, DEFAULT_ITEM_NAME_EXAMPLE)
