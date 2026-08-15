""" User-facing wording for the GUI.

Kept here as plain data, deliberately separate from layout, so the guidance can
be written and reviewed without touching component code.

Every description below was written against the executor it describes
(`infra/engine/executors/`). If an executor's behaviour changes, change the
sentence here too — wrong help is worse than no help.
"""

from sportindex import StageTier

from sports_calendar.core.calendar import STAGE_TIER_LABELS
from sports_calendar.core.selection import FilterType, Rule

# ---- Filter types ---------------------------------------------------------

# `{competitors}` and friends are filled in per sport by `_for_sport` below:
# "teams" for football, "players" for tennis. Written as placeholders rather
# than patched afterwards, because the previous approach — labelling everything
# "Top Teams" and then running `.replace("Top Teams", "Top Players")` — only
# worked for the exact strings someone remembered to spell that way.
FILTER_TYPE_LABELS: dict[FilterType, str] = {
    FilterType.EMPTY: "Not set yet",
    FilterType.COMPETITIONS: "Specific competitions",
    FilterType.COMPETITORS: "Specific {competitors}",
    FilterType.MIN_RANKING: "Top {Competitors}",
    FilterType.WORLD_RANKING: "Top {Competitors} (world rankings)",
    FilterType.SESSIONS: "Race sessions",
}

FILTER_TYPE_HELP: dict[FilterType, str] = {
    FilterType.EMPTY:
        "A placeholder for a filter you haven't configured yet. On its own it "
        "adds no events — pick one of the other types above.",
    FilterType.COMPETITIONS:
        "Follow whole competitions: every fixture of the current season for each "
        "one you pick. You can also start from a round, to follow only the "
        "knockout stages of a cup.",
    FilterType.COMPETITORS:
        "Follow {competitors} you name. Use the rule below to say whether you "
        "want every match they play, or only certain match-ups.",
    FilterType.MIN_RANKING:
        "Follow whoever currently sits at the top of a table. Pick the standings "
        "to read and a cut-off position — you then get every match those "
        "{competitors} play, not only their matches in that competition. Read "
        "again on each build, so it follows the table rather than a fixed list. "
        "For a ranking that spans every competition, use "
        "\"{world_ranking_label}\".",
    FilterType.WORLD_RANKING:
        "Follow whoever currently sits near the top of a world ranking — FIFA, "
        "the ATP, World Rugby. The only difference from \"{min_ranking_label}\" "
        "is where the ranking is read from: a global ranking rather than one "
        "competition's table. Either way you get every match those "
        "{competitors} play.",
    FilterType.SESSIONS:
        "For motorsport and other staged events: pick a championship, then choose "
        "which session types you care about (the race only, or practice and "
        "qualifying too).",
}

# ---- Selection rules ------------------------------------------------------

RULE_LABELS: dict[Rule, str] = {
    Rule.ANY: "Any of them is playing",
    Rule.BOTH: "Two of them play each other",
    # Names the picked side first, like the other two, so the three read as
    # variations of one sentence rather than three unrelated ones.
    Rule.OPPONENT: "One of them plays a specific opponent",
}

# NOT CURRENTLY SHOWN ANYWHERE. `SelectField` has no per-option help, so what
# the user actually reads about this choice is `FIELD_HELP["selection_rule"]`.
# Kept because the wording is better than the one line that is displayed, and
# because surfacing it is a small UI change rather than a rewrite.
RULE_HELP: dict[Rule, str] = {
    Rule.ANY:
        "Keep a match if at least one of the {competitors} you picked is in it. "
        "This is the usual choice.",
    Rule.BOTH:
        "Keep a match only when both sides are {competitors} you picked — the "
        "big match-ups between them, and nothing else.",
    Rule.OPPONENT:
        "Keep a match only when the opponent named below is playing, and it is "
        "up against one of the {competitors} you picked.",
}

# ---- Sessions -------------------------------------------------------------

# Taken from core rather than restated here: the words a user picks in this
# dropdown are the same words that end up in the calendar entry, so there must
# be one copy of them. Core already excludes the structural tiers (SPORT,
# SEASON, EVENT, LAP) that nobody would ever choose.
SESSION_LABELS: dict[StageTier, str] = STAGE_TIER_LABELS

# ---- Search hints ---------------------------------------------------------

# Examples are written against a sport *slug*, never a raw id: slugs are
# readable, and several sports would otherwise share the placeholder id used for
# "not looked up yet" and silently overwrite each other in this table.
# Every row is gender-balanced wherever the sport allows it, alternating which
# comes first so neither reads as the default. Where one name covers both
# editions (Roland Garros, the Tour), balance is in *which* competitions are
# named rather than in a suffix.
SEARCH_EXAMPLES: dict[tuple[str, str], tuple[str, ...]] = {
    ("competition", "football"): ("UEFA Women's Champions League", "Ligue 1"),
    ("competitor", "football"): ("Paris Saint-Germain", "PSG", "Arsenal Women"),

    # Three rather than two: MotoGP is the obvious second championship to name,
    # and dropping F1 Academy for it would leave motorsport the only row with no
    # women's series in it.
    ("competition", "motorsport"): ("Formula 1", "MotoGP", "F1 Academy"),
    ("competitor", "motorsport"): ("Doriane Pin", "Max Verstappen"),

    ("competition", "tennis"): ("Roland Garros", "Wimbledon"),
    ("competitor", "tennis"): ("Carlos Alcaraz", "Iga Swiatek"),

    ("competition", "rugby"): ("Six Nations", "Women's Six Nations"),
    ("competitor", "rugby"): ("France Women", "Stade Toulousain"),

    ("competition", "basketball"): ("WNBA", "EuroLeague"),
    ("competitor", "basketball"): ("ASVEL Féminin", "Boston Celtics"),

    ("competition", "handball"): ("EHF Champions League", "EHF Women's Champions League"),
    ("competitor", "handball"): ("Metz Handball", "Paris Saint-Germain"),

    ("competition", "cycling"): ("Tour de France", "Tour de France Femmes"),
    ("competitor", "cycling"): ("Demi Vollering", "Tadej Pogacar"),
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
        "Type to search. Only competitions for this sport are shown.",
    "competition_id":
        "The championship whose sessions you want to follow.",
    "competitor_ids":
        "Type to search for {competitors} in this sport.",
    "selection_rule":
        "Decides which matches are kept once your {competitors} are known.",
    # Says what the field is for rather than naming the rule that uses it: the
    # field is only visible under that rule now, so repeating its name was both
    # the longest line here and the least informative.
    #
    # Phrased without a pronoun for the competitor, because the right one
    # changes with the sport — "it" is fine for a club and wrong for a person.
    "selection_reference":
        "Keeps only the matches where one of those above plays this "
        "{competitor}.",
    "sessions":
        "Which parts of a race weekend to put in the calendar.",
    "from_round":
        "Empty keeps every match. Otherwise: that round, and everything after.",
    # "Each governing body keeps its own" meant nothing to anyone who did not
    # already know the answer. Says what you will actually see in the menu.
    "ranking_id":
        "Which ranking to read. Most sports publish more than one — men's and "
        "women's, singles and doubles.",
    "world_ranking":
        "How far down the ranking to go. 20 means the current top twenty.",
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
SPORT_NAME_HELP = "Only a label for you. Leave it blank and the sport's name is used."

NO_SPORTS_YET = "No sports yet. Add one to start choosing what to follow."

# A sport already in the calendar is not offered again: everything a second card
# could hold belongs on the first one, and would behave identically there.
ALL_SPORTS_ADDED = "Every sport is already in this calendar. Add what you follow to the cards below."

DELETE_SPORT_TITLE = "Remove this sport?"
DELETE_SPORT_MESSAGE = "This also removes everything you follow in it."

ROUNDS_LABEL = "From which round (optional)"

# Shown in place of an empty menu. Competitions only share round names when they
# are structured alike — several cups share "Final" and "Semifinals", but a
# league has nothing in common with a cup, so there is nothing to offer.
ROUNDS_NONE_SHARED = (
    "These competitions don't share any rounds, so every match is kept. "
    "Group competitions with similar rounds to filter by round."
)

FOLLOW_TITLE = "What do you want to follow?"
FOLLOW_TYPE_LABEL = "Choose one"

# Not "follow something else": the button reads the same whether the card is
# empty or full, and "else" contradicts the "Nothing followed yet" sitting
# right above it. It also names the act of defining a rule, not of appending.
ADD_RULE_BUTTON = "Add something to follow"

RULE_NAME_LABEL = "Name (optional)"
RULE_NAME_HELP = "Only a label for you. Leave it blank and the rule describes itself."
RULE_NAME_EXAMPLES = "e.g. My big clubs, Title race, Finals only"

DELETE_RULE_TITLE = "Remove this?"
DELETE_RULE_MESSAGE = "It stops being included in the calendar."

# The dialog opens straight away and fills itself in, so this sits where the
# fields will be. Names what is being waited on rather than saying "Loading".
MODAL_LOADING = "Looking up what this rule follows…"
MODAL_LOAD_FAILED = "Couldn't load this rule. Check your connection and try again."

# Names the likely cause, because the symptom is indistinguishable from "no such
# team" and the cause is almost never obvious from where the user is sitting.
SEARCH_UNAVAILABLE = (
    "Couldn't reach the sports data service. A VPN, proxy, or corporate network "
    "is the usual cause — try turning it off."
)
SEARCH_FAILED = "Something went wrong with that search. Try again."

# Only shown for the "opponent" rule, which is the only one that reads it.
OPPONENT_LABEL = "Opponent Team"

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
# A calendar spans several sports, so examples named after one sport misdescribe
# it. What actually distinguishes two calendars is whose they are and what
# occasion they serve.
SELECTION_NAME_EXAMPLES = "e.g. Everything I follow, Watching with friends, Just the big nights"

ITEM_NAME_EXAMPLES: dict[int, str] = {
    1: "e.g. Ligue 1 + PSG",
    2: "e.g. EuroLeague nights",
    5: "e.g. Grand Slams",
    6: "e.g. Starligue + PSG Handball",
    11: "e.g. F1 race weekends",
    12: "e.g. Six Nations",
    65: "e.g. Grand Tours",
}
DEFAULT_ITEM_NAME_EXAMPLE = "e.g. Everything I want to watch"


# Sports whose competitors are individuals rather than clubs. "Top Teams" is
# wrong for tennis, and "Top Competitors" is worse than either.
_INDIVIDUAL_SPORT_IDS = frozenset({5, 65})  # tennis, cycling


def competitors_word(sport_id: int | None) -> str:
    """ "teams" or "players", whichever this sport's competitors are. """
    return "players" if sport_id in _INDIVIDUAL_SPORT_IDS else "teams"


def _for_sport(text: str, sport_id: int | None) -> str:
    """ Fill a copy template's `{competitors}` placeholders for one sport.

    `{world_ranking_label}` and `{min_ranking_label}` are available too, so that
    a sentence pointing at another menu entry names it by reference. One help
    text used to say 'use "Top of the world rankings"', which was not what the
    menu had said for some time — a cross-reference that can go stale is worse
    than none, because it sends the reader looking for something that isn't
    there.
    """
    plural = competitors_word(sport_id)
    singular = plural.removesuffix("s")
    return text.format(
        competitor=singular,
        Competitor=singular.capitalize(),
        competitors=plural,
        Competitors=plural.capitalize(),
        min_ranking_label=filter_type_label(FilterType.MIN_RANKING, sport_id),
        world_ranking_label=filter_type_label(FilterType.WORLD_RANKING, sport_id),
    )


def filter_type_label(filter_type: FilterType, sport_id: int | None = None) -> str:
    """ Human label for a filter type, falling back to its raw value. """
    label = FILTER_TYPE_LABELS.get(filter_type, filter_type.value)
    plural = competitors_word(sport_id)
    # Not `_for_sport`: that resolves the two label placeholders by calling back
    # here, which would recurse.
    return label.format(competitors=plural, Competitors=plural.capitalize())


def filter_type_help(filter_type: FilterType, sport_id: int | None = None) -> str | None:
    """ The paragraph explaining a filter type, in this sport's vocabulary. """
    help_text = FILTER_TYPE_HELP.get(filter_type)
    return _for_sport(help_text, sport_id) if help_text else None


def rule_label(rule: Rule, sport_id: int | None = None) -> str:
    """ Human label for a selection rule. """
    return _for_sport(RULE_LABELS.get(rule, rule.name.capitalize()), sport_id)


def rule_help(rule: Rule, sport_id: int | None = None) -> str | None:
    """ The explanation of one selection rule. Not currently displayed — see
    `RULE_HELP`. """
    help_text = RULE_HELP.get(rule)
    return _for_sport(help_text, sport_id) if help_text else None


def field_help(field_name: str, sport_id: int | None = None) -> str | None:
    """ The hint under one form field, in this sport's vocabulary. """
    help_text = FIELD_HELP.get(field_name)
    return _for_sport(help_text, sport_id) if help_text else None


def item_name_example(sport_id: int) -> str:
    """ A naming example appropriate to the sport, when one is known. """
    return ITEM_NAME_EXAMPLES.get(sport_id, DEFAULT_ITEM_NAME_EXAMPLE)
