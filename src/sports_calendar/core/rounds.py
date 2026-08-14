""" Which rounds of a competition can be filtered on.

sport-index reports two kinds of round. Knockout rounds carry a name and a slug
("Quarterfinals" / `quarterfinals`). Everything else is an unnamed numbered
round — a matchday.

A matchday number is not a stage anyone filters on, so unnamed rounds are not
offered individually. But a *group* of them sometimes is: the Champions League
league phase and the World Cup group stage are both just unnamed numbered
rounds, and "the group phase" is exactly the kind of thing someone wants to
follow. A league season is made of the same shape and is not a group phase at
all.

Telling them apart needs two signals, because neither alone is enough:

- **Something must follow it.** A group phase exists to feed a knockout stage.
  Six Nations has five unnamed rounds and no knockout — it is a whole
  competition, not a group phase, and a count alone would mislabel it.
- **It must be short.** A league's regular season also feeds a knockout in
  places, but at 26 rounds (Top 14), 34 (Ligue 1) or 38 (Premier League,
  EuroLeague) it is plainly a season. Real group phases run 3 (World Cup) to 14
  (EHF Champions League).
"""

from __future__ import annotations

from typing import Protocol

#: Stored in filters and matched against events. Not a sport-index slug — the
#: provider has no slug for the group phase, which is the whole problem — so it
#: is chosen not to collide with any of the real ones.
GROUP_PHASE_SLUG = "group-phase"

#: What to call it when the unnamed rounds feed a knockout stage. "League
#: phase" is the Champions League's own term, but "group phase" reads the same
#: way to everyone and covers the World Cup too.
GROUP_PHASE_LABEL = "Group phase"

#: What to call it when they don't — Ligue 1's 34 matchdays, Six Nations' five.
#: Selecting it there means the whole competition, which is exactly right.
LEAGUE_PHASE_LABEL = "League phase"

#: Shown when competitions are grouped and disagree on which of the two it is.
MIXED_PHASE_LABEL = "Group / league phase"

#: Above this many unnamed rounds it is a season, not a group phase.
#: Sits above the EHF Champions League's 14 and below Top 14's 26.
MAX_GROUP_PHASE_ROUNDS = 16


class _RoundLike(Protocol):
    name: str | None
    slug: str | None


def selectable_rounds(rounds: list[_RoundLike]) -> dict[str, str]:
    """ Rounds worth offering, as slug -> label, in the provider's own order.

    The group phase, when there is one, is emitted where its rounds actually
    sit — after qualifying, before the knockouts — rather than appended.
    """
    has_knockouts = any(r.name and r.slug for r in rounds)
    unnamed_count = sum(1 for r in rounds if not r.name)

    # Both signals decide the wording, and neither is enough alone. A short run
    # is only a group phase if something follows it — Six Nations' five rounds
    # are a whole competition. And a long run is a season even when a play-off
    # follows, as in Ligue 1 or the EuroLeague.
    phase_label = (
        GROUP_PHASE_LABEL
        if has_knockouts and unnamed_count <= MAX_GROUP_PHASE_ROUNDS
        else LEAGUE_PHASE_LABEL
    )

    selectable: dict[str, str] = {}
    phase_emitted = False
    for round_ in rounds:
        if round_.name and round_.slug:
            selectable[round_.slug] = round_.name
        elif not phase_emitted:
            selectable[GROUP_PHASE_SLUG] = phase_label
            phase_emitted = True
    return selectable


def round_label_from_slug(slug: str) -> str:
    """ A readable name for a round, derived from its slug alone.

    Deliberately offline. The provider's own label is better — "Quarterfinals"
    rather than a guess — but reading it costs a competition fetch plus a season
    fetch, and the collapsed filter cards were paying that on every page render
    just to write "from Quarterfinals" in a subtitle.

    Slugs are well behaved enough for this to land on the real name almost
    every time: `round-of-16` -> "Round of 16", `quarterfinals` ->
    "Quarterfinals", `playoff-round` -> "Playoff round".
    """
    if slug == GROUP_PHASE_SLUG:
        return GROUP_PHASE_LABEL
    return slug.replace("-", " ").capitalize()


def is_group_phase_event(event_round: _RoundLike | None) -> bool:
    """ Whether an event belongs to the group phase of its competition.

    Group-phase fixtures are precisely the ones whose round has no name: they
    are numbered matchdays within the competition.
    """
    return event_round is not None and not event_round.name


def rounds_for_competition(competition) -> dict[str, str]:
    """ A competition's selectable rounds, slug -> label, in order.

    Read from the *previous* season: the current one only lists what has been
    drawn so far, so the Champions League publishes four rounds in August and
    seventeen by spring. Shared by the modal that offers the rounds and the
    executor that matches them, so the two can never disagree about the ladder.

    Duck-typed rather than importing sport-index, so this stays in core where
    both callers can reach it.
    """
    seasons = getattr(competition, "seasons", None)
    if not seasons:
        return {}
    season = seasons[1] if len(seasons) > 1 else seasons[0]
    return selectable_rounds(list(season.rounds or []))


def round_position(ordered_slugs: list[str], event_round: _RoundLike | None) -> int | None:
    """ Where an event sits in its competition's ladder, or None if unplaceable.

    Group-phase fixtures have no slug of their own, so they take the position of
    the single phase entry that stands in for all of them.
    """
    if event_round is None:
        return None

    slug = event_round.slug if event_round.name else GROUP_PHASE_SLUG
    if slug is None:
        return None

    try:
        return ordered_slugs.index(slug)
    except ValueError:
        return None


def keeps_event_from(ordered_slugs: list[str], from_slug: str, event_round: _RoundLike | None) -> bool:
    """ Whether an event is at or beyond `from_slug` in this competition's ladder.

    Ordering is the provider's own — `Season.rounds` comes back in the order the
    rounds are played — so "from the quarter-finals" means the same thing in
    every competition without us ranking anything ourselves.
    """
    try:
        start = ordered_slugs.index(from_slug)
    except ValueError:
        # The chosen round does not exist in this competition. "From here on"
        # has no meaning, so nothing is kept rather than everything.
        return False

    position = round_position(ordered_slugs, event_round)
    return position is not None and position >= start
