""" Competitor matching across sport-index's two id views.

The bug these cover: a team picked from the search box is stored as
`team:1644`, but the same club appears on its own fixtures as `t-cpt:1644`.
Comparing the stored strings matched nothing, so "follow PSG" produced an
empty calendar while looking entirely healthy.
"""

from types import SimpleNamespace

import pytest

from sports_calendar.core import raw_entity_id
from sports_calendar.core.selection import CompetitorsFilterFields, EntitySelectionRule, Rule
from sports_calendar.infra.engine.executors import CompetitorsExecutor

PSG_SEARCH = "team:1644"     # what the search box stores
PSG_EVENT = "t-cpt:1644"     # what a fixture carries
ARSENAL_EVENT = "t-cpt:42"
ARSENAL_SEARCH = "team:42"


def _event(home_id: str, away_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        id="mch:1",
        competitors=SimpleNamespace(
            home=SimpleNamespace(id=home_id),
            away=SimpleNamespace(id=away_id),
        ),
    )


def _matches(event, ids: list[str], rule: EntitySelectionRule | None = None) -> bool:
    fields = CompetitorsFilterFields(
        competitor_ids=ids,
        selection_rule=rule or EntitySelectionRule(rule=Rule.ANY),
    )
    return CompetitorsExecutor._matches_selection_rule(event, fields)


def test_raw_entity_id_strips_the_view_prefix() -> None:
    assert raw_entity_id(PSG_SEARCH) == raw_entity_id(PSG_EVENT) == "1644"


def test_raw_entity_id_keeps_only_the_entitys_own_segment() -> None:
    """ Compound ids lead with their parent. """
    assert raw_entity_id("stgc:40:stgs:214140") == "214140"


def test_search_id_matches_a_fixture_carrying_the_competitor_id() -> None:
    """ The regression: these two spellings are the same club. """
    assert _matches(_event(PSG_EVENT, ARSENAL_EVENT), [PSG_SEARCH])


def test_unrelated_team_still_does_not_match() -> None:
    assert not _matches(_event(PSG_EVENT, ARSENAL_EVENT), ["team:9999"])


def test_both_rule_requires_every_side_to_be_wanted() -> None:
    event = _event(PSG_EVENT, ARSENAL_EVENT)
    rule = EntitySelectionRule(rule=Rule.BOTH)

    assert _matches(event, [PSG_SEARCH, ARSENAL_SEARCH], rule)
    assert not _matches(event, [PSG_SEARCH], rule)


def test_opponent_rule_matches_across_both_id_views() -> None:
    event = _event(PSG_EVENT, ARSENAL_EVENT)
    rule = EntitySelectionRule(rule=Rule.OPPONENT, reference=PSG_SEARCH)

    assert _matches(event, [ARSENAL_SEARCH], rule)
    assert not _matches(event, ["team:9999"], rule)


def test_opponent_rule_ignores_events_without_the_reference() -> None:
    event = _event("t-cpt:100", "t-cpt:200")
    rule = EntitySelectionRule(rule=Rule.OPPONENT, reference=PSG_SEARCH)

    assert not _matches(event, ["team:100"], rule)


@pytest.mark.parametrize("rule", [Rule.ANY, Rule.BOTH, Rule.OPPONENT])
def test_event_without_competitors_never_matches(rule: Rule) -> None:
    """ Used to raise NameError: the competitor set was only bound inside the
    `if event.competitors` branch, then read unconditionally. """
    event = SimpleNamespace(id="mch:1", competitors=None)
    reference = PSG_SEARCH if rule is Rule.OPPONENT else None

    assert not _matches(event, [PSG_SEARCH], EntitySelectionRule(rule=rule, reference=reference))
