""" The opponent picker follows the rule that uses it.

"Which matches to keep" offers three rules and only one of them, OPPONENT, reads
a reference team. The picker used to sit there permanently, asking every user of
"any match" for an opponent that nothing would ever look at.
"""

from sports_calendar.core.selection import Rule
from sports_calendar.interfaces.gui.components.modals.filter_modal.field_builders import (
    _rule_and_reference_fields,
)
from sports_calendar.interfaces.gui.components.modals.filter_modal.payloads import (
    _format_selection_rule,
)

FOOTBALL = 1


class _StubProvider:
    def search_competitor(self, query, sport_id):  # noqa: ARG002
        return {}


def _fields(rule: str, reference=None):
    rule_field, reference_field = _rule_and_reference_fields(
        rule, reference, _StubProvider(), FOOTBALL
    )
    return rule_field, reference_field


def test_the_picker_is_hidden_for_rules_that_ignore_it() -> None:
    for rule in (Rule.ANY.value, Rule.BOTH.value):
        _, reference_field = _fields(rule)
        assert reference_field._visible is False, rule


def test_the_picker_is_shown_for_the_opponent_rule() -> None:
    _, reference_field = _fields(Rule.OPPONENT.value, reference="t-cpt:80")
    assert reference_field._visible is True


def test_choosing_the_opponent_rule_reveals_the_picker() -> None:
    rule_field, reference_field = _fields(Rule.ANY.value)
    assert reference_field._visible is False

    rule_field.on_change(Rule.OPPONENT.value)
    assert reference_field._visible is True

    rule_field.on_change(Rule.ANY.value)
    assert reference_field._visible is False


def test_the_reference_is_dropped_when_the_rule_does_not_use_it() -> None:
    """ Otherwise switching back to "any match" saved an opponent the form no
    longer showed. """
    payload = {"selection_rule": Rule.ANY.value, "selection_reference": "t-cpt:80"}
    assert _format_selection_rule(payload) == {"rule": Rule.ANY.value, "reference": None}


def test_the_reference_is_kept_for_the_opponent_rule() -> None:
    payload = {"selection_rule": Rule.OPPONENT.value, "selection_reference": "t-cpt:80"}
    assert _format_selection_rule(payload) == {
        "rule": Rule.OPPONENT.value,
        "reference": "t-cpt:80",
    }
