""" Reordering the rules inside an item.

Presentation only — an item is the union of its filters, so order cannot change
the calendar. It exists so a card holding eight rules can be arranged to read
well. See `Resolver` for why order stopped mattering.
"""

import pytest

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import SelectionFilter

SELECTION = "sel"
FOOTBALL = 1


@pytest.fixture
def item(tmp_path, monkeypatch):
    from sports_calendar.application.selection.registry import SelectionRegistry
    from sports_calendar.infra.config import Paths

    selections = tmp_path / "selections"
    selections.mkdir()
    monkeypatch.setattr(Paths, "CONFIG_DIR", tmp_path, raising=False)
    monkeypatch.setattr(Paths, "SELECTIONS_FOLDER", selections, raising=False)
    monkeypatch.setattr(Paths, "_setup", True, raising=False)
    monkeypatch.setattr(SelectionRegistry, "_initialized", False)
    monkeypatch.setattr(SelectionRegistry, "_selections", [])

    SelectionService.add_empty_selection(SELECTION)
    created = SelectionService.add_empty_item(SELECTION, FOOTBALL, name="Football")
    for name in ("a", "b", "c"):
        SelectionService.add_filter(
            created.uid, SelectionFilter(sport_id=FOOTBALL, name=name, uid=name)
        )
    return created


def _order(item_uid: str) -> list[str]:
    return [f.uid for f in SelectionService.get_item(item_uid).filters]


def test_a_rule_moves_in_front_of_another(item) -> None:
    SelectionService.move_filter(item.uid, "c", before_uid="a")
    assert _order(item.uid) == ["c", "a", "b"]


def test_a_rule_moves_backwards(item) -> None:
    SelectionService.move_filter(item.uid, "a", before_uid="c")
    assert _order(item.uid) == ["b", "a", "c"]


def test_no_target_sends_it_to_the_end(item) -> None:
    SelectionService.move_filter(item.uid, "a", before_uid=None)
    assert _order(item.uid) == ["b", "c", "a"]


def test_dropping_a_rule_on_itself_changes_nothing(item) -> None:
    SelectionService.move_filter(item.uid, "b", before_uid="b")
    assert _order(item.uid) == ["a", "b", "c"]


def test_an_unknown_rule_is_ignored(item) -> None:
    """ A drop that lands after the page moved on must not raise. """
    SelectionService.move_filter(item.uid, "nope", before_uid="a")
    assert _order(item.uid) == ["a", "b", "c"]


def test_an_unknown_target_leaves_the_order_untouched(item) -> None:
    SelectionService.move_filter(item.uid, "a", before_uid="nope")
    assert _order(item.uid) == ["a", "b", "c"]


def test_the_new_order_survives_a_reload(item, tmp_path) -> None:
    """ Order lives in the stored YAML list, so it has to round-trip. """
    from sports_calendar.application.selection.registry import SelectionRegistry

    SelectionService.move_filter(item.uid, "c", before_uid="a")

    SelectionRegistry._initialized = False
    SelectionRegistry._selections = []
    SelectionService.initialize_registry()

    assert _order(item.uid) == ["c", "a", "b"]
