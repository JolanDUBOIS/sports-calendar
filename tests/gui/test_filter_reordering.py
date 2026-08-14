""" Dragging a rule to a new position on the card.

Cosmetic by design — filters are unioned, so order cannot change the calendar —
but it has to survive a reload, which means it has to reach storage.

The gesture itself runs in the browser, because where a rule lands depends on
where in the target row the cursor was released: dropping below a row's midpoint
means *after* it. An earlier version resolved this server-side and always
inserted before the target, so dragging a rule one place down put it back
exactly where it started. These tests cover the two halves that are testable —
the markup the browser needs, and what the app does with a resolved drop.
"""

from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import SelectionFilter
from sports_calendar.interfaces.gui.views.item_card import apply_filter_reorder

SELECTION = "my-sel"
FOOTBALL = 1


async def _open_page_with_rules(user: User, *names: str):
    SelectionService.add_empty_selection(SELECTION)
    item = SelectionService.add_empty_item(SELECTION, FOOTBALL, name="Football")
    for name in names:
        SelectionService.add_filter(
            item.uid, SelectionFilter(sport_id=FOOTBALL, name=name, uid=name)
        )
    await user.open(f"/selections/{SELECTION}")
    return item


def _order(item_uid: str) -> list[str]:
    return [f.uid for f in SelectionService.get_item(item_uid).filters]


# ---- what the browser needs in order to resolve a drop ---- #

async def test_every_rule_is_marked_up_for_dragging(user: User) -> None:
    await _open_page_with_rules(user, "a", "b", "c")

    rows = [e for e in user.client.elements.values() if "filter-row" in e.classes]
    assert len(rows) == 3
    uids = {r._props.get("data-filter-uid") for r in rows}  # noqa: SLF001
    assert uids == {"a", "b", "c"}


async def test_the_list_names_the_item_it_belongs_to(user: User) -> None:
    """ So a drop can be told apart from one on another sport's card. """
    item = await _open_page_with_rules(user, "a")

    lists = [e for e in user.client.elements.values() if "filter-list" in e.classes]
    assert [lst._props.get("data-item-uid") for lst in lists] == [item.uid]  # noqa: SLF001


async def test_every_rule_has_a_handle(user: User) -> None:
    await _open_page_with_rules(user, "a", "b", "c")
    assert len(user.find(marker="drag-handle").elements) == 3


async def test_the_drag_script_is_installed_once(user: User) -> None:
    """ Once per page, however many cards it holds — it binds on `document`. """
    await _open_page_with_rules(user, "a")
    head = "".join(user.client.head_html)
    assert head.count("filters_reordered") == 1
    assert "drop-before" in head and "drop-after" in head


# ---- what the app does with a drop the browser has resolved ---- #

async def test_a_rule_moves_in_front_of_another(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b", "c")

    assert apply_filter_reorder({"item": item.uid, "moved": "c", "before": "a"}, item.uid)
    assert _order(item.uid) == ["c", "a", "b"]


async def test_dropping_below_the_last_rule_sends_it_to_the_end(user: User) -> None:
    """ `before: None` is how the browser reports "past everything". """
    item = await _open_page_with_rules(user, "a", "b", "c")

    assert apply_filter_reorder({"item": item.uid, "moved": "a", "before": None}, item.uid)
    assert _order(item.uid) == ["b", "c", "a"]


async def test_dragging_one_place_down_actually_moves_it(user: User) -> None:
    """ The regression: this used to resolve to "before b" and change nothing. """
    item = await _open_page_with_rules(user, "a", "b", "c")

    assert apply_filter_reorder({"item": item.uid, "moved": "a", "before": "c"}, item.uid)
    assert _order(item.uid) == ["b", "a", "c"]


async def test_a_drop_meant_for_another_card_is_ignored(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b")

    assert not apply_filter_reorder({"item": "other", "moved": "a", "before": "b"}, item.uid)
    assert _order(item.uid) == ["a", "b"]


async def test_a_payload_naming_no_rule_is_ignored(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b")

    assert not apply_filter_reorder({"item": item.uid, "moved": None, "before": "b"}, item.uid)
    assert _order(item.uid) == ["a", "b"]


async def test_the_new_order_survives_a_reload(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b", "c")
    apply_filter_reorder({"item": item.uid, "moved": "c", "before": "a"}, item.uid)

    await user.open(f"/selections/{SELECTION}")
    assert _order(item.uid) == ["c", "a", "b"]
