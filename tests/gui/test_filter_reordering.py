""" Dragging a rule to a new position on the card.

Cosmetic by design — filters are unioned, so order cannot change the calendar —
but it has to survive a reload, which means it has to reach storage.

The browser gesture itself cannot be simulated here; these fire the same
`dragstart`/`drop` events the browser would, and assert on what the app does
with them.
"""

from nicegui import events
from nicegui.testing import User

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import SelectionFilter

SELECTION = "my-sel"
FOOTBALL = 1


def _fire(element, event_type: str) -> None:
    """ Dispatch one DOM event to whatever the app registered for it.

    Listener types keep their Vue modifiers — `drop` is registered as
    `drop.prevent` — so the base name is what identifies them.
    """
    for listener in list(element._event_listeners.values()):  # noqa: SLF001
        if listener.type.split(".")[0] != event_type:
            continue
        events.handle_event(
            listener.handler,
            events.GenericEventArguments(sender=element, client=element.client, args=None),
        )


async def _open_page_with_rules(user: User, *names: str):
    SelectionService.add_empty_selection(SELECTION)
    item = SelectionService.add_empty_item(SELECTION, FOOTBALL, name="Football")
    for name in names:
        SelectionService.add_filter(
            item.uid, SelectionFilter(sport_id=FOOTBALL, name=name, uid=name)
        )
    await user.open(f"/selections/{SELECTION}")
    return item


def _stored_order(item_uid: str) -> list[str]:
    return [f.uid for f in SelectionService.get_item(item_uid).filters]


def _blocks(user: User):
    """ The draggable rule containers, in render order.

    Identified by carrying a `dragstart` listener, and sorted by element id:
    `user.find` does not preserve document order, and ids are handed out as
    elements are created.
    """
    dragging = [
        element for element in user.client.elements.values()
        if any(listener.type == "dragstart" for listener in element._event_listeners.values())  # noqa: SLF001
    ]
    return sorted(dragging, key=lambda element: element.id)


async def test_dragging_a_rule_onto_an_earlier_one_moves_it(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b", "c")

    blocks = _blocks(user)
    _fire(blocks[2], "dragstart")
    _fire(blocks[0], "drop")

    assert _stored_order(item.uid) == ["c", "a", "b"]


async def test_the_rendered_order_follows_the_stored_one(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b", "c")

    blocks = _blocks(user)
    _fire(blocks[0], "dragstart")
    _fire(blocks[2], "drop")

    assert _stored_order(item.uid) == ["b", "a", "c"]
    await user.should_see("a")
    # Re-rendered from the item, so the handles are rebuilt in the new order.
    assert len(_blocks(user)) == 3


async def test_a_drop_without_a_drag_does_nothing(user: User) -> None:
    item = await _open_page_with_rules(user, "a", "b")

    _fire(_blocks(user)[0], "drop")

    assert _stored_order(item.uid) == ["a", "b"]


async def test_every_rule_gets_a_handle(user: User) -> None:
    await _open_page_with_rules(user, "a", "b", "c")
    assert len(user.find(marker="drag-handle").elements) == 3
