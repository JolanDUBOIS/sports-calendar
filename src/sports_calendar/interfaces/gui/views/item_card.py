import asyncio
import logging
from collections.abc import Callable

from nicegui import ui

from sports_calendar.application.selection import SelectionService

from .. import copy
from ..catalog import SportIndexFilterSearchProvider
from ..components import (
    ConfirmModal,
    ExpandableCard,
    FilterBlock,
    FilterModal,
    info_icon,
)
from ..context import app_context
from ..presenters import SelectionFilterPresenter, SelectionItemPresenter

logger = logging.getLogger(__name__)


def _handle_delete_item(
    item_presenter: SelectionItemPresenter,
    card: ExpandableCard,
    on_removed: Callable[[], None] | None,
) -> None:
    try:
        item_presenter.delete()
    except KeyError:
        logger.exception("Item '%s' could not be deleted", item_presenter.uid)
        ui.notify('That sport was not found.', type='negative')
        return

    # Removed in place rather than reloading the page: a reload collapses every
    # expanded card on screen, which is jarring mid-edit.
    card.container.delete()
    if on_removed is not None:
        on_removed()
    ui.notify(copy.SPORT_REMOVED, type='positive')


def _open_delete_item_modal(
    item_presenter: SelectionItemPresenter,
    card: ExpandableCard,
    on_removed: Callable[[], None] | None,
) -> None:
    ConfirmModal(
        title=copy.DELETE_SPORT_TITLE,
        message=copy.DELETE_SPORT_MESSAGE,
        confirm_label='Remove',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_item(item_presenter, card, on_removed))


def _handle_delete_filter(filter_presenter: SelectionFilterPresenter, block: FilterBlock, item_presenter: SelectionItemPresenter, card: ExpandableCard) -> None:
    try:
        filter_presenter.delete()
    except KeyError:
        logger.exception(f"Filter '{filter_presenter.uid}' could not be deleted")
        ui.notify('That was not found.', type='negative')
        return

    item_presenter.refresh()
    _refresh_card_header(item_presenter, card)
    block.container.delete()
    ui.notify(copy.RULE_REMOVED, type='positive')


def _open_delete_filter_modal(filter_presenter: SelectionFilterPresenter, block: FilterBlock, item_presenter: SelectionItemPresenter, card: ExpandableCard) -> None:
    ConfirmModal(
        title=copy.DELETE_RULE_TITLE,
        message=copy.DELETE_RULE_MESSAGE,
        confirm_label='Remove',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_filter(filter_presenter, block, item_presenter, card))


def _refresh_card_header(item_presenter: SelectionItemPresenter, card: ExpandableCard) -> None:
    """ Keep the card's title and subtitle in step with its contents.

    Both, not just the title: cards are updated in place rather than
    re-rendered, and only refreshing the title left "Nothing followed yet"
    sitting above a card full of rules.
    """
    card.title_label.set_text(item_presenter.title)
    card.set_subtitle(item_presenter.subtitle)


def _refresh_filter_block(filter_presenter: SelectionFilterPresenter, block: FilterBlock) -> None:
    block.title_label.set_text(filter_presenter.title)
    block.set_subtitle(filter_presenter.subtitle)


def _handle_edit_filter(filter_presenter: SelectionFilterPresenter, block: FilterBlock, payload) -> None:
    try:
        filter_presenter.update(payload)
        _refresh_filter_block(filter_presenter, block)
        ui.notify(copy.RULE_SAVED, type="positive")
    except (KeyError, ValueError) as e:
        logger.exception(f"Failed to update filter '{filter_presenter.uid}': {e}")
        ui.notify("Could not save that.", type="negative")


async def _open_edit_filter_modal(filter_presenter: SelectionFilterPresenter, block: FilterBlock) -> None:
    """ Show the dialog first, then fill it in.

    Building the fields up front means resolving every competition and
    competitor the rule names, which is about a second each — long enough that
    the button looked broken.
    """
    modal = FilterModal(
        initial_filter=filter_presenter.filter,
        search_provider=SportIndexFilterSearchProvider(app_context.client),
        title=copy.FOLLOW_TITLE,
        message=None,
        initial_filter_type=filter_presenter.filter.fields.filter_type,
        defer_fields=True,
        confirm_label='Save',
        cancel_label='Cancel',
        confirm_color='primary',
    )
    modal.open(on_confirm=lambda payload: _handle_edit_filter(filter_presenter, block, payload))
    await modal.load_fields()


class _DragState:
    """ Which rule is currently being dragged, for one card.

    Per card rather than global: dragging a rule from one sport into another
    would be meaningless, and the drop handlers should simply not see it.
    """

    def __init__(self) -> None:
        self.uid: str | None = None


async def _fill_subtitles(pairs: list[tuple[SelectionFilterPresenter, FilterBlock]]) -> None:
    """ Fill in what each rule contains, once the card is already on screen.

    A subtitle names the competitions or teams a rule holds, and resolving those
    ids costs about a second each — twenty seconds for a real selection. Doing
    it while rendering blocked the page with nothing shown at all.

    One rule at a time, so the card fills in progressively instead of sitting
    blank until the last lookup returns.
    """
    for filter_presenter, block in pairs:
        try:
            subtitle = await asyncio.to_thread(lambda p=filter_presenter: p.subtitle)
        except Exception:  # noqa: BLE001 - UI boundary, one bad rule must not stop the rest
            logger.exception("Could not describe filter '%s'", filter_presenter.uid)
            continue
        try:
            block.set_subtitle(subtitle)
        except (RuntimeError, KeyError):
            # The rule was deleted, or the page navigated away, while we were
            # resolving it. Nothing left to label.
            logger.debug("Filter block for '%s' went away before it was labelled", filter_presenter.uid)


def _render_filter_block(
    filter_presenter: SelectionFilterPresenter,
    item_presenter: SelectionItemPresenter,
    card: ExpandableCard,
    drag: _DragState | None = None,
    on_reordered: Callable[[], None] | None = None,
) -> FilterBlock:
    block = FilterBlock(
        title=filter_presenter.title,
        # Filled in by `_fill_subtitles` after the page is up; see there.
        subtitle=None,
        on_delete=lambda: _open_delete_filter_modal(filter_presenter, block, item_presenter, card),
        on_edit=lambda: _open_edit_filter_modal(filter_presenter, block),
        draggable=drag is not None,
    )
    if drag is not None:
        _make_draggable(block, filter_presenter.uid, item_presenter, drag, on_reordered)
    return block


def _make_draggable(
    block: FilterBlock,
    filter_uid: str,
    item_presenter: SelectionItemPresenter,
    drag: _DragState,
    on_reordered: Callable[[], None] | None,
) -> None:
    """ Wire one rule up for drag-and-drop reordering.

    Native HTML5 drag events rather than a sortable library: nothing here needs
    animation, and the app has to keep working once it is packaged offline.

    Order is presentation only — filters are unioned — so a drop that lands
    somewhere unexpected costs nothing but a tidy-up.
    """
    def start() -> None:
        drag.uid = filter_uid

    def drop() -> None:
        dropped = drag.uid
        drag.uid = None
        if dropped is None or dropped == filter_uid:
            return
        SelectionService.move_filter(item_presenter.uid, dropped, before_uid=filter_uid)
        if on_reordered is not None:
            on_reordered()

    block.container.props('draggable')
    block.container.on('dragstart', start)
    # Without preventDefault on dragover the browser refuses the drop outright.
    block.container.on('dragover.prevent', lambda: None)
    block.container.on('drop.prevent', drop)


async def _handle_create_filter(
    item_presenter: SelectionItemPresenter,
    filters_container: ui.column,
    card: ExpandableCard,
    drag: '_DragState | None' = None,
    on_reordered: Callable[[], None] | None = None,
) -> None:
    try:
        created_filter = item_presenter.create_empty_filter()
    except ValueError as exc:
        logger.exception("Filter could not be created for item '%s'", item_presenter.uid)
        ui.notify(str(exc), type='negative')
        return

    logger.debug("Created filter '%s' for item '%s'", created_filter.uid, item_presenter.uid)
    item_presenter.refresh()
    _refresh_card_header(item_presenter, card)

    with filters_container:
        block = _render_filter_block(created_filter, item_presenter, card, drag, on_reordered)

    ui.notify(copy.RULE_ADDED, type='positive')
    await _open_edit_filter_modal(created_filter, block)


def render_item_card(
    item_presenter: SelectionItemPresenter,
    container: ui.column,
    on_removed: Callable[[], None] | None = None,
) -> ExpandableCard:
    """ Render one item card into `container` and return it.

    The card is returned so callers can reposition or remove it without
    re-rendering (and thus collapsing) the whole page.
    """
    with container:
        card = ExpandableCard(
            title=item_presenter.title,
            subtitle=item_presenter.subtitle,
            on_delete=lambda: _open_delete_item_modal(item_presenter, card, on_removed),
        )
        with card:
            filters_container = ui.column().classes('w-full gap-2')
            drag = _DragState()

            def redraw_filters() -> None:
                """ Re-render the rules in their stored order.

                The whole list rather than a swap of two: the presenters are
                rebuilt from the item, so this cannot drift out of step with
                what was actually saved.
                """
                item_presenter.refresh()
                filters_container.clear()
                pairs = []
                with filters_container:
                    for filter_p in item_presenter.get_filter_presenters():
                        block = _render_filter_block(filter_p, item_presenter, card, drag, redraw_filters)
                        pairs.append((filter_p, block))
                # Off the render path: the page returns immediately and the
                # detail lines appear as their lookups come back.
                ui.timer(0.05, lambda: _fill_subtitles(pairs), once=True)

            redraw_filters()

            with ui.row().classes('w-full items-center justify-center gap-2 mt-2'):
                ui.button(copy.ADD_RULE_BUTTON, on_click=lambda: _handle_create_filter(item_presenter, filters_container, card, drag, redraw_filters)).props('outline')
                info_icon(copy.WHAT_IS_A_RULE)
    return card
