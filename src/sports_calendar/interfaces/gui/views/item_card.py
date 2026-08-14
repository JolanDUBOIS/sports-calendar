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


# Reordering runs in the browser and reports one result, rather than being
# assembled from server-side dragstart/drop events.
#
# Two reasons. The insertion point depends on where the cursor is relative to a
# row's midpoint — above it means before, below means after — and the server
# cannot know that; without it, dragging a rule one place down put it back
# exactly where it started. And the line showing where the rule will land has to
# follow the cursor, which is not something a round trip per dragover can do.
_DRAG_AND_DROP_JS = """
<style>
  .filter-row.drag-source { opacity: .45; }
  .filter-row.drop-before { box-shadow: inset 0 3px 0 0 #1976d2; }
  .filter-row.drop-after  { box-shadow: inset 0 -3px 0 0 #1976d2; }
</style>
<script>
(() => {
  let dragged = null;

  const clear = () => document.querySelectorAll('.filter-row').forEach(
    r => r.classList.remove('drop-before', 'drop-after'));

  // Rows are only draggable while the handle is held, so selecting the title
  // text still behaves normally.
  document.addEventListener('mousedown', e => {
    const handle = e.target.closest('.drag-handle');
    if (handle) handle.closest('.filter-row')?.setAttribute('draggable', 'true');
  });
  document.addEventListener('mouseup', () => document.querySelectorAll('.filter-row')
    .forEach(r => r.removeAttribute('draggable')));

  document.addEventListener('dragstart', e => {
    const row = e.target.closest?.('.filter-row');
    if (!row) return;
    dragged = row;
    row.classList.add('drag-source');
    e.dataTransfer.effectAllowed = 'move';
  });

  document.addEventListener('dragover', e => {
    if (!dragged) return;
    const row = e.target.closest?.('.filter-row');
    if (!row || row.parentElement !== dragged.parentElement) return;
    e.preventDefault();                     // without this the drop is refused
    clear();
    const box = row.getBoundingClientRect();
    row.classList.add(e.clientY < box.top + box.height / 2 ? 'drop-before' : 'drop-after');
  });

  document.addEventListener('drop', e => {
    if (!dragged) return;
    const row = e.target.closest?.('.filter-row');
    if (!row || row.parentElement !== dragged.parentElement) return;
    e.preventDefault();
    const box = row.getBoundingClientRect();
    const after = e.clientY >= box.top + box.height / 2;
    // The rule the moved one should end up in front of; null means the end.
    let before = after ? row.nextElementSibling : row;
    if (before === dragged) before = dragged.nextElementSibling;
    emitEvent('filters_reordered', {
      item: dragged.parentElement.dataset.itemUid,
      moved: dragged.dataset.filterUid,
      before: before ? before.dataset.filterUid : null,
    });
    clear();
  });

  document.addEventListener('dragend', () => {
    dragged?.classList.remove('drag-source');
    dragged = null;
    clear();
  });
})();
</script>
"""


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
    draggable: bool = False,
) -> FilterBlock:
    block = FilterBlock(
        title=filter_presenter.title,
        # Filled in by `_fill_subtitles` after the page is up; see there.
        subtitle=None,
        on_delete=lambda: _open_delete_filter_modal(filter_presenter, block, item_presenter, card),
        on_edit=lambda: _open_edit_filter_modal(filter_presenter, block),
        draggable=draggable,
    )
    if draggable:
        # What the browser side reads to know which rule it is moving.
        block.container.classes('filter-row').props(
            f'data-filter-uid="{filter_presenter.uid}"'
        )
    return block


async def _handle_create_filter(
    item_presenter: SelectionItemPresenter,
    filters_container: ui.column,
    card: ExpandableCard,
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
        block = _render_filter_block(created_filter, item_presenter, card, draggable=True)

    ui.notify(copy.RULE_ADDED, type='positive')
    await _open_edit_filter_modal(created_filter, block)


def enable_filter_reordering() -> None:
    """ Install the browser-side drag behaviour. Call once per page. """
    ui.add_head_html(_DRAG_AND_DROP_JS)


def apply_filter_reorder(payload: dict, item_uid: str) -> bool:
    """ Record a drop the browser has already worked out. True if it applied.

    Every card on the page hears every drop, so each checks whether the rule
    came from its own list. `before` is the rule the moved one should end up in
    front of, or None for the end of the list — the browser decides which,
    because it depends on where in the target row the cursor was released.
    """
    if payload.get('item') != item_uid:
        return False

    moved = payload.get('moved')
    if not moved:
        return False

    SelectionService.move_filter(item_uid, moved, before_uid=payload.get('before'))
    return True


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
            filters_container = ui.column().classes('w-full gap-2 filter-list').props(
                f'data-item-uid="{item_presenter.uid}"'
            )

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
                        block = _render_filter_block(filter_p, item_presenter, card, draggable=True)
                        pairs.append((filter_p, block))
                # Off the render path: the page returns immediately and the
                # detail lines appear as their lookups come back.
                ui.timer(0.05, lambda: _fill_subtitles(pairs), once=True)

            def on_reordered(event) -> None:
                if apply_filter_reorder(event.args or {}, item_presenter.uid):
                    redraw_filters()

            ui.on('filters_reordered', on_reordered)
            redraw_filters()

            with ui.row().classes('w-full items-center justify-center gap-2 mt-2'):
                ui.button(copy.ADD_RULE_BUTTON, on_click=lambda: _handle_create_filter(item_presenter, filters_container, card)).props('outline')
                info_icon(copy.WHAT_IS_A_RULE)
    return card
