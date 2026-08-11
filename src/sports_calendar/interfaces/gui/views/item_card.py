import logging
from collections.abc import Callable

from nicegui import ui

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

    item_presenter.item.filters = [f for f in item_presenter.item.filters if f.uid != filter_presenter.uid]
    card.title_label.set_text(item_presenter.title)
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


def _handle_edit_filter(filter_presenter: SelectionFilterPresenter, block: FilterBlock, payload) -> None:
    try:
        filter_presenter.update(payload)
        block.title_label.set_text(filter_presenter.title)
        ui.notify(copy.RULE_SAVED, type="positive")
    except (KeyError, ValueError) as e:
        logger.exception(f"Failed to update filter '{filter_presenter.uid}': {e}")
        ui.notify("Could not save that.", type="negative")


def _open_edit_filter_modal(filter_presenter: SelectionFilterPresenter, block: FilterBlock) -> None:
    FilterModal(
        initial_filter=filter_presenter.filter,
        search_provider=SportIndexFilterSearchProvider(app_context.client),
        title=copy.FOLLOW_TITLE,
        message=None,
        initial_filter_type=filter_presenter.filter.fields.filter_type,
        confirm_label='Save',
        cancel_label='Cancel',
        confirm_color='primary',
    ).open(on_confirm=lambda payload: _handle_edit_filter(filter_presenter, block, payload))


def _render_filter_block(filter_presenter: SelectionFilterPresenter, item_presenter: SelectionItemPresenter, card: ExpandableCard) -> FilterBlock:
    block = FilterBlock(
        title=filter_presenter.title,
        on_delete=lambda: _open_delete_filter_modal(filter_presenter, block, item_presenter, card),
        on_edit=lambda: _open_edit_filter_modal(filter_presenter, block),
    )
    return block


def _handle_create_filter(item_presenter: SelectionItemPresenter, filters_container: ui.column, card: ExpandableCard) -> None:
    try:
        created_filter = item_presenter.create_empty_filter()
    except ValueError as exc:
        logger.exception("Filter could not be created for item '%s'", item_presenter.uid)
        ui.notify(str(exc), type='negative')
        return

    logger.debug("Created filter '%s' for item '%s'", created_filter.uid, item_presenter.uid)
    item_presenter.item.filters.append(created_filter.filter)
    card.title_label.set_text(item_presenter.title)

    with filters_container:
        block = _render_filter_block(created_filter, item_presenter, card)

    ui.notify(copy.RULE_ADDED, type='positive')
    _open_edit_filter_modal(created_filter, block)


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
            with filters_container:
                for filter_p in item_presenter.get_filter_presenters():
                    _render_filter_block(filter_p, item_presenter, card)

            with ui.row().classes('w-full items-center justify-center gap-2 mt-2'):
                ui.button('Follow something else', on_click=lambda: _handle_create_filter(item_presenter, filters_container, card)).props('outline')
                info_icon(copy.WHAT_IS_A_RULE)
    return card
