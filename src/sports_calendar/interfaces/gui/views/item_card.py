import logging

from nicegui import ui

from ..catalog import SportIndexFilterSearchProvider
from ..components import ConfirmModal, ExpandableCard, FilterBlock, FilterModal
from ..context import app_context
from ..presenters import SelectionFilterPresenter, SelectionItemPresenter

logger = logging.getLogger(__name__)


def _handle_delete_item(item_presenter: SelectionItemPresenter) -> None:
    try:
        item_presenter.delete()
    except KeyError:
        logger.exception("Item '%s' could not be deleted", item_presenter.uid)
        ui.notify('Item was not found.', type='negative')
        return

    ui.notify('Item deleted.', type='positive')
    ui.navigate.reload()


def _open_delete_item_modal(item_presenter: SelectionItemPresenter) -> None:
    ConfirmModal(
        title='Delete Item',
        message='Are you sure you want to delete this item? This cannot be undone.',
        confirm_label='Delete',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_item(item_presenter))


def _handle_delete_filter(filter_presenter: SelectionFilterPresenter, block: FilterBlock, item_presenter: SelectionItemPresenter, card: ExpandableCard) -> None:
    try:
        filter_presenter.delete()
    except KeyError:
        logger.exception(f"Filter '{filter_presenter.uid}' could not be deleted")
        ui.notify('Filter was not found.', type='negative')
        return

    item_presenter.item.filters = [f for f in item_presenter.item.filters if f.uid != filter_presenter.uid]
    card.title_label.set_text(item_presenter.title)
    block.container.delete()
    ui.notify('Filter deleted.', type='positive')


def _open_delete_filter_modal(filter_presenter: SelectionFilterPresenter, block: FilterBlock, item_presenter: SelectionItemPresenter, card: ExpandableCard) -> None:
    ConfirmModal(
        title='Delete Filter',
        message='Are you sure you want to delete this filter? This cannot be undone.',
        confirm_label='Delete',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_filter(filter_presenter, block, item_presenter, card))


def _handle_edit_filter(filter_presenter: SelectionFilterPresenter, block: FilterBlock, payload) -> None:
    try:
        filter_presenter.update(payload)
        block.title_label.set_text(filter_presenter.title)
        ui.notify("Filter updated successfully", type="positive")
    except (KeyError, ValueError) as e:
        logger.exception(f"Failed to update filter '{filter_presenter.uid}': {e}")
        ui.notify("Failed to edit filter.", type="negative")


def _open_edit_filter_modal(filter_presenter: SelectionFilterPresenter, block: FilterBlock) -> None:
    FilterModal(
        initial_filter=filter_presenter.filter,
        search_provider=SportIndexFilterSearchProvider(app_context.client),
        title='Edit Filter',
        message='Select a filter type, then fill the fields for that type.',
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

    ui.notify('Filter created.', type='positive')
    _open_edit_filter_modal(created_filter, block)


def render_item_card(item_presenter: SelectionItemPresenter) -> None:
    with ExpandableCard(
        title=item_presenter.title,
        subtitle=item_presenter.subtitle,
        on_delete=lambda p=item_presenter: _open_delete_item_modal(p),
    ) as card:
        filters_container = ui.column().classes('w-full gap-2')
        with filters_container:
            for filter_p in item_presenter.get_filter_presenters():
                _render_filter_block(filter_p, item_presenter, card)

        ui.button('+', on_click=lambda: _handle_create_filter(item_presenter, filters_container, card)).props('outline').classes('w-4/5 self-center mt-2')
