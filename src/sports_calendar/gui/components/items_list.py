from nicegui import ui

from . import logger
from .modals import Modal
from .filters import filters_list
from .filters.filter_modal import open_filter_modal
from sports_calendar.core.selection import SelectionService, Selection, SelectionItem, SelectionFilter


def items_list(selection: Selection):
    items = selection.items
    logger.debug(f"Loaded {len(items)} items for selection '{selection.name}'.")

    with ui.column().classes('w-full p-4 gap-4'):
        if not items:
            ui.label('Empty selection').classes('text-sm italic text-gray-500')

        for item in items:
            item_card(selection.name, item)

def item_card(selection_name: str, item: SelectionItem):
    n_filters = len(item.filters)

    def on_delete_click():
        logger.debug(f"Delete clicked for item {item.uid} in selection {selection_name}")
        Modal(f'Are you sure you want to delete this item?', confirm_color='red', reload_on_confirm=True).open(
            lambda: SelectionService.remove_item(selection_name, item.uid)
        )

    def on_add_filter_click():
        logger.debug(f"Add Filter clicked for item {item.uid} in selection {selection_name}")
        new_filter = SelectionService.add_empty_filter(selection_name, item.uid)

        def on_confirm(**filter_updates):
            logger.debug(f"New filter updates: {filter_updates}")
            updated_filter = new_filter.with_updates(**filter_updates)
            SelectionService.replace_filter(
                selection_name=selection_name,
                item_uid=item.uid,
                filter=updated_filter
            )

        open_filter_modal(
            filter=new_filter,
            title="Add Filter",
            on_confirm_callback=on_confirm
        )

    with ui.expansion().classes('w-full bg-white rounded shadow') as exp:

        # HEADER
        with exp.add_slot('header'):
            with ui.row().classes('w-full items-center justify-between'):
                ui.label(
                    f'{item.sport.capitalize()} ({n_filters} filters)'
                ).classes('font-bold')

                ui.label(item.uid).classes('text-sm italic text-gray-500')

                with ui.row().classes('gap-2 ml-auto'):
                    ui.button('+ Filter').props('flat dense small').on('click.stop', on_add_filter_click)
                    ui.button('Delete').props('color=red flat dense small').on('click.stop', on_delete_click)

        # BODY
        filters_list(selection_name, item)
