from nicegui import ui

from . import logger
from .filters_body import filter_body
from .filter_modal import open_filter_modal
from ..modals import Modal
from sports_calendar.core.selection import SelectionService, SelectionItem, SelectionFilter


def filters_list(selection_name: str, selection_item: SelectionItem):
    for i, filter in enumerate(selection_item.filters):
        if i > 0:
            ui.separator()
        filter_block(filter, selection_name=selection_name, item_uid=selection_item.uid)

def filter_block(filter: SelectionFilter, **kwargs):
    def on_modify_click():
        logger.debug("Modify clicked for filter")

        def on_confirm(**filter_updates):
            logger.debug(f"Filter modified kwargs: {filter_updates}")
            updated_filter = filter.with_updates(**filter_updates)
            SelectionService.replace_filter(
                selection_name=kwargs['selection_name'],
                item_uid=kwargs['item_uid'],
                filter=updated_filter
            )
        
        open_filter_modal(
            filter=filter,
            title="Modify Filter",
            on_confirm_callback=on_confirm
        )


    def on_delete_click():
        logger.debug("Delete clicked for filter")
        Modal('Are you sure you want to delete this filter?', confirm_color='red', reload_on_confirm=True).open(
            lambda: SelectionService.remove_filter(
                selection_name=kwargs['selection_name'],
                item_uid=kwargs['item_uid'],
                filter_uid=filter.uid
            )
        )

    with ui.card().classes('w-full p-2'):
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column():
                filter_body(filter)
            with ui.row().classes('gap-2'):
                ui.button('Modify').props('small flat').on('click.stop', on_modify_click)
                ui.button('Delete').props('small flat color=red').on('click.stop', on_delete_click)
