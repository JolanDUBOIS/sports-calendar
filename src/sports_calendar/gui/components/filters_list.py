from nicegui import ui

from . import logger
from .modals import Modal
from .filters_body import filter_body
from sports_calendar.core.selection import SelectionService, SelectionItem, SelectionFilter


def filters_list(selection_name: str, selection_item: SelectionItem):
    for i, filter in enumerate(selection_item.filters):
        if i > 0:
            ui.separator()
        filter_block(filter, selection_name=selection_name, item_uid=selection_item.uid)

def filter_block(filter: SelectionFilter, **kwargs):
    def on_modify_click():
        logger.debug("Modify clicked for filter")

    def on_delete_click():
        logger.debug("Delete clicked for filter")
        Modal('Are you sure you want to delete this filter?', confirm_color='red', reload_on_confirm=True).open(
            lambda: SelectionService.remove_filter(
                selection_name=kwargs['selection_name'],
                item_uid=kwargs['item_uid'],
                filter=filter
            )
        )

    with ui.card().classes('w-full p-2'):
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column():
                filter_body(filter)
            with ui.row().classes('gap-2'):
                ui.button('Modify').props('small flat').on('click.stop', on_modify_click)
                ui.button('Delete').props('small flat color=red').on('click.stop', on_delete_click)
