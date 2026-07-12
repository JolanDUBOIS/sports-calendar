from nicegui import ui

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.selection import Selection

from . import logger
from .modals import Modal


def selections_list():
    selections = SelectionService.get_all_selections()
    logger.debug(f"Loaded {len(selections)} selections for selection list.")

    with ui.column().classes('w-full p-4 gap-4'):
        for selection in selections:
            selection_card(selection)

def selection_card(selection: Selection):
    logger.debug(f"Creating selection card for selection {selection.name}")
    sname = selection.name

    def go_to_selection():
        ui.navigate.to(f'/selections/{sname}')

    def on_delete_click():
        logger.debug(f"Delete clicked for selection {selection.name}")
        Modal(f'Are you sure you want to delete "{sname}"?', confirm_color='red', reload_on_confirm=True).open(
            lambda: SelectionService.remove_selection(sname)
        )

    with ui.card().classes('w-full cursor-pointer').on('click', go_to_selection):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label(f'Selection "{selection.name}"').classes('text-lg font-bold')
            ui.button('Delete').props('color=red flat dense').classes('ml-auto').on('click.stop', on_delete_click)
        sports = selection.sports
        n_items = len(selection.items)
        label_classes = 'text-sm italic text-gray-500'
        if n_items == 0:
            ui.label('Empty selection').classes(label_classes)
        elif n_items == 1:
            ui.label(f'1 item (sport: {sports[0]})').classes(label_classes)
        else:
            sports_str = ', '.join(sports)
            ui.label(f'{n_items} items (sports: {sports_str})').classes(label_classes)
