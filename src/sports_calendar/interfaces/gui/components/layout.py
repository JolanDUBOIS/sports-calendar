from contextlib import contextmanager

from nicegui import ui


def global_header():
    with ui.header().classes('items-center justify-between w-full p-4'):
        ui.label('Sports Calendar').classes('text-xl font-bold text-white cursor-pointer hover:opacity-85').on(
            'click', lambda: ui.navigate.to('/')
        )

        with ui.row().classes('gap-4'):
            ui.button('Language', icon='language').props('flat color=white')
            ui.button('Settings', icon='settings').props('flat color=white')
            ui.button('Help', icon='help').props('flat color=white')

@contextmanager
def base_layout():
    global_header()
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        yield
