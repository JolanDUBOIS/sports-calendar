import logging

from nicegui import ui
from sportindex import Sport

from sports_calendar.application.selection import SelectionService

from ..components import AddCard, FormModal, SelectField, TextField, base_layout
from ..context import app_context
from ..presenters import SelectionPresenter
from .item_card import render_item_card

logger = logging.getLogger(__name__)


def _handle_create_item(presenter: SelectionPresenter, sport_value: int | str | None, item_name: str | None) -> bool:
    if sport_value in (None, ""):
        ui.notify('Please select a sport.', type='warning')
        return False

    try:
        created_item = presenter.create_item(int(sport_value), name=item_name or '')
    except ValueError as exc:
        ui.notify(str(exc), type='negative')
        return False

    logger.debug("Created item '%s' for selection '%s'", created_item.uid, presenter.name)
    ui.notify('Item created.', type='positive')
    ui.navigate.reload()
    return True

def _open_create_item_modal(presenter: SelectionPresenter) -> None:
    sports = sorted(app_context.client.list(Sport), key=lambda sport: sport.name.lower())
    sport_options = {sport.id: sport.name.capitalize() for sport in sports}

    modal = FormModal(
        title='Add Item',
        message='Select the sport and optionally set a name for the new item.',
        fields=[
            SelectField(name='sport', label='Sport', options=sport_options),
            TextField(name='name', label='Name'),
        ],
        confirm_label='Create',
        cancel_label='Cancel',
        confirm_color='primary',
        min_width='360px',
        max_width='560px',
    )

    modal.open(on_confirm=lambda payload: _handle_create_item(
        presenter=presenter,
        sport_value=payload.get('sport'),
        item_name=payload.get('name')
    ))


@ui.page('/selections/{selection_name}')
def selection_page(selection_name: str):
    with base_layout():
        try:
            selection_core = SelectionService.get_selection(selection_name)
        except KeyError:
            logger.exception(f"Selection '{selection_name}' not found.")
            ui.label(f'Selection "{selection_name}" not found.').classes('text-red-500 text-xl font-bold mb-4')
            ui.button('Back to Selections', on_click=lambda: ui.navigate.to('/selections')).props('flat')
            return

        presenter = SelectionPresenter(
            selection=selection_core,
            client=app_context.client
        )

        with ui.row().classes('w-full items-baseline justify-between mb-6'):
            with ui.column().classes('gap-1'):
                ui.label(presenter.title).classes('text-3xl font-bold')

            with ui.row().classes('gap-2'):
                ui.button('Back', on_click=lambda: ui.navigate.to('/selections')).props('outline')

        ui.separator().classes('mb-6')

        item_presenters = presenter.get_item_presenters(sort_by='updated_at', order='desc')
        logger.debug(f"Retrieved {len(item_presenters)} items for selection '{selection_name}'")

        if not item_presenters:
            ui.label('No items in this selection.').classes('text-gray-500 italic')
        else:
            with ui.column().classes('w-full gap-2'):
                for item_p in item_presenters:
                    render_item_card(item_p)

        AddCard(on_click=lambda: _open_create_item_modal(presenter))
