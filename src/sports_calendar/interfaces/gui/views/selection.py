import logging
from collections.abc import Callable

from nicegui import ui
from sportindex import Sport

from sports_calendar.application.selection import SelectionService

from .. import copy
from ..components import (
    AddCard,
    FormModal,
    SelectField,
    TextField,
    base_layout,
    info_icon,
)
from ..context import app_context
from ..presenters import SelectionPresenter
from .item_card import render_item_card
from .preview import render_preview_drawer

logger = logging.getLogger(__name__)


def _handle_create_item(
    presenter: SelectionPresenter,
    sport_value: int | None,
    item_name: str | None,
    items_container: ui.column,
    on_changed: Callable[[], None],
) -> bool:
    if sport_value in (None, ""):
        ui.notify('Please choose a sport.', type='warning')
        return False

    try:
        created_item = presenter.create_item(sport_value, name=item_name or '')
    except ValueError as exc:
        ui.notify(str(exc), type='negative')
        return False

    logger.debug("Created item '%s' for selection '%s'", created_item.uid, presenter.name)

    # Added in place rather than reloading the page: a reload collapses every
    # expanded card on screen, which is jarring mid-edit.
    card = render_item_card(created_item, items_container, on_removed=on_changed)
    card.container.move(items_container, target_index=0)  # list is newest-first
    on_changed()

    ui.notify(copy.SPORT_ADDED, type='positive')
    return True

def _open_create_item_modal(
    presenter: SelectionPresenter,
    items_container: ui.column,
    on_changed: Callable[[], None],
) -> None:
    sports = sorted(app_context.client.list(Sport), key=lambda sport: sport.name.lower())
    sport_options = {Sport.decode_id(sport.id)[2]: sport.name.capitalize() for sport in sports}

    modal = FormModal(
        title=copy.ADD_SPORT_TITLE,
        message=copy.ADD_SPORT_MESSAGE,
        fields=[
            SelectField(name='sport', label='Sport', options=sport_options),
            TextField(name='name', label=copy.SPORT_NAME_LABEL),
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
        item_name=payload.get('name'),
        items_container=items_container,
        on_changed=on_changed,
    ))


@ui.page('/selections/{selection_name}')
def selection_page(selection_name: str):
    try:
        selection_core = SelectionService.get_selection(selection_name)
    except KeyError:
        logger.exception(f"Selection '{selection_name}' not found.")
        with base_layout():
            ui.label(f'No calendar called "{selection_name}".').classes('text-red-500 text-xl font-bold mb-4')
            ui.button('Back to my calendars', on_click=lambda: ui.navigate.to('/selections')).props('flat')
        return

    # A drawer is a top-level layout element: NiceGUI rejects it if it is nested
    # inside the page's content column, so it is built before base_layout().
    preview_drawer = render_preview_drawer(selection_name)

    with base_layout():
        presenter = SelectionPresenter(
            selection=selection_core,
            client=app_context.client
        )

        with ui.row().classes('w-full items-baseline justify-between mb-6'):
            with ui.column().classes('gap-1'), ui.row().classes('items-center gap-2'):
                ui.label(presenter.title).classes('text-3xl font-bold')
                info_icon(copy.WHAT_IS_A_SPORT_SECTION, size='sm')

            with ui.row().classes('gap-2'):
                ui.button('Back', on_click=lambda: ui.navigate.to('/selections')).props('outline')
                ui.button('Preview', icon='event', on_click=preview_drawer.toggle).props('outline')

        ui.separator().classes('mb-6')

        item_presenters = presenter.get_item_presenters(sort_by='updated_at', order='desc')
        logger.debug(f"Retrieved {len(item_presenters)} items for selection '{selection_name}'")

        empty_label = ui.label(copy.NO_SPORTS_YET).classes('text-gray-500 italic')
        items_container = ui.column().classes('w-full gap-2')

        def refresh_empty_state() -> None:
            """ Show the placeholder only while the item list is actually empty. """
            empty_label.set_visibility(not items_container.default_slot.children)

        for item_p in item_presenters:
            render_item_card(item_p, items_container, on_removed=refresh_empty_state)
        refresh_empty_state()

        AddCard(on_click=lambda: _open_create_item_modal(
            presenter, items_container, refresh_empty_state,
        ))
