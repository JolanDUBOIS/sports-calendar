import logging
from collections.abc import Callable

from nicegui import ui
from sportindex import Sport

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.sports import is_supported

from .. import copy, theme
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
from .item_card import enable_filter_reordering, render_item_card
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
    # Only sports the backend can actually turn into calendar events. Offering
    # the rest would let someone build a calendar that raises at sync time,
    # because build_calendar has no event class to map them to.
    #
    # And only sports not already in the calendar: an item is now nothing but a
    # grouping by sport — its filters are unioned, and so are the items — so a
    # second football card would be exactly equivalent to putting those rules on
    # the first one, while looking like it meant something different.
    already_added = {item.sport_id for item in presenter.selection.items}
    sports = sorted(app_context.client.list(Sport), key=lambda sport: sport.name.lower())
    sport_options = {}
    for sport in sports:
        sport_id = Sport.decode_id(sport.id)[2]
        if is_supported(sport_id) and sport_id not in already_added:
            sport_options[sport_id] = sport.name.capitalize()

    name_field = TextField(
        name='name',
        label=copy.SPORT_NAME_LABEL,
        placeholder=copy.DEFAULT_ITEM_NAME_EXAMPLE,
        help_text=copy.SPORT_NAME_HELP,
    )

    modal = FormModal(
        title=copy.ADD_SPORT_TITLE,
        message=copy.ADD_SPORT_MESSAGE,
        fields=[
            SelectField(
                name='sport',
                label='Sport',
                options=sport_options,
                empty_note=copy.ALL_SPORTS_ADDED,
                # "e.g. Formula 1, MotoGP" is only helpful once the sport is
                # known, so the example follows the choice.
                on_change=lambda sport_id: name_field.set_placeholder(
                    copy.item_name_example(sport_id)
                ),
            ),
            name_field,
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
        logger.exception("Selection '%s' not found", selection_name)
        with base_layout():
            ui.label(f'No calendar called "{selection_name}".').classes(f'{theme.PAGE_TITLE} mb-4')
            ui.button('Back to my calendars', on_click=lambda: ui.navigate.to('/selections')).props('flat no-caps')
        return

    # A drawer is a top-level layout element: NiceGUI rejects it if it is nested
    # inside the page's content column, so it is built before base_layout().
    preview_drawer = render_preview_drawer(selection_name)
    enable_filter_reordering()

    with base_layout():
        presenter = SelectionPresenter(
            selection=selection_core,
            client=app_context.client
        )

        with ui.row().classes('w-full items-center justify-between mb-6'):
            with ui.column().classes('gap-1'), ui.row().classes('items-center gap-2'):
                ui.label(presenter.title).classes(theme.PAGE_TITLE)
                info_icon(copy.WHAT_IS_A_SPORT_SECTION, size='sm')

            with ui.row().classes('gap-2'):
                ui.button('Back', on_click=lambda: ui.navigate.to('/selections')).props('outline no-caps')
                ui.button('Preview', icon='event', on_click=preview_drawer.toggle).props('outline no-caps')

        item_presenters = presenter.get_item_presenters(sort_by='updated_at', order='desc')
        logger.debug("Retrieved %d items for selection '%s'", len(item_presenters), selection_name)

        # One column so the cards, the placeholder and the add button are spaced
        # by a single rule rather than by margins that have to agree with it.
        with ui.column().classes('w-full gap-3'):
            empty_label = ui.label(copy.NO_SPORTS_YET).classes(theme.MUTED)
            items_container = ui.column().classes('w-full gap-3')

            def refresh_empty_state() -> None:
                """ Show the placeholder only while the item list is actually empty. """
                empty_label.set_visibility(not items_container.default_slot.children)

            for item_p in item_presenters:
                render_item_card(item_p, items_container, on_removed=refresh_empty_state)
            refresh_empty_state()

            AddCard(on_click=lambda: _open_create_item_modal(
                presenter, items_container, refresh_empty_state,
            ))
