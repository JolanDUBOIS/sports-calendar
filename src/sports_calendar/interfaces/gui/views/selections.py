import logging

from nicegui import ui

from sports_calendar.application.selection import SelectionService

from .. import copy, theme
from ..components import (
    AddCard,
    ConfirmModal,
    FormModal,
    InteractiveCard,
    TextField,
    TextValidatorResult,
    base_layout,
    explanation,
    info_icon,
)
from ..context import app_context
from ..presenters import SelectionPresenter

logger = logging.getLogger(__name__)


def _render_selection_card(presenter: SelectionPresenter) -> InteractiveCard:
    """ Render one selection card into the caller's current container.

    The caller owns the `with container:` block. Entering the container here as
    well would nest it inside itself, and the card is then dropped.

    `card` is referenced inside its own on_delete lambda: the lambda only runs on
    click, by which point the name is bound.
    """
    card = InteractiveCard(
        title=presenter.title,
        subtitle=presenter.subtitle,
        on_click=lambda: ui.navigate.to(f'/selections/{presenter.name}'),
        on_delete=lambda: _open_delete_modal(presenter, card),
    )
    card.container.mark('selection-card')
    return card


def _handle_create_selection(name: str, container: ui.column) -> bool:
    normalized_name = name.strip()
    if not normalized_name:
        ui.notify('Please enter a name.', type='warning')
        return False

    try:
        presenter = SelectionPresenter.create(
            client=app_context.client,
            name=normalized_name,
        )
    except ValueError as exc:
        ui.notify(str(exc), type='negative')
        return False

    # Added in place rather than reloading the page: a reload collapses every
    # expanded card on screen, which is jarring mid-edit.
    with container:
        card = _render_selection_card(presenter)
    card.container.move(container, target_index=0)  # list is newest-first

    ui.notify(copy.CALENDAR_CREATED.format(name=normalized_name), type='positive')
    return True

def _validate_selection_name(name: str) -> TextValidatorResult:
    if not name.strip():
        return False, "Name cannot be empty."
    if SelectionService.selection_exists(name.strip()):
        return False, "You already have a calendar with this name."
    return True, None

def _open_create_modal(container: ui.column) -> None:
    modal = FormModal(
        title=copy.NEW_CALENDAR_TITLE,
        message=copy.NEW_CALENDAR_MESSAGE,
        fields=[
            TextField(
                name='name',
                label=copy.CALENDAR_NAME_LABEL,
                placeholder=copy.SELECTION_NAME_EXAMPLES,
                validator=_validate_selection_name,
            )
        ],
        confirm_label='Create',
        cancel_label='Cancel',
        confirm_color='primary',
    )
    modal.open(on_confirm=lambda payload: _handle_create_selection(payload['name'], container))

def _handle_delete_selection(presenter: SelectionPresenter, card: InteractiveCard) -> None:
    try:
        presenter.delete()
    except KeyError:
        logger.exception("Selection '%s' could not be deleted", presenter.name)
        ui.notify(f'Calendar "{presenter.name}" was not found.', type='negative')
        return

    card.container.delete()
    ui.notify(copy.CALENDAR_DELETED.format(name=presenter.name), type='positive')

def _open_delete_modal(presenter: SelectionPresenter, card: InteractiveCard) -> None:
    ConfirmModal(
        title=copy.DELETE_CALENDAR_TITLE,
        message=copy.DELETE_CALENDAR_MESSAGE.format(name=presenter.name),
        confirm_label='Delete',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_selection(presenter, card))


@ui.page('/selections')
def selections_page():
    with base_layout():
        with ui.row().classes('items-center gap-2'):
            ui.label(copy.CALENDARS_PAGE_TITLE).classes(theme.PAGE_TITLE)
            info_icon(copy.WHAT_IS_A_CALENDAR, size='sm')
        explanation(copy.CALENDARS_PAGE_INTRO).classes('mb-6')

        with ui.column().classes('w-full gap-3'):
            cards_container = ui.column().classes('w-full gap-3')
            with cards_container:
                for selection in SelectionService.get_all_selections(sort_by='updated_at', order='desc'):
                    _render_selection_card(
                        SelectionPresenter(selection=selection, client=app_context.client)
                    )

            AddCard(on_click=lambda: _open_create_modal(cards_container))
