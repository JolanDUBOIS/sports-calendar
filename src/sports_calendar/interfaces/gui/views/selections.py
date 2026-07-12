import logging

from nicegui import ui

from sports_calendar.application.selection import SelectionService

from ..components import (
    AddCard,
    ConfirmModal,
    FormModal,
    InteractiveCard,
    TextField,
    TextValidatorResult,
    base_layout,
)
from ..context import app_context
from ..presenters import SelectionPresenter

logger = logging.getLogger(__name__)


def _handle_create_selection(name: str) -> bool:
    normalized_name = name.strip()
    if not normalized_name:
        ui.notify('Please enter a selection name.', type='warning')
        return False

    try:
        SelectionPresenter.create(
            client=app_context.client,
            name=normalized_name,
        )
    except ValueError as exc:
        ui.notify(str(exc), type='negative')
        return False

    ui.notify(f'Selection "{normalized_name}" created.', type='positive')
    ui.navigate.reload()
    return True

def _validate_selection_name(name: str) -> TextValidatorResult:
    if not name.strip():
        return False, "Name cannot be empty."
    if SelectionService.selection_exists(name.strip()):
        return False, "A selection with this name already exists."
    return True, None

def _open_create_modal() -> None:
    modal = FormModal(
        title='Create Selection',
        message='Enter a name for the new selection.',
        fields=[
            TextField(
                name='name',
                label='Selection name',
                validator=_validate_selection_name,
            )
        ],
        confirm_label='Create',
        cancel_label='Cancel',
        confirm_color='primary',
    )
    modal.open(on_confirm=lambda payload: _handle_create_selection(payload['name']))

def _handle_delete_selection(presenter: SelectionPresenter) -> None:
    try:
        presenter.delete()
    except KeyError:
        logger.exception("Selection '%s' could not be deleted", presenter.name)
        ui.notify(f'Selection "{presenter.name}" was not found.', type='negative')
        return

    ui.notify(f'Selection "{presenter.name}" deleted.', type='positive')
    ui.navigate.reload()

def _open_delete_modal(presenter: SelectionPresenter) -> None:
    ConfirmModal(
        title='Delete Selection',
        message=f'Are you sure you want to delete "{presenter.name}"? This cannot be undone.',
        confirm_label='Delete',
        cancel_label='Cancel',
        confirm_color='negative',
    ).open(on_confirm=lambda _: _handle_delete_selection(presenter))


@ui.page('/selections')
def selections_page():
    with base_layout():
        ui.label('All Selections').classes('text-3xl font-bold mb-4')

        with ui.column().classes('w-full gap-2'):
            for selection in SelectionService.get_all_selections(sort_by='updated_at', order='desc'):
                presenter = SelectionPresenter(
                    selection=selection,
                    client=app_context.client
                )

                InteractiveCard(
                    title=presenter.title,
                    subtitle=presenter.subtitle,
                    on_click=lambda p=presenter: ui.navigate.to(f'/selections/{p.name}'),
                    on_delete=lambda p=presenter: _open_delete_modal(p),
                )

            AddCard(on_click=_open_create_modal)
