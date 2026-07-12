from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List

from nicegui import ui

from . import logger

if TYPE_CHECKING:
    from .filters.fields import BaseField


class Modal:
    """Reusable modal with card + buttons layout."""

    def __init__(
        self,
        title: str = None,
        message: str = None,
        min_width: str = '300px',
        max_width: str = '500px',
        confirm_label: str = 'Confirm',
        confirm_color: str = 'blue',
        cancel_label: str = 'Cancel',
        reload_on_confirm: bool = False,
    ):
        self.title = title
        self.message = message
        self.min_width = min_width
        self.max_width = max_width
        self.confirm_label = confirm_label
        self.confirm_color = confirm_color
        self.cancel_label = cancel_label
        self.reload_on_confirm = reload_on_confirm

        self.fields: Dict[str, BaseField] = {}
        self._fields_container = None  # NEW: dedicated container for fields only
        self._card_container = None
        self._dialog = None

    def build_content(self, container, fields: list[BaseField]):
        """Render all fields inside the given container."""
        for f in fields:
            f.render(container)
            self.fields[f.key] = f

    def update_fields(self, new_fields: list[BaseField]):
        """Replace existing fields dynamically (reactive forms)."""
        if not self._fields_container:
            return

        self._fields_container.clear()
        self.fields = {}
        self.build_content(self._fields_container, new_fields)

    def open(self, on_confirm: Callable, fields: List[BaseField] = None):
        """Open the modal and handle confirm/cancel."""
        self.fields = {}

        with ui.dialog() as dialog:
            self._dialog = dialog

            with ui.card().classes('p-4 rounded-lg shadow-lg').style(
                f'min-width: {self.min_width}; max-width: {self.max_width}; width: fit-content; margin: auto;'
            ) as card:
                self._card_container = card

                # title and message
                if self.title:
                    ui.label(self.title).classes('text-h6 mb-4')
                if self.message:
                    ui.label(self.message).classes('text-base mb-4')

                # NEW: Create dedicated container for fields
                with ui.column().classes('w-full gap-2') as fields_container:
                    self._fields_container = fields_container
                    if fields:
                        self.build_content(fields_container, fields)

                # action buttons (these stay below the fields container)
                with ui.row().classes('justify-end gap-2 mt-4'):
                    ui.button(self.cancel_label, on_click=dialog.close).props('flat')

                    def confirm(event=None):
                        logger.debug("Modal confirmed")
                        try:
                            values = {k: f.value for k, f in self.fields.items()}
                            logger.debug(f"Calling modal on_confirm with values: {values}")
                            on_confirm(**values)
                        except Exception:
                            logger.exception("Error in modal confirm callback:")
                            raise

                        if self.reload_on_confirm:
                            ui.navigate.reload()
                        dialog.close()

                    ui.button(self.confirm_label, on_click=confirm).props(f'color={self.confirm_color}')

            ui.keyboard(
                on_key=lambda e: confirm() if e.action.keydown and e.key.enter else None
            )

        dialog.open()
