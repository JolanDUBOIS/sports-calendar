from typing import Callable, List, Dict, Any

from attrs import field
from nicegui import ui

from . import logger
from .fields import BaseField


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

        self.fields: Dict[str, BaseField] = {}  # store field instances

    def build_content(self, container, fields: list[BaseField]):
        """Render all fields inside the given container."""
        for f in fields:
            f.render(container)
            self.fields[f.key] = f

    def open(self, on_confirm: Callable, fields: List[BaseField] = None):
        """Open the modal and handle confirm/cancel."""
        self.fields = {}

        with ui.dialog() as dialog:
            with ui.card().classes('p-4 rounded-lg shadow-lg').style(
                f'min-width: {self.min_width}; max-width: {self.max_width}; width: fit-content; margin: auto;'
            ) as card:
                if self.title:
                    ui.label(self.title).classes('text-h6 mb-4')
                if self.message:
                    ui.label(self.message).classes('text-base mb-4')

                # render fields inside the card
                if fields:
                    self.build_content(container=card, fields=fields)

                # action buttons
                with ui.row().classes('justify-end gap-2 mt-4'):
                    ui.button(self.cancel_label, on_click=dialog.close).props('flat')

                    def confirm(event=None):
                        try:
                            # collect current values from all fields
                            values = {k: f.value for k, f in self.fields.items()}
                            on_confirm(**values)
                        except TypeError:
                            on_confirm()
                        except Exception:
                            logger.exception("Error in modal confirm callback:")
                            raise

                        if self.reload_on_confirm:
                            ui.navigate.reload()
                        dialog.close()

                    ui.button(self.confirm_label, on_click=confirm).props(f'color={self.confirm_color}')

        dialog.open()
