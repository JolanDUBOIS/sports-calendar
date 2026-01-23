from typing import Callable, List, Dict, Any

from nicegui import ui

from . import logger


class Modal:
    """ Reusable modal with card + buttons layout. """

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

        self.fields: Dict[str, Any] = {}  # to hold input fields

    def build_content(self, fields: List[Dict[str, Any]]):
        """
        Build input fields dynamically.

        Args:
            fields: List of dictionaries describing each input.
                Each dict can have:
                    - key: identifier for the field (required)
                    - label: label to display
                    - type: 'text', 'select', 'checkbox', etc.
                    - options: list of options for 'select'
                    - default: initial value
        """
        for f in fields:
            key = f['key']
            label = f.get('label', key)
            field_type = f.get('type', 'text')
            default = f.get('default', '')

            if field_type == 'text':
                self.fields[key] = ui.input(label, value=default).classes('w-full')
            elif field_type == 'select':
                options = f.get('options', [])
                self.fields[key] = ui.select(options, label=label, value=default).classes('w-full')
            elif field_type == 'checkbox':
                self.fields[key] = ui.checkbox(label, value=default)
            else:
                raise ValueError(f"Unsupported field type: {field_type}")

    def open(self, on_confirm: Callable, fields: List[Dict[str, Any]] = None):
        """
        Open the modal.

        Args:
            on_confirm: callable that will receive a dict of field values keyed by 'key'
            fields: list of input specifications (see build_content)
        """
        self.fields = {}

        with ui.dialog() as dialog:
            with ui.card().classes('p-4 rounded-lg shadow-lg').style(
                f'min-width: {self.min_width}; max-width: {self.max_width}; width: fit-content; margin: auto;'
            ):
                if self.title:
                    ui.label(self.title).classes('text-h6 mb-4')
                if self.message:
                    ui.label(self.message).classes('text-base mb-4')

                if fields:
                    self.build_content(fields)

                with ui.row().classes('justify-end gap-2 mt-4'):
                    ui.button(self.cancel_label, on_click=dialog.close).props('flat')

                    def confirm(event=None):
                        try:
                            values = {k: getattr(f, 'value', None) for k, f in self.fields.items()}
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
