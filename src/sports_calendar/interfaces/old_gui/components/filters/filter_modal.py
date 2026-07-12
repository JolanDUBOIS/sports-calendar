from nicegui import ui

from sports_calendar.core.selection import SelectionFilter

from ..modals import Modal
from . import logger
from .filter_forms import get_fields_for_filter


def open_filter_modal(filter: SelectionFilter | None = None, title: str = "", on_confirm_callback: callable = None):
    """ TODO """

    modal = Modal(
        title=title,
        min_width='500px',
        max_width='800px',
        reload_on_confirm=True
    )

    # Initial fields
    initial_fields = get_fields_for_filter(filter=filter)

    def on_filter_type_change(e):
        filter_type_field = modal.fields.get('filter_type')
        if not filter_type_field:
            logger.error("filter_type field not found in modal.fields")
            return

        selected_label = e.args['label'] if isinstance(e.args, dict) else e.args
        new_type = next((c.value for c in filter_type_field.options if c.label == selected_label), None)

        if not new_type:
            logger.error(f"Could not find value for label: {selected_label}")
            return

        logger.debug(f"Filter type changed to: {new_type}")

        # Get new fields for the selected filter type
        new_fields = get_fields_for_filter(
            filter=None,
            filter_meta={"sport": filter.sport if filter else None, "filter_type": new_type}
        )

        # Update modal fields
        modal.update_fields(new_fields)

        # Re-attach handler to the new filter_type field
        new_filter_type_field = modal.fields.get('filter_type')
        if new_filter_type_field and new_filter_type_field.select:
            new_filter_type_field.select.on('update:model-value', on_filter_type_change)

    # Open modal
    modal.open(on_confirm=on_confirm_callback, fields=initial_fields)

    # Attach initial filter_type handler
    filter_type_field = modal.fields.get('filter_type')
    if filter_type_field and filter_type_field.select:
        filter_type_field.select.on('update:model-value', on_filter_type_change)
