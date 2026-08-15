from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING

from nicegui import ui

from sports_calendar.core.selection import FilterType, SelectionFilter
from sports_calendar.core.sports import allowed_filter_types

from ....copy import (
    FOLLOW_TITLE,
    FOLLOW_TYPE_LABEL,
    MODAL_LOAD_FAILED,
    MODAL_LOADING,
    RULE_NAME_EXAMPLES,
    RULE_NAME_HELP,
    RULE_NAME_LABEL,
    filter_type_help,
    filter_type_label,
)
from ..base import FormModal
from ..fields import LoadingField, ModalField, SelectField, TextField
from .field_builders import FIELD_BUILDER_MAP
from .payloads import ADAPTER_MAP

if TYPE_CHECKING:
    from ....catalog import FilterSearchProvider

logger = logging.getLogger(__name__)


def filter_type_options_for(sport_id: int, current: FilterType | None = None) -> dict[str, str]:
    """ The filter types worth offering for a sport.

    Offering all of them everywhere is what made the modal misleading: picking
    "Race sessions" for football, or "Top-ranked teams" for tennis, produced an
    empty calendar with no explanation rather than an error.

    `current` is always kept, even if the sport does not allow it, so a filter
    saved earlier can still be opened and read instead of silently losing its
    own type from the dropdown.
    """
    allowed = allowed_filter_types(sport_id)
    return {
        filter_type.value: filter_type_label(filter_type, sport_id)
        for filter_type in FilterType
        if filter_type in allowed or filter_type is current
    }


class FilterModal(FormModal):
    """ The dialog for building one rule.

    Its fields depend on the filter type, and their defaults come off the
    network: the names of the competitions the rule already holds, the rounds
    those competitions share. That is seconds of work for a rule naming five
    competitions, and doing it before showing the dialog meant the Edit button
    appeared to do nothing at all — long enough that clicking again, and opening
    a second dialog behind the first, was the natural response.

    So the dialog opens first and fills itself in: pass `defer_fields=True` and
    await `load_fields()` once it is on screen. Save stays disabled until the
    real fields have arrived, so a half-built form cannot overwrite a rule.
    """

    def __init__(
        self,
        initial_filter: SelectionFilter,
        search_provider: FilterSearchProvider,
        title: str = FOLLOW_TITLE,
        message: str | None = None,
        initial_filter_type: FilterType = FilterType.EMPTY,
        defer_fields: bool = False,
        **kwargs,
    ):
        self._initial_filter = initial_filter
        self._search_provider = search_provider
        self._selected_filter_type = initial_filter_type
        self._loading = defer_fields

        self._filter_type_field = SelectField(
            name="filter_type",
            label=FOLLOW_TYPE_LABEL,
            options=filter_type_options_for(initial_filter.sport_id, initial_filter_type),
            default=self._selected_filter_type.value,
            help_text=filter_type_help(self._selected_filter_type, initial_filter.sport_id),
            # These run to a paragraph and would overlap the next field's label
            # if rendered as a Quasar hint.
            help_as_caption=True,
        )

        self._name_field = TextField(
            name="name",
            label=RULE_NAME_LABEL,
            default=initial_filter.name or "",
            placeholder=RULE_NAME_EXAMPLES,
            help_text=RULE_NAME_HELP,
        )

        self._dynamic_fields = (
            [LoadingField(MODAL_LOADING)] if self._loading else self._build_dynamic_fields()
        )

        kwargs["payload_adapter"] = self._current_adapter()

        # Name last: the substance of the rule is what it follows, and the label
        # is optional garnish for telling two similar rules apart afterwards.
        super().__init__(
            title=title,
            fields=[self._filter_type_field, *self._dynamic_fields, self._name_field],
            message=message,
            **kwargs,
        )

    def _current_adapter(self):
        """ The payload adapter for the type currently selected. """
        return partial(
            ADAPTER_MAP[self._selected_filter_type],
            self._initial_filter.sport_id,
            self._initial_filter.uid,
        )

    def _build_body(self) -> None:
        super()._build_body()
        self._bind_filter_type_change()

    def _bind_filter_type_change(self) -> None:
        """ Re-arm the type dropdown's handler.

        Needed after every `replace_fields`: re-rendering destroys the select
        element and builds a new one, which does not carry the old handler.
        """
        self._filter_type_field.bind_value_change(
            lambda value: self.update(
                value if isinstance(value, FilterType) else FilterType(value)
            )
        )

    def _build_dynamic_fields(self) -> list[ModalField]:
        """ The fields for the selected type. Hits the network — never call it
        on the event loop; see `load_fields`. """
        return FIELD_BUILDER_MAP[self._selected_filter_type](
            initial_filter=self._initial_filter,
            search_provider=self._search_provider,
            sport_id=self._initial_filter.sport_id,
        )

    def _is_ready(self) -> bool:
        return not self._loading

    def _show_fields(self, fields: list[ModalField]) -> None:
        self._dynamic_fields = fields
        self.replace_fields([self._filter_type_field, *self._dynamic_fields, self._name_field])
        self.payload_adapter = self._current_adapter()
        self._bind_filter_type_change()

    def _set_loading(self, loading: bool) -> None:
        self._loading = loading
        if self._confirm_button is not None:
            self._confirm_button.set_enabled(not loading)

    async def load_fields(self) -> None:
        """ Fetch the real fields and swap them in, with the dialog already up.

        Off the event loop, so the page stays responsive while it runs — the
        provider's client is synchronous and a competitions rule costs roughly a
        second per competition.
        """
        try:
            fields = await asyncio.to_thread(self._build_dynamic_fields)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to build fields for filter '%s'", self._initial_filter.uid)
            ui.notify(MODAL_LOAD_FAILED, type="negative")
            self._close()
            return

        self._set_loading(False)
        self._show_fields(fields)

    async def update(self, new_filter_type: FilterType) -> None:
        """ Rebuild the form around a newly chosen filter type. """
        self._selected_filter_type = new_filter_type
        self._filter_type_field.default = self._selected_filter_type.value
        # Explanation follows the choice, so the modal describes what is selected.
        self._filter_type_field.help_text = filter_type_help(
            self._selected_filter_type, self._initial_filter.sport_id
        )

        # Some types need the network for their defaults, so the form shows a
        # placeholder in the meantime rather than freezing on the dropdown.
        self._set_loading(True)
        self._show_fields([LoadingField(MODAL_LOADING)])
        try:
            fields = await asyncio.to_thread(self._build_dynamic_fields)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to build fields for type '%s'", new_filter_type)
            ui.notify(MODAL_LOAD_FAILED, type="negative")
            fields = []
        self._set_loading(False)
        self._show_fields(fields)

    def _close(self) -> None:
        if self._dialog is not None:
            self._dialog.close()
