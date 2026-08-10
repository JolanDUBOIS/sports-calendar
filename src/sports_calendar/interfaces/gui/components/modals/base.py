from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from nicegui import ui

if TYPE_CHECKING:
    from collections.abc import Callable

    from .fields import ModalField

logger = logging.getLogger(__name__)


class BaseModal(ABC):
    def __init__(
        self,
        title: str,
        message: str | None = None,
        confirm_label: str = "Confirm",
        cancel_label: str = "Cancel",
        confirm_color: str = "primary",
        min_width: str = "320px",
        max_width: str = "520px",
        payload_adapter: Callable[[Any], Any] | None = None,
    ):
        self.title = title
        self.message = message
        self.confirm_label = confirm_label
        self.cancel_label = cancel_label
        self.confirm_color = confirm_color
        self.min_width = min_width
        self.max_width = max_width
        self.payload_adapter = payload_adapter

    @abstractmethod
    def _build_body(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _get_payload(self) -> Any:
        raise NotImplementedError

    def _confirm_button_props(self) -> str:
        return f"color={self.confirm_color}"

    def open(self, on_confirm: Callable[[Any], bool | None]) -> None:
        with ui.dialog() as dialog, ui.card().classes("p-5 rounded-lg shadow-lg").style(
            f"min-width: {self.min_width}; max-width: {self.max_width}; width: 100%;"
        ):
            ui.label(self.title).classes("text-h6 mb-2")
            if self.message:
                ui.label(self.message).classes("text-body2 text-gray-700")

            self._build_body()

            with ui.row().classes("justify-end gap-2 mt-6 w-full"):
                ui.button(self.cancel_label, on_click=dialog.close).props("flat")

                def confirm() -> None:
                    payload = self._get_payload()
                    if self.payload_adapter:
                        payload = self.payload_adapter(payload)
                    should_close = on_confirm(payload)
                    if should_close is not False:
                        dialog.close()

                ui.button(self.confirm_label, on_click=confirm).props(self._confirm_button_props())

        dialog.open()


class ConfirmModal(BaseModal):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("confirm_color", "negative")
        super().__init__(*args, **kwargs)

    def _build_body(self) -> None:
        pass

    def _get_payload(self) -> None:
        return None

    def _confirm_button_props(self) -> str:
        return f"color={self.confirm_color} autofocus"


class FormModal(BaseModal):
    def __init__(
        self,
        title: str,
        fields: list[ModalField],
        message: str | None = None,
        **kwargs
    ):
        super().__init__(title=title, message=message, **kwargs)
        self.fields = fields
        self._body_container = None

    def _build_body(self) -> None:
        self._body_container = ui.column().classes("w-full gap-4 mt-4")
        self._render_fields()

    def _render_fields(self) -> None:
        if self._body_container is None:
            return

        self._body_container.clear()
        with self._body_container:
            for field in self.fields:
                field.render()

    def replace_fields(self, new_fields: list[ModalField]) -> None:
        self.fields = new_fields
        self._render_fields()

    def _get_payload(self) -> dict[str, Any]:
        return {field.name: field.value for field in self.fields}


class SearchModal(BaseModal):
    def __init__(
        self,
        title: str,
        search_fn: Callable[[str], dict[Any, str]],
        message: str | None = None,
        **kwargs
    ):
        super().__init__(title=title, message=message, **kwargs)
        self.search_fn = search_fn
        self._search_id = 0
        self.options: dict[Any, str] = {}
        self._selected_id = None
        self._selected_name = None
        self._ignore_next_change = False
        self._cache: dict[str, dict[Any, str]] = {}

    def _build_body(self) -> None:
        with ui.column().classes("w-full mt-4 gap-1"):
            # Put the input and the spinner in a row
            with ui.row().classes("w-full items-center no-wrap gap-2"):
                self._search_input = ui.input(
                    label="Search...",
                    on_change=self._handle_search
                ).classes("flex-grow").props("autofocus clearable")

                # A subtle loading spinner, hidden by default
                self._spinner = ui.spinner(color="primary", size="1.5em").classes("hidden")

            self._results_container = ui.scroll_area().classes(
                "w-full h-48 border border-gray-200 rounded hidden"
            )

    async def _handle_search(self, event: Any) -> None:
        if self._ignore_next_change:
            self._ignore_next_change = False
            return

        query = event.value if event.value else ""
        query = query.strip()

        self._search_id += 1
        current_id = self._search_id

        if len(query) < 2:
            self._results_container.classes("hidden")
            self._results_container.clear()
            self._spinner.classes("hidden")
            return

        # Show the spinner instantly so the app feels highly responsive
        self._spinner.classes(remove="hidden")

        # You can drop the debounce even lower to 0.1 if you have the cache
        await asyncio.sleep(0.1)
        if current_id != self._search_id:
            return

        try:
            # Check the cache first! If it's there, skip the API entirely.
            if query in self._cache:
                results = self._cache[query]
            else:
                results = await asyncio.to_thread(self.search_fn, query)

                # Safety check: if they typed more while waiting, don't overwrite UI
                if current_id != self._search_id:
                    return

                # Save the fresh API result to the cache
                self._cache[query] = results

            self.options = results
            self._results_container.clear()

            if results:
                self._results_container.classes(remove="hidden")
                with self._results_container, ui.list().classes("w-full"):
                    for item_id, item_name in results.items():
                        ui.item(
                            item_name,
                            on_click=lambda e, i=item_id, n=item_name: self._select_item(i, n)
                        ).classes("cursor-pointer hover:bg-blue-50 transition-colors")
            else:
                self._results_container.classes("hidden")

        except Exception:
            logger.exception(f"Search failed for query: '{query}'")

        finally:
            # Always hide the spinner when done, even if it crashed
            if current_id == self._search_id:
                self._spinner.classes("hidden")

    def _select_item(self, item_id: Any, item_name: str) -> None:
        self._selected_id = item_id
        self._selected_name = item_name

        self._ignore_next_change = True
        self._search_input.value = item_name

        self._results_container.classes("hidden")

    def _get_payload(self) -> Any:
        if self._selected_id is None:
            return None
        return (self._selected_id, self._selected_name)
