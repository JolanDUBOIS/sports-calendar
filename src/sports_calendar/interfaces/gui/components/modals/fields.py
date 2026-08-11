import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from nicegui import ui

from .base import SearchModal

logger = logging.getLogger(__name__)


def _normalize_options(options: list[Any] | dict[Any, str]) -> dict[Any, str]:
    logger.debug(f"Normalizing {len(options)} options for field: {options}")
    if isinstance(options, dict):
        return options
    return {option: str(option) for option in options}


SearchOptionsFn = Callable[[str], list[Any] | dict[Any, str]]
TextValidatorResult = bool | tuple[bool, str | None]
TextValidatorFn = Callable[[str], TextValidatorResult]


def _extract_query_from_event(event: Any) -> str:
    args = getattr(event, "args", None)
    if isinstance(args, str):
        return args
    if isinstance(args, list) and args:
        return str(args)
    if isinstance(args, dict):
        value = args.get("value", args.get("inputValue", ""))
        return str(value)
    return ""


class ModalField(ABC):
    def __init__(self, name: str, label: str, help_text: str | None = None):
        self.name = name
        self.label = label
        self.help_text = help_text
        self._element = None

    def _apply_help(self) -> None:
        """ Show the field's explanation as a persistent Quasar hint.

        Called by subclasses at the end of render(). Quotes are stripped because
        the text is interpolated into a prop string.
        """
        if self.help_text and self._element is not None:
            self._element.props(f'hint="{self.help_text.replace(chr(34), chr(39))}"')

    @abstractmethod
    def render(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def value(self) -> Any:
        raise NotImplementedError


class NumberField(ModalField):
    def __init__(self, name: str, label: str, default: int = 0):
        super().__init__(name, label)
        self.default = default

    def render(self) -> None:
        self._element = ui.number(
            label=self.label,
            value=self.default,
            format="%d",
        ).props("step=1").classes("w-full")

    @property
    def value(self) -> int:
        raw_value = self._element.value if self._element else self.default
        if raw_value is None or raw_value == "":
            return int(self.default)
        return int(raw_value)


class TextField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        default: str = "",
        validator: TextValidatorFn | None = None,
    ):
        super().__init__(name, label)
        self.default = default
        self.validator = validator

    def render(self) -> None:
        with ui.column().classes("w-full gap-1"):
            self._element = ui.input(
                label=self.label,
                value=self.default,
                validation=self._adapter if self.validator else None
            ).classes("w-full")

    def _adapter(self, value: str) -> str | None:
        if self.validator is None:
            return None

        result = self.validator(value)
        is_valid, message = result if isinstance(result, tuple) else (bool(result), None)

        if is_valid:
            return None

        return message or "Invalid value."

    @property
    def value(self) -> str:
        return self._element.value if self._element else self.default


class SelectField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        options: list[Any] | dict[Any, str],
        default: Any = None,
        help_text: str | None = None
    ):
        super().__init__(name, label, help_text)
        self.options = _normalize_options(options)
        self.default = default

    def render(self) -> None:
        self._element = ui.select(
            label=self.label,
            options=self.options,
            value=self.default
        ).classes("w-full")
        self._apply_help()

    @property
    def value(self) -> Any:
        return self._element.value if self._element else self.default


class MultipleSelectField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        options: list[Any] | dict[Any, str],
        default: list[Any] | None = None
    ):
        super().__init__(name, label)
        self.options = _normalize_options(options)
        self.default = default or []

    def render(self) -> None:
        self._element = ui.select(
            label=self.label,
            options=self.options,
            value=self.default,
            multiple=True,
        ).props("use-chips").classes("w-full")

    @property
    def value(self) -> list[Any]:
        if not self._element:
            return list(self.default)
        return list(self._element.value or [])


# class SearchableSelectField(SelectField):
#     def __init__(
#         self,
#         name: str,
#         label: str,
#         options: list[Any] | dict[Any, str],
#         default: Any = None
#     ):
#         super().__init__(name, label, options, default)

#     def render(self) -> None:
#         self._element = ui.select(
#             label=self.label,
#             options=self.options,
#             value=self.default,
#             with_input=True,
#         ).classes("w-full")


# class SearchableMultipleSelectField(MultipleSelectField):
#     def __init__(
#         self,
#         name: str,
#         label: str,
#         options: list[Any] | dict[Any, str],
#         default: list[Any] | None = None
#     ):
#         super().__init__(name, label, options, default)

#     def render(self) -> None:
#         self._element = ui.select(
#             label=self.label,
#             options=self.options,
#             value=self.default,
#             multiple=True,
#             with_input=True,
#         ).props("use-chips").classes("w-full")


class SearchableSelectField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        search_fn: Callable[[str], dict[Any, str]],
        default_value: Any = None,
        default_label: str = "",
        search_hint: str = "Search..."
    ):
        super().__init__(name, label)
        self.search_fn = search_fn
        self.search_hint = search_hint
        self._value = default_value

        if default_value and default_label:
            self._display_text = default_label
        elif default_value:
            self._display_text = str(default_value)
        else:
            self._display_text = f"Select {label.lower()}..."

        self._label_element = None

    def render(self) -> None:
        with ui.column().classes("w-full gap-1"):
            ui.label(self.label).classes("text-sm text-gray-700 font-medium")

            # We use a clickable ui.row instead of a button.
            # This mimics standard input fields perfectly and avoids q-btn reactivity issues.
            self._box = ui.row().classes(
                "w-full px-3 py-2 bg-white border border-gray-300 rounded text-gray-800 "
                "hover:bg-gray-50 cursor-pointer items-center justify-between min-h-[42px] transition-colors"
            ).on('click', self._open_modal)

            with self._box:
                # The label is now an independent element that updates reliably
                self._label_element = ui.label(self._display_text).classes("truncate")
                ui.icon('search', size="sm").classes("text-gray-500")

    def _open_modal(self) -> None:
        modal = SearchModal(title=f"Search {self.label}", search_fn=self.search_fn, search_hint=self.search_hint)

        def on_confirm(payload: tuple[Any, str] | None) -> bool:
            if payload is not None:
                item_id, item_label = payload
                self._value = item_id
                self._display_text = item_label

                # Directly update the label's text property.
                # ui.label handles its own DOM patching automatically.
                if self._label_element:
                    self._label_element.text = self._display_text
            return True

        modal.open(on_confirm=on_confirm)

    @property
    def value(self) -> Any:
        return self._value


class SearchableMultipleSelectField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        search_fn: Callable[[str], dict[Any, str]],
        default_values: dict[Any, str] | None = None,
        search_hint: str = "Search..."
    ):
        super().__init__(name, label)
        self.search_fn = search_fn
        self.search_hint = search_hint
        self._selections = default_values or {}
        self._chips_container = None

    def render(self) -> None:
        with ui.column().classes("w-full gap-1"):
            ui.label(self.label).classes("text-sm text-gray-700 font-medium")

            # The main visual box
            with ui.row().classes(
                "w-full gap-1 p-1.5 bg-white border border-gray-300 rounded items-center wrap min-h-[42px]"
            ):
                # A dedicated sub-container strictly for the chips
                self._chips_container = ui.row().classes("gap-1 items-center wrap flex-grow m-0 p-0")
                self._render_chips()

                # The "Add" button stays safely outside the clearable chip area
                ui.button(
                    "Add",
                    on_click=self._open_modal
                ).props("flat size=sm color=primary icon=add no-caps").classes("p-1 ml-auto")

    def _render_chips(self) -> None:
        if self._chips_container is None:
            return

        self._chips_container.clear()
        with self._chips_container:
            for item_id, item_label in self._selections.items():
                # Bind to the underlying Quasar 'remove' event correctly
                ui.chip(
                    item_label,
                    removable=True
                ).classes("m-0").on('remove', lambda e, idx=item_id: self._remove_item(idx))

    def _remove_item(self, item_id: Any) -> None:
        if item_id in self._selections:
            del self._selections[item_id]
        self._render_chips()

    def _open_modal(self) -> None:
        modal = SearchModal(title=f"Add to {self.label}", search_fn=self.search_fn, search_hint=self.search_hint)

        def on_confirm(payload: tuple[Any, str] | None) -> bool:
            if payload is not None:
                item_id, item_label = payload
                if item_id not in self._selections:
                    self._selections[item_id] = item_label
                    self._render_chips()
            return True

        modal.open(on_confirm=on_confirm)

    @property
    def value(self) -> list[Any]:
        return list(self._selections.keys())
