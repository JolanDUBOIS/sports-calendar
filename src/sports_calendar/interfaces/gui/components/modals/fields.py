import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from nicegui import ui

from ... import theme
from .base import SearchModal

logger = logging.getLogger(__name__)


def _normalize_options(options: list[Any] | dict[Any, str]) -> dict[Any, str]:
    logger.debug("Normalizing %d options for a field", len(options))
    if isinstance(options, dict):
        return options
    return {option: str(option) for option in options}


TextValidatorResult = bool | tuple[bool, str | None]
TextValidatorFn = Callable[[str], TextValidatorResult]


class ModalField(ABC):
    def __init__(self, name: str, label: str, help_text: str | None = None):
        self.name = name
        self.label = label
        self.help_text = help_text
        self._element = None
        # The outermost element a field renders, so it can be hidden whole.
        # Set by render() in the subclasses that support being hidden.
        self._root = None
        self._visible = True

    def set_visible(self, visible: bool) -> None:
        """ Show or hide the field, before or after it has been rendered.

        Hidden rather than removed: rebuilding the form to drop one field would
        reset everything else the user had already filled in.
        """
        self._visible = visible
        if self._root is not None:
            self._root.set_visibility(visible)

    def _apply_visibility(self) -> None:
        """ Called at the end of render(), for fields hidden before they existed. """
        if self._root is not None:
            self._root.set_visibility(self._visible)

    def _apply_help(self) -> None:
        """ Show the field's explanation as a persistent Quasar hint.

        Called by subclasses at the end of render(). Quotes are stripped because
        the text is interpolated into a prop string.
        """
        if self.help_text and self._element is not None:
            self._element.props(f'hint="{self.help_text.replace(chr(34), chr(39))}"')

    def _render_help_caption(self) -> None:
        """ Show the explanation as a caption instead of a Quasar hint.

        For fields built out of plain rows rather than Quasar inputs, where the
        `hint` prop has nothing to attach to and would silently do nothing.
        """
        if self.help_text:
            ui.label(self.help_text).classes(theme.HINT)

    @abstractmethod
    def render(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def value(self) -> Any:
        raise NotImplementedError


class LoadingField(ModalField):
    """ Stands in for fields that are still being fetched.

    A form whose defaults come off the network — the names of the competitions
    a rule already holds, the rounds they share — cannot be built before the
    dialog is shown without the dialog taking seconds to appear. It is shown
    empty instead, with this in place of the fields that are still coming.

    Carries no value: `FormModal._get_payload` collects one entry per field, and
    a placeholder must not contribute a key that an adapter might read.
    """

    def __init__(self, message: str):
        super().__init__(name="", label="", help_text=None)
        self.message = message

    def render(self) -> None:
        with ui.row().classes("w-full items-center gap-3 py-4"):
            ui.spinner(size="1.5em", color="primary")
            ui.label(self.message).classes(theme.MUTED)

    @property
    def value(self) -> None:
        return None


class NumberField(ModalField):
    def __init__(self, name: str, label: str, default: int = 0, help_text: str | None = None):
        super().__init__(name, label, help_text)
        self.default = default

    def render(self) -> None:
        self._element = ui.number(
            label=self.label,
            value=self.default,
            format="%d",
        ).props("step=1").classes("w-full")
        self._apply_help()

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
        help_text: str | None = None,
        placeholder: str | None = None,
    ):
        super().__init__(name, label, help_text)
        self.default = default
        self.validator = validator
        self.placeholder = placeholder
        self._touched = bool(default)

    def render(self) -> None:
        with ui.column().classes("w-full gap-1") as self._root:
            self._element = ui.input(
                label=self.label,
                value=self.default,
                placeholder=self.placeholder,
                validation=self._adapter if self.validator else None
            ).classes("w-full")
            self._apply_help()
        self._apply_visibility()

    def set_placeholder(self, placeholder: str) -> None:
        """ Change the example shown in an empty field.

        The example is only useful if it matches what is being named, and what
        is being named is chosen by another field in the same form.
        """
        self.placeholder = placeholder
        if self._element is not None:
            self._element.props(f'placeholder="{placeholder.replace(chr(34), chr(39))}"')

    def _adapter(self, value: str) -> str | None:
        if self.validator is None:
            return None

        # An untouched empty field is not an error yet. Validating it on render
        # opened every "New calendar" dialog already showing "Name cannot be
        # empty" in red, before the user had done anything wrong.
        if not self._touched:
            if not value:
                return None
            self._touched = True

        result = self.validator(value)
        is_valid, message = result if isinstance(result, tuple) else (bool(result), None)

        if is_valid:
            return None

        return message or "Invalid value."

    @property
    def value(self) -> str:
        return self._element.value if self._element else self.default


class _OptionsField(ModalField):
    """ Shared machinery for the two fields backed by a menu of choices.

    Both can have their choices replaced while the form is open — the rounds on
    offer depend on which competitions are chosen — and both have to explain an
    empty menu rather than opening onto nothing. The only thing that differs is
    what counts as a selection worth keeping once the choices change, which is
    `_surviving_selection`.
    """

    def __init__(self, name: str, label: str, help_text: str | None, empty_note: str | None):
        super().__init__(name, label, help_text)
        self.empty_note = empty_note
        self._empty_note = None

    def set_options(self, options: list[Any] | dict[Any, str]) -> None:
        """ Replace the choices in place, dropping a selection no longer valid.

        In place rather than by rebuilding the modal: these options depend on
        another field, and re-rendering the form would discard everything the
        user had already filled in.
        """
        self.options = _normalize_options(options)
        self.default = self._surviving_selection()
        if self._element is not None:
            self._element.set_options(self.options, value=self.default)
        self._refresh_empty_note()

    @abstractmethod
    def _surviving_selection(self) -> Any:
        """ What is left of the current selection under the new choices. """
        raise NotImplementedError

    def _render_empty_note(self) -> None:
        """ Build the note. Called from render(), inside the field's column. """
        self._empty_note = ui.label(self.empty_note or "").classes(theme.DANGER_TEXT)
        self._refresh_empty_note()

    def _refresh_empty_note(self) -> None:
        """ Explain an empty menu, rather than opening onto nothing. """
        if self._empty_note is None:
            return
        self._empty_note.set_text(self.empty_note or "")
        self._empty_note.set_visibility(bool(self.empty_note) and not self.options)


class SelectField(_OptionsField):
    def __init__(
        self,
        name: str,
        label: str,
        options: list[Any] | dict[Any, str],
        default: Any = None,
        help_text: str | None = None,
        help_as_caption: bool = False,
        clearable: bool = False,
        empty_note: str | None = None,
        on_change: Callable[[Any], None] | None = None
    ):
        super().__init__(name, label, help_text, empty_note)
        self.options = _normalize_options(options)
        self.default = default
        self.clearable = clearable
        self.on_change = on_change
        # Quasar renders `hint` into a fixed-height strip, so anything longer
        # than a line overflows onto whatever field comes next. Paragraph-length
        # help has to be a caption instead, which flows normally.
        self.help_as_caption = help_as_caption

    def render(self) -> None:
        with ui.column().classes("w-full gap-1") as self._root:
            self._element = ui.select(
                label=self.label,
                options=self.options,
                value=self.default
            ).classes("w-full")
            if self.clearable:
                self._element.props("clearable")
            if self.on_change is not None:
                self._element.on_value_change(lambda event: self.on_change(event.value))
            self._render_empty_note()
            if self.help_as_caption:
                self._render_help_caption()
            else:
                self._apply_help()
        self._apply_visibility()

    def _surviving_selection(self) -> Any:
        return self.value if self.value in self.options else None

    def bind_value_change(self, handler: Callable[[Any], None]) -> None:
        """ Call `handler` with the new value whenever the selection changes.

        Exists so callers do not have to reach through `_element`: the select is
        destroyed and rebuilt every time the form re-renders, so anything
        holding on to it is holding a dead element, and re-arming the handler is
        a normal part of that cycle rather than a private detail.
        """
        if self._element is None:
            return
        self._element.on_value_change(lambda event: handler(event.value))

    @property
    def value(self) -> Any:
        return self._element.value if self._element else self.default


class MultipleSelectField(_OptionsField):
    def __init__(
        self,
        name: str,
        label: str,
        options: list[Any] | dict[Any, str],
        default: list[Any] | None = None,
        help_text: str | None = None,
        empty_note: str | None = None
    ):
        super().__init__(name, label, help_text, empty_note)
        self.options = _normalize_options(options)
        self.default = default or []

    def render(self) -> None:
        with ui.column().classes("w-full gap-1"):
            self._element = ui.select(
                label=self.label,
                options=self.options,
                value=self.default,
                multiple=True,
            ).props("use-chips").classes("w-full")
            self._render_empty_note()
            self._apply_help()

    def _surviving_selection(self) -> list[Any]:
        return [value for value in self.value if value in self.options]

    @property
    def value(self) -> list[Any]:
        if not self._element:
            return list(self.default)
        return list(self._element.value or [])


class SearchableSelectField(ModalField):
    def __init__(
        self,
        name: str,
        label: str,
        search_fn: Callable[[str], dict[Any, str]],
        default_value: Any = None,
        default_label: str = "",
        search_hint: str = "Search...",
        help_text: str | None = None
    ):
        super().__init__(name, label, help_text)
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
        with ui.column().classes("w-full gap-1") as self._root:
            ui.label(self.label).classes(theme.FIELD_LABEL)

            # We use a clickable ui.row instead of a button.
            # This mimics standard input fields perfectly and avoids q-btn reactivity issues.
            self._box = ui.row().classes(
                "w-full px-3 py-2 cursor-pointer items-center justify-between "
                f"min-h-[42px] {theme.PICKER}"
            ).on('click', self._open_modal)

            with self._box:
                # The label is now an independent element that updates reliably
                self._label_element = ui.label(self._display_text).classes("truncate")
                ui.icon('search', size="sm").classes(theme.MUTED)

            self._render_help_caption()
        self._apply_visibility()

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
        search_hint: str = "Search...",
        help_text: str | None = None,
        on_change: Callable[[list[Any]], None] | None = None
    ):
        super().__init__(name, label, help_text)
        self.search_fn = search_fn
        self.search_hint = search_hint
        self._selections = default_values or {}
        self._chips_container = None
        # Lets another field react to this one — the round menu depends on which
        # competitions are chosen here.
        self.on_change = on_change

    def render(self) -> None:
        with ui.column().classes("w-full gap-1"):
            ui.label(self.label).classes(theme.FIELD_LABEL)

            # The main visual box
            with ui.row().classes(
                f"w-full gap-1 p-1.5 items-center wrap min-h-[42px] {theme.CHIP_TRAY}"
            ):
                # A dedicated sub-container strictly for the chips
                self._chips_container = ui.row().classes("gap-1 items-center wrap flex-grow m-0 p-0")
                self._render_chips()

                # The "Add" button stays safely outside the clearable chip area
                ui.button(
                    "Add",
                    on_click=self._open_modal
                ).props("flat size=sm color=primary icon=add no-caps").classes("p-1 ml-auto")

            self._render_help_caption()

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
        self._notify_change()

    def _notify_change(self) -> None:
        if self.on_change is not None:
            self.on_change(self.value)

    def _open_modal(self) -> None:
        modal = SearchModal(title=f"Add to {self.label}", search_fn=self.search_fn, search_hint=self.search_hint)

        def on_confirm(payload: tuple[Any, str] | None) -> bool:
            if payload is not None:
                item_id, item_label = payload
                if item_id not in self._selections:
                    self._selections[item_id] = item_label
                    self._render_chips()
                    self._notify_change()
            return True

        modal.open(on_confirm=on_confirm)

    @property
    def value(self) -> list[Any]:
        return list(self._selections.keys())
