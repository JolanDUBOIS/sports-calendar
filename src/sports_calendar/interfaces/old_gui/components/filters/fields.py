from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from nicegui import ui


@dataclass
class Choice:
    label: str  # what user sees
    value: any  # what backend receives


class BaseField(ABC):
    """Base class for all modal fields."""

    def __init__(self, key: str, label: str):
        self.key = key
        self.label = label
        self._value = None  # latest value

    @abstractmethod
    def render(self, container) -> None:
        """Create UI inside container and attach event handlers."""
        ...

    @property
    @abstractmethod
    def value(self):
        """Return current value of the field."""
        ...


class SelectField(BaseField):
    def __init__(self, key: str, label: str, options: list[Choice], default: any = None):
        super().__init__(key, label)
        self.options = options
        self._value = default

        # value -> label (same as other selects)
        self._options_map = {c.value: c.label for c in options}

        self.select = None

    def render(self, container):
        with container:
            self.select = ui.select(
                options=self._options_map,   # value -> label
                label=self.label,
                value=self._value,
            ).classes('w-full')

        self.select.on(
            'update:model-value',
            lambda e: setattr(self, '_value', self.select.value)
        )

    @property
    def value(self):
        return self._value

    @property
    def display(self):
        return self._options_map.get(self._value, '')


class NumberField(BaseField):
    def __init__(self, key: str, label: str, default: float = 0.0):
        super().__init__(key, label)
        self._value = default
        self.input = None

    def render(self, container):
        with container:
            self.input = ui.input(self.label, value=str(self._value)).classes('w-full')
        self.input.on('input', lambda e: setattr(self, '_value', float(self.input.value) if self.input.value else 0.0))

    @property
    def value(self):
        return float(self.input.value) if self.input else self._value


class TextField(BaseField):
    def __init__(self, key: str, label: str, default: str = ''):
        super().__init__(key, label)
        self._value = default
        self.input = None

    def render(self, container):
        with container:
            self.input = ui.input(self.label, value=self._value).classes('w-full')
        self.input.on('input', lambda e: setattr(self, '_value', self.input.value))

    @property
    def value(self):
        return self.input.value if self.input else self._value


class ValidatedTextField(TextField):
    def __init__(self, key, label, validator: Callable[[str], bool | str], default=''):
        super().__init__(key, label, default)
        self.validator = validator
        self.error_label = None

    def render(self, container):
        with container:
            with ui.column().classes('w-full'):
                self.input = ui.input(self.label, value=self._value).props('autocomplete=off').classes('w-full')
                self.error_label = ui.label('').classes('text-red-500 text-sm')

        def on_input(e):
            val = self.input.value
            self._value = val
            result = self.validator(val)

            if result is True:
                self.input.classes(remove='border-red-500')
                self.error_label.text = ''
            else:
                self.input.classes(add='border-red-500')
                self.error_label.text = result

        self.input.on_value_change(on_input)

    @property
    def value(self):
        return self.input.value if self.input else self._value


class SearchableSelectField(BaseField):
    def __init__(self, key: str, label: str, options: list[Choice], default: any = None):
        super().__init__(key, label)
        self.options = options
        self._value = default

        # CORRECT: value -> label
        self._options_map = {c.value: c.label for c in options}

        self.select = None

    def render(self, container):
        with container:
            self.select = ui.select(
                options=self._options_map,   # value -> label
                label=self.label,
                value=self._value,
                with_input=True,
            ).classes('w-full')

        self.select.on(
            'update:model-value',
            lambda e: setattr(self, '_value', self.select.value)
        )

    @property
    def value(self):
        return self._value

    @property
    def display(self):
        return self._options_map.get(self._value, '')


class MultipleSelectField(BaseField):
    def __init__(self, key: str, label: str, options: list[Choice], default: list[any] = None):
        super().__init__(key, label)
        self.options = options
        self._value = default or []

        self._options_map = {c.value: c.label for c in options}
        self.select = None

    def render(self, container):
        with container:
            self.select = ui.select(
                options=self._options_map,
                label=self.label,
                value=self._value,
                multiple=True,
            ).props('use-chips').classes('w-full')

        self.select.on(
            'update:model-value',
            lambda e: setattr(self, '_value', list(self.select.value or []))
        )

    @property
    def value(self):
        return self._value

    @property
    def display(self):
        return ', '.join(self._options_map[v] for v in self._value if v in self._options_map)


class SearchableMultipleSelectField(BaseField):
    def __init__(self, key: str, label: str, options: list[Choice], default: list[any] = None):
        super().__init__(key, label)
        self.options = options
        self._value = default or []

        self._options_map = {c.value: c.label for c in options}
        self.select = None

    def render(self, container):
        with container:
            self.select = ui.select(
                options=self._options_map,
                label=self.label,
                value=self._value,
                multiple=True,
                with_input=True,
            ).props('use-chips').classes('w-full')

        self.select.on(
            'update:model-value',
            lambda e: setattr(self, '_value', list(self.select.value or []))
        )

    @property
    def value(self):
        return self._value

    @property
    def display(self):
        return ', '.join(self._options_map[v] for v in self._value if v in self._options_map)
