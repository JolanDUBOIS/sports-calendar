from abc import ABC, abstractmethod
from typing import Callable

from nicegui import ui


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
