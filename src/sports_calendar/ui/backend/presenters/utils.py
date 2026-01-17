from dataclasses import dataclass

from . import logger
from sports_calendar.core.utils import validate


@dataclass
class SelectorOption:
    value: str
    display: str

@dataclass
class FieldDescriptor:
    label: str
    path: str
    current_value: list
    input_type: str
    current_display: list = None
    lookup_endpoint: str = None
    select_options: list[SelectorOption] = None

    def __post_init__(self):
        if self.current_display is None:
            self.current_display = self.current_value
        validate(self.input_type in self.get_valid_input_types(), f"Invalid input_type '{self.input_type}'", logger)

    def get_valid_input_types(self) -> list[str]:
        return ["text", "number", "select", "lookup", "multiselect", "multilookup"]