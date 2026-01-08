from typing import Any
from dataclasses import dataclass, field
from sports_calendar.sc_core import SpecModel


@dataclass
class ProcessingIOInfo(SpecModel):
    output_key: str
    config_key: str | None = None
    input_keys: dict[str, str] = field(default_factory=dict)

@dataclass
class ProcessingStepSpec(SpecModel):
    processor: str
    io_info: ProcessingIOInfo
    params: list[dict[str, Any]] | None = None
