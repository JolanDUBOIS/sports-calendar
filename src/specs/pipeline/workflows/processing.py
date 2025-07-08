from typing import Any
from dataclasses import dataclass, field
from src.specs import BaseModel


@dataclass
class ProcessingIOInfo(BaseModel):
    output_key: str
    config_key: str | None = None
    input_keys: dict[str, str] = field(default_factory=dict)

@dataclass
class ProcessingStepSpec(BaseModel):
    processor: str
    io_info: ProcessingIOInfo
    params: list[dict[str, Any]] | None = None
